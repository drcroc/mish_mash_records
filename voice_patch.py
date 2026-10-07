"""
Voice receive for discord.py 2.7.x (no external extensions).

Pipeline per UDP packet (runs on discord.py's SocketReader thread):
  RTP header -> transport decrypt (aead_xchacha20_poly1305_rtpsize)
  -> strip RTP extension -> DAVE E2EE decrypt (davey) -> Opus frame
"""
import logging
import struct
from typing import Callable, Dict, Optional

import discord
import nacl.secret
from discord.voice_state import VoiceConnectionState

try:
    import davey
except ImportError:  # discord.py 2.7 requires it for voice anyway
    davey = None

log = logging.getLogger(__name__)

OP_SPEAKING = 5
OP_CLIENT_DISCONNECT = 13
OPUS_PT = 0x78          # payload type 120
DAVE_MAGIC = b'\xfa\xfa'

FrameCallback = Callable[[int, int, int, bytes], None]  # (ssrc, user_id|0, rtp_ts, opus)


async def _ws_hook(ws, msg: dict) -> None:
    """Fills SSRC <-> user map from voice gateway events."""
    vc: 'RecvVoiceClient' = ws._connection.voice_client
    op, d = msg.get('op'), msg.get('d') or {}
    if op == OP_SPEAKING and 'ssrc' in d:
        vc.ssrc_to_user[d['ssrc']] = int(d['user_id'])
    elif op == OP_CLIENT_DISCONNECT and 'user_id' in d:
        uid = int(d['user_id'])
        for s in [s for s, u in vc.ssrc_to_user.items() if u == uid]:
            vc.ssrc_to_user.pop(s, None)


class RecvVoiceClient(discord.VoiceClient):
    """Use with: await channel.connect(cls=RecvVoiceClient)"""

    def __init__(self, client, channel):
        super().__init__(client, channel)
        self.ssrc_to_user: Dict[int, int] = {}
        self._frame_cb: Optional[FrameCallback] = None
        self._listening = False
        self._bad_packets = 0

    def create_connection_state(self) -> VoiceConnectionState:
        return VoiceConnectionState(self, hook=_ws_hook)

    # ---- public API ----
    def start_receiving(self, callback: FrameCallback) -> None:
        if self._listening:
            return
        self._frame_cb = callback
        self._connection.add_socket_listener(self._on_packet)
        self._listening = True
        log.info('Receiving started (mode=%s, dave=%s)', self.mode,
                 bool(self._connection.dave_session))

    def stop_receiving(self) -> None:
        if not self._listening:
            return
        try:
            self._connection.remove_socket_listener(self._on_packet)
        except Exception:
            pass
        self._listening = False
        self._frame_cb = None
        log.info('Receiving stopped')

    def cleanup(self) -> None:
        self.stop_receiving()
        super().cleanup()

    # ---- packet path ----
    def _on_packet(self, data: bytes) -> None:
        cb = self._frame_cb
        if cb is None or len(data) < 16:
            return
        if 200 <= data[1] <= 204:          # RTCP
            return
        if (data[0] >> 6) != 2 or (data[1] & 0x7F) != OPUS_PT:
            return

        try:
            opus, ts, ssrc = self._transport_decrypt(data)
        except Exception:
            self._count_bad('transport decrypt failed')
            return

        uid = self.ssrc_to_user.get(ssrc, 0)
        opus = self._dave_decrypt(uid, opus)
        if opus is None:
            return
        cb(ssrc, uid, ts, opus)

    def _transport_decrypt(self, data: bytes):
        if self.mode != 'aead_xchacha20_poly1305_rtpsize':
            raise RuntimeError(f'unsupported mode {self.mode}')

        cc = data[0] & 0x0F
        has_ext = bool(data[0] & 0x10)
        has_pad = bool(data[0] & 0x20)
        ts, ssrc = struct.unpack_from('>II', data, 4)

        hdr_len = 12 + 4 * cc + (4 if has_ext else 0)   # rtpsize: ext preamble is AAD
        ext_words = struct.unpack_from('>H', data, hdr_len - 2)[0] if has_ext else 0

        nonce = data[-4:] + b'\x00' * 20
        box = nacl.secret.Aead(bytes(self.secret_key))
        plain = box.decrypt(data[hdr_len:-4], data[:hdr_len], nonce)

        if has_pad and plain:
            plain = plain[:-plain[-1]]
        return plain[ext_words * 4:], ts, ssrc

    def _dave_decrypt(self, uid: int, opus: bytes) -> Optional[bytes]:
        if not opus.endswith(DAVE_MAGIC):
            return opus                     # not E2E-encrypted (passthrough)
        sess = self._connection.dave_session
        if not uid or sess is None or not sess.ready or davey is None:
            return None                     # can't decrypt yet; drop
        try:
            return sess.decrypt(uid, davey.MediaType.audio, opus)
        except Exception:
            self._count_bad(f'DAVE decrypt failed uid={uid}')
            return None

    def _count_bad(self, why: str) -> None:
        self._bad_packets += 1
        if self._bad_packets in (1, 10, 100) or self._bad_packets % 1000 == 0:
            log.warning('%s (%d bad packets so far)', why, self._bad_packets)
