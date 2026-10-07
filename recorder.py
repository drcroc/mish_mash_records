"""
Per-user WAV recorder. Decodes each Opus frame on arrival (frame boundaries
are only known per packet) and streams PCM to disk. RTP timestamp gaps are
filled with silence so every track stays on the real timeline.
"""
import logging
import threading
import wave
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import discord
from discord import opus

log = logging.getLogger(__name__)

RATE, CHANNELS, WIDTH = 48000, 2, 2
FRAME_SAMPLES = 960
MAX_GAP_SAMPLES = RATE * 60 * 10       # cap silence fill at 10 min


class _Track:
    def __init__(self, path: Path):
        self.path = path
        self.wav = wave.open(str(path), 'wb')
        self.wav.setnchannels(CHANNELS)
        self.wav.setsampwidth(WIDTH)
        self.wav.setframerate(RATE)
        self.decoder = opus.Decoder()
        self.last_ts: Optional[int] = None
        self.samples = 0

    def write(self, ts: int, frame: bytes) -> None:
        if self.last_ts is not None:
            gap = ((ts - self.last_ts) & 0xFFFFFFFF) - FRAME_SAMPLES
            if gap > 0x7FFFFFFF:            # late/out-of-order packet
                return
            if 0 < gap <= MAX_GAP_SAMPLES:
                self.wav.writeframes(b'\x00' * gap * CHANNELS * WIDTH)
                self.samples += gap
        pcm = self.decoder.decode(frame, fec=False)
        self.wav.writeframes(pcm)
        self.samples += len(pcm) // (CHANNELS * WIDTH)
        self.last_ts = ts

    def close(self) -> float:
        self.wav.close()
        return self.samples / RATE


class ChannelRecorder:
    def __init__(self, guild_id: int, root: Path = Path('recordings'),
                 only_user: Optional[int] = None):
        self.guild_id = guild_id
        self.only_user = only_user                  # None = record everyone
        self.root = root
        self.vc = None
        self.session_dir: Optional[Path] = None
        self._tracks: Dict[int, _Track] = {}       # keyed by SSRC
        self._lock = threading.Lock()
        self._names: Dict[int, str] = {}

    def start(self, vc) -> None:
        if not opus.is_loaded():
            opus._load_default()                    # bundled libopus DLL
        self.vc = vc
        stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.session_dir = self.root / f'session_{self.guild_id}_{stamp}'
        self.session_dir.mkdir(parents=True, exist_ok=True)
        vc.start_receiving(self._on_frame)
        log.info('Recording (%s) -> %s',
                 f'only {self.only_user}' if self.only_user else 'everyone', self.session_dir)

    def _name(self, uid: int, ssrc: int) -> str:
        if not uid:
            return f'ssrc_{ssrc}'
        if uid not in self._names:
            m = self.vc.guild.get_member(uid) if self.vc else None
            raw = m.name if m else str(uid)
            self._names[uid] = ''.join(c if c.isalnum() or c in '-_.' else '_' for c in raw)
        return f'{self._names[uid]}_{uid}'

    def _on_frame(self, ssrc: int, uid: int, ts: int, frame: bytes) -> None:
        with self._lock:
            if self.session_dir is None:
                return
            tr = self._tracks.get(ssrc)
            if tr is None:
                if not uid:                          # wait for SPEAKING mapping
                    return
                if self.only_user and uid != self.only_user:
                    return
                tr = self._tracks[ssrc] = _Track(self.session_dir / f'{self._name(uid, ssrc)}.wav')
                log.info('New track: %s', tr.path.name)
            try:
                tr.write(ts, frame)
            except opus.OpusError as e:
                log.debug('decode error ssrc=%s: %s', ssrc, e)

    def stop(self) -> Optional[Path]:
        if self.vc:
            self.vc.stop_receiving()
        with self._lock:
            for tr in self._tracks.values():
                secs = tr.close()
                log.info('Saved %s (%.1fs)', tr.path.name, secs)
            n = len(self._tracks)
            self._tracks.clear()
            out, self.session_dir = self.session_dir, None
        log.info('Saved %d track(s) to %s', n, out)
        return out
