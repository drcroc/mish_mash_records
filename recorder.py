"""
Per-speaker voice-activity recorder.

Audio is split into segments (speech bursts); only segments with real speech
are written to disk:

  - A silence gap longer than `split_gap` seconds ends the current segment;
    a shorter pause is kept inside it, so "speak, 2s pause, speak" is one file.
  - `pad` seconds of silence is added before and after each saved segment.
  - A segment with less than `min_duration` seconds of actual speech is dropped.

Two clocks are used:
  - RTP media timestamps -> sample-accurate silence fill *inside* a segment.
  - wall-clock (monotonic) -> detect when a speaker stopped and no more packets
    arrive, so the segment can be closed without waiting for a future packet.

Output: 48 kHz stereo 16-bit WAV, one file per speech burst, named
<speaker>_<seq>_<MMmSSs>.wav where the time is the burst's offset into the
session.
"""
import logging
import threading
import time
import wave
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from discord import opus

log = logging.getLogger(__name__)

RATE, CHANNELS, WIDTH = 48000, 2, 2
FRAME_SAMPLES = 960
BPS = CHANNELS * WIDTH                       # bytes per sample frame
MAX_SEGMENT_SAMPLES = RATE * 60 * 30         # 30 min hard cap (memory safety)


def _silence(samples: int) -> bytes:
    return b'\x00' * (samples * BPS)


class _Segment:
    __slots__ = ('uid', 'pcm', 'voiced_samples', 'last_media_ts',
                 'last_wall', 'start_wall')

    def __init__(self, uid: int, now: float):
        self.uid = uid
        self.pcm = bytearray()
        self.voiced_samples = 0              # real speech only (for min check)
        self.last_media_ts: Optional[int] = None
        self.last_wall = now
        self.start_wall = now

    def add(self, media_ts: int, pcm: bytes, now: float, max_fill: int) -> None:
        if self.last_media_ts is not None:
            gap = ((media_ts - self.last_media_ts) & 0xFFFFFFFF) - FRAME_SAMPLES
            if gap > 0x7FFFFFFF:             # negative -> out-of-order/dup
                return
            if gap > 0:                      # pause inside the burst -> keep it
                self.pcm += _silence(min(gap, max_fill))
        self.pcm += pcm
        self.voiced_samples += len(pcm) // BPS
        self.last_media_ts = media_ts
        self.last_wall = now


class ChannelRecorder:
    def __init__(self, guild_id: int, root: Path = Path('recordings'),
                 only_user: Optional[int] = None,
                 split_gap: float = 3.0, pad: float = 1.0,
                 min_duration: float = 5.0):
        self.guild_id = guild_id
        self.root = root
        self.only_user = only_user                 # None = record everyone
        self.split_samples = int(split_gap * RATE)
        self.split_wall = split_gap
        self.pad_bytes = int(pad * RATE) * BPS
        self.min_samples = int(min_duration * RATE)

        self.vc = None
        self.session_dir: Optional[Path] = None
        self._session_wall = 0.0
        self._decoders: Dict[int, opus.Decoder] = {}   # ssrc -> decoder
        self._segments: Dict[int, _Segment] = {}        # ssrc -> open segment
        self._seq: Dict[str, int] = {}                  # name -> file counter
        self._names: Dict[int, str] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._flusher: Optional[threading.Thread] = None

    # ---- lifecycle ----
    def start(self, vc) -> None:
        if not opus.is_loaded():
            opus._load_default()
        self.vc = vc
        stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.session_dir = self.root / f'session_{self.guild_id}_{stamp}'
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self._session_wall = time.monotonic()
        self._stop.clear()
        self._flusher = threading.Thread(target=self._flush_loop, daemon=True)
        self._flusher.start()
        vc.start_receiving(self._on_frame)
        log.info('Recording (%s) -> %s',
                 f'only {self.only_user}' if self.only_user else 'everyone',
                 self.session_dir)

    def stop(self) -> Optional[Path]:
        if self.vc:
            try:
                self.vc.stop_receiving()
            except Exception:
                pass
        self._stop.set()
        if self._flusher:
            self._flusher.join(timeout=2)
        with self._lock:
            for ssrc in list(self._segments):
                self._flush(ssrc, self._segments[ssrc])
            out, self.session_dir = self.session_dir, None
            self._segments.clear()
            self._decoders.clear()
        log.info('Session saved -> %s', out)
        return out

    # ---- naming ----
    def _name(self, uid: int, ssrc: int) -> str:
        if not uid:
            return f'ssrc_{ssrc}'
        if uid not in self._names:
            m = self.vc.guild.get_member(uid) if self.vc else None
            raw = m.name if m else str(uid)
            self._names[uid] = ''.join(c if c.isalnum() or c in '-_.' else '_'
                                       for c in raw)
        return self._names[uid]

    # ---- audio path (socket-reader thread) ----
    def _on_frame(self, ssrc: int, uid: int, ts: int, frame: bytes) -> None:
        if not uid:                                  # wait for SPEAKING mapping
            return
        if self.only_user and uid != self.only_user:
            return
        dec = self._decoders.get(ssrc)
        if dec is None:
            dec = self._decoders[ssrc] = opus.Decoder()
        try:
            pcm = dec.decode(frame, fec=False)
        except opus.OpusError as e:
            log.debug('decode error ssrc=%s: %s', ssrc, e)
            return

        now = time.monotonic()
        with self._lock:
            if self.session_dir is None:
                return
            seg = self._segments.get(ssrc)
            if seg is not None and seg.last_media_ts is not None:
                gap = ((ts - seg.last_media_ts) & 0xFFFFFFFF) - FRAME_SAMPLES
                if 0 <= gap <= 0x7FFFFFFF and gap > self.split_samples:
                    self._flush(ssrc, seg)           # pause too long -> new file
                    seg = None
                elif seg.voiced_samples >= MAX_SEGMENT_SAMPLES:
                    log.warning('%s hit 30min cap, splitting', self._name(uid, ssrc))
                    self._flush(ssrc, seg)
                    seg = None
            if seg is None:
                seg = self._segments[ssrc] = _Segment(uid, now)
            seg.add(ts, pcm, now, self.split_samples)

    # ---- flushing ----
    def _flush_idle(self, now: float) -> None:
        with self._lock:
            for ssrc in list(self._segments):
                if now - self._segments[ssrc].last_wall > self.split_wall:
                    self._flush(ssrc, self._segments[ssrc])

    def _flush_loop(self) -> None:
        while not self._stop.wait(0.5):
            self._flush_idle(time.monotonic())

    def _flush(self, ssrc: int, seg: _Segment) -> None:
        """Write a finished segment if it has enough speech. Caller holds lock."""
        self._segments.pop(ssrc, None)
        name = self._name(seg.uid, ssrc)
        if seg.voiced_samples < self.min_samples:
            log.debug('skip %s burst (%.1fs < %.1fs)',
                      name, seg.voiced_samples / RATE, self.min_samples / RATE)
            return
        self._seq[name] = self._seq.get(name, 0) + 1
        seq = self._seq[name]
        off = int(seg.start_wall - self._session_wall)
        fn = self.session_dir / f'{name}_{seq:03d}_{off // 60:02d}m{off % 60:02d}s.wav'
        body = bytes(self.pad_bytes) + bytes(seg.pcm) + bytes(self.pad_bytes)
        try:
            with wave.open(str(fn), 'wb') as w:
                w.setnchannels(CHANNELS)
                w.setsampwidth(WIDTH)
                w.setframerate(RATE)
                w.writeframes(body)
            log.info('Saved %s (%.1fs speech, %.1fs total)',
                     fn.name, seg.voiced_samples / RATE, len(body) // BPS / RATE)
        except Exception as e:
            log.error('write failed %s: %s', fn.name, e)
