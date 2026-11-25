"""Audio archiving, real-time paced playback and timestamp-based replay.

The archive mirrors the "buffer full -> flush to disk" behaviour: numeric
sample blocks accumulate in memory and are flushed as single-channel WAV
files.  Each segment records its [start, end] timestamp window, which makes
it possible to jump to an arbitrary point in the recorded stream.

Playback is deliberately sink-agnostic so the whole pipeline runs headless
in CI.  A null sink is the default; callers may supply any object exposing
``write(samples, sample_rate)``.
"""

from __future__ import annotations

import threading
import wave
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Protocol, Tuple

import numpy as np

from .protocol import Packet, PacketType


class AudioSink(Protocol):
    def write(self, samples: np.ndarray, sample_rate: int) -> None: ...


class NullSink:
    """Discards samples; keeps the pipeline runnable on headless machines."""

    def write(self, samples: np.ndarray, sample_rate: int) -> None:
        return None


@dataclass(frozen=True)
class Segment:
    start_ms: int
    end_ms: int
    path: Path
    n_samples: int


class AudioArchive:
    """Accumulate sample blocks, flush to WAV, and index by source timestamp."""

    def __init__(
        self,
        output_dir: Path,
        sample_rate: int = 16_000,
        block_samples: int = 4096,
    ) -> None:
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.sample_rate = sample_rate
        self.block_samples = block_samples
        self.segments: List[Segment] = []
        self._pending: List[np.ndarray] = []
        self._pending_n = 0
        self._window_start: Optional[int] = None
        self._window_end: Optional[int] = None
        self._lock = threading.Lock()
        self._seq = 0

    def append(self, packet: Packet) -> Optional[Path]:
        """Add one sample block; return the WAV path when a flush happened."""
        if packet.type is not PacketType.AUDIO:
            raise ValueError("AudioArchive expects AUDIO packets")
        samples = np.asarray(packet.values, dtype=np.int16).copy()
        duration_ms = int(1000 * len(samples) / self.sample_rate)
        with self._lock:
            self._pending.append(samples)
            self._pending_n += len(samples)
            if self._window_start is None:
                self._window_start = packet.timestamp_ms
            self._window_end = packet.timestamp_ms + duration_ms
            if self._pending_n < self.block_samples:
                return None
            return self._flush_locked()

    def _flush_locked(self) -> Path:
        data = np.concatenate(self._pending) if self._pending else np.empty(0, dtype=np.int16)
        self._seq += 1
        path = self.output_dir / f"segment_{self._seq:05d}.wav"
        with wave.open(str(path), "wb") as wav:
            wav.setnchannels(1)
            wav.setsampwidth(2)
            wav.setframerate(self.sample_rate)
            wav.writeframes(data.tobytes())
        self.segments.append(
            Segment(
                start_ms=int(self._window_start or 0),
                end_ms=int(self._window_end or 0),
                path=path,
                n_samples=int(len(data)),
            )
        )
        self._pending.clear()
        self._pending_n = 0
        self._window_start = None
        self._window_end = None
        return path

    def flush(self) -> Optional[Path]:
        """Force out any partial block (called on shutdown)."""
        with self._lock:
            if self._pending_n == 0:
                return None
            return self._flush_locked()

    def locate(self, timestamp_ms: int) -> Optional[Segment]:
        """Find the archived segment covering a source timestamp."""
        with self._lock:
            for seg in self.segments:
                if seg.start_ms <= timestamp_ms <= seg.end_ms:
                    return seg
            return None

    @staticmethod
    def read(segment: Segment) -> Tuple[np.ndarray, int]:
        with wave.open(str(segment.path), "rb") as wav:
            rate = wav.getframerate()
            raw = wav.readframes(wav.getnframes())
        return np.frombuffer(raw, dtype=np.int16), rate

    def __len__(self) -> int:
        with self._lock:
            return len(self.segments)


class AudioPlayer:
    """Live paced playback + historical seek/replay."""

    def __init__(self, archive: AudioArchive, sink: Optional[AudioSink] = None) -> None:
        self.archive = archive
        self.sink: AudioSink = sink or NullSink()

    def play_live(self, max_segments: int = 1) -> int:
        """Feed the most recent segments to the sink at the source sample rate."""
        written = 0
        for seg in list(self.archive.segments)[-max_segments:]:
            samples, rate = self.archive.read(seg)
            self.sink.write(samples, rate)
            written += len(samples)
        return written

    def play_history(self, timestamp_ms: int) -> Tuple[np.ndarray, int, Optional[Segment]]:
        """Locate the archive position for a timestamp and return its samples."""
        seg = self.archive.locate(timestamp_ms)
        if seg is None:
            return np.empty(0, dtype=np.int16), self.archive.sample_rate, None
        samples, rate = self.archive.read(seg)
        offset = int((timestamp_ms - seg.start_ms) * rate / 1000)
        offset = max(0, min(offset, len(samples)))
        tail = samples[offset:]
        self.sink.write(tail, rate)
        return tail, rate, seg
