"""Renderer-agnostic aggregation of the four rolling views.

Both the headless PNG renderer and the interactive browser GUI consume this
single state object, so the two front-ends cannot disagree about what the
pipeline produced.  ``snapshot()`` serialises the heatmaps as quantised
single-channel pixels and the traces as short float lists; that keeps a state
poll small enough for a 10 Hz browser refresh without any plotting in the
consumer's hot loop.
"""

from __future__ import annotations

import base64
import threading
from typing import Dict, Iterable, Optional, Tuple

import numpy as np

from .metrics import Counters, LatencyMeter
from .protocol import PacketType, now_ms

# The spectrum payloads are normalised by a per-record reference, so a fixed
# ceiling keeps colours comparable across frames and front-ends.  The energy
# view, by contrast, integrates batches with a 0.85 decay: its steady-state
# magnitude depends on the packet rate, the display timer and the *domain*
# (ECG energy ~1, band-passed strain ~0.1).  An absolute floor would flatten
# one domain and saturate the other, so both renderers share this relative
# adaptive scale instead of matplotlib's autoscale (which over-amplifies the
# tiny difference between two channels and hides the time structure).
_SPECTRUM_SCALE = 1.5


def energy_scale(energy: np.ndarray) -> float:
    """Relative colour ceiling used by BOTH the PNG and browser renderers."""
    ceiling = float(np.percentile(energy, 99)) * 1.15
    return max(ceiling, float(np.max(energy)) * 0.4, 1e-6)


def trace_ceiling(values: np.ndarray) -> float:
    """Upper y-limit for the spectrum trace, shared by both renderers."""
    return max(0.1, float(np.max(values)) * 1.15)


def _quantise(array: np.ndarray, scale: float) -> str:
    q = np.clip(np.asarray(array, dtype=np.float32) / scale, 0.0, 1.0)
    pixels = np.round(q * 255).astype(np.uint8).tobytes()
    return base64.b64encode(pixels).decode("ascii")


class ViewState:
    """Thread-safe rolling views updated by exactly one display consumer."""

    def __init__(
        self,
        history: int = 120,
        n_bins: int = 128,
        n_channels: int = 4,
        waveform_len: int = 256,
    ) -> None:
        self.history = history
        self.n_bins = n_bins
        self.n_channels = n_channels
        self.waterfall = np.zeros((history, n_bins), dtype=np.float32)
        self.energy = np.zeros((n_channels, history), dtype=np.float32)
        self.last_waveform = np.zeros(waveform_len, dtype=np.float32)
        self.last_spectrum = np.zeros(n_bins, dtype=np.float32)
        self.version = 0
        self.frames = 0
        self._lock = threading.Lock()

    def ingest(
        self,
        packets: Iterable,
        latency: Optional[LatencyMeter] = None,
        counters: Optional[Counters] = None,
        staleness: Optional[LatencyMeter] = None,
    ) -> None:
        """Fold one drained batch of packets into the rolling views.

        ``latency`` records every packet's own age, while ``staleness`` records
        one sample per batch: the age of the *newest* packet that was actually
        rendered.  The two answer different questions -- "how old is the
        average sample" versus "how far behind live is this frame" -- and only
        the second one exposes what drop-oldest buys.
        """
        packets = list(packets)
        with self._lock:
            display_ms = now_ms()
            had_packets = False
            if packets and staleness is not None:
                newest_ms = max(int(p.timestamp_ms) for p in packets)
                staleness.add(display_ms - newest_ms)
            for pkt in packets:
                had_packets = True
                if latency is not None:
                    latency.add(display_ms - pkt.timestamp_ms)
                values = np.asarray(pkt.values, dtype=np.float32)
                if pkt.type is PacketType.WAVEFORM:
                    if len(values) == len(self.last_waveform):
                        self.last_waveform = values.copy()
                    continue
                if pkt.type is not PacketType.SPECTRUM:
                    continue  # audio is archived by the parser, not displayed
                spec = values[: self.n_bins]
                if len(spec) < self.n_bins:
                    spec = np.pad(spec, (0, self.n_bins - len(spec)))
                self.waterfall = np.roll(self.waterfall, -1, axis=0)
                self.waterfall[-1] = spec
                channel = min(pkt.channel, self.n_channels - 1)
                previous = self.energy[:, -1].copy()
                self.energy = np.roll(self.energy, -1, axis=1)
                # Decaying per-channel activity instead of binary stripes.
                self.energy[:, -1] = previous * 0.85
                self.energy[channel, -1] += float(np.mean(np.abs(spec)))
                self.last_spectrum = spec.copy()
                if counters is not None:
                    counters.inc("display_frames")
            if had_packets:
                self.frames += 1
                self.version += 1

    def arrays(self) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        """Cheap independent copies, used by the shutdown PNG renderer."""
        with self._lock:
            return (
                self.waterfall.copy(),
                self.energy.copy(),
                self.last_waveform.copy(),
                self.last_spectrum.copy(),
            )

    def snapshot(self) -> Dict:
        """JSON-ready, compact representation of the current views."""
        with self._lock:
            e_scale = energy_scale(self.energy)
            return {
                "version": self.version,
                "frames": self.frames,
                "views": {
                    "spectrogram": {
                        "shape": [self.history, self.n_bins],
                        "scale": _SPECTRUM_SCALE,
                        "pixels": _quantise(self.waterfall, _SPECTRUM_SCALE),
                    },
                    "energy": {
                        "shape": [self.n_channels, self.history],
                        "scale": round(e_scale, 4),
                        "pixels": _quantise(self.energy, e_scale),
                    },
                    "waveform": [round(float(v), 4) for v in self.last_waveform],
                    "spectrum": [round(float(v), 4) for v in self.last_spectrum],
                },
                "trace_ceiling": round(trace_ceiling(self.last_spectrum), 4),
            }
