"""Timer-driven display thread with four rolling views.

The consumer waits on the cache-full signal with a timeout, so it is both an
event listener and a periodic timer.  Four independent views are kept:
  1. rolling spectrogram (time x bins)
  2. per-channel energy heatmap (channel x time)
  3. most recent waveform
  4. most recent spectrum

Rendering uses the non-interactive Agg backend so the pipeline is fully
reproducible on headless/CI machines.  Up to six PNG snapshots are written,
matching the figure limit of a typical software-article template.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import List, Optional

import numpy as np

from .buffers import BoundedFIFO, Signal
from .metrics import Counters, LatencyMeter
from .protocol import now_ms
from .viewstate import _SPECTRUM_SCALE, ViewState, energy_scale, trace_ceiling


class DrainThread(threading.Thread):
    """Renderer-free consumer for tests and benchmarks (no plotting deps).

    It still folds every consumed packet into a real :class:`ViewState`.  That
    fold is not free — roughly 0.2 ms per packet on the reference machine — and
    a consumer whose per-tick work grows with the backlog is precisely what
    distinguishes an unbounded buffer from a bounded one.  Skipping the fold
    would make an unbounded buffer look free.
    """

    def __init__(
        self,
        frame_buffer: BoundedFIFO,
        ready_signal: Signal,
        counters: Counters,
        latency: LatencyMeter,
        stop_event: threading.Event,
        refresh_hz: float = 30.0,
        staleness: Optional[LatencyMeter] = None,
        consumer_delay_ms: float = 0.0,
        history: int = 120,
        n_bins: int = 128,
        n_channels: int = 4,
        daemon: bool = True,
    ) -> None:
        super().__init__(name="drain", daemon=daemon)
        self.frame_buffer = frame_buffer
        self.ready_signal = ready_signal
        self.counters = counters
        self.latency = latency
        self.staleness = staleness
        self.consumer_delay_ms = max(0.0, consumer_delay_ms)
        self.stop_event = stop_event
        self.refresh_hz = max(1.0, refresh_hz)
        self.state = ViewState(history=history, n_bins=n_bins, n_channels=n_channels)
        self.frames_rendered = 0
        self.snapshots: List[Path] = []

    def _consume(self, packets) -> bool:
        if not packets:
            return False
        self.state.ingest(packets, self.latency, self.counters, self.staleness)
        self.frames_rendered += 1
        return True

    def run(self) -> None:
        # Fixed-rate ticks, not a free-running loop.  When the buffer is
        # over-subscribed the ready signal is set continuously; a consumer that
        # re-checks immediately would spin and starve the parser of the GIL,
        # which inflates latency instead of measuring it.  A tick lasts at
        # least as long as the work it actually did.
        budget = max(1.0 / self.refresh_hz, self.consumer_delay_ms / 1000.0)
        try:
            while not self.stop_event.is_set():
                started = time.perf_counter()
                self._consume(self.frame_buffer.drain())
                spent = time.perf_counter() - started
                self.stop_event.wait(max(0.0, budget - spent))
        finally:
            # Packets still buffered at shutdown were never displayed.  Count
            # them as a shutdown drop: that closes the conservation chain
            # without letting their (very old) timestamps pollute percentiles.
            residual = self.frame_buffer.drain()
            if residual:
                self.counters.inc("dropped_at_shutdown", len(residual))


class DisplayThread(threading.Thread):
    def __init__(
        self,
        frame_buffer: BoundedFIFO,
        ready_signal: Signal,
        counters: Counters,
        latency: LatencyMeter,
        stop_event: threading.Event,
        refresh_hz: float = 30.0,
        history: int = 120,
        n_bins: int = 128,
        n_channels: int = 4,
        output_dir: Optional[Path] = None,
        snapshot_every: int = 20,
        max_snapshots: int = 6,
        staleness: Optional[LatencyMeter] = None,
        consumer_delay_ms: float = 0.0,
        daemon: bool = True,
    ) -> None:
        super().__init__(name="display", daemon=daemon)
        self.frame_buffer = frame_buffer
        self.ready_signal = ready_signal
        self.counters = counters
        self.latency = latency
        self.staleness = staleness
        self.stop_event = stop_event
        self.refresh_hz = max(1.0, refresh_hz)
        self.history = history
        self.n_bins = n_bins
        self.n_channels = n_channels
        self.output_dir = Path(output_dir) if output_dir else None
        self.snapshot_every = snapshot_every
        self.max_snapshots = max_snapshots
        self.consumer_delay_ms = max(0.0, consumer_delay_ms)
        self.snapshots: List[Path] = []
        self.state = ViewState(history=history, n_bins=n_bins, n_channels=n_channels)
        self._states: list = []

    @property
    def frames_rendered(self) -> int:
        return self.state.frames

    def _ingest(self, packets) -> None:
        self.state.ingest(packets, self.latency, self.counters, self.staleness)

    def _consume_tick(self) -> None:
        packets = self.frame_buffer.drain()
        if packets:
            self._ingest(packets)

    def _capture(self) -> None:
        """Cheap copy of the current view state; no plotting in the hot loop."""
        if len(self._states) >= self.max_snapshots:
            return
        self._states.append(self.state.arrays())

    def run(self) -> None:
        # Snapshots are collected as plain arrays while the consumer is live,
        # then rendered once on shutdown.  Disk/image encoding in the hot loop
        # would stall the consumer and overflow the display FIFO.
        tick = 0
        budget = max(1.0 / self.refresh_hz, self.consumer_delay_ms / 1000.0)
        try:
            while not self.stop_event.is_set():
                started = time.perf_counter()
                self._consume_tick()
                tick += 1
                if self.snapshot_every and tick % self.snapshot_every == 0:
                    self._capture()
                spent = time.perf_counter() - started
                self.stop_event.wait(max(0.0, budget - spent))
        finally:
            # Same rule as the drain consumer: residual packets were never
            # rendered, so they are counted as shutdown drops rather than
            # folded into the latency statistics.
            residual = self.frame_buffer.drain()
            if residual:
                self.counters.inc("dropped_at_shutdown", len(residual))
            if not self._states:
                self._capture()
            self._render_all()

    def _render_all(self) -> None:
        if not self.output_dir:
            return
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        self.output_dir.mkdir(parents=True, exist_ok=True)
        for stale in self.output_dir.glob("view_*.png"):
            stale.unlink()  # avoid mixing figures from an earlier run
        fig, axes = plt.subplots(2, 2, figsize=(11, 7))
        ax_water, ax_energy, ax_wave, ax_spec = axes.ravel()
        im_water = ax_water.imshow(
            self._states[0][0], aspect="auto", origin="lower", cmap="viridis",
            vmin=0.0, vmax=_SPECTRUM_SCALE,
        )
        ax_water.set_title("Rolling spectrogram")
        ax_water.set_xlabel("bin")
        ax_water.set_ylabel("time")
        im_energy = ax_energy.imshow(
            self._states[0][1], aspect="auto", origin="lower", cmap="magma",
            vmin=0.0, vmax=energy_scale(self._states[0][1]),
        )
        ax_energy.set_title("Per-channel energy")
        ax_energy.set_xlabel("time")
        ax_energy.set_ylabel("channel")
        (line_wave,) = ax_wave.plot(self._states[0][2], lw=0.8)
        ax_wave.set_title("Waveform")
        ax_wave.set_ylim(-1.5, 1.5)
        (line_spec,) = ax_spec.plot(self._states[0][3], lw=0.8)
        ax_spec.set_title("Spectrum")
        fig.tight_layout()
        try:
            for idx, (waterfall, energy, waveform, spectrum) in enumerate(self._states, 1):
                im_water.set_data(waterfall)
                im_energy.set_data(energy)
                im_energy.set_clim(0.0, energy_scale(energy))
                line_wave.set_ydata(waveform)
                line_spec.set_ydata(spectrum)
                ax_spec.set_ylim(0.0, trace_ceiling(spectrum))
                path = self.output_dir / f"view_{idx:02d}.png"
                fig.savefig(path, dpi=110)
                self.snapshots.append(path)
        finally:
            plt.close(fig)
