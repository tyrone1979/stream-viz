"""Pipeline orchestration: the seven stages as one lifecycle-managed object.

    S1  start receiver + parser + display threads
    S2  UDP datagrams land in the raw source buffer
    S3  parser validates/cleans and classifies packets
    S4  typed FIFOs fill; cache-full raises a signal; audio flushes to WAV
    S5  display timer consumes the signal and refreshes four views
    S6  paced real-time playback of the most recent audio
    S7  timestamp-seek replay of historical audio

The data source is pluggable: a synthetic generator for smoke tests, or a
verified open recording replayed by ``src/open_dataset.py``.  Both use the
same wire protocol and the same metering path.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import numpy as np

from .buffers import BoundedFIFO, Datagram, OverflowPolicy, Signal
from .metrics import Counters, LatencyMeter
from .parser import ParserThread
from .player import AudioArchive, AudioPlayer, AudioSink
from .protocol import now_ms
from .receiver import UdpReceiver
from .visualizer import DisplayThread, DrainThread


@dataclass
class PipelineConfig:
    host: str = "127.0.0.1"
    port: int = 19000
    source_capacity: int = 4096
    frame_capacity: int = 64
    overflow_policy: str = "drop_oldest"
    recv_buf_bytes: int = 0          # 0 keeps the OS default (SO_RCVBUF)
    consumer_delay_ms: float = 0.0   # injected per-tick cost of a real front-end
    archive_audio: bool = True
    audio_block_samples: int = 4096
    sample_rate: int = 16_000
    refresh_hz: float = 30.0
    history: int = 120
    n_bins: int = 128
    n_channels: int = 4
    output_dir: Path = field(default_factory=lambda: Path("output"))
    snapshot_every: int = 20
    recv_timeout: float = 0.2
    enable_display: bool = True


class StreamPipeline:
    def __init__(self, config: Optional[PipelineConfig] = None,
                 sink: Optional[AudioSink] = None,
                 display_factory: Optional[Callable[["StreamPipeline"], Any]] = None) -> None:
        self.config = config or PipelineConfig()
        cfg = self.config
        cfg.output_dir.mkdir(parents=True, exist_ok=True)

        self.counters = Counters()
        self.ingest_latency = LatencyMeter()
        self.display_latency = LatencyMeter()
        self.staleness = LatencyMeter()
        self.stop_event = threading.Event()

        policy = OverflowPolicy(cfg.overflow_policy)
        # The source buffer holds raw datagrams, so the only age available is
        # queueing time since arrival.  The frame buffer holds decoded packets,
        # so its age is the true staleness of the sample at discard time.
        self.source_buffer: BoundedFIFO[Datagram] = BoundedFIFO(
            cfg.source_capacity,
            overflow_policy=policy,
            age_fn=lambda d: max(0, now_ms() - d.received_ms),
            stop_event=self.stop_event,
        )
        self.frame_buffer: BoundedFIFO = BoundedFIFO(
            cfg.frame_capacity,
            overflow_policy=policy,
            age_fn=lambda p: max(0, now_ms() - p.timestamp_ms),
            stop_event=self.stop_event,
        )
        self.ready_signal = Signal()
        self.audio_archive = AudioArchive(
            cfg.output_dir / "audio",
            sample_rate=cfg.sample_rate,
            block_samples=cfg.audio_block_samples,
        )
        self.player = AudioPlayer(self.audio_archive, sink=sink)

        self.receiver = UdpReceiver(
            cfg.host, cfg.port, self.source_buffer, self.counters,
            self.stop_event, recv_timeout=cfg.recv_timeout,
            recv_buf_bytes=cfg.recv_buf_bytes,
        )
        # parser records arrival latency, display records render latency
        self.parser = ParserThread(
            self.source_buffer, self.frame_buffer, self.audio_archive,
            self.ready_signal, self.counters, self.ingest_latency, self.stop_event,
            archive_audio=cfg.archive_audio,
        )
        if display_factory is not None:
            # An interactive front-end supplies its own consumer.  It receives
            # the half-built pipeline so it can share the very same buffers,
            # latency meters and stop event as the ingest threads.
            self.display = display_factory(self)
        else:
            consumer_cls = DisplayThread if cfg.enable_display else DrainThread
            consumer_kwargs = dict(
                frame_buffer=self.frame_buffer,
                ready_signal=self.ready_signal,
                counters=self.counters,
                latency=self.display_latency,
                staleness=self.staleness,
                stop_event=self.stop_event,
                refresh_hz=cfg.refresh_hz,
                consumer_delay_ms=cfg.consumer_delay_ms,
            )
            if cfg.enable_display:
                consumer_kwargs.update(
                    history=cfg.history,
                    n_bins=cfg.n_bins,
                    n_channels=cfg.n_channels,
                    output_dir=cfg.output_dir / "figures",
                    snapshot_every=cfg.snapshot_every,
                )
            self.display = consumer_cls(**consumer_kwargs)
        self._started_at: Optional[float] = None
        self._threads = [self.receiver, self.parser, self.display]

    def start(self) -> "StreamPipeline":
        if self._started_at is not None:
            return self
        self._started_at = time.perf_counter()
        for thread in self._threads:
            thread.start()
        return self

    def stop(self, timeout: float = 20.0) -> None:
        self.stop_event.set()
        self.receiver.close()
        for thread in self._threads:
            thread.join(timeout=timeout)
        self.audio_archive.flush()

    def play_realtime(self) -> int:
        return self.player.play_live()

    def play_at(self, timestamp_ms: int):
        return self.player.play_history(timestamp_ms)

    def report(self) -> Dict:
        duration = (time.perf_counter() - self._started_at) if self._started_at else 0.0
        counters = self.counters.snapshot()
        bytes_in = counters.get("udp_bytes", 0)
        # Measure throughput over the active reception window only; shutdown
        # and thread-join time must not inflate the denominator.
        active_s = duration
        if self.receiver.first_ms is not None and self.receiver.last_ms is not None:
            active_s = max(0.0, (self.receiver.last_ms - self.receiver.first_ms) / 1000.0)

        received = counters.get("udp_datagrams", 0)
        parsed = counters.get("parsed_packets", 0)
        # UDP has no delivery guarantee, so loss is attributed per stage
        # instead of collapsed into one "delivery ratio" that hides where the
        # data actually went.  `received -> parsed` is what this software can
        # observe; `sent -> received` must be joined from the sender's own
        # count, which the benchmark scripts do.
        flow = {
            "udp_received": received,
            "parsed": parsed,
            "parse_yield": round(parsed / received, 4) if received else 0.0,
            "parsed_by_type": {
                kind: counters.get(f"parsed_{kind}", 0)
                for kind in ("spectrum", "waveform", "audio")
            },
            "delivered_to_consumer": counters.get("display_frames", 0),
            "sequence_gaps": counters.get("sequence_gaps", 0),
            "duplicate_or_reorder": counters.get("duplicate_or_reorder", 0),
            "dropped_at_parse": {
                name: value for name, value in counters.items()
                if name.startswith("dropped_") and name != "dropped_at_shutdown"
            },
            "dropped_at_shutdown": counters.get("dropped_at_shutdown", 0),
        }
        return {
            "duration_s": round(duration, 3),
            "active_receive_s": round(active_s, 3),
            "throughput_mbps": round(bytes_in * 8 / 1e6 / active_s, 3) if active_s > 0 else 0.0,
            "counters": counters,
            "ingest_latency_ms": self.ingest_latency.snapshot(),
            "end_to_end_latency_ms": self.display_latency.snapshot(),
            "staleness_ms": self.staleness.snapshot(),
            "overflow_policy": self.config.overflow_policy,
            "buffers": {
                "source": self.source_buffer.stats(),
                "frame": self.frame_buffer.stats(),
            },
            "flow": flow,
            "audio_segments": len(self.audio_archive),
            "rendered_frames": self.display.frames_rendered,
            "snapshots": [str(p) for p in self.display.snapshots],
            "fifo_overflow_dropped": self.source_buffer.dropped + self.frame_buffer.dropped,
        }

    def __enter__(self) -> "StreamPipeline":
        return self.start()

    def __exit__(self, *_exc) -> None:
        self.stop()
