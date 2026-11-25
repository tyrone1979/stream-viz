"""Interactive browser GUI and its HTTP control API.

The GUI is built on the Python standard library only: no Qt, no Tk, no browser
extension.  A reviewer runs one command, opens a local URL, and can inspect the
running pipeline, change its parameters, export a figure and replay archived
audio by source timestamp.  The same API is used by ``tools/capture_gui.py``
to reproduce the manuscript figure with a headless browser.

    StreamVizApp     session state (start/stop, metrics, audio, event log)
    GuiConsumer      drop-in display consumer feeding a shared ViewState
    StreamVizServer  /api/state, /api/control, /api/audio, /api/figure
                     plus the static assets in webui/
"""

from __future__ import annotations

import io
import json
import threading
import time
import wave
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import numpy as np

from .metrics import Counters, LatencyMeter
from .pipeline import PipelineConfig, StreamPipeline
from .synthetic import StreamSpec, send_stream
from .viewstate import ViewState

_ROOT = Path(__file__).resolve().parents[1]
_WEBUI = _ROOT / "webui"
_STATIC = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/index.html": ("index.html", "text/html; charset=utf-8"),
    "/app.js": ("app.js", "application/javascript; charset=utf-8"),
    "/style.css": ("style.css", "text/css; charset=utf-8"),
}


class GuiConsumer:
    """A display consumer that keeps a :class:`ViewState` fresh for the GUI.

    It implements the small ``start()`` / ``join()`` / ``frames_rendered`` /
    ``snapshots`` interface that :class:`StreamPipeline` expects, so the GUI
    is a true front-end on the production pipeline rather than a mock-up.
    """

    def __init__(
        self,
        frame_buffer,
        ready_signal,
        counters: Counters,
        latency: LatencyMeter,
        stop_event: threading.Event,
        refresh_hz: float = 30.0,
        history: int = 120,
        n_bins: int = 128,
        n_channels: int = 4,
    ) -> None:
        self.state = ViewState(history=history, n_bins=n_bins, n_channels=n_channels)
        self.frame_buffer = frame_buffer
        self.ready_signal = ready_signal
        self.counters = counters
        self.latency = latency
        self.stop_event = stop_event
        self.refresh_hz = max(1.0, refresh_hz)
        self.snapshots: List[Path] = []
        self._thread = threading.Thread(
            target=self._run, name="gui-consumer", daemon=True
        )

    @classmethod
    def for_pipeline(cls, pipeline: StreamPipeline) -> "GuiConsumer":
        """Factory handed to ``StreamPipeline(display_factory=...)``."""
        cfg = pipeline.config
        return cls(
            frame_buffer=pipeline.frame_buffer,
            ready_signal=pipeline.ready_signal,
            counters=pipeline.counters,
            latency=pipeline.display_latency,
            stop_event=pipeline.stop_event,
            refresh_hz=cfg.refresh_hz,
            history=cfg.history,
            n_bins=cfg.n_bins,
            n_channels=cfg.n_channels,
        )

    @property
    def frames_rendered(self) -> int:
        return self.state.frames

    def start(self) -> None:
        self._thread.start()

    def join(self, timeout: Optional[float] = None) -> None:
        self._thread.join(timeout)

    def _run(self) -> None:
        while not self.stop_event.is_set():
            self.ready_signal.wait(1.0 / self.refresh_hz)
            packets = self.frame_buffer.drain()
            if packets:
                self.state.ingest(packets, self.latency, self.counters)


def _render_png(arrays: Tuple[np.ndarray, ...], path: Path) -> Path:
    """Render one four-panel figure for the manuscript (Agg, headless-safe)."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    waterfall, energy, waveform, spectrum = arrays
    fig, axes = plt.subplots(2, 2, figsize=(11, 7), facecolor="white")
    ax_water, ax_energy, ax_wave, ax_spec = axes.ravel()
    ax_water.imshow(waterfall, aspect="auto", origin="lower", cmap="viridis")
    ax_water.set_title("Rolling spectrogram")
    ax_water.set_xlabel("bin")
    ax_water.set_ylabel("time")
    ax_energy.imshow(energy, aspect="auto", origin="lower", cmap="magma")
    ax_energy.set_title("Per-channel energy")
    ax_energy.set_xlabel("time")
    ax_energy.set_ylabel("channel")
    ax_wave.plot(waveform, lw=0.8, color="#1f6feb")
    ax_wave.set_title("Waveform")
    ax_wave.set_ylim(-1.5, 1.5)
    ax_spec.plot(spectrum, lw=0.8, color="#d97706")
    ax_spec.set_title("Spectrum")
    ax_spec.set_ylim(0.0, max(1.5, float(np.max(spectrum)) * 1.1))
    fig.tight_layout()
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=110)
    plt.close(fig)
    return path


def _wav_bytes(samples: np.ndarray, rate: int) -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wav:
        wav.setnchannels(1)
        wav.setsampwidth(2)
        wav.setframerate(rate)
        wav.writeframes(np.asarray(samples, dtype=np.int16).tobytes())
    return buf.getvalue()


class StreamVizApp:
    """Owns one running session: pipeline, synthetic sender and metrics."""

    def __init__(
        self,
        udp_port: int = 19000,
        output_dir: Optional[Path] = None,
    ) -> None:
        self.udp_port = udp_port
        self.output_dir = Path(output_dir) if output_dir else (_ROOT / "output" / "gui")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.pipeline: Optional[StreamPipeline] = None
        self.consumer: Optional[GuiConsumer] = None
        self._sender: Optional[threading.Thread] = None
        self._sender_stop: Optional[threading.Event] = None
        self.status = "idle"
        self.params: Dict[str, Any] = {
            "rate_mbps": 4.0,
            "frame_capacity": 64,
            "refresh_hz": 30.0,
            "malformed_ratio": 0.01,
            "duration_s": 3600.0,
        }
        self.events: List[str] = []
        self.last_report: Dict[str, Any] = {}
        self._idle_state = ViewState()
        self._metrics_cache: Dict[str, Any] = {}
        self._metrics_ts = 0.0

    def _log(self, message: str) -> None:
        stamp = time.strftime("%H:%M:%S")
        self.events.append(f"[{stamp}] {message}")
        del self.events[:-12]

    def start(self, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        with self._lock:
            if self.status == "running":
                return {"ok": False, "message": "already running"}
            merged = dict(self.params)
            for key, value in (params or {}).items():
                if key in merged:
                    merged[key] = value
            if self.status == "stopped" and self.pipeline is not None:
                self._stop_locked()
            cfg = PipelineConfig(
                host="127.0.0.1",
                port=self.udp_port,
                frame_capacity=max(1, int(merged["frame_capacity"])),
                refresh_hz=max(1.0, float(merged["refresh_hz"])),
                output_dir=self.output_dir,
                enable_display=False,
            )
            self.pipeline = StreamPipeline(cfg, display_factory=GuiConsumer.for_pipeline)
            self.consumer = self.pipeline.display
            self.pipeline.start()
            spec = StreamSpec(
                host="127.0.0.1",
                port=self.udp_port,
                duration_s=max(0.2, float(merged["duration_s"])),
                rate_mbps=max(0.1, float(merged["rate_mbps"])),
                malformed_ratio=min(0.5, max(0.0, float(merged["malformed_ratio"]))),
                n_bins=cfg.n_bins,
                n_channels=cfg.n_channels,
                audio_len=512,
                sample_rate=cfg.sample_rate,
            )
            self._sender_stop = threading.Event()
            self._sender = threading.Thread(
                target=self._run_sender, args=(spec,), name="gui-sender", daemon=True
            )
            self._sender.start()
            self.params = merged
            self.status = "running"
            self._log(
                f"stream started: {merged['rate_mbps']} Mbit/s, "
                f"display FIFO {merged['frame_capacity']}, "
                f"refresh {merged['refresh_hz']} Hz"
            )
            return {"ok": True, "message": "stream started"}

    def _run_sender(self, spec: StreamSpec) -> None:
        try:
            stats = send_stream(spec, stop_event=self._sender_stop)
            self._log(f"sender finished: {stats['sent_packets']} datagrams")
        except Exception as exc:  # keep the GUI alive if the sender fails
            with self._lock:
                self._log(f"sender error: {type(exc).__name__}: {exc}")
        finally:
            # A finite run ends on its own; an explicit Stop sets the event
            # first, so the two paths never race to shut the pipeline down.
            if self._sender_stop is not None and not self._sender_stop.is_set():
                time.sleep(0.15)
                self.stop()

    def stop(self) -> Dict[str, Any]:
        with self._lock:
            if self.status != "running":
                return {"ok": False, "message": "not running"}
            self._stop_locked()
            return {"ok": True, "message": "pipeline stopped"}

    def _stop_locked(self) -> None:
        if self._sender_stop is not None:
            self._sender_stop.set()
        if self._sender is not None and self._sender is not threading.current_thread():
            self._sender.join(timeout=2.0)
        if self.pipeline is not None:
            self.pipeline.stop()
            self.last_report = self.pipeline.report()
            (self.output_dir / "gui_report.json").write_text(
                json.dumps(self.last_report, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
        self.status = "stopped"
        self._log("pipeline stopped; metrics written to gui_report.json")

    def save_figure(self) -> Dict[str, Any]:
        with self._lock:
            state = self.consumer.state if self.consumer else self._idle_state
            path = _render_png(state.arrays(), self.output_dir / "gui_view.png")
            self._log(f"figure exported: {path.name}")
            return {"ok": True, "path": str(path), "name": path.name}

    def audio_wav(self, mode: str, timestamp_ms: int) -> Optional[bytes]:
        with self._lock:
            if self.pipeline is None:
                return None
            archive = self.pipeline.audio_archive
            if mode == "live":
                archive.flush()
                segments = list(archive.segments)
                if not segments:
                    return None
                samples, rate = archive.read(segments[-1])
            else:
                samples, rate, segment = self.pipeline.play_at(int(timestamp_ms))
                if segment is None or len(samples) == 0:
                    return None
            return _wav_bytes(samples, rate)

    def state(self) -> Dict[str, Any]:
        with self._lock:
            snap = (
                self.consumer.state.snapshot()
                if self.consumer is not None
                else self._idle_state.snapshot()
            )
            if self.pipeline is not None:
                segments = list(self.pipeline.audio_archive.segments)
                self._segments_after_stop = list(segments)
            else:
                segments = list(getattr(self, "_segments_after_stop", []))
            audio_range = (
                [segments[0].start_ms, segments[-1].end_ms] if segments else [0, 0]
            )

            # Percentiles over the full latency sample set are comparatively
            # expensive.  Cache them briefly so a fast-polling browser cannot
            # steal CPU from the timer-driven display consumer.
            now = time.monotonic()
            if not self._metrics_cache or now - self._metrics_ts >= 0.25:
                report = (self.pipeline.report() if self.pipeline is not None
                          else self.last_report)
                self._metrics_cache = {
                    "throughput_mbps": report.get("throughput_mbps", 0.0),
                    "counters": report.get("counters", {}),
                    "ingest_latency_ms": report.get("ingest_latency_ms", {}),
                    "end_to_end_latency_ms": report.get("end_to_end_latency_ms", {}),
                    "fifo_overflow_dropped": report.get("fifo_overflow_dropped", 0),
                }
                self._metrics_ts = now
            metrics = dict(self._metrics_cache)
            metrics["rendered_frames"] = snap["frames"]
            return {
                "status": self.status,
                "config": {**self.params, "udp_port": self.udp_port},
                "view_version": snap["version"],
                "frames": snap["frames"],
                "views": snap["views"],
                "metrics": metrics,
                "audio": {
                    "count": len(segments),
                    "range": audio_range,
                    "segments": [
                        {"start_ms": s.start_ms, "end_ms": s.end_ms, "name": s.path.name}
                        for s in segments[-40:]
                    ],
                },
                "log": list(self.events),
            }


class _Handler(BaseHTTPRequestHandler):
    server_version = "StreamViz/0.2"

    def _send(self, code: int, body: bytes, content_type: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass  # the browser closed a poll; harmless

    def _json(self, payload: Dict[str, Any], code: int = 200) -> None:
        self._send(
            code,
            json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            "application/json; charset=utf-8",
        )

    def do_GET(self) -> None:  # noqa: N802 (stdlib API)
        url = urlparse(self.path)
        if url.path in _STATIC:
            name, content_type = _STATIC[url.path]
            path = _WEBUI / name
            if not path.is_file():
                self._send(404, b"missing asset", "text/plain; charset=utf-8")
                return
            self._send(200, path.read_bytes(), content_type)
            return
        if url.path == "/api/state":
            self._json(self.server.app.state())
            return
        if url.path == "/api/audio":
            query = parse_qs(url.query)
            mode = query.get("mode", ["live"])[0]
            try:
                timestamp = int(query.get("ts", ["0"])[0])
            except ValueError:
                timestamp = 0
            data = self.server.app.audio_wav(mode, timestamp)
            if data is None:
                self._send(404, b"no audio archived yet", "text/plain; charset=utf-8")
            else:
                self._send(200, data, "audio/wav")
            return
        if url.path == "/favicon.ico":
            self._send(204, b"", "image/x-icon")
            return
        self._send(404, b"not found", "text/plain; charset=utf-8")

    def do_POST(self) -> None:  # noqa: N802 (stdlib API)
        url = urlparse(self.path)
        if url.path not in ("/api/control", "/api/figure"):
            self._json({"ok": False, "message": "not found"}, 404)
            return
        try:
            length = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(length) or b"{}")
        except (ValueError, TypeError):
            self._json({"ok": False, "message": "invalid JSON"}, 400)
            return
        app = self.server.app
        if url.path == "/api/figure":
            self._json(app.save_figure())
            return
        action = body.get("action")
        if action == "start":
            self._json(app.start(body))
        elif action == "stop":
            self._json(app.stop())
        elif action == "figure":
            self._json(app.save_figure())
        else:
            self._json({"ok": False, "message": f"unknown action: {action}"}, 400)

    def log_message(self, fmt: str, *args: Any) -> None:
        return  # keep the console clean; the GUI has its own event log


class StreamVizServer(ThreadingHTTPServer):
    """Localhost-only server, safe to leave running while reviewing."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address: Tuple[str, int], app: StreamVizApp) -> None:
        super().__init__(address, _Handler)
        self.app = app
