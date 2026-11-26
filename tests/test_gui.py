"""Tests for the shared view state and the browser GUI control API.

These run headless: no browser and no display server is needed.  The HTTP
layer is exercised with urllib against a real local server, which is exactly
the path a reviewer's browser uses.
"""

from __future__ import annotations

import base64
import json
import socket
import tempfile
import threading
import time
import unittest
import urllib.request
from pathlib import Path

import numpy as np

from src.gui import StreamVizApp, StreamVizServer
from src.protocol import Packet, PacketType, now_ms
from src.viewstate import ViewState


def _free_udp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _get(port: int, path: str, raw: bool = False):
    with urllib.request.urlopen(
        f"http://127.0.0.1:{port}{path}", timeout=5
    ) as response:
        body = response.read()
    return body if raw else json.loads(body)


def _post(port: int, path: str, payload: dict) -> dict:
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    # /api/figure renders a matplotlib PNG on first use; under a loaded host
    # (e.g. a GUI session running alongside the suite) this can exceed 10 s.
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read())


class ViewStateTests(unittest.TestCase):
    def test_ingest_updates_all_views_and_serialises(self):
        state = ViewState(history=8, n_bins=16, n_channels=2, waveform_len=32)
        packets = [
            Packet(PacketType.SPECTRUM, 1, 0, now_ms(),
                   np.full(16, 0.5, dtype=np.float32)),
            Packet(PacketType.WAVEFORM, 0, 1, now_ms(),
                   np.full(32, 0.25, dtype=np.float32)),
            Packet(PacketType.AUDIO, 0, 2, now_ms(),
                   np.zeros(16, dtype=np.int16)),
        ]
        state.ingest(packets)
        self.assertEqual(state.version, 1)
        self.assertEqual(state.frames, 1)
        arrays = state.arrays()
        self.assertTrue(np.allclose(arrays[0][-1], 0.5))
        self.assertTrue(np.allclose(arrays[2], 0.25))
        self.assertGreater(arrays[1][1, -1], 0)

        snap = state.snapshot()
        self.assertEqual(snap["views"]["spectrogram"]["shape"], [8, 16])
        self.assertEqual(
            len(base64.b64decode(snap["views"]["spectrogram"]["pixels"])), 8 * 16
        )
        self.assertEqual(
            len(base64.b64decode(snap["views"]["energy"]["pixels"])), 2 * 8
        )
        self.assertEqual(len(snap["views"]["waveform"]), 32)
        self.assertEqual(len(snap["views"]["spectrum"]), 16)

    def test_short_spectrum_is_padded_and_version_advances(self):
        state = ViewState(history=4, n_bins=8)
        state.ingest([Packet(PacketType.SPECTRUM, 0, 0, now_ms(),
                             np.ones(3, dtype=np.float32))])
        state.ingest([Packet(PacketType.SPECTRUM, 0, 1, now_ms(),
                             np.ones(3, dtype=np.float32))])
        self.assertEqual(state.version, 2)
        self.assertEqual(state.arrays()[0].shape, (4, 8))
        self.assertEqual(state.arrays()[0][-1][3:].tolist(), [0.0] * 5)


class GuiApiTests(unittest.TestCase):
    def test_http_api_lifecycle(self):
        with tempfile.TemporaryDirectory() as tmp:
            app = StreamVizApp(udp_port=_free_udp_port(), output_dir=Path(tmp))
            server = StreamVizServer(("127.0.0.1", 0), app)
            port = server.server_address[1]
            thread = threading.Thread(
                target=server.serve_forever,
                kwargs={"poll_interval": 0.02},
                daemon=True,
            )
            thread.start()
            try:
                self.assertEqual(_get(port, "/api/state")["status"], "idle")
                with urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/", timeout=5
                ) as page:
                    self.assertIn(b"StreamViz", page.read())

                started = _post(port, "/api/control", {
                    "action": "start", "rate_mbps": 1.0,
                    "frame_capacity": 8, "refresh_hz": 30,
                    "malformed_ratio": 0.0, "duration_s": 1.0,
                })
                self.assertTrue(started["ok"])

                deadline = time.time() + 6.0
                state = {}
                while time.time() < deadline:
                    state = _get(port, "/api/state")
                    counters = state["metrics"]["counters"]
                    if (counters.get("parsed_packets", 0) > 30
                            and state["view_version"] > 0
                            and state["audio"]["count"] > 0):
                        break
                    time.sleep(0.05)
                self.assertGreater(state["view_version"], 0)
                self.assertGreater(
                    state["metrics"]["counters"].get("parsed_packets", 0), 30
                )
                self.assertEqual(
                    len(base64.b64decode(
                        state["views"]["spectrogram"]["pixels"])), 120 * 128
                )
                self.assertGreater(
                    state["metrics"]["end_to_end_latency_ms"]["count"], 0
                )

                wav = _get(port, "/api/audio?mode=live", raw=True)
                self.assertTrue(wav.startswith(b"RIFF"))
                self.assertIn(b"WAVE", wav[:12])

                figure = _post(port, "/api/figure", {})
                self.assertTrue(figure["ok"])
                self.assertTrue(Path(figure["path"]).is_file())

                _post(port, "/api/control", {"action": "stop"})
                self.assertEqual(_get(port, "/api/state")["status"], "stopped")
                self.assertTrue((Path(tmp) / "gui_report.json").is_file())
            finally:
                if app.status == "running":
                    app.stop()
                server.shutdown()
                server.server_close()

    def test_overflow_policy_is_selectable_from_the_ui(self):
        with tempfile.TemporaryDirectory() as tmp:
            app = StreamVizApp(udp_port=_free_udp_port(), output_dir=Path(tmp))
            try:
                rejected = app.start({"overflow_policy": "keep_everything"})
                self.assertFalse(rejected["ok"])
                self.assertIn("unknown overflow policy", rejected["message"])

                started = app.start({
                    "overflow_policy": "drop_newest",
                    "rate_mbps": 1.0, "duration_s": 0.4,
                })
                self.assertTrue(started["ok"])
                # The switch must reach the buffers, not just the form.
                self.assertEqual(
                    app.pipeline.report()["overflow_policy"], "drop_newest"
                )
                state = app.state()
                self.assertEqual(state["config"]["overflow_policy"], "drop_newest")
                self.assertIn("staleness_ms", state["metrics"])
            finally:
                if app.status == "running":
                    app.stop()

    def test_restart_reuses_a_fresh_pipeline(self):
        with tempfile.TemporaryDirectory() as tmp:
            app = StreamVizApp(udp_port=_free_udp_port(), output_dir=Path(tmp))
            try:
                first = app.start({"rate_mbps": 1.0, "duration_s": 0.3})
                self.assertTrue(first["ok"])
                # The finite sender shuts the session down by itself.  Wait
                # for that state instead of assuming a fixed 0.5 s is enough
                # on a loaded host.
                deadline = time.time() + 5.0
                while time.time() < deadline and app.status == "running":
                    time.sleep(0.02)
                second = app.start({"rate_mbps": 2.0, "duration_s": 0.3})
                self.assertTrue(second["ok"])
                self.assertEqual(app.params["rate_mbps"], 2.0)
                app.stop()
            finally:
                if app.status == "running":
                    app.stop()


if __name__ == "__main__":
    unittest.main()
