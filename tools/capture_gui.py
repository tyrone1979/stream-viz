"""Reproduce the manuscript GUI figure with a headless Chrome/Edge browser.

    python tools/capture_gui.py

Starts the real GUI server with a short stream from any registered source, lets
data accumulate, then takes a screenshot of the live dashboard.  No browser
extension or third-party Python package is required; only a locally installed
Chrome or Edge.  The screenshot is a genuine capture of the running
application, not a hand-drawn diagram.

    python tools/capture_gui.py --source gwosc --output output/figures/gui_gwosc.png
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.datasets import REGISTRY  # noqa: E402
from src.gui import StreamVizApp, StreamVizServer  # noqa: E402

_BROWSER_PATHS = [
    Path("C:/Program Files/Google/Chrome/Application/chrome.exe"),
    Path("C:/Program Files (x86)/Google/Chrome/Application/chrome.exe"),
    Path("C:/Program Files/Microsoft/Edge/Application/msedge.exe"),
    Path("C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe"),
    Path("/usr/bin/google-chrome"),
    Path("/usr/bin/chromium"),
]


def find_browser() -> str:
    for path in _BROWSER_PATHS:
        if path.is_file():
            return str(path)
    for name in ("google-chrome", "chromium", "chromium-browser", "msedge"):
        found = shutil.which(name)
        if found:
            return found
    raise SystemExit("No Chrome/Edge/Chromium found; install one or use --browser")


def main() -> int:
    parser = argparse.ArgumentParser(description="Capture the StreamViz GUI figure")
    parser.add_argument("--browser", default=None, help="path to Chrome/Edge executable")
    parser.add_argument("--output", default="output/figures/gui_dashboard.png")
    parser.add_argument("--port", type=int, default=8766)
    parser.add_argument("--udp-port", type=int, default=19001)
    parser.add_argument("--rate", type=float, default=4.0)
    parser.add_argument("--source", choices=("synthetic", *sorted(REGISTRY)),
                        default="synthetic",
                        help="capture the synthetic smoke test or any registered real dataset")
    parser.add_argument("--wait", type=float, default=1.5,
                        help="seconds of traffic to accumulate before capture")
    parser.add_argument("--width", type=int, default=1440)
    parser.add_argument("--height", type=int, default=1160)
    parser.add_argument("--virtual-time", type=int, default=1500,
                        help="headless browser virtual time budget in ms")
    args = parser.parse_args()

    browser = args.browser or find_browser()
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)

    app = StreamVizApp(udp_port=args.udp_port, output_dir=_ROOT / "output" / "gui_capture")
    app.params.update({
        "source": args.source,
        "rate_mbps": args.rate,
        # Keep the session alive well past the screenshot: a finite sender
        # auto-stops the pipeline, which would otherwise leave the captured
        # figure showing STOPPED instead of a live measurement.
        "duration_s": max(90.0, args.wait + 80),
        "malformed_ratio": 0.0 if args.source != "synthetic" else 0.01,
    })
    started = app.start()
    if not started.get("ok"):
        print(started["message"])
        return 1
    server = StreamVizServer(("127.0.0.1", args.port), app)
    thread = threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
    )
    thread.start()

    result = None
    try:
        time.sleep(args.wait)  # fill the rolling views with real data
        with tempfile.TemporaryDirectory() as profile:
            cmd = [
                browser,
                "--headless=new",
                "--disable-gpu",
                "--no-first-run",
                "--no-default-browser-check",
                "--hide-scrollbars",
                f"--user-data-dir={profile}",
                f"--window-size={args.width},{args.height}",
                f"--virtual-time-budget={args.virtual_time}",
                f"--screenshot={out.resolve()}",
                f"http://127.0.0.1:{args.port}/",
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
    finally:
        server.shutdown()
        if app.status == "running":
            app.stop()
        server.server_close()

    if not out.is_file() or out.stat().st_size < 2000:
        if result is not None:
            print((result.stdout or "")[-1500:])
            print((result.stderr or "")[-1500:])
        print("FAIL - headless browser did not produce a usable screenshot")
        return 1
    print(f"GUI figure written to {out.resolve()} ({out.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
