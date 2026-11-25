"""One-command interactive GUI.

    python examples/run_gui.py                 # opens the dashboard in a browser
    python examples/run_gui.py --no-browser   # serve only (for remote/CI use)

The dashboard starts a synthetic stream immediately unless ``--idle`` is
given.  Stopping it with Ctrl-C shuts down the pipeline and writes
``output/gui/gui_report.json``.
"""

from __future__ import annotations

import argparse
import sys
import threading
import webbrowser
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.gui import StreamVizApp, StreamVizServer  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="StreamViz interactive GUI")
    parser.add_argument("--port", type=int, default=8765, help="HTTP port (default 8765)")
    parser.add_argument("--udp-port", type=int, default=19000)
    parser.add_argument("--rate", type=float, default=4.0, help="target Mbit/s")
    parser.add_argument("--capacity", type=int, default=64, help="display FIFO capacity")
    parser.add_argument("--refresh", type=float, default=30.0, help="display timer Hz")
    parser.add_argument("--malformed", type=float, default=0.01)
    parser.add_argument("--duration", type=float, default=3600.0,
                        help="sender duration in seconds (default: until Stop)")
    parser.add_argument("--output", default="output/gui")
    parser.add_argument("--idle", action="store_true", help="do not start a stream")
    parser.add_argument("--no-browser", action="store_true", help="do not open a browser")
    args = parser.parse_args()

    app = StreamVizApp(udp_port=args.udp_port, output_dir=Path(args.output))
    app.params.update({
        "rate_mbps": args.rate,
        "frame_capacity": args.capacity,
        "refresh_hz": args.refresh,
        "malformed_ratio": args.malformed,
        "duration_s": args.duration,
    })
    server = StreamVizServer(("127.0.0.1", args.port), app)
    url = f"http://127.0.0.1:{args.port}/"

    if not args.idle:
        app.start()
    print(f"StreamViz GUI ready at {url}")
    print("Press Ctrl-C to stop the pipeline and exit.")
    if not args.no_browser:
        threading.Timer(0.4, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever(poll_interval=0.1)
    except KeyboardInterrupt:
        print("\nShutting down…")
    finally:
        if app.status == "running":
            app.stop()
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
