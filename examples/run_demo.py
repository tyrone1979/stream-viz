"""One-command end-to-end demo.

    python examples/run_demo.py --duration 4 --rate 4

Starts the pipeline, streams synthetic UDP traffic, exercises real-time (S6)
and timestamp-seek (S7) replay, then writes a JSON report and PNG views under
``output/``.
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from examples.generate_stream import StreamSpec, send_stream
from src.pipeline import PipelineConfig, StreamPipeline


def main() -> int:
    parser = argparse.ArgumentParser(description="StreamViz end-to-end demo")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=19000)
    parser.add_argument("--duration", type=float, default=4.0)
    parser.add_argument("--rate", type=float, default=4.0, help="target Mbit/s")
    parser.add_argument("--malformed", type=float, default=0.01,
                        help="fraction of deliberately corrupted datagrams")
    parser.add_argument("--no-figures", action="store_true")
    parser.add_argument("--output", default="output")
    args = parser.parse_args()

    cfg = PipelineConfig(
        host=args.host,
        port=args.port,
        output_dir=Path(args.output),
        enable_display=not args.no_figures,
        snapshot_every=15,
    )
    spec = StreamSpec(
        host=args.host,
        port=args.port,
        duration_s=args.duration,
        rate_mbps=args.rate,
        malformed_ratio=args.malformed,
        n_bins=cfg.n_bins,
        n_channels=cfg.n_channels,
        audio_len=512,
        sample_rate=cfg.sample_rate,
    )

    pipeline = StreamPipeline(cfg).start()
    stop = threading.Event()
    sender_stats: dict = {}
    sender = threading.Thread(
        target=lambda: sender_stats.update(send_stream(spec, stop_event=stop)),
        name="sender",
    )
    sender.start()
    sender.join(timeout=spec.duration_s + 10)
    stop.set()
    pipeline.stop()
    if sender.is_alive():
        sender.join(timeout=2)

    # S6: paced playback of the newest audio; S7: seek by source timestamp.
    live_samples = pipeline.play_realtime()
    first = pipeline.audio_archive.segments[0] if len(pipeline.audio_archive) else None
    if first is not None:
        history_samples, rate, seg = pipeline.play_at(first.start_ms + 10)
        replay = {"samples": int(len(history_samples)), "sample_rate": rate,
                  "segment": seg.path.name if seg else None}
    else:
        replay = {"samples": 0, "sample_rate": cfg.sample_rate, "segment": None}

    report = {
        "config": {
            "duration_s": spec.duration_s,
            "target_rate_mbps": spec.rate_mbps,
            "port": cfg.port,
        },
        "sender": sender_stats,
        "pipeline": pipeline.report(),
        "replay": {
            "live_samples": int(live_samples),
            "history": replay,
        },
    }
    out = cfg.output_dir / "demo_report.json"
    out.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(report, indent=2, ensure_ascii=False))
    print(f"\nReport written to: {out.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
