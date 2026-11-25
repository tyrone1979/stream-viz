"""Parameter sweep used to produce the quantitative results for the article.

    python examples/benchmark.py

Sweeps target throughput and display-buffer capacity, recording end-to-end
latency percentiles, packet loss and cleaning statistics.  Results are written
to ``output/benchmark.json`` for direct reuse in a manuscript/table.
"""

from __future__ import annotations

import json
import sys
import threading
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from examples.generate_stream import StreamSpec, send_stream
from src.pipeline import PipelineConfig, StreamPipeline

RATES_MBPS = [1.0, 2.0, 4.0, 8.0]
FRAME_CAPACITIES = [16, 64]
DURATION_S = 1.5
MALFORMED_RATIO = 0.02


def run_case(rate: float, capacity: int, port: int, root: Path) -> dict:
    cfg = PipelineConfig(
        port=port,
        frame_capacity=capacity,
        enable_display=False,
        output_dir=root / f"r{rate:g}_c{capacity}",
    )
    spec = StreamSpec(
        port=port,
        duration_s=DURATION_S,
        rate_mbps=rate,
        malformed_ratio=MALFORMED_RATIO,
        n_bins=cfg.n_bins,
        n_channels=cfg.n_channels,
        sample_rate=cfg.sample_rate,
        seed=int(rate * 10) + capacity,
    )
    pipeline = StreamPipeline(cfg).start()
    stop = threading.Event()
    stats: dict = {}
    sender = threading.Thread(
        target=lambda: stats.update(send_stream(spec, stop_event=stop)),
        name="sender",
    )
    sender.start()
    sender.join(timeout=DURATION_S + 10)
    stop.set()
    pipeline.stop()
    if sender.is_alive():
        sender.join(timeout=2)

    report = pipeline.report()
    counters = report["counters"]
    parsed = counters.get("parsed_packets", 0)
    sent = stats.get("sent_packets", 0)
    return {
        "target_rate_mbps": rate,
        "frame_capacity": capacity,
        "sent_packets": sent,
        "parsed_packets": parsed,
        "delivery_ratio": round(parsed / sent, 4) if sent else 0.0,
        "measured_mbps": report["throughput_mbps"],
        "end_to_end_latency_ms": report["end_to_end_latency_ms"],
        "ingest_latency_ms": report["ingest_latency_ms"],
        "dropped_malformed": counters.get("dropped_bad_magic", 0),
        "sequence_gaps": counters.get("sequence_gaps", 0),
        "fifo_overflow_dropped": report["fifo_overflow_dropped"],
        "cache_full_notifications": counters.get("cache_full_notifications", 0),
        "audio_segments": report["audio_segments"],
    }


def main() -> int:
    root = Path("output") / "benchmark"
    root.mkdir(parents=True, exist_ok=True)
    results = []
    port = 19100
    for rate in RATES_MBPS:
        for capacity in FRAME_CAPACITIES:
            case = run_case(rate, capacity, port, root)
            results.append(case)
            port += 1
            print(
                f"rate={rate:>4} Mbps  cap={capacity:>3}  "
                f"delivery={case['delivery_ratio']:.3f}  "
                f"p95={case['end_to_end_latency_ms']['p95_ms']:.2f} ms"
            )
    payload = {
        "duration_per_case_s": DURATION_S,
        "malformed_ratio": MALFORMED_RATIO,
        "cases": results,
    }
    out = root / "benchmark.json"
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nResults written to: {out.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
