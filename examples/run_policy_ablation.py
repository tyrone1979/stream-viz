"""Overflow-policy ablation: what a full buffer should discard, and what it costs.

    python tools/fetch_dataset.py                       # once
    python examples/run_policy_ablation.py --dataset mitdb \
        --policies drop_oldest,drop_newest,block,unbounded --rates 2,4

The pipeline is deliberately over-subscribed: the display consumer ticks at
``--refresh-hz`` (default 5 Hz) while the sender pushes several Mbit/s, so the
display buffer *will* fill.  Under that condition the four overflow policies
produce different, measurable behaviour:

    drop_oldest  newest data preserved; discarded items are the stalest ones
    drop_newest  sequence stays contiguous; the rendered sample ages linearly
    block        application layer keeps everything; loss moves to the kernel
    unbounded    nothing discarded until a hard cap; latency and memory grow

Every run reports the full conservation chain
``sent -> udp_received -> parsed -> delivered_to_consumer`` plus the age of the
items that were discarded.  Nothing here is synthetic: payloads come from a
verified open recording (see :mod:`src.datasets`).

Keep ``--duration`` short with ``unbounded`` — it trades memory for integrity
and is meant to show where that trade stops being affordable.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import queue
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.buffers import OverflowPolicy  # noqa: E402
from src.datasets import REGISTRY, load_dataset  # noqa: E402
from src.open_dataset import ReplaySpec, send_record  # noqa: E402
from src.pipeline import PipelineConfig, StreamPipeline  # noqa: E402


def _sender_worker(spec: ReplaySpec, result_queue) -> None:
    """External acquisition device: a separate process, outside the GIL."""
    try:
        result_queue.put(send_record(spec))
    except Exception as exc:  # noqa: BLE001 - surface the child failure verbatim
        result_queue.put({"error": f"{type(exc).__name__}: {exc}"})


def _ratio(num: int, den: int) -> float:
    return round(num / den, 4) if den else 0.0


def _one_run(
    dataset: str,
    policy: str,
    rate: float,
    capacity: int,
    refresh_hz: float,
    consumer_delay_ms: float,
    duration: float,
    max_packets: int,
    recv_buf: int,
    port: int,
    output_dir: Path,
) -> dict:
    cfg = PipelineConfig(
        port=port,
        frame_capacity=capacity,
        overflow_policy=policy,
        recv_buf_bytes=recv_buf,
        refresh_hz=refresh_hz,
        consumer_delay_ms=consumer_delay_ms,
        archive_audio=False,           # keep file I/O off the parse path
        output_dir=output_dir,
        enable_display=False,          # renderer-free: isolate the policy effect
    )
    spec = ReplaySpec(
        port=port,
        duration_s=duration,
        rate_mbps=rate,
        malformed_ratio=0.0,
        n_bins=cfg.n_bins,
        waveform_len=256,
        audio_len=512,
        sample_rate=cfg.sample_rate,
        dataset=dataset,
        loop=True,
        max_packets=max_packets,
    )
    pipeline = StreamPipeline(cfg).start()
    ctx = mp.get_context("spawn")
    results = ctx.Queue()
    sender = ctx.Process(
        target=_sender_worker, args=(spec, results),
        name="ablation-source", daemon=True,
    )
    sender.start()
    try:
        stats = results.get(timeout=duration + 40)
    except queue.Empty:
        stats = {"error": "sender produced no statistics"}
    if sender.is_alive():
        sender.join(timeout=3)
        if sender.is_alive():
            sender.terminate()
    pipeline.stop()

    report = pipeline.report()
    flow = report["flow"]
    sent = int(stats.get("sent_packets", 0))
    received = flow["udp_received"]
    parsed = flow["parsed"]
    e2e = report["end_to_end_latency_ms"]
    stale = report["staleness_ms"]
    source = report["buffers"]["source"]
    frame = report["buffers"]["frame"]
    # The display buffer only ever sees SPECTRUM and WAVEFORM packets; audio is
    # archived by the parser.  Closing this chain is what makes the numbers
    # auditable: nothing may vanish between parse and display.
    frame_in = flow["parsed_by_type"]["spectrum"] + flow["parsed_by_type"]["waveform"]
    frame_out = (
        flow["delivered_to_consumer"]
        + frame["dropped"]
        + flow["dropped_at_shutdown"]
        + frame["occupancy"]
    )
    conservation_closed = (frame_in == frame_out)

    return {
        "dataset": dataset,
        "policy": policy,
        "target_rate_mbps": rate,
        "display_refresh_hz": refresh_hz,
        "consumer_delay_ms": consumer_delay_ms,
        "frame_capacity": capacity,
        "recv_buf_bytes": recv_buf,
        "sender_error": stats.get("error"),
        "conservation": {
            "sent": sent,
            "udp_received": received,
            "parsed": parsed,
            "delivered_to_consumer": flow["delivered_to_consumer"],
            "udp_delivery_ratio": _ratio(received, sent),
            "parse_yield": flow["parse_yield"],
            "end_to_end_yield": _ratio(parsed, sent),
            "sequence_gaps": flow["sequence_gaps"],
            "duplicate_or_reorder": flow["duplicate_or_reorder"],
            "frame_buffer_in": frame_in,
            "frame_buffer_out": frame_out,
            "conservation_closed": conservation_closed,
        },
        "discard": {
            "source_dropped": source["dropped"],
            "source_block_timeout": source["dropped_block_timeout"],
            "source_hard_cap": source["dropped_hard_cap"],
            "source_occupancy": source["occupancy"],
            "frame_dropped": frame["dropped"],
            "frame_block_timeout": frame["dropped_block_timeout"],
            "frame_hard_cap": frame["dropped_hard_cap"],
            "frame_occupancy": frame["occupancy"],
            # How stale was the data that got thrown away?  An age near the
            # end-to-end latency means stale samples were discarded; an age
            # near zero means fresh data was.
            "dropped_age_ms_p50": frame["dropped_age_ms"]["p50_ms"],
            "dropped_age_ms_p95": frame["dropped_age_ms"]["p95_ms"],
        },
        "latency": {
            "e2e_p50_ms": e2e["p50_ms"],
            "e2e_p95_ms": e2e["p95_ms"],
            "ingest_p95_ms": report["ingest_latency_ms"]["p95_ms"],
            "staleness_p50_ms": stale["p50_ms"],
            "staleness_p95_ms": stale["p95_ms"],
            "staleness_max_ms": stale["max_ms"],
        },
        "throughput_mbps": report["throughput_mbps"],
        "rendered_frames": report["rendered_frames"],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Overflow-policy ablation")
    parser.add_argument("--dataset", default="mitdb", choices=("all", *sorted(REGISTRY)))
    parser.add_argument("--policies", default="drop_oldest,drop_newest,block,unbounded",
                        help="comma-separated OverflowPolicy values")
    parser.add_argument("--rates", default="2,4", help="comma-separated target Mbit/s")
    parser.add_argument("--capacity", type=int, default=32,
                        help="display-buffer capacity (small on purpose)")
    parser.add_argument("--refresh-hz", type=float, default=30.0,
                        help="display timer; the consumer is also event-driven "
                             "so this alone does not create over-subscription")
    parser.add_argument("--consumer-delay-ms", type=float, default=20.0,
                        help="injected per-tick render cost; this is what makes "
                             "the buffer overflow the way a real front-end does")
    parser.add_argument("--duration", type=float, default=30.0,
                        help="safety cap; with --packets the run ends on packet count")
    parser.add_argument("--packets", type=int, default=3000,
                        help="fixed datagram count per run so every policy sees "
                             "the same injected load (0 = pace by duration)")
    parser.add_argument("--recv-buf", type=int, default=0,
                        help="SO_RCVBUF in bytes; small values shift loss upstream")
    parser.add_argument("--port", type=int, default=19300)
    parser.add_argument("--output", default="output/policy_ablation")
    args = parser.parse_args()

    policies = [OverflowPolicy(p.strip()) for p in args.policies.split(",") if p.strip()]
    rates = [float(r) for r in args.rates.split(",") if r.strip()]
    keys = sorted(REGISTRY) if args.dataset == "all" else [args.dataset]
    root = Path(args.output)
    root.mkdir(parents=True, exist_ok=True)

    all_runs = []
    port = args.port
    for key in keys:
        rec = load_dataset(key)
        spec = REGISTRY[key]
        print(f"\n{'=' * 88}")
        print("Dataset :", spec.name, "| record", rec.record, "| domain", spec.domain)
        print("License :", spec.license, "| DOI", spec.doi)
        print(f"Overload: consumer {args.consumer_delay_ms:g} ms/tick x "
              f"capacity {args.capacity} = "
              f"{1000.0 / max(1e-3, args.consumer_delay_ms) * args.capacity:.0f} "
              f"packets/s consumer budget")
        for policy in policies:
            for rate in rates:
                out = root / key / policy.value / f"run_{rate:g}mbps"
                out.mkdir(parents=True, exist_ok=True)
                run = _one_run(key, policy.value, rate, args.capacity, args.refresh_hz,
                               args.consumer_delay_ms, args.duration, args.packets,
                               args.recv_buf, port, out)
                port += 1
                all_runs.append(run)
                if run["sender_error"]:
                    print(f"  {policy.value:<12} {rate:>4g} Mbit/s | SENDER ERROR: "
                          f"{run['sender_error']}")
                    continue
                c, d, lat = run["conservation"], run["discard"], run["latency"]
                print(
                    f"  {policy.value:<12} {rate:>4g} Mbit/s | "
                    f"udp {c['udp_delivery_ratio']:.3f} "
                    f"({c['udp_received']:>5d}/{c['sent']:<5d}) | "
                    f"drop src {d['source_dropped']:>5d} frm {d['frame_dropped']:>5d} | "
                    f"dropped-age p95 {d['dropped_age_ms_p95']:>7.1f} ms | "
                    f"e2e p95 {lat['e2e_p95_ms']:>7.1f} | "
                    f"stale p95 {lat['staleness_p95_ms']:>7.1f} ms | "
                    f"chain {'ok' if c['conservation_closed'] else 'BROKEN'}"
                )

    payload = {
        "workload": {
            "description": (
                "Verified open recording replayed over the same UDP transport; "
                "no synthetic signal values. The display consumer is deliberately "
                "over-subscribed so the buffer overflows."
            ),
            "seconds_per_run": args.duration,
            "packets_per_run": args.packets,
            "frame_capacity": args.capacity,
            "display_refresh_hz": args.refresh_hz,
            "consumer_delay_ms": args.consumer_delay_ms,
            "consumer_budget_packets_per_s": round(
                1000.0 / max(1e-3, args.consumer_delay_ms) * args.capacity, 1
            ),
            "audio_archiving": (
                "disabled: WAV writing puts file I/O on the parse thread and "
                "would conflate display-buffer behaviour with disk latency"
            ),
            "recv_buf_bytes": args.recv_buf,
            "note": (
                "udp_delivery_ratio is received/sent and therefore includes kernel "
                "and transport loss; parse_yield is parsed/received and isolates "
                "the parser. Do not report either as a lossless guarantee."
            ),
        },
        "datasets": {k: REGISTRY[k].public for k in keys},
        "runs": all_runs,
    }
    path = root / "policy_ablation.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nReport: {path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
