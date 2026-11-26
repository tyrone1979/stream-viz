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


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    if not ordered:
        return 0.0
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return round(float(ordered[mid]), 3)
    return round((ordered[mid - 1] + ordered[mid]) / 2.0, 3)


# Metrics whose run-to-run spread matters: a single unlucky scheduling hiccup
# can move a p95 by an order of magnitude, so the summary carries both the
# median and the observed range.
_SPREAD_FIELDS = (
    ("latency", "e2e_p95_ms"),
    ("latency", "staleness_p95_ms"),
    ("discard", "frame_dropped"),
    ("discard", "dropped_age_ms_p95"),
)


def _aggregate(runs: list[dict]) -> dict:
    """Median-of-N summary for one (dataset, policy, rate) cell."""
    first = runs[0]
    cell = {
        "dataset": first["dataset"],
        "policy": first["policy"],
        "target_rate_mbps": first["target_rate_mbps"],
        "consumer_delay_ms": first["consumer_delay_ms"],
        "repeats": len(runs),
        "conservation_closed_all": all(
            r["conservation"]["conservation_closed"] for r in runs
        ),
        "sender_errors": sum(1 for r in runs if r["sender_error"]),
    }
    for section, metric in _SPREAD_FIELDS:
        values = [float(r[section][metric]) for r in runs]
        cell[f"{section}.{metric}"] = {
            "median": _median(values), "min": round(min(values), 3),
            "max": round(max(values), 3),
        }
    for section, keys in (
        ("conservation", ("sent", "udp_received", "parsed", "delivered_to_consumer",
                          "sequence_gaps", "duplicate_or_reorder")),
        ("discard", ("source_dropped", "frame_occupancy",
                     "frame_block_timeout", "frame_hard_cap")),
        ("latency", ("e2e_p50_ms", "staleness_p50_ms", "ingest_p95_ms")),
    ):
        for key in keys:
            cell[f"{section}.{key}"] = _median([float(r[section][key]) for r in runs])
    ratios = [r["conservation"]["udp_delivery_ratio"] for r in runs]
    cell["conservation.udp_delivery_ratio"] = {
        "median": _median(ratios), "min": min(ratios), "max": max(ratios),
    }
    cell["frame.packets_per_frame"] = _median(
        [float(r["frame"]["packets_per_frame"]) for r in runs]
    )
    cell["frame.mean_interval_ms"] = _median(
        [float(r["frame"]["mean_interval_ms"]) for r in runs]
    )
    cell["throughput_mbps"] = _median([float(r["throughput_mbps"]) for r in runs])
    return cell


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
    rounds = report["rendered_frames"]
    duration_s = report["duration_s"]

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
        "rendered_frames": rounds,
        # A policy that keeps every packet does not get that for free: the
        # work per frame grows with the backlog it accumulated.  For a real
        # renderer this is the cost that eventually dominates.
        "frame": {
            "duration_s": duration_s,
            "rounds": rounds,
            "packets_per_frame": round(
                flow["delivered_to_consumer"] / rounds, 2
            ) if rounds else 0.0,
            "mean_interval_ms": round(
                duration_s * 1000.0 / rounds, 2
            ) if rounds else 0.0,
        },
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
    parser.add_argument("--consumer-delays", default="",
                        help="comma-separated render budgets to sweep, e.g. "
                             "20,40,80; overrides --consumer-delay-ms")
    parser.add_argument("--duration", type=float, default=30.0,
                        help="safety cap; with --packets the run ends on packet count")
    parser.add_argument("--packets", type=int, default=3000,
                        help="fixed datagram count per run so every policy sees "
                             "the same injected load (0 = pace by duration)")
    parser.add_argument("--repeat", type=int, default=3,
                        help="runs per cell; the summary reports the median and "
                             "the observed range, because a single run is a "
                             "scheduling sample, not a measurement")
    parser.add_argument("--recv-buf", type=int, default=0,
                        help="SO_RCVBUF in bytes; small values shift loss upstream")
    parser.add_argument("--port", type=int, default=19300)
    parser.add_argument("--output", default="output/policy_ablation")
    args = parser.parse_args()

    policies = [OverflowPolicy(p.strip()) for p in args.policies.split(",") if p.strip()]
    rates = [float(r) for r in args.rates.split(",") if r.strip()]
    delays = [float(d) for d in args.consumer_delays.split(",") if d.strip()] \
        if args.consumer_delays else [args.consumer_delay_ms]
    keys = sorted(REGISTRY) if args.dataset == "all" else [args.dataset]
    repeats = max(1, args.repeat)
    root = Path(args.output)
    root.mkdir(parents=True, exist_ok=True)

    all_runs: list[dict] = []
    summary: list[dict] = []
    port = args.port
    for key in keys:
        rec = load_dataset(key)
        spec = REGISTRY[key]
        print(f"\n{'=' * 88}")
        print("Dataset :", spec.name, "| record", rec.record, "| domain", spec.domain)
        print("License :", spec.license, "| DOI", spec.doi)
        print(f"Sweep   : render budget {delays} ms/tick x capacity "
              f"{args.capacity} | repeats {repeats}")
        for policy in policies:
            for rate in rates:
                for delay in delays:
                    cell_runs = []
                    for rep in range(repeats):
                        out = (root / key / policy.value
                               / f"run_{rate:g}mbps_d{delay:g}ms" / f"rep{rep + 1}")
                        out.mkdir(parents=True, exist_ok=True)
                        run = _one_run(key, policy.value, rate, args.capacity,
                                       args.refresh_hz, delay,
                                       args.duration, args.packets, args.recv_buf,
                                       port, out)
                        port += 1
                        run["repeat_index"] = rep + 1
                        all_runs.append(run)
                        cell_runs.append(run)
                        if run["sender_error"]:
                            print(f"  {policy.value:<12} {rate:>4g}Mbit/s "
                                  f"d{delay:g}ms rep{rep + 1} | SENDER ERROR: "
                                  f"{run['sender_error']}")
                            continue
                        if repeats > 1:
                            lat = run["latency"]
                            print(f"  {policy.value:<12} {rate:>4g}Mbit/s "
                                  f"d{delay:>4g}ms rep{rep + 1} | stale p95 "
                                  f"{lat['staleness_p95_ms']:>7.1f} ms | frm drop "
                                  f"{run['discard']['frame_dropped']:>5d}")
                    cell = _aggregate(cell_runs)
                    summary.append(cell)
                    if repeats == 1:
                        c, d, lat = cell_runs[0]["conservation"], cell_runs[0]["discard"], cell_runs[0]["latency"]
                        print(
                            f"  {policy.value:<12} {rate:>4g}Mbit/s d{delay:>4g}ms | "
                            f"udp {c['udp_delivery_ratio']:.3f} "
                            f"({c['udp_received']:>5d}/{c['sent']:<5d}) | "
                            f"drop src {d['source_dropped']:>5d} frm {d['frame_dropped']:>5d} | "
                            f"dropped-age p95 {d['dropped_age_ms_p95']:>7.1f} ms | "
                            f"e2e p95 {lat['e2e_p95_ms']:>7.1f} | "
                            f"stale p95 {lat['staleness_p95_ms']:>7.1f} ms | "
                            f"chain {'ok' if c['conservation_closed'] else 'BROKEN'}"
                        )
                    else:
                        stale = cell["latency.staleness_p95_ms"]
                        dropped = cell["discard.frame_dropped"]
                        print(
                            f"  {policy.value:<12} {rate:>4g}Mbit/s d{delay:>4g}ms | "
                            f"median of {repeats} | stale p95 {stale['median']:>7.1f} ms "
                            f"[{stale['min']:.1f}-{stale['max']:.1f}] | "
                            f"pkt/frame {cell['frame.packets_per_frame']:>5.1f} | "
                            f"frm drop {dropped['median']:>5.0f} "
                            f"[{dropped['min']:.0f}-{dropped['max']:.0f}] | "
                            f"chain {'ok' if cell['conservation_closed_all'] else 'BROKEN'}"
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
            "repeats_per_cell": repeats,
            "aggregation": (
                "summary[] holds the median of the repeats; each cell also carries "
                "the observed min/max so the host-dependent spread stays visible"
            ),
            "frame_capacity": args.capacity,
            "display_refresh_hz": args.refresh_hz,
            "consumer_delay_ms": args.consumer_delay_ms,
            "consumer_delays_ms": delays,
            "consumer_budget_packets_per_s": {
                f"{d:g}ms": round(1000.0 / max(1e-3, d) * args.capacity, 1)
                for d in delays
            },
            "per_packet_fold_cost": (
                "measured, not injected: the consumer folds every accepted "
                "packet into a real ViewState (~0.2 ms/packet on the reference "
                "machine), so draining a large backlog costs real time"
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
        "summary": summary,
        "runs": all_runs,
    }
    path = root / "policy_ablation.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nReport: {path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
