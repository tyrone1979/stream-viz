"""Extreme-condition replay of a multi-sensor acoustic array.

    python examples/run_extreme_scenario.py --policy drop_oldest --capacity 16
    python examples/run_extreme_scenario.py --policy block --capacity 16

The load is bursty on purpose: a quiet stretch, a broadband burst at
``--burst-gain`` times the base rate, then a short outage.  That is the regime
where a display pipeline's admission policy actually decides what the operator
sees, and it is the regime a steady-rate benchmark never reaches.

The payloads are synthetic (see :mod:`src.scenarios`): the field recordings
that would make a faithful replay possible are access-restricted and cannot be
redistributed.  The report states this in ``workload.data_status`` so a number
produced by this script can never be mistaken for a field measurement.
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
from src.pipeline import PipelineConfig, StreamPipeline  # noqa: E402
from src.scenarios import ArrayScenarioSpec, send_scenario  # noqa: E402


def _sender_worker(spec: ArrayScenarioSpec, result_queue) -> None:
    try:
        result_queue.put(send_scenario(spec))
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


# A single scheduler hiccup can move a p95 by an order of magnitude on a shared
# machine, so every reported latency carries the observed range next to the
# median instead of pretending one run is the answer.
_SPREAD_FIELDS = (
    ("latency", "e2e_p95_ms"),
    ("latency", "staleness_p95_ms"),
    ("latency", "staleness_max_ms"),
    ("discard", "frame_dropped"),
    ("discard", "dropped_age_ms_p95"),
)


def _aggregate(runs: list[dict]) -> dict:
    """Median-of-N summary for one (policy) cell of the scenario sweep."""
    first = runs[0]
    cell = {
        "policy": first["policy"],
        "repeats": len(runs),
        "conservation_closed_all": all(
            r["conservation"]["conservation_closed"] for r in runs
        ),
        "sender_errors": sum(1 for r in runs if r["sender_error"]),
    }
    for section, metric in _SPREAD_FIELDS:
        values = [float(r[section][metric]) for r in runs]
        cell[f"{section}.{metric}"] = {
            "median": _median(values),
            "min": round(min(values), 3),
            "max": round(max(values), 3),
        }
    for section, keys in (
        ("conservation", ("sent", "udp_received", "parsed",
                          "delivered_to_consumer", "sequence_gaps")),
        ("discard", ("source_dropped", "frame_occupancy",
                     "frame_block_timeout", "frame_hard_cap")),
        ("latency", ("e2e_p50_ms", "staleness_p50_ms", "ingest_p95_ms")),
        ("frame", ("packets_per_frame",)),
    ):
        for key in keys:
            cell[f"{section}.{key}"] = _median(
                [float(r[section][key]) for r in runs]
            )
    ratios = [r["conservation"]["udp_delivery_ratio"] for r in runs]
    cell["conservation.udp_delivery_ratio"] = {
        "median": _median(ratios), "min": min(ratios), "max": max(ratios),
    }
    cell["throughput_mbps"] = _median([float(r["throughput_mbps"]) for r in runs])
    return cell


def _one_run(args, policy: str, output_dir: Path, port: int) -> dict:
    cfg = PipelineConfig(
        port=port,
        frame_capacity=args.capacity,
        overflow_policy=policy,
        refresh_hz=args.refresh,
        archive_audio=args.audio,
        output_dir=output_dir,
        enable_display=True,
        snapshot_every=args.snapshot_every,
    )
    spec = ArrayScenarioSpec(
        port=port,
        duration_s=args.duration,
        base_rate_mbps=args.rate,
        n_sensors=args.sensors,
        cycle_s=args.cycle,
        burst_s=args.burst,
        outage_s=args.outage,
        burst_gain=args.burst_gain,
        malformed_ratio=args.malformed,
        n_bins=cfg.n_bins,
    )
    pipeline = StreamPipeline(cfg).start()
    ctx = mp.get_context("spawn")
    results = ctx.Queue()
    sender = ctx.Process(target=_sender_worker, args=(spec, results),
                         name="scenario-source", daemon=True)
    sender.start()
    try:
        stats = results.get(timeout=args.duration + 60)
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
    frame = report["buffers"]["frame"]
    source = report["buffers"]["source"]
    frame_in = flow["parsed_by_type"]["spectrum"] + flow["parsed_by_type"]["waveform"]
    frame_out = (flow["delivered_to_consumer"] + frame["dropped"]
                 + flow["dropped_at_shutdown"] + frame["occupancy"])
    rounds = report["rendered_frames"]
    stale = report["staleness_ms"]
    e2e = report["end_to_end_latency_ms"]

    return {
        "policy": policy,
        "workload": {
            "data_status": stats.get("provenance"),
            "note": (
                "Synthetic burst/outage scenario standing in for a field "
                "deployment whose recordings are access-restricted. Not a "
                "field measurement; not a model of any specific sensor."
            ),
            "profile": stats.get("profile"),
            "peak_packets_per_s": stats.get("peak_packets_per_s"),
            "audio_archiving": bool(args.audio),
        },
        "conservation": {
            "sent": sent,
            "udp_received": received,
            "parsed": parsed,
            "delivered_to_consumer": flow["delivered_to_consumer"],
            "udp_delivery_ratio": _ratio(received, sent),
            "parse_yield": flow["parse_yield"],
            "sequence_gaps": flow["sequence_gaps"],
            "duplicate_or_reorder": flow["duplicate_or_reorder"],
            "frame_buffer_in": frame_in,
            "frame_buffer_out": frame_out,
            "conservation_closed": frame_in == frame_out,
        },
        "discard": {
            "source_dropped": source["dropped"],
            "frame_dropped": frame["dropped"],
            "frame_block_timeout": frame["dropped_block_timeout"],
            "frame_hard_cap": frame["dropped_hard_cap"],
            "dropped_age_ms_p50": frame["dropped_age_ms"]["p50_ms"],
            "dropped_age_ms_p95": frame["dropped_age_ms"]["p95_ms"],
            "frame_occupancy": frame["occupancy"],
        },
        "latency": {
            "ingest_p95_ms": report["ingest_latency_ms"]["p95_ms"],
            "e2e_p50_ms": e2e["p50_ms"],
            "e2e_p95_ms": e2e["p95_ms"],
            "staleness_p50_ms": stale["p50_ms"],
            "staleness_p95_ms": stale["p95_ms"],
            "staleness_max_ms": stale["max_ms"],
        },
        "frame": {
            "rounds": rounds,
            "packets_per_frame": round(
                flow["delivered_to_consumer"] / rounds, 2) if rounds else 0.0,
            "mean_interval_ms": round(
                args.duration * 1000.0 / rounds, 2) if rounds else 0.0,
        },
        "throughput_mbps": report["throughput_mbps"],
        "audio_segments": report["audio_segments"],
        "figures": [str(p) for p in report["snapshots"]],
        "sender_error": stats.get("error"),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extreme-condition multi-sensor acoustic scenario"
    )
    parser.add_argument("--policy", default="drop_oldest",
                        choices=[p.value for p in OverflowPolicy])
    parser.add_argument("--policies", default="",
                        help="comma-separated policies to compare in one run")
    parser.add_argument("--capacity", type=int, default=16)
    parser.add_argument("--refresh", type=float, default=20.0)
    parser.add_argument("--rate", type=float, default=3.0,
                        help="base rate in Mbit/s; bursts multiply this")
    parser.add_argument("--sensors", type=int, default=6)
    parser.add_argument("--duration", type=float, default=12.0)
    parser.add_argument("--cycle", type=float, default=3.0)
    parser.add_argument("--burst", type=float, default=0.9)
    parser.add_argument("--outage", type=float, default=0.4)
    parser.add_argument("--burst-gain", type=float, default=3.0)
    parser.add_argument("--malformed", type=float, default=0.0)
    parser.add_argument("--audio", action="store_true",
                        help="archive WAV segments (adds file I/O to the parse "
                             "path, so timing numbers are no longer clean)")
    parser.add_argument("--snapshot-every", type=int, default=25)
    parser.add_argument("--repeat", type=int, default=3,
                        help="runs per policy; the summary reports the median "
                             "and observed range (single runs are not enough on "
                             "a shared machine)")
    parser.add_argument("--port", type=int, default=19500)
    parser.add_argument("--output", default="output/scenario")
    args = parser.parse_args()

    policies = ([p.strip() for p in args.policies.split(",") if p.strip()]
                or [args.policy])
    repeats = max(1, args.repeat)
    root = Path(args.output)
    root.mkdir(parents=True, exist_ok=True)

    runs = []
    summary = []
    port = args.port
    print(f"Scenario: {args.sensors} sensors, base {args.rate} Mbit/s, "
          f"burst x{args.burst_gain} for {args.burst}s, outage {args.outage}s "
          f"every {args.cycle}s | buffer {args.capacity} | synthetic payloads | "
          f"repeats {repeats}")
    for policy in policies:
        cell_runs = []
        for rep in range(repeats):
            out = root / policy / f"rep{rep + 1}"
            out.mkdir(parents=True, exist_ok=True)
            run = _one_run(args, policy, out, port)
            port += 1
            run["repeat_index"] = rep + 1
            runs.append(run)
            cell_runs.append(run)
            if run["sender_error"]:
                print(f"  {policy:<12} rep{rep + 1} SENDER ERROR: "
                      f"{run['sender_error']}")
                continue
            lat = run["latency"]
            if repeats > 1:
                print(f"  {policy:<12} rep{rep + 1} stale p95 "
                      f"{lat['staleness_p95_ms']:>8.1f} ms | max "
                      f"{lat['staleness_max_ms']:>8.1f} ms | frm drop "
                      f"{run['discard']['frame_dropped']:>5d}")
        cell = _aggregate(cell_runs)
        summary.append(cell)
        if repeats == 1:
            c, d, lat = cell_runs[0]["conservation"], cell_runs[0]["discard"], cell_runs[0]["latency"]
            print(
                f"  {policy:<12} sent {c['sent']:>5d} recv {c['udp_received']:>5d} "
                f"parsed {c['parsed']:>5d} | drop src {d['source_dropped']:>4d} "
                f"frm {d['frame_dropped']:>4d} | stale p50/p95/max "
                f"{lat['staleness_p50_ms']:>7.1f}/{lat['staleness_p95_ms']:>7.1f}/"
                f"{lat['staleness_max_ms']:>7.1f} ms | pkt/frame "
                f"{cell_runs[0]['frame']['packets_per_frame']:>5.1f} | "
                f"chain {'ok' if c['conservation_closed'] else 'BROKEN'}"
            )
        else:
            stale = cell["latency.staleness_p95_ms"]
            smax = cell["latency.staleness_max_ms"]
            dropped = cell["discard.frame_dropped"]
            print(
                f"  {policy:<12} median of {repeats} | stale p95 "
                f"{stale['median']:>8.1f} ms [{stale['min']:.1f}-{stale['max']:.1f}] "
                f"| max {smax['median']:>8.1f} ms | frm drop "
                f"{dropped['median']:>5.0f} "
                f"[{dropped['min']:.0f}-{dropped['max']:.0f}] | "
                f"pkt/frame {cell['frame.packets_per_frame']:>5.1f} | "
                f"chain {'ok' if cell['conservation_closed_all'] else 'BROKEN'}"
            )

    payload = {
        "scenario": {
            "description": (
                "Multi-sensor acoustic array replayed with a burst/outage load "
                "profile. Payloads are synthetic; the field recordings are "
                "access-restricted and are not redistributed or reconstructed."
            ),
            "data_status": "synthetic",
            "sensors": args.sensors,
            "base_rate_mbps": args.rate,
            "cycle_s": args.cycle,
            "burst_s": args.burst,
            "burst_gain": args.burst_gain,
            "outage_s": args.outage,
            "frame_capacity": args.capacity,
            "display_refresh_hz": args.refresh,
            "seconds_per_run": args.duration,
            "repeats": repeats,
            "note": (
                "A single run on a shared machine can move a p95 by an order of "
                "magnitude; use summary[] (median + observed range) rather than "
                "any single row of runs[]."
            ),
        },
        "runs": runs,
        "summary": summary,
    }
    path = root / "extreme_scenario.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nReport: {path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
