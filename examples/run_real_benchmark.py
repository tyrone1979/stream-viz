"""Measure StreamViz against registered REAL open datasets (cross-domain).

    python tools/fetch_dataset.py                     # once: download + verify
    python examples/run_real_benchmark.py --dataset mitdb
    python examples/run_real_benchmark.py --dataset all   # both domains

Every registered dataset is a verified, openly licensed recording from a
different scientific domain (biomedical ECG and gravitational-wave strain).
Recorded samples are packed into the same UDP protocol as the synthetic demo,
so the ingest/display/replay code is identical for both; only the adapter
in ``src/datasets.py`` changes.  See that module for provenance, checksums
and the exact preprocessing of each dataset.

Outputs ``<output>/real_benchmark.json`` (or ``<output>/<key>/...`` when
``--dataset all``) plus four-view PNG snapshots for one rate per dataset.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import queue
import sys
from pathlib import Path

import numpy as np

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.datasets import FRAME_SAMPLES, REGISTRY, load_dataset  # noqa: E402
from src.open_dataset import ReplaySpec, _frame_payloads, send_record  # noqa: E402
from src.pipeline import PipelineConfig, StreamPipeline  # noqa: E402


def _payload_authenticity(rec) -> dict:
    """Show that the emitted payloads equal the recorded samples, not a model."""
    spec = ReplaySpec()
    frame = 137
    n_frames = rec.n_samples // FRAME_SAMPLES
    start = (frame % n_frames) * FRAME_SAMPLES
    packets = _frame_payloads(rec, spec, frame)
    spectrum, waveform, audio_a, audio_b = (p[1] for p in packets)
    source = rec.signals[start : start + FRAME_SAMPLES, frame % rec.n_channels]
    segment = source[:512].copy()
    segment -= segment.mean()
    magnitude = (
        np.abs(np.fft.rfft(segment * np.hanning(512).astype(np.float32)))
        / 512.0
        * 2.0
    )
    magnitude[0] = 0.0
    return {
        "waveform_equals_recorded_samples": bool(
            np.allclose(waveform, source[:256].astype(np.float32), atol=1e-6)
        ),
        "spectrum_is_fft_of_recorded_window": bool(
            np.allclose(
                spectrum,
                magnitude[:128] / rec.spectrum_reference,
                atol=1e-5,
            )
        ),
        "audio_is_audification_of_recorded_samples": bool(
            np.allclose(
                audio_a.astype(np.float32),
                np.clip(source[:512] * 25_000, -32768, 32767).astype(np.float32),
                atol=1.0,
            )
        ),
        "recorded_window_std": round(float(source.std()), 6),
        "recorded_window_is_not_constant": bool(source.std() > 1e-4),
    }


def _sender_worker(spec, result_queue) -> None:
    """Run the data source in its own process, like a real acquisition device.

    Python's GIL would otherwise make the sender compete with the receiver,
    parser and display threads for the same interpreter, capping the sender
    far below its standalone capability and conflating two different limits.
    """
    try:
        result_queue.put(send_record(spec))
    except Exception as exc:  # noqa: BLE001 - report the child failure verbatim
        result_queue.put({"error": f"{type(exc).__name__}: {exc}"})


def _one_run(rec, dataset_key: str, rate: float, capacity: int, duration: float,
             port: int, output_dir: Path, figures: bool, loop: bool) -> dict:
    cfg = PipelineConfig(
        port=port,
        frame_capacity=capacity,
        output_dir=output_dir / f"run_{rate:g}mbps",
        enable_display=figures,
        snapshot_every=15,
        n_channels=rec.n_channels,
    )
    spec = ReplaySpec(
        port=port,
        duration_s=duration,
        rate_mbps=rate,
        malformed_ratio=0.0,  # no artificially corrupted packets in this run
        n_bins=cfg.n_bins,
        waveform_len=256,
        audio_len=512,
        sample_rate=cfg.sample_rate,
        dataset=dataset_key,
        loop=loop,
    )
    pipeline = StreamPipeline(cfg).start()
    ctx = mp.get_context("spawn")
    results = ctx.Queue()
    sender = ctx.Process(
        target=_sender_worker, args=(spec, results), name="real-data-source",
        daemon=True,
    )
    sender.start()
    try:
        stats = results.get(timeout=duration + 30)
    except queue.Empty:
        stats = {"error": "sender produced no statistics"}
    if sender.is_alive():
        sender.join(timeout=3)
        if sender.is_alive():
            sender.terminate()
    pipeline.stop()

    report = pipeline.report()
    received = report["counters"].get("udp_datagrams", 0)
    parsed = report["counters"].get("parsed_packets", 0)
    return {
        "target_rate_mbps": rate,
        "frame_capacity": capacity,
        "sender": stats,
        "measured": {
            "throughput_mbps": report["throughput_mbps"],
            "datagrams_received": received,
            "packets_parsed": parsed,
            "delivery_ratio": round(parsed / received, 4) if received else 0.0,
            "ingest_latency_ms": report["ingest_latency_ms"],
            "end_to_end_latency_ms": report["end_to_end_latency_ms"],
            "fifo_overflow_dropped": report["fifo_overflow_dropped"],
            "audio_segments": report["audio_segments"],
            "rendered_frames": report["rendered_frames"],
            "snapshots": report["snapshots"],
        },
    }


def _measure(key: str, args, output_dir: Path) -> dict:
    rec = load_dataset(key)
    spec = REGISTRY[key]
    rates = [float(x) for x in args.rates.split(",") if x.strip()]

    print(f"\n{'=' * 72}")
    print("Dataset :", spec.name, "| record", rec.record, "| domain", spec.domain)
    print("License :", spec.license, "| DOI", spec.doi)
    print("Signal  :", rec.sig_names, rec.fs, "Hz,", rec.n_samples,
          "samples/ch,", round(rec.n_samples / rec.fs, 2), "s")
    print("Units   :", rec.physical_units)
    print("Prep    :", rec.preprocessing)
    for name, digest in rec.checksums.items():
        print(f"SHA256  : {name} {digest}")
    print()

    runs = []
    for i, rate in enumerate(rates):
        figures = (not args.no_figures) and abs(rate - args.figure_rate) < 1e-9
        result = _one_run(rec, key, rate, args.capacity, args.duration,
                          args.port + i, output_dir, figures, not args.no_loop)
        runs.append(result)
        m = result["measured"]
        e2e = m["end_to_end_latency_ms"]
        if result["sender"].get("error"):
            print(f"{rate:>5g} Mbit/s | SENDER ERROR: {result['sender']['error']}")
            continue
        print(f"{rate:>5g} Mbit/s | throughput {m['throughput_mbps']:>6.2f} | "
              f"parsed {m['packets_parsed']:>5d}/{m['datagrams_received']:<5d} | "
              f"e2e p50/p95 {e2e.get('p50_ms', 0):>6.1f}/{e2e.get('p95_ms', 0):>6.1f} ms | "
              f"FIFO drops {m['fifo_overflow_dropped']:>4d} | "
              f"real signal {result['sender'].get('distinct_signal_seconds', 0):>6.2f} s"
              + ("  [figures]" if figures else ""))

    report = {
        "dataset": rec.provenance,
        "workload": {
            "description": (
                "Recorded samples packed into SPECTRUM/WAVEFORM/AUDIO UDP "
                "datagrams; no synthetic signal values are used."
            ),
            "sender": (
                "Separate OS process (spawn), representing an external "
                "acquisition device; keeps the sender out of the GIL shared "
                "with the receiver, parser and display threads."
            ),
            "looped": not args.no_loop,
            "malformed_injection": 0.0,
            "seconds_per_run": args.duration,
            "note": (
                "The same verified record is looped to sustain each target "
                "rate; 'distinct_signal_seconds' reports how much of the "
                "recording was actually consumed."
            ),
        },
        "payload_authenticity": _payload_authenticity(rec),
        "runs": runs,
    }
    path = output_dir / "real_benchmark.json"
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print("Authenticity:", report["payload_authenticity"])
    print("Report:", path.resolve())
    return {"key": key, "domain": spec.domain, "doi": spec.doi,
            "report": str(path), "runs": runs}


def main() -> int:
    parser = argparse.ArgumentParser(description="Cross-domain real open-data benchmark")
    parser.add_argument("--dataset", default="mitdb",
                        choices=("all", *sorted(REGISTRY)),
                        help="registered dataset to measure, or 'all'")
    parser.add_argument("--rates", default="1,2,4,8",
                        help="comma-separated target Mbit/s")
    parser.add_argument("--capacity", type=int, default=64)
    parser.add_argument("--duration", type=float, default=6.0,
                        help="seconds per run")
    parser.add_argument("--port", type=int, default=19100)
    parser.add_argument("--figure-rate", type=float, default=4.0,
                        help="which run also renders PNG views")
    parser.add_argument("--no-figures", action="store_true")
    parser.add_argument("--no-loop", action="store_true",
                        help="use each recorded sample at most once")
    parser.add_argument("--output", default="output/real_benchmark")
    args = parser.parse_args()

    keys = sorted(REGISTRY) if args.dataset == "all" else [args.dataset]
    root = Path(args.output)
    root.mkdir(parents=True, exist_ok=True)
    summaries = []
    for key in keys:
        out = root / key if len(keys) > 1 else root
        out.mkdir(parents=True, exist_ok=True)
        summaries.append(_measure(key, args, out))

    if len(keys) > 1:
        summary_path = root / "cross_domain_summary.json"
        summary_path.write_text(
            json.dumps({
                "domains": keys,
                "datasets": [REGISTRY[k].public for k in keys],
                "summaries": summaries,
            }, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        print("\nCross-domain summary:", summary_path.resolve())
        print("Cite every dataset above in any figure caption built from it.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
