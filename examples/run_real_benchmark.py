"""Measure StreamViz against a REAL open dataset, not synthetic traffic.

    python tools/fetch_dataset.py            # once: download + verify
    python examples/run_real_benchmark.py    # measure + figure

The workload is record 100 of the MIT-BIH Arrhythmia Database (PhysioNet,
ODC-By 1.0, DOI 10.13026/C2F305): a 30-minute two-lead ECG at 360 Hz.  The
recorded samples are packed into the same UDP protocol as the synthetic demo,
so every packet in this benchmark is derived from measured human physiology.
See ``src/open_dataset.py`` for the provenance, checksums and the exact
derivation of each payload type.

Outputs ``output/real_benchmark/real_benchmark.json`` and, unless disabled,
four-view PNG snapshots under ``output/real_benchmark/figures/``.
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

from src.open_dataset import (  # noqa: E402
    DATASET,
    ReplaySpec,
    load_record,
    send_record,
)
from src.pipeline import PipelineConfig, StreamPipeline  # noqa: E402


def _payload_authenticity(rec) -> dict:
    """Show that the emitted payloads equal the decoded recording, not a model."""
    from src.open_dataset import _frame_payloads

    spec = ReplaySpec()
    packets = _frame_payloads(rec, spec, 137)
    spectrum, waveform, audio_a, audio_b = (p[1] for p in packets)
    start = 137 * 1024
    source = rec.signals[start : start + 1024, 137 % rec.n_channels]
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


def _one_run(rec, rate: float, capacity: int, duration: float, port: int,
             output_dir: Path, figures: bool, loop: bool) -> dict:
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Real open-data benchmark")
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

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)
    rec = load_record("100")
    rates = [float(x) for x in args.rates.split(",") if x.strip()]

    print("Dataset :", DATASET["name"], "record", rec.record)
    print("License :", DATASET["license"], "| DOI", DATASET["doi"])
    print("Signal  :", rec.sig_names, rec.fs, "Hz,", rec.n_samples,
          "samples/lead,", round(rec.n_samples / rec.fs / 60, 2), "min")
    print("SHA256  :", rec.checksums["100.dat"])
    print()

    runs = []
    for i, rate in enumerate(rates):
        figures = (not args.no_figures) and abs(rate - args.figure_rate) < 1e-9
        result = _one_run(rec, rate, args.capacity, args.duration,
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
    print("\nAuthenticity checks:", report["payload_authenticity"])
    print("Report written to:", path.resolve())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
