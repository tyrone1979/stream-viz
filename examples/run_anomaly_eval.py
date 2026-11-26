"""Evaluate the outlier-flagging consumer on annotated data and live replay.

Two questions, deliberately separated:

**Part 1 -- does the detector work?**  Offline, over MIT-BIH record 100, where
every beat carries an expert label.  Reported at frame granularity (the unit a
display consumer shows) plus beat-level QRS localisation, and split into the
clean and degraded segments of the recording so a reader can see that the
detector's errors are the recording's, not a claim that quietly excludes them.

**Part 2 -- what does the buffer policy cost it?**  The same detector runs as a
downstream consumer of a live UDP replay under two admission policies.  The
measured quantity is the *alert delay*: the age, at the moment the flag is
raised, of the sample that triggered it.  Detection work is a few hundred
microseconds; the rest of the delay is frame staleness, which is the policy.

    python examples/run_anomaly_eval.py                  # both parts
    python examples/run_anomaly_eval.py --part detection # offline only
    python examples/run_anomaly_eval.py --part alert --repeat 3
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import queue
import statistics
import sys
import time
from pathlib import Path
from typing import List, Optional

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import numpy as np  # noqa: E402

from src.annotations import abnormal_positions, beats, count_by_symbol, read  # noqa: E402
from src.anomaly import (  # noqa: E402
    DEFAULT_THRESHOLD,
    StreamingBeatDetector,
    evaluate_windows,
    match_beats,
)
from src.datasets import REGISTRY, load_dataset  # noqa: E402
from src.open_dataset import ReplaySpec, send_record  # noqa: E402
from src.pipeline import PipelineConfig, StreamPipeline  # noqa: E402
from src.protocol import PacketType, now_ms  # noqa: E402
from src.visualizer import DrainThread  # noqa: E402

WINDOW = 1024          # == FRAME_SAMPLES: one frame, the unit a consumer shows
ANNOTATION = "data/mitdb/100.atr"


# ---------------------------------------------------------------------------
# Part 1: offline detection quality
# ---------------------------------------------------------------------------

def _quantiles(values: List[float]) -> dict:
    if not values:
        return {"p50": 0.0, "p95": 0.0, "max": 0.0}
    ordered = sorted(values)
    def q(p: float) -> float:
        return round(float(ordered[min(len(ordered) - 1, int(p * len(ordered)))]), 3)
    return {"p50": q(0.50), "p95": q(0.95), "max": round(float(ordered[-1]), 3)}


def detection_part(dataset: str = "mitdb", window: int = WINDOW) -> dict:
    rec = load_dataset(dataset)
    ann = read(_ROOT / ANNOTATION)
    abnormal = abnormal_positions(ann)
    reference = [b.sample for b in beats(ann)]
    fs = rec.fs

    # The detector subscribes to one lead.  MLII (channel 0) carries the larger
    # R wave; the V5 lead in this record is roughly half the amplitude and
    # localises beats far worse, which is why the choice is stated rather than
    # left implicit.
    signal = rec.signals[:, 0]

    result = evaluate_windows(signal, fs, window_samples=window,
                              abnormal_samples=abnormal)

    # Beat-level localisation, and the per-window detection cost.
    detector = StreamingBeatDetector(fs=fs)
    offsets: List[int] = []
    costs: List[float] = []
    for start in range(0, len(signal) - window + 1, window):
        began = time.perf_counter()
        verdict = detector.observe(signal[start:start + window])
        costs.append((time.perf_counter() - began) * 1e3)
        offsets.extend(start + b.offset for b in verdict.beats)

    tolerance = int(0.15 * fs)
    localisation = match_beats(offsets, reference, tolerance)

    # Split by signal quality: in the last ~280 s of this record the QRS
    # detector stops localising (366 spurious complexes), so the frame-level
    # numbers are reported for the whole record *and* for the clean head.
    split_sample = 547_000
    head = [r for r in result["windows"] if r["start_sample"] < split_sample]
    tail = [r for r in result["windows"] if r["start_sample"] >= split_sample]

    def score(rows: List[dict]) -> dict:
        tp = sum(1 for r in rows if r["flagged"] and r["actual_abnormal"])
        fp = sum(1 for r in rows if r["flagged"] and not r["actual_abnormal"])
        fn = sum(1 for r in rows if not r["flagged"] and r["actual_abnormal"])
        tn = len(rows) - tp - fp - fn
        p = tp / (tp + fp) if tp + fp else 0.0
        rr = tp / (tp + fn) if tp + fn else 0.0
        return {
            "windows": len(rows), "tp": tp, "fp": fp, "fn": fn, "tn": tn,
            "precision": round(p, 4), "recall": round(rr, 4),
            "f1": round(2 * p * rr / (p + rr), 4) if p + rr else 0.0,
            "abnormal_frames": tp + fn,
        }

    return {
        "dataset": dataset,
        "record": "100",
        "annotation_file": ANNOTATION,
        "annotation_sha256": REGISTRY[dataset].files.get("100.atr"),
        "lead": "MLII (channel 0)",
        "window_samples": window,
        "window_s": round(window / fs, 3),
        "sampling_rate_hz": fs,
        "beat_annotations": len(ann),
        "beat_classes": count_by_symbol(ann),
        "abnormal_beats": int(len(abnormal)),
        "threshold": DEFAULT_THRESHOLD,
        "threshold_basis": "1 - PREMATURE_RATIO; RR 20% below the running "
                           "median, the lower bound of the ectopic-beat rule. "
                           "Chosen before any label is read; not tuned.",
        "whole_record": {
            **score(result["windows"]),
            "pr_auc": result["pr_auc"],
            "specificity": result["specificity"],
            "prevalence": result["prevalence"],
        },
        "clean_head": score(head),
        "degraded_tail": score(tail),
        "qrs_localisation": {
            **localisation,
            "tolerance_ms": round(tolerance / fs * 1000.0, 1),
            "note": "localisation degrades in the last ~280 s of the record, "
                    "where the QRS detector also emits 366 spurious complexes; "
                    "no abnormal beat in that segment is lost at frame level "
                    "beyond what the tail row reports.",
        },
        "detection_cost_ms": _quantiles(costs),
        "windows_evaluated": len(result["windows"]),
    }


# ---------------------------------------------------------------------------
# Part 2: alert delay under two admission policies
# ---------------------------------------------------------------------------

class AnomalyConsumer(DrainThread):
    """Drain consumer that also feeds a downstream outlier detector.

    The detection work happens after the view fold, so the staleness the
    pipeline reports is untouched: this consumer adds a *consumer*, it does not
    change what the pipeline measures.
    """

    def __init__(self, pipeline: StreamPipeline, detector: StreamingBeatDetector,
                 stride: int) -> None:
        cfg = pipeline.config
        super().__init__(
            frame_buffer=pipeline.frame_buffer,
            ready_signal=pipeline.ready_signal,
            counters=pipeline.counters,
            latency=pipeline.display_latency,
            stop_event=pipeline.stop_event,
            refresh_hz=cfg.refresh_hz,
            staleness=pipeline.staleness,
            consumer_delay_ms=cfg.consumer_delay_ms,
            history=cfg.history,
            n_bins=cfg.n_bins,
            n_channels=cfg.n_channels,
        )
        self.detector = detector
        self.stride = stride
        self.alerts: List[dict] = []
        self.costs: List[float] = []
        self.windows_seen = 0

    def _consume(self, packets) -> bool:
        if not packets:
            return False
        self.state.ingest(packets, self.latency, self.counters, self.staleness)
        self.frames_rendered += 1
        visible = now_ms()

        began = time.perf_counter()
        for pkt in packets:
            if pkt.type is not PacketType.WAVEFORM or pkt.channel != 0:
                continue
            self.windows_seen += 1
            verdict = self.detector.observe(pkt.values, stride=self.stride)
            if verdict.flagged:
                # The alert delay is measured from the *source* timestamp of
                # the packet that raised it: how old was the data at the
                # moment the operator could have been told?
                self.alerts.append({
                    "delay_ms": int(visible - pkt.timestamp_ms),
                    "score": round(verdict.score, 4),
                    "reasons": verdict.reasons,
                })
        self.costs.append((time.perf_counter() - began) * 1e3)
        return True


def _sender_worker(spec: ReplaySpec, result_queue) -> None:
    try:
        result_queue.put(send_record(spec))
    except Exception as exc:  # noqa: BLE001
        result_queue.put({"error": f"{type(exc).__name__}: {exc}"})


def alert_run(policy: str, rate: float, capacity: int, budget_ms: float,
              duration: float, max_packets: int, port: int, output_dir: Path) -> dict:
    rec = load_dataset("mitdb")
    n_channels = rec.n_channels
    cfg = PipelineConfig(
        port=port,
        frame_capacity=capacity,
        overflow_policy=policy,
        refresh_hz=30.0,
        consumer_delay_ms=budget_ms,
        archive_audio=False,
        output_dir=output_dir,
        n_channels=n_channels,
    )
    holder: dict = {}

    def factory(pipeline: StreamPipeline):
        consumer = AnomalyConsumer(
            pipeline, StreamingBeatDetector(fs=rec.fs), stride=WINDOW * n_channels,
        )
        holder["consumer"] = consumer
        return consumer

    spec = ReplaySpec(
        port=port, duration_s=duration, rate_mbps=rate,
        n_bins=cfg.n_bins, waveform_len=WINDOW, audio_len=512,
        sample_rate=cfg.sample_rate, dataset="mitdb", loop=True,
        max_packets=max_packets,
    )

    pipeline = StreamPipeline(cfg, display_factory=factory).start()
    ctx = mp.get_context("spawn")
    results = ctx.Queue()
    sender = ctx.Process(target=_sender_worker, args=(spec, results), daemon=True)
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

    consumer = holder["consumer"]
    report = pipeline.report()
    flow = report["flow"]
    frame = report["buffers"]["frame"]
    stale = report["staleness_ms"]
    delays = [a["delay_ms"] for a in consumer.alerts]

    frame_in = flow["parsed_by_type"]["spectrum"] + flow["parsed_by_type"]["waveform"]
    frame_out = (flow["delivered_to_consumer"] + frame["dropped"]
                 + flow["dropped_at_shutdown"] + frame["occupancy"])

    return {
        "policy": policy,
        "target_rate_mbps": rate,
        "frame_capacity": capacity,
        "consumer_budget_ms": budget_ms,
        "sender_error": stats.get("error"),
        "conservation_closed": frame_in == frame_out,
        "conservation": {
            "sent": int(stats.get("sent_packets", 0)),
            "udp_received": flow["udp_received"],
            "parsed": flow["parsed"],
            "displayed": flow["delivered_to_consumer"],
            "fifo_dropped": frame["dropped"],
        },
        "staleness_p95_ms": stale["p95_ms"],
        "windows_observed": consumer.windows_seen,
        "alerts": len(consumer.alerts),
        "alert_delay_ms": _quantiles(delays),
        "detector_cost_ms": _quantiles(consumer.costs),
        "detection_share_of_delay": (
            round(_quantiles(consumer.costs)["p50"] / _quantiles(delays)["p50"], 4)
            if delays and _quantiles(delays)["p50"] else None
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Outlier consumer: quality and alert delay")
    parser.add_argument("--part", default="both",
                        choices=("both", "detection", "alert"))
    parser.add_argument("--window", type=int, default=WINDOW)
    parser.add_argument("--policies", default="drop_oldest,block")
    parser.add_argument("--rate", type=float, default=8.0)
    parser.add_argument("--capacity", type=int, default=8)
    parser.add_argument("--budget-ms", type=float, default=40.0)
    parser.add_argument("--duration", type=float, default=12.0)
    parser.add_argument("--packets", type=int, default=7500)
    parser.add_argument("--port", type=int, default=19321)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--out", default="measurements/anomaly_detection.json")
    args = parser.parse_args()

    output_dir = _ROOT / "output" / "anomaly"
    payload: dict = {
        "generated_by": "examples/run_anomaly_eval.py",
        "dataset": REGISTRY["mitdb"].metadata,
    }

    if args.part in ("both", "detection"):
        print("[detection] MIT-BIH record 100, frame granularity")
        det = detection_part("mitdb", args.window)
        payload["detection"] = det
        w = det["whole_record"]
        print(f"  abnormal beats {det['abnormal_beats']} / {det['beat_annotations']}")
        print(f"  frames {w['windows']}: P={w['precision']} R={w['recall']} "
              f"F1={w['f1']} AP={w['pr_auc']}")
        print(f"  clean head F1={det['clean_head']['f1']}  "
              f"degraded tail F1={det['degraded_tail']['f1']}")
        print(f"  QRS localisation recall={det['qrs_localisation']['recall']}  "
              f"cost p50={det['detection_cost_ms']['p50']} ms")

    if args.part in ("both", "alert"):
        runs: List[dict] = []
        for policy in args.policies.split(","):
            for rep in range(args.repeat):
                print(f"[alert] {policy} run {rep + 1}/{args.repeat}")
                runs.append(alert_run(
                    policy.strip(), args.rate, args.capacity, args.budget_ms,
                    args.duration, args.packets, args.port, output_dir,
                ))
        payload["replay"] = runs

    out_path = _ROOT / args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"\nwrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
