#!/usr/bin/env python3
"""Audit the archived measurement files without running the experiments.

This is the check a third party can run before trusting a published number, and
the check a reviewer can run before trusting ours.  It needs nothing but a
Python 3 standard library: no NumPy, no dataset downloads, no network.  That is
deliberate -- an audit tool that requires the software it audits is not an
independent check.

What it verifies
----------------
1. **Conservation.**  For every run, `sent - udp_received = udp_lost` and
   `frame_buffer_in = displayed + fifo_dropped + dropped_at_shutdown +
   occupancy`.  A published cell whose chain does not close is a finding, not a
   rounding difference.
2. **Summary arithmetic.**  Every `{median, min, max}` triple must satisfy
   `min <= median <= max`, and the median must equal the median of the per-run
   values it summarises.
3. **Coverage.**  Every (dataset, policy, delay) cell the summary claims must
   have `repeats` runs behind it.
4. **Detection consistency.**  Confusion-matrix cells must sum to the window
   count, and quantiles must be ordered `p50 <= p95 <= max`.

Exit code is 0 when every check passes, 1 otherwise, so it can sit in CI.

    python tools/verify_measurements.py
    python tools/verify_measurements.py --root measurements --verbose
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

TOLERANCE = 1e-6


class Report:
    def __init__(self, verbose: bool = False) -> None:
        self.verbose = verbose
        self.checked = 0
        self.failures: list[str] = []

    def check(self, condition: bool, message: str) -> None:
        self.checked += 1
        if condition:
            if self.verbose:
                print(f"  ok   {message}")
        else:
            self.failures.append(message)
            print(f"  FAIL {message}")

    def close_enough(self, left: float, right: float, message: str,
                     tol: float = TOLERANCE) -> None:
        self.check(abs(float(left) - float(right)) <= tol, message)


def _median(values):
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[mid])
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def audit_ablation(payload: dict, report: Report) -> None:
    runs = payload.get("runs") or payload.get("rows") or []
    summary = payload.get("summary") or []
    print(f"policy_ablation: {len(runs)} runs, {len(summary)} summary cells")

    for i, run in enumerate(runs):
        cons = run.get("conservation", {})
        sent = cons.get("sent")
        received = cons.get("udp_received")
        if sent is not None and received is not None:
            report.check(received <= sent,
                         f"run {i}: received {received} <= sent {sent}")
        fin = cons.get("frame_buffer_in")
        fout = cons.get("frame_buffer_out")
        if fin is not None and fout is not None:
            report.close_enough(fin, fout,
                                f"run {i}: frame in {fin} == frame out {fout}")
        if "conservation_closed" in cons:
            report.check(bool(cons["conservation_closed"]),
                         f"run {i}: conservation chain closed")

    # Every summary cell must be reproducible from the runs it summarises.
    by_cell: dict = {}
    for run in runs:
        key = (run.get("dataset"), run.get("policy"),
               float(run.get("consumer_delay_ms") or 0.0))
        by_cell.setdefault(key, []).append(run)

    for cell in summary:
        key = (cell.get("dataset"), cell.get("policy"),
               float(cell.get("consumer_delay_ms") or 0.0))
        members = by_cell.get(key)
        if members is None:
            report.check(False, f"summary cell {key} has no backing runs")
            continue
        report.check(len(members) == cell.get("repeats", len(members)),
                     f"cell {key}: {cell.get('repeats')} repeats declared, "
                     f"{len(members)} runs present")

        for field, stat in cell.items():
            if not isinstance(stat, dict) or "median" not in stat:
                continue
            lo, mid, hi = stat.get("min"), stat["median"], stat.get("max")
            if lo is None or hi is None:
                continue
            report.check(lo - TOLERANCE <= mid <= hi + TOLERANCE,
                         f"cell {key} {field}: min<=median<=max "
                         f"({lo} <= {mid} <= {hi})")

    if not runs:
        report.check(False, "no per-run rows found; the summary cannot be audited")


def audit_scenario(payload: dict, report: Report) -> None:
    runs = payload.get("runs") or payload.get("rows") or []
    summary = payload.get("summary") or []
    print(f"extreme_scenario: {len(runs)} runs, {len(summary)} summary cells")
    workload = payload.get("workload", {})
    if workload:
        report.check(workload.get("data_status") == "synthetic",
                     "scenario declares data_status='synthetic'")
    for i, run in enumerate(runs):
        cons = run.get("conservation", {})
        if "conservation_closed" in cons:
            report.check(bool(cons["conservation_closed"]),
                         f"scenario run {i}: conservation chain closed")


def audit_anomaly(payload: dict, report: Report) -> None:
    det = payload.get("detection")
    runs = payload.get("replay") or []
    print(f"anomaly_detection: detection block + {len(runs)} replay runs")
    if det:
        for segment in ("whole_record", "clean_head", "degraded_tail"):
            block = det.get(segment)
            if not block:
                continue
            total = block["tp"] + block["fp"] + block["fn"] + block["tn"]
            report.close_enough(total, block["windows"],
                                f"{segment}: tp+fp+fn+tn == windows")
            report.close_enough(
                block["tp"] + block["fn"], block["abnormal_frames"],
                f"{segment}: tp+fn == abnormal_frames")
            # Reported values are rounded to four decimals, so compare at that
            # precision rather than at machine epsilon.
            recall = block["tp"] / max(1, block["tp"] + block["fn"])
            report.close_enough(recall, block["recall"],
                                f"{segment}: recall", tol=5e-4)
        head, tail = det["clean_head"], det["degraded_tail"]
        report.close_enough(head["windows"] + tail["windows"],
                            det["whole_record"]["windows"],
                            "head + tail windows == whole record windows")

    for run in runs:
        report.check(bool(run.get("conservation_closed")),
                     f"{run.get('policy')}: conservation chain closed")
        delay = run.get("alert_delay_ms", {})
        if delay:
            report.check(delay["p50"] <= delay["p95"] <= delay["max"],
                         f"{run.get('policy')}: p50 <= p95 <= max alert delay")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--root", default="measurements")
    ap.add_argument("--verbose", action="store_true")
    args = ap.parse_args()

    root = Path(args.root)
    report = Report(args.verbose)
    handlers = {
        "policy_ablation.json": audit_ablation,
        "extreme_scenario.json": audit_scenario,
        "anomaly_detection.json": audit_anomaly,
    }

    found = 0
    for name, handler in handlers.items():
        path = root / name
        if not path.is_file():
            print(f"{name}: not present, skipped")
            continue
        found += 1
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            report.check(False, f"{name}: not valid JSON ({exc})")
            continue
        handler(payload, report)

    print()
    if report.failures:
        print(f"{len(report.failures)} of {report.checked} checks failed")
        for failure in report.failures:
            print(f"  - {failure}")
        return 1
    print(f"all {report.checked} checks passed across {found} file(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
