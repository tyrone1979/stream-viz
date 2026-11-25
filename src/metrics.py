"""Lightweight counters and end-to-end latency statistics."""

from __future__ import annotations

import threading
from typing import Dict, List

import numpy as np


class Counters:
    """Thread-safe named counters."""

    def __init__(self) -> None:
        self._values: Dict[str, int] = {}
        self._lock = threading.Lock()

    def inc(self, name: str, amount: int = 1) -> None:
        with self._lock:
            self._values[name] = self._values.get(name, 0) + amount

    def snapshot(self) -> Dict[str, int]:
        with self._lock:
            return dict(self._values)


class LatencyMeter:
    """Collects end-to-end latency samples (arrival/display minus source time)."""

    def __init__(self, capacity: int = 200_000) -> None:
        self.capacity = capacity
        self._samples: List[float] = []
        self._lock = threading.Lock()

    def add(self, latency_ms: float) -> None:
        with self._lock:
            if len(self._samples) < self.capacity:
                self._samples.append(float(latency_ms))

    def extend(self, values: List[float]) -> None:
        with self._lock:
            room = self.capacity - len(self._samples)
            if room > 0:
                self._samples.extend(float(v) for v in values[:room])

    def snapshot(self) -> Dict[str, float]:
        with self._lock:
            data = np.asarray(self._samples, dtype=np.float64)
        if data.size == 0:
            return {"count": 0, "mean_ms": 0.0, "p50_ms": 0.0, "p95_ms": 0.0,
                    "p99_ms": 0.0, "max_ms": 0.0}
        return {
            "count": int(data.size),
            "mean_ms": round(float(data.mean()), 3),
            "p50_ms": round(float(np.percentile(data, 50)), 3),
            "p95_ms": round(float(np.percentile(data, 95)), 3),
            "p99_ms": round(float(np.percentile(data, 99)), 3),
            "max_ms": round(float(data.max()), 3),
        }
