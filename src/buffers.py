"""Thread-safe bounded buffers, overflow policies and the cache-full signal.

The design deliberately separates *ingestion* from *presentation*: a producer
never touches the display, it only fills a buffer and raises a signal when the
buffer is full.  A consumer waits on that signal with a timeout, so it behaves
both as an event-driven listener and as a periodic timer.

Overflow behaviour is a *policy*, not a constant.  Which end of the buffer is
sacrificed when the consumer cannot keep up is the central trade-off this
software exists to measure, so it is switchable at construction time and every
drop is attributed:

    drop_oldest  keep the newest data, discard the head (real-time bias)
    drop_newest  keep continuity, refuse the arriving item (latency grows)
    block        apply back-pressure to the producer (loss moves upstream)
    unbounded    keep everything up to a hard cap, then degrade to drop_oldest

:attr:`BoundedFIFO.dropped_age` records how old the discarded items were, which
is what separates "discarding stale data" from "discarding data".
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Deque, Generic, List, Optional, TypeVar

from .metrics import LatencyMeter

T = TypeVar("T")


class OverflowPolicy(str, Enum):
    """What a full buffer does; values double as CLI/JSON identifiers."""

    DROP_OLDEST = "drop_oldest"
    DROP_NEWEST = "drop_newest"
    BLOCK = "block"
    UNBOUNDED = "unbounded"


@dataclass(frozen=True)
class Datagram:
    """Raw UDP payload plus the host-side arrival time."""

    data: bytes
    received_ms: int


class Signal:
    """Level-triggered, auto-clearing notification."""

    def __init__(self) -> None:
        self._event = threading.Event()

    def set(self) -> None:
        self._event.set()

    def wait(self, timeout: float) -> bool:
        """Wait up to ``timeout`` seconds; returns True if the signal fired."""
        fired = self._event.wait(timeout)
        if fired:
            self._event.clear()
        return fired

    def clear(self) -> None:
        self._event.clear()


class BoundedFIFO(Generic[T]):
    """Fixed-capacity FIFO whose overflow behaviour is selectable.

    ``age_fn`` maps a stored item to its current age in milliseconds; when
    supplied, the age of every discarded item is collected so a run can report
    *how stale* the data it threw away was, not merely how much.
    """

    def __init__(
        self,
        capacity: int,
        overflow_policy: OverflowPolicy | str = OverflowPolicy.DROP_OLDEST,
        age_fn: Optional[Callable[[T], float]] = None,
        stop_event: Optional[threading.Event] = None,
        block_budget: float = 2.0,
        unbounded_growth: int = 50,
    ) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        self.capacity = capacity
        self.policy = OverflowPolicy(overflow_policy)
        self.age_fn = age_fn
        self.stop_event = stop_event
        self.block_budget = block_budget
        self.unbounded_limit = capacity * max(1, unbounded_growth)

        self._items: Deque[T] = deque()
        self._cond = threading.Condition()
        self._pushed = 0
        self.dropped = 0
        self.dropped_block_timeout = 0
        self.dropped_hard_cap = 0
        self.dropped_age = LatencyMeter(capacity=20_000)

    # -- internals ---------------------------------------------------------
    def _limit(self) -> int:
        return self.unbounded_limit if self.policy is OverflowPolicy.UNBOUNDED \
            else self.capacity

    def _record_drop(self, item: T) -> None:
        self.dropped += 1
        if self.age_fn is not None:
            self.dropped_age.add(self.age_fn(item))

    # -- producer side -----------------------------------------------------
    def push(self, item: T) -> bool:
        """Append one item; return True when the buffer is at capacity."""
        with self._cond:
            self._pushed += 1

            if (self.policy is OverflowPolicy.DROP_NEWEST
                    and len(self._items) >= self.capacity):
                self._record_drop(item)
                return True

            if self.policy is OverflowPolicy.BLOCK:
                waited = 0.0
                while len(self._items) >= self.capacity:
                    stopping = self.stop_event is not None and self.stop_event.is_set()
                    if stopping or waited >= self.block_budget:
                        # Back-pressure has a budget: an unbounded wait would
                        # wedge the receiver thread and make shutdown hang.
                        self._record_drop(item)
                        self.dropped_block_timeout += 1
                        return True
                    started = time.perf_counter()
                    self._cond.wait(min(0.05, max(0.001, self.block_budget - waited)))
                    waited += time.perf_counter() - started

            self._items.append(item)
            while len(self._items) > self._limit():
                victim = self._items.popleft()
                if self.policy is OverflowPolicy.UNBOUNDED:
                    self.dropped_hard_cap += 1
                self._record_drop(victim)
            return len(self._items) >= self.capacity

    def push_many(self, items: List[T]) -> bool:
        full = False
        for item in items:
            full = self.push(item)
        return full

    # -- consumer side -----------------------------------------------------
    def drain(self) -> List[T]:
        """Atomically remove and return everything currently buffered."""
        with self._cond:
            out = list(self._items)
            self._items.clear()
            if out:
                self._cond.notify_all()  # release any BLOCK-ed producer
            return out

    def __len__(self) -> int:
        with self._cond:
            return len(self._items)

    # -- reporting ---------------------------------------------------------
    def stats(self) -> dict:
        with self._cond:
            occupancy = len(self._items)
        return {
            "capacity": self.capacity,
            "policy": self.policy.value,
            "limit": self._limit(),
            "pushed": self._pushed,
            "dropped": self.dropped,
            "dropped_block_timeout": self.dropped_block_timeout,
            "dropped_hard_cap": self.dropped_hard_cap,
            "occupancy": occupancy,
            "dropped_age_ms": self.dropped_age.snapshot(),
        }
