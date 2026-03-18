from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal accumulator types
# ---------------------------------------------------------------------------

@dataclass
class _Counter:
    value: int = 0

    def incr(self, n: int = 1) -> None:
        self.value += n


@dataclass
class _Gauge:
    value: float = 0.0

    def set(self, v: float) -> None:
        self.value = v


@dataclass
class _Timing:
    count: int = 0
    total_ms: float = 0.0
    min_ms: float = float("inf")
    max_ms: float = float("-inf")

    def record(self, v_ms: float) -> None:
        self.count += 1
        self.total_ms += v_ms
        if v_ms < self.min_ms:
            self.min_ms = v_ms
        if v_ms > self.max_ms:
            self.max_ms = v_ms

    @property
    def avg_ms(self) -> float:
        return self.total_ms / self.count if self.count else 0.0


# ---------------------------------------------------------------------------
# InMemoryMetrics
# ---------------------------------------------------------------------------

class InMemoryMetrics:
    """Thread-safe in-memory metrics store with periodic log dumps.

    All metric names are arbitrary strings.  Tags are encoded into the key as
    ``name{k=v,k2=v2}`` so that different tag combinations are stored
    separately.

    Key metrics this system should track (from summary.md §11):
    - binance_feed_latency_ms
    - chainlink_feed_interval_ms
    - polymarket_market_ws_age_ms
    - polymarket_user_ws_age_ms
    - fair_value_vs_mid_bps
    - pair_cost_vs_one_bps
    - two_leg_fill_rate
    - recovery_success_rate
    - cancel_latency_ms
    - close_phase_unhedged_exposure
    - ghost_fill_count
    - order_rejected_count
    - next_market_discovery_lead_ms
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, _Counter] = defaultdict(_Counter)
        self._gauges: dict[str, _Gauge] = defaultdict(_Gauge)
        self._timings: dict[str, _Timing] = defaultdict(_Timing)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def incr(self, name: str, value: int = 1, **tags: str) -> None:
        key = _make_key(name, tags)
        with self._lock:
            self._counters[key].incr(value)

    def gauge(self, name: str, value: float, **tags: str) -> None:
        key = _make_key(name, tags)
        with self._lock:
            self._gauges[key].set(value)

    def timing(self, name: str, value_ms: float, **tags: str) -> None:
        key = _make_key(name, tags)
        with self._lock:
            self._timings[key].record(value_ms)

    # ------------------------------------------------------------------
    # Snapshot / reporting
    # ------------------------------------------------------------------

    def snapshot(self) -> dict[str, Any]:
        """Return a point-in-time copy of all metrics (no reset)."""
        with self._lock:
            return {
                "counters": {k: v.value for k, v in self._counters.items()},
                "gauges": {k: v.value for k, v in self._gauges.items()},
                "timings": {
                    k: {
                        "count": v.count,
                        "avg_ms": round(v.avg_ms, 3),
                        "min_ms": round(v.min_ms, 3) if v.count else None,
                        "max_ms": round(v.max_ms, 3) if v.count else None,
                        "total_ms": round(v.total_ms, 3),
                    }
                    for k, v in self._timings.items()
                },
            }

    def log_summary(self) -> None:
        """Emit a summary of all metrics to the logger (INFO level)."""
        snap = self.snapshot()
        logger.info("=== metrics snapshot ===")
        for k, v in snap["counters"].items():
            logger.info("  counter  %s = %d", k, v)
        for k, v in snap["gauges"].items():
            logger.info("  gauge    %s = %.4f", k, v)
        for k, v in snap["timings"].items():
            logger.info(
                "  timing   %s  count=%d avg=%.3fms min=%.3fms max=%.3fms",
                k,
                v["count"],
                v["avg_ms"],
                v["min_ms"] or 0.0,
                v["max_ms"] or 0.0,
            )

    def reset(self) -> None:
        """Clear all accumulators (useful between replay runs)."""
        with self._lock:
            self._counters.clear()
            self._gauges.clear()
            self._timings.clear()


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make_key(name: str, tags: dict[str, str]) -> str:
    if not tags:
        return name
    tag_str = ",".join(f"{k}={v}" for k, v in sorted(tags.items()))
    return f"{name}{{{tag_str}}}"
