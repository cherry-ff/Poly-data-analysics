from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Alert levels (from summary.md §11.2)
# ---------------------------------------------------------------------------

class AlertLevel:
    P0 = "P0"  # unable to trade, state inconsistency, market metadata missing
    P1 = "P1"  # feed disconnect, cancel success rate drop, close-phase exposure
    P2 = "P2"  # non-critical: side-car lag, replay delay, non-critical panel


# ---------------------------------------------------------------------------
# Alert record
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class Alert:
    level: str
    title: str
    detail: str
    ts_ms: int


# ---------------------------------------------------------------------------
# LoggingAlerts
# ---------------------------------------------------------------------------

class LoggingAlerts:
    """Alert dispatcher that emits structured log lines.

    P0 -> CRITICAL
    P1 -> WARNING
    P2 -> INFO

    The alert history is kept in memory so that callers can inspect recent
    alerts programmatically (e.g. for tests or the monitoring panel).
    """

    def __init__(self, max_history: int = 500) -> None:
        self._max_history = max_history
        self._history: list[Alert] = []
        self._lock = asyncio.Lock()

    async def emit(self, level: str, title: str, detail: str) -> None:
        """Emit an alert at the given level."""
        alert = Alert(
            level=level,
            title=title,
            detail=detail,
            ts_ms=time.time_ns() // 1_000_000,
        )
        async with self._lock:
            self._history.append(alert)
            if len(self._history) > self._max_history:
                self._history.pop(0)

        _log_alert(alert)

    def recent(self, level: str | None = None, limit: int = 50) -> list[Alert]:
        """Return the most recent alerts, optionally filtered by level."""
        items = list(self._history)
        if level is not None:
            items = [a for a in items if a.level == level]
        return items[-limit:]

    def p0_count(self) -> int:
        return sum(1 for a in self._history if a.level == AlertLevel.P0)

    def p1_count(self) -> int:
        return sum(1 for a in self._history if a.level == AlertLevel.P1)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _log_alert(alert: Alert) -> None:
    msg = "[ALERT][%s] %s | %s"
    if alert.level == AlertLevel.P0:
        logger.critical(msg, alert.level, alert.title, alert.detail)
    elif alert.level == AlertLevel.P1:
        logger.warning(msg, alert.level, alert.title, alert.detail)
    else:
        logger.info(msg, alert.level, alert.title, alert.detail)
