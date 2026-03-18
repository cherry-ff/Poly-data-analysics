from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

from core.enums import MarketPhase, OrderRole, OrderStatus

if TYPE_CHECKING:
    from domain.models import OrderIntent
    from risk.engine import RiskContext


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class RuleResult:
    allowed: bool
    reason: str
    severity: str  # "hard" -> reject immediately; "soft" -> warn only


_OK = RuleResult(allowed=True, reason="ok", severity="none")


def _reject(reason: str, severity: str = "hard") -> RuleResult:
    return RuleResult(allowed=False, reason=reason, severity=severity)


# ---------------------------------------------------------------------------
# Individual rules
# ---------------------------------------------------------------------------

class MetadataIntegrityRule:
    """Market metadata must be loaded and have valid tick_size / fee_rate_bps."""

    def check(self, intent: "OrderIntent", ctx: "RiskContext") -> RuleResult:
        market = ctx.registry.get(intent.market_id)
        if market is None:
            return _reject("metadata_missing: market not in registry")
        if market.tick_size <= Decimal("0"):
            return _reject("metadata_invalid: tick_size is zero or negative")
        if market.fee_rate_bps < Decimal("0"):
            return _reject("metadata_invalid: fee_rate_bps is negative")
        if not market.up_token_id or not market.down_token_id:
            return _reject("metadata_invalid: token_id mapping incomplete")
        return _OK


class MarketPhaseRule:
    """Reject new-open pairs in phases where trading is forbidden."""

    _TRADING_PHASES = {
        MarketPhase.ACTIVE,
        MarketPhase.FAST_CLOSE,
        MarketPhase.FINAL_SECONDS,
    }

    def check(self, intent: "OrderIntent", ctx: "RiskContext") -> RuleResult:
        phase = ctx.lifecycle.get_phase(intent.market_id)
        # Recovery / unwind are allowed through CLOSED_WAIT_RESOLUTION
        # (summary.md §9.2: "处理剩余 full set") but not after resolution.
        if intent.role in {OrderRole.RECOVERY, OrderRole.UNWIND}:
            if phase in {MarketPhase.RESOLVED, MarketPhase.ARCHIVED}:
                return _reject(f"phase_forbidden: {phase.value} disallows recovery")
            return _OK
        if phase not in self._TRADING_PHASES:
            return _reject(f"phase_forbidden: {phase.value} does not allow new orders")
        return _OK


class FreshnessRule:
    """Reject if a required feed has gone stale."""

    def __init__(self, max_binance_staleness_ms: int, max_polymarket_staleness_ms: int) -> None:
        self._max_binance_ms = max_binance_staleness_ms
        self._max_pm_ms = max_polymarket_staleness_ms

    def check(self, intent: "OrderIntent", ctx: "RiskContext") -> RuleResult:
        now = ctx.now_ms
        binance_age = now - ctx.freshness.get("binance", 0)
        pm_market_age = now - ctx.freshness.get("polymarket_market", 0)

        if ctx.freshness.get("binance", 0) == 0:
            # Feed has never ticked; allow in dev/test, warn
            return RuleResult(allowed=True, reason="freshness_warn: binance never ticked", severity="soft")
        if binance_age > self._max_binance_ms:
            return _reject(f"freshness_stale: binance {binance_age}ms > {self._max_binance_ms}ms")
        if ctx.freshness.get("polymarket_market", 0) > 0 and pm_market_age > self._max_pm_ms:
            return _reject(f"freshness_stale: polymarket_market {pm_market_age}ms > {self._max_pm_ms}ms")
        return _OK


class MaxSingleSizeRule:
    """Single order size must not exceed configured maximum."""

    def __init__(self, max_size: Decimal) -> None:
        self._max_size = max_size

    def check(self, intent: "OrderIntent", ctx: "RiskContext") -> RuleResult:
        if intent.size > self._max_size:
            return _reject(
                f"size_limit: {intent.size} > max {self._max_size}"
            )
        return _OK


class MaxDirectionalInventoryRule:
    """Per-token net inventory must not exceed configured maximum."""

    def __init__(self, max_inventory: Decimal) -> None:
        self._max = max_inventory

    def check(self, intent: "OrderIntent", ctx: "RiskContext") -> RuleResult:
        positions = {
            pos.token_id: pos
            for pos in ctx.inventory_state.get_inventory(intent.market_id)
        }
        current = positions.get(intent.token_id)
        current_size = current.net_size if current else Decimal("0")
        projected = current_size + intent.size
        if projected > self._max:
            return _reject(
                f"inventory_limit: projected {projected} > max {self._max} for {intent.token_id}"
            )
        return _OK


class MaxUnhedgedExposureRule:
    """Unhedged exposure limit is tightened in FAST_CLOSE / FINAL_SECONDS phases."""

    def __init__(
        self,
        max_active: Decimal,
        max_fast_close: Decimal,
        max_final: Decimal,
    ) -> None:
        self._limits = {
            MarketPhase.ACTIVE: max_active,
            MarketPhase.FAST_CLOSE: max_fast_close,
            MarketPhase.FINAL_SECONDS: max_final,
        }

    def check(self, intent: "OrderIntent", ctx: "RiskContext") -> RuleResult:
        phase = ctx.lifecycle.get_phase(intent.market_id)
        limit = self._limits.get(phase)
        if limit is None:
            return _OK  # phase has no explicit limit (PREWARM, DISCOVERED, etc.)

        current_unhedged = ctx.inventory_state.get_unhedged_exposure(intent.market_id)
        # Pessimistic: assume this order adds to the unhedged side
        projected = current_unhedged + intent.size
        if projected > limit:
            return _reject(
                f"unhedged_limit: {projected} > {limit} in phase {phase.value}"
            )
        return _OK


class MaxOpenOrdersRule:
    """Total open orders for a market must not exceed configured maximum."""

    def __init__(self, max_open: int) -> None:
        self._max = max_open

    def check(self, intent: "OrderIntent", ctx: "RiskContext") -> RuleResult:
        open_orders = ctx.order_state.get_open_orders(intent.market_id)
        if len(open_orders) >= self._max:
            return _reject(
                f"open_orders_limit: {len(open_orders)} >= {self._max}"
            )
        return _OK


class GhostFillGuardRule:
    """Block new orders for a market while any order has a state mismatch (ghost fill)."""

    def check(self, intent: "OrderIntent", ctx: "RiskContext") -> RuleResult:
        open_orders = ctx.order_state.get_open_orders(intent.market_id)
        mismatched = [o for o in open_orders if o.mismatch]
        if mismatched:
            return _reject(
                f"ghost_fill_guard: {len(mismatched)} order(s) have state mismatch, "
                f"resolve before placing new orders"
            )
        return _OK


class MinOrderSizeRule:
    """Order size must be at least the market's min_order_size."""

    def check(self, intent: "OrderIntent", ctx: "RiskContext") -> RuleResult:
        market = ctx.registry.get(intent.market_id)
        if market is None:
            return _OK  # MetadataIntegrityRule will catch this
        if intent.size < market.min_order_size:
            return _reject(
                f"min_size: {intent.size} < market min {market.min_order_size}"
            )
        return _OK
