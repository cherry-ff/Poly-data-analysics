from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from domain.models import OrderIntent
from market.lifecycle import LifecycleManager
from market.registry import InMemoryMarketRegistry
from risk.rules import (
    FreshnessRule,
    GhostFillGuardRule,
    MarketPhaseRule,
    MaxDirectionalInventoryRule,
    MaxOpenOrdersRule,
    MaxSingleSizeRule,
    MaxUnhedgedExposureRule,
    MetadataIntegrityRule,
    MinOrderSizeRule,
    RuleResult,
)
from state.inventory_state import InMemoryInventoryStore
from state.order_state import InMemoryOrderStateStore


# ---------------------------------------------------------------------------
# RiskDecision — public output contract
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class RiskDecision:
    """Result of a risk evaluation.

    ``allowed`` is True only when all hard rules pass.
    ``reason`` contains the first rejection reason or "ok".
    ``severity`` is "none" on approval, "hard" on hard reject, "soft" on warn.
    """

    allowed: bool
    reason: str
    severity: str  # "none" | "soft" | "hard"


# ---------------------------------------------------------------------------
# RiskConfig — all thresholds in one place
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class RiskConfig:
    # Per-order size cap (USDC notional equivalent)
    max_single_order_size: Decimal = Decimal("5000")

    # Per-token net inventory cap
    max_directional_inventory: Decimal = Decimal("20000")

    # Unhedged exposure caps per phase
    max_unhedged_active: Decimal = Decimal("5000")
    max_unhedged_fast_close: Decimal = Decimal("1000")
    max_unhedged_final: Decimal = Decimal("200")

    # Max open orders per market
    max_open_orders: int = 30

    # Feed staleness thresholds
    max_binance_staleness_ms: int = 5_000
    max_polymarket_staleness_ms: int = 10_000


# ---------------------------------------------------------------------------
# RiskContext — read-only snapshot passed to each rule
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class RiskContext:
    """Everything a rule needs to make a decision (no side effects)."""

    registry: InMemoryMarketRegistry
    lifecycle: LifecycleManager
    order_state: InMemoryOrderStateStore
    inventory_state: InMemoryInventoryStore
    freshness: dict[str, int]   # feed_name -> last_recv_ts_ms
    now_ms: int


# ---------------------------------------------------------------------------
# RiskEngine
# ---------------------------------------------------------------------------

class RiskEngine:
    """Synchronous hot-path risk engine.

    All rules are evaluated in order.  The first hard reject stops evaluation.
    Soft rejects are accumulated but do not block the order.

    Usage::

        engine = RiskEngine.from_context(ctx, RiskConfig())
        decision = engine.evaluate(intent, clock.now_ms())
        if not decision.allowed:
            # handle rejection
    """

    def __init__(
        self,
        registry: InMemoryMarketRegistry,
        lifecycle: LifecycleManager,
        order_state: InMemoryOrderStateStore,
        inventory_state: InMemoryInventoryStore,
        config: RiskConfig | None = None,
    ) -> None:
        cfg = config or RiskConfig()
        self._registry = registry
        self._lifecycle = lifecycle
        self._order_state = order_state
        self._inventory_state = inventory_state
        self._config = cfg
        self._freshness: dict[str, int] = {}

        # Rules evaluated in order; first hard reject wins
        self._rules = [
            MetadataIntegrityRule(),
            MarketPhaseRule(),
            FreshnessRule(
                max_binance_staleness_ms=cfg.max_binance_staleness_ms,
                max_polymarket_staleness_ms=cfg.max_polymarket_staleness_ms,
            ),
            MinOrderSizeRule(),
            MaxSingleSizeRule(max_size=cfg.max_single_order_size),
            MaxDirectionalInventoryRule(max_inventory=cfg.max_directional_inventory),
            MaxUnhedgedExposureRule(
                max_active=cfg.max_unhedged_active,
                max_fast_close=cfg.max_unhedged_fast_close,
                max_final=cfg.max_unhedged_final,
            ),
            MaxOpenOrdersRule(max_open=cfg.max_open_orders),
            GhostFillGuardRule(),
        ]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def evaluate(self, intent: OrderIntent, now_ms: int) -> RiskDecision:
        """Evaluate all rules for the given intent.  Returns on first hard reject."""
        ctx = RiskContext(
            registry=self._registry,
            lifecycle=self._lifecycle,
            order_state=self._order_state,
            inventory_state=self._inventory_state,
            freshness=dict(self._freshness),
            now_ms=now_ms,
        )

        soft_reasons: list[str] = []
        for rule in self._rules:
            result: RuleResult = rule.check(intent, ctx)
            if not result.allowed and result.severity == "hard":
                return RiskDecision(
                    allowed=False,
                    reason=result.reason,
                    severity="hard",
                )
            if not result.allowed and result.severity == "soft":
                soft_reasons.append(result.reason)

        if soft_reasons:
            # All hard rules passed but there are soft warnings — still allow
            return RiskDecision(
                allowed=True,
                reason="; ".join(soft_reasons),
                severity="soft",
            )
        return RiskDecision(allowed=True, reason="ok", severity="none")

    def on_feed_heartbeat(self, feed_name: str, ts_ms: int) -> None:
        """Update feed freshness timestamp.  Call this on every incoming tick."""
        self._freshness[feed_name] = ts_ms

    @property
    def config(self) -> RiskConfig:
        return self._config
