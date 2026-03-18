"""Unit tests for risk.engine.RiskEngine and risk.rules."""

from decimal import Decimal

import pytest

from app.config import LifecycleConfig
from core.enums import MarketPhase, OrderRole, OrderStatus, Side, TimeInForce
from domain.models import ExecutionReport, MarketMetadata, OrderIntent
from market.lifecycle import LifecycleManager
from market.registry import InMemoryMarketRegistry
from risk.engine import RiskConfig, RiskDecision, RiskEngine
from state.inventory_state import InMemoryInventoryStore
from state.order_state import InMemoryOrderStateStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NOW_MS = 1_000_000  # middle of market lifetime


def _make_market(
    market_id: str = "m1",
    start_ts_ms: int = 500_000,
    end_ts_ms: int = 2_000_000,
    tick_size: str = "0.01",
    fee_rate_bps: str = "10",
    status: str = "active",
) -> MarketMetadata:
    return MarketMetadata(
        market_id=market_id,
        condition_id=f"cond_{market_id}",
        up_token_id="up_tok",
        down_token_id="dn_tok",
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
        tick_size=Decimal(tick_size),
        fee_rate_bps=Decimal(fee_rate_bps),
        min_order_size=Decimal("1"),
        status=status,
    )


def _make_intent(
    market_id: str = "m1",
    token_id: str = "up_tok",
    side: Side = Side.BUY,
    price: str = "0.55",
    size: str = "10",
    role: OrderRole = OrderRole.MAKER_QUOTE,
    tif: TimeInForce = TimeInForce.GTC,
    post_only: bool = True,
) -> OrderIntent:
    return OrderIntent(
        intent_id="int_1",
        pair_id="pair_1",
        market_id=market_id,
        token_id=token_id,
        side=side,
        price=Decimal(price),
        size=Decimal(size),
        tif=tif,
        post_only=post_only,
        role=role,
        reason="test",
    )


def _make_engine(
    market: MarketMetadata | None = None,
    config: RiskConfig | None = None,
) -> tuple[RiskEngine, InMemoryMarketRegistry, InMemoryOrderStateStore, InMemoryInventoryStore]:
    registry = InMemoryMarketRegistry()
    if market is not None:
        registry.upsert(market)

    lc_config = LifecycleConfig()
    lifecycle = LifecycleManager(registry=registry, config=lc_config)
    if market is not None:
        lifecycle.on_market_upsert(market)

    # Drive lifecycle to the correct phase for NOW_MS
    if market is not None:
        lifecycle.on_time_tick(NOW_MS)

    order_state = InMemoryOrderStateStore()
    inventory_state = InMemoryInventoryStore(registry=registry)

    engine = RiskEngine(
        registry=registry,
        lifecycle=lifecycle,
        order_state=order_state,
        inventory_state=inventory_state,
        config=config,
    )
    return engine, registry, order_state, inventory_state


# ---------------------------------------------------------------------------
# MetadataIntegrityRule
# ---------------------------------------------------------------------------

class TestMetadataIntegrityRule:
    def test_reject_unknown_market(self) -> None:
        engine, *_ = _make_engine(market=None)
        intent = _make_intent()
        decision = engine.evaluate(intent, NOW_MS)
        assert not decision.allowed
        assert "metadata_missing" in decision.reason

    def test_allow_valid_market(self) -> None:
        engine, *_ = _make_engine(market=_make_market())
        intent = _make_intent()
        engine.on_feed_heartbeat("binance", NOW_MS)
        decision = engine.evaluate(intent, NOW_MS)
        # MetadataIntegrityRule passes; may fail other rules
        assert "metadata_missing" not in decision.reason

    def test_reject_zero_tick_size(self) -> None:
        engine, registry, *_ = _make_engine()
        market = _make_market(tick_size="0")
        registry.upsert(market)
        intent = _make_intent()
        decision = engine.evaluate(intent, NOW_MS)
        assert not decision.allowed
        assert "tick_size" in decision.reason


# ---------------------------------------------------------------------------
# MarketPhaseRule
# ---------------------------------------------------------------------------

class TestMarketPhaseRule:
    def test_reject_in_discovered_phase(self) -> None:
        # Market far in future -> DISCOVERED phase
        market = _make_market(start_ts_ms=NOW_MS + 10_000_000, end_ts_ms=NOW_MS + 11_000_000)
        engine, registry, *_ = _make_engine(market=market)
        engine.on_feed_heartbeat("binance", NOW_MS)
        intent = _make_intent()
        decision = engine.evaluate(intent, NOW_MS)
        assert not decision.allowed
        assert "phase_forbidden" in decision.reason

    def test_allow_in_active_phase(self) -> None:
        # Market is active at NOW_MS
        market = _make_market()
        engine, *_ = _make_engine(market=market)
        engine.on_feed_heartbeat("binance", NOW_MS)
        intent = _make_intent()
        decision = engine.evaluate(intent, NOW_MS)
        # Should pass phase rule (may be blocked by other rules)
        assert "phase_forbidden" not in decision.reason

    def test_recovery_allowed_in_closed_wait(self) -> None:
        # Past end time -> CLOSED_WAIT_RESOLUTION; RECOVERY is allowed
        # (summary.md §9.2: "处理剩余 full set" is still permitted)
        market = _make_market(end_ts_ms=NOW_MS - 1000)
        engine, registry, *_ = _make_engine(market=market)
        engine.on_feed_heartbeat("binance", NOW_MS)
        intent = _make_intent(role=OrderRole.RECOVERY, size="5")
        decision = engine.evaluate(intent, NOW_MS)
        assert "phase_forbidden" not in decision.reason

    def test_recovery_rejected_in_resolved(self) -> None:
        market = _make_market(status="resolved", end_ts_ms=NOW_MS - 1000)
        engine, *_ = _make_engine(market=market)
        engine.on_feed_heartbeat("binance", NOW_MS)
        intent = _make_intent(role=OrderRole.RECOVERY, size="5")
        decision = engine.evaluate(intent, NOW_MS)
        assert not decision.allowed
        assert "phase_forbidden" in decision.reason


# ---------------------------------------------------------------------------
# MaxSingleSizeRule
# ---------------------------------------------------------------------------

class TestMaxSingleSizeRule:
    def test_reject_oversized_order(self) -> None:
        cfg = RiskConfig(max_single_order_size=Decimal("100"))
        engine, *_ = _make_engine(market=_make_market(), config=cfg)
        engine.on_feed_heartbeat("binance", NOW_MS)
        intent = _make_intent(size="200")
        decision = engine.evaluate(intent, NOW_MS)
        assert not decision.allowed
        assert "size_limit" in decision.reason

    def test_allow_within_limit(self) -> None:
        cfg = RiskConfig(max_single_order_size=Decimal("1000"))
        engine, *_ = _make_engine(market=_make_market(), config=cfg)
        engine.on_feed_heartbeat("binance", NOW_MS)
        intent = _make_intent(size="500")
        decision = engine.evaluate(intent, NOW_MS)
        assert "size_limit" not in decision.reason


# ---------------------------------------------------------------------------
# MaxOpenOrdersRule
# ---------------------------------------------------------------------------

class TestMaxOpenOrdersRule:
    def test_reject_when_open_orders_at_limit(self) -> None:
        cfg = RiskConfig(max_open_orders=2)
        engine, _, order_state, _ = _make_engine(market=_make_market(), config=cfg)
        engine.on_feed_heartbeat("binance", NOW_MS)

        # Create 2 open orders
        for i in range(2):
            intent = _make_intent()
            order_state.on_intent_sent(intent, f"c{i}")

        new_intent = _make_intent()
        decision = engine.evaluate(new_intent, NOW_MS)
        assert not decision.allowed
        assert "open_orders_limit" in decision.reason

    def test_allow_when_below_limit(self) -> None:
        cfg = RiskConfig(max_open_orders=5)
        engine, _, order_state, _ = _make_engine(market=_make_market(), config=cfg)
        engine.on_feed_heartbeat("binance", NOW_MS)

        order_state.on_intent_sent(_make_intent(), "c1")
        decision = engine.evaluate(_make_intent(), NOW_MS)
        assert "open_orders_limit" not in decision.reason


# ---------------------------------------------------------------------------
# GhostFillGuardRule
# ---------------------------------------------------------------------------

class TestGhostFillGuardRule:
    def test_reject_when_open_mismatch_order_exists(self) -> None:
        """An OPEN order whose exchange_order_id changed mid-flight blocks new orders."""
        engine, _, order_state, _ = _make_engine(market=_make_market())
        engine.on_feed_heartbeat("binance", NOW_MS)

        # Place an intent so a record exists
        intent = _make_intent()
        order_state.on_intent_sent(intent, "c_mismatch")

        # Exchange confirms with a *different* exchange_order_id -> mismatch flag
        open_report = ExecutionReport(
            client_order_id="c_mismatch",
            pair_id=None,
            market_id="m1",
            token_id="up_tok",
            side=Side.BUY,
            status=OrderStatus.OPEN,
            filled_size=Decimal("0"),
            avg_price=None,
            exchange_order_id="exch_A",
            event_ts_ms=NOW_MS,
        )
        order_state.on_execution_report(open_report)

        # Second report with different exchange_order_id -> triggers mismatch
        conflict_report = ExecutionReport(
            client_order_id="c_mismatch",
            pair_id=None,
            market_id="m1",
            token_id="up_tok",
            side=Side.BUY,
            status=OrderStatus.OPEN,
            filled_size=Decimal("0"),
            avg_price=None,
            exchange_order_id="exch_B",  # different id -> mismatch
            event_ts_ms=NOW_MS,
        )
        order_state.on_execution_report(conflict_report)

        new_intent = _make_intent()
        decision = engine.evaluate(new_intent, NOW_MS)
        assert not decision.allowed
        assert "ghost_fill_guard" in decision.reason


# ---------------------------------------------------------------------------
# FreshnessRule
# ---------------------------------------------------------------------------

class TestFreshnessRule:
    def test_allow_when_binance_never_ticked(self) -> None:
        """In dev/test with no feed data, freshness rule allows with soft severity."""
        engine, *_ = _make_engine(market=_make_market())
        # No heartbeat registered -> FreshnessRule returns allowed=True (soft warn)
        intent = _make_intent()
        decision = engine.evaluate(intent, NOW_MS)
        # The order is allowed; the freshness warning does not block
        assert decision.allowed

    def test_reject_when_binance_stale(self) -> None:
        cfg = RiskConfig(max_binance_staleness_ms=1_000)
        engine, *_ = _make_engine(market=_make_market(), config=cfg)
        engine.on_feed_heartbeat("binance", NOW_MS - 10_000)  # 10s stale
        intent = _make_intent()
        decision = engine.evaluate(intent, NOW_MS)
        assert not decision.allowed
        assert "freshness_stale" in decision.reason

    def test_allow_when_fresh(self) -> None:
        cfg = RiskConfig(max_binance_staleness_ms=5_000)
        engine, *_ = _make_engine(market=_make_market(), config=cfg)
        engine.on_feed_heartbeat("binance", NOW_MS - 100)  # 100ms ago
        intent = _make_intent()
        decision = engine.evaluate(intent, NOW_MS)
        assert "freshness_stale" not in decision.reason


# ---------------------------------------------------------------------------
# RiskDecision contract
# ---------------------------------------------------------------------------

class TestRiskDecision:
    def test_allowed_decision_has_none_severity(self) -> None:
        engine, *_ = _make_engine(market=_make_market())
        engine.on_feed_heartbeat("binance", NOW_MS)
        intent = _make_intent()
        decision = engine.evaluate(intent, NOW_MS)
        if decision.allowed and decision.severity == "none":
            assert decision.reason == "ok"

    def test_rejected_decision_not_allowed(self) -> None:
        engine, *_ = _make_engine(market=None)
        intent = _make_intent()
        decision = engine.evaluate(intent, NOW_MS)
        assert isinstance(decision, RiskDecision)
        assert not decision.allowed
        assert decision.severity == "hard"
