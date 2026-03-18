"""Unit tests for execution.cancel_manager.CancelManager."""

import asyncio
from decimal import Decimal

import pytest

from core.enums import OrderRole, OrderStatus, Side, TimeInForce
from domain.models import ExecutionReport, OrderIntent
from execution.cancel_manager import CancelManager
from execution.polymarket_gateway import PolymarketGateway
from state.order_state import InMemoryOrderStateStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NOW_MS = 1_000_000


def _make_intent(
    market_id: str = "m1",
    token_id: str = "up_tok",
    role: OrderRole = OrderRole.MAKER_QUOTE,
) -> OrderIntent:
    return OrderIntent(
        intent_id="int_1",
        pair_id="pair_1",
        market_id=market_id,
        token_id=token_id,
        side=Side.BUY,
        price=Decimal("0.55"),
        size=Decimal("10"),
        tif=TimeInForce.GTC,
        post_only=True,
        role=role,
        reason="test",
    )


def _open_report(cid: str, market_id: str = "m1", ts_ms: int = NOW_MS) -> ExecutionReport:
    return ExecutionReport(
        client_order_id=cid,
        pair_id=None,
        market_id=market_id,
        token_id="up_tok",
        side=Side.BUY,
        status=OrderStatus.OPEN,
        filled_size=Decimal("0"),
        avg_price=None,
        exchange_order_id=f"exch_{cid}",
        event_ts_ms=ts_ms,
    )


def _make_cm(max_age_ms: int = 30_000) -> tuple[CancelManager, InMemoryOrderStateStore, PolymarketGateway]:
    order_state = InMemoryOrderStateStore()
    gateway = PolymarketGateway(dry_run=True)
    cm = CancelManager(order_state=order_state, gateway=gateway, max_maker_quote_age_ms=max_age_ms)
    return cm, order_state, gateway


# ---------------------------------------------------------------------------
# cancel_stale_quotes
# ---------------------------------------------------------------------------

class TestCancelStaleQuotes:
    @pytest.mark.asyncio
    async def test_no_cancels_when_no_open_orders(self) -> None:
        cm, *_ = _make_cm()
        count = await cm.cancel_stale_quotes("m1", NOW_MS)
        assert count == 0

    @pytest.mark.asyncio
    async def test_no_cancel_for_fresh_order(self) -> None:
        cm, order_state, _ = _make_cm(max_age_ms=30_000)
        intent = _make_intent()
        order_state.on_intent_sent(intent, "c1")
        # Mark as OPEN with timestamp = NOW_MS (fresh)
        order_state.on_execution_report(_open_report("c1", ts_ms=NOW_MS))
        count = await cm.cancel_stale_quotes("m1", NOW_MS)
        assert count == 0

    @pytest.mark.asyncio
    async def test_cancels_stale_maker_quote(self) -> None:
        cm, order_state, _ = _make_cm(max_age_ms=5_000)
        intent = _make_intent()
        order_state.on_intent_sent(intent, "c1")
        # Mark as OPEN 10 seconds ago
        order_state.on_execution_report(_open_report("c1", ts_ms=NOW_MS - 10_000))
        count = await cm.cancel_stale_quotes("m1", NOW_MS)
        assert count == 1

    @pytest.mark.asyncio
    async def test_does_not_cancel_recovery_orders(self) -> None:
        cm, order_state, _ = _make_cm(max_age_ms=5_000)
        recovery_intent = _make_intent(role=OrderRole.RECOVERY)
        order_state.on_intent_sent(recovery_intent, "c1")
        order_state.on_execution_report(_open_report("c1", ts_ms=NOW_MS - 10_000))
        # Only MAKER_QUOTE gets auto-cancelled
        count = await cm.cancel_stale_quotes("m1", NOW_MS)
        assert count == 0

    @pytest.mark.asyncio
    async def test_does_not_cancel_order_with_last_event_ts_zero(self) -> None:
        """Orders that haven't received any execution report yet are skipped."""
        cm, order_state, _ = _make_cm(max_age_ms=0)
        intent = _make_intent()
        order_state.on_intent_sent(intent, "c1")
        # No execution report -> last_event_ts_ms = 0
        count = await cm.cancel_stale_quotes("m1", NOW_MS)
        assert count == 0


# ---------------------------------------------------------------------------
# cancel_all_for_market
# ---------------------------------------------------------------------------

class TestCancelAllForMarket:
    @pytest.mark.asyncio
    async def test_cancels_all_open_orders(self) -> None:
        cm, order_state, _ = _make_cm()
        for cid in ("c1", "c2", "c3"):
            order_state.on_intent_sent(_make_intent(), cid)
            order_state.on_execution_report(_open_report(cid))
        count = await cm.cancel_all_for_market("m1", reason="test_shutdown")
        assert count == 3

    @pytest.mark.asyncio
    async def test_cancel_all_returns_zero_for_empty_market(self) -> None:
        cm, *_ = _make_cm()
        count = await cm.cancel_all_for_market("m1", reason="test")
        assert count == 0

    @pytest.mark.asyncio
    async def test_cancel_all_skips_different_market(self) -> None:
        cm, order_state, _ = _make_cm()
        order_state.on_intent_sent(_make_intent(market_id="m2"), "c1")
        order_state.on_execution_report(_open_report("c1", market_id="m2"))
        # Cancel for "m1" should not touch "m2" orders
        count = await cm.cancel_all_for_market("m1", reason="test")
        assert count == 0


# ---------------------------------------------------------------------------
# cancel_by_pair
# ---------------------------------------------------------------------------

class TestCancelByPair:
    @pytest.mark.asyncio
    async def test_cancels_both_legs_of_pair(self) -> None:
        cm, order_state, _ = _make_cm()
        intent_a = OrderIntent(
            intent_id="int_a",
            pair_id="pair_X",
            market_id="m1",
            token_id="up_tok",
            side=Side.BUY,
            price=Decimal("0.55"),
            size=Decimal("10"),
            tif=TimeInForce.GTC,
            post_only=True,
            role=OrderRole.MAKER_QUOTE,
            reason="test",
        )
        intent_b = OrderIntent(
            intent_id="int_b",
            pair_id="pair_X",
            market_id="m1",
            token_id="dn_tok",
            side=Side.BUY,
            price=Decimal("0.43"),
            size=Decimal("10"),
            tif=TimeInForce.GTC,
            post_only=True,
            role=OrderRole.MAKER_QUOTE,
            reason="test",
        )
        order_state.on_intent_sent(intent_a, "ca")
        order_state.on_intent_sent(intent_b, "cb")
        order_state.on_execution_report(_open_report("ca"))
        order_state.on_execution_report(_open_report("cb"))

        count = await cm.cancel_by_pair("pair_X", reason="test")
        assert count == 2
