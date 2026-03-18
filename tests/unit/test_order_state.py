"""Unit tests for state.order_state.InMemoryOrderStateStore."""

from decimal import Decimal

import pytest

from core.enums import OrderRole, OrderStatus, Side, TimeInForce
from domain.models import ExecutionReport, OrderIntent
from state.order_state import InMemoryOrderStateStore


def _make_intent(
    intent_id: str = "int_1",
    market_id: str = "m1",
    token_id: str = "up_token",
    pair_id: str | None = "pair_1",
) -> OrderIntent:
    return OrderIntent(
        intent_id=intent_id,
        pair_id=pair_id,
        market_id=market_id,
        token_id=token_id,
        side=Side.BUY,
        price=Decimal("0.55"),
        size=Decimal("10"),
        tif=TimeInForce.GTC,
        post_only=True,
        role=OrderRole.MAKER_QUOTE,
        reason="test",
    )


def _make_report(
    client_order_id: str,
    status: OrderStatus,
    market_id: str = "m1",
    token_id: str = "up_token",
    pair_id: str | None = "pair_1",
    filled_size: str = "0",
    avg_price: str | None = None,
    exchange_order_id: str | None = "exch_1",
) -> ExecutionReport:
    return ExecutionReport(
        client_order_id=client_order_id,
        pair_id=pair_id,
        market_id=market_id,
        token_id=token_id,
        side=Side.BUY,
        status=status,
        filled_size=Decimal(filled_size),
        avg_price=Decimal(avg_price) if avg_price else None,
        exchange_order_id=exchange_order_id,
        event_ts_ms=1000,
    )


class TestInMemoryOrderStateStore:
    def test_intent_creates_pending_record(self) -> None:
        store = InMemoryOrderStateStore()
        intent = _make_intent()
        store.on_intent_sent(intent, "clord_1")
        record = store.get("clord_1")
        assert record is not None
        assert record.status == OrderStatus.PENDING_SUBMIT

    def test_execution_report_updates_record(self) -> None:
        store = InMemoryOrderStateStore()
        intent = _make_intent()
        store.on_intent_sent(intent, "clord_1")
        report = _make_report("clord_1", OrderStatus.OPEN)
        store.on_execution_report(report)
        record = store.get("clord_1")
        assert record is not None
        assert record.status == OrderStatus.OPEN

    def test_fill_report_updates_filled_size(self) -> None:
        store = InMemoryOrderStateStore()
        intent = _make_intent()
        store.on_intent_sent(intent, "clord_1")
        report = _make_report("clord_1", OrderStatus.FILLED, filled_size="10", avg_price="0.55")
        store.on_execution_report(report)
        record = store.get("clord_1")
        assert record is not None
        assert record.filled_size == Decimal("10")
        assert record.avg_price == Decimal("0.55")

    def test_ghost_fill_creates_mismatch_record(self) -> None:
        """A report arriving without a prior intent should be flagged as mismatch."""
        store = InMemoryOrderStateStore()
        report = _make_report("ghost_1", OrderStatus.FILLED, filled_size="5", avg_price="0.55")
        store.on_execution_report(report)
        record = store.get("ghost_1")
        assert record is not None
        assert record.mismatch is True

    def test_exchange_id_conflict_sets_mismatch(self) -> None:
        store = InMemoryOrderStateStore()
        intent = _make_intent()
        store.on_intent_sent(intent, "clord_1")
        # First report sets exchange_order_id
        store.on_execution_report(
            _make_report("clord_1", OrderStatus.OPEN, exchange_order_id="exch_A")
        )
        # Second report arrives with a different exchange_order_id
        store.on_execution_report(
            _make_report("clord_1", OrderStatus.FILLED, exchange_order_id="exch_B", filled_size="10", avg_price="0.55")
        )
        record = store.get("clord_1")
        assert record is not None
        assert record.mismatch is True

    def test_intent_sent_indexes_exchange_order_id(self) -> None:
        store = InMemoryOrderStateStore()
        intent = _make_intent()
        store.on_intent_sent(intent, "clord_1", exchange_order_id="exch_live_1")
        record = store.get_by_exchange_order_id("exch_live_1")
        assert record is not None
        assert record.client_order_id == "clord_1"

    def test_execution_report_can_match_existing_record_by_exchange_order_id(self) -> None:
        store = InMemoryOrderStateStore()
        intent = _make_intent()
        store.on_intent_sent(intent, "clord_1", exchange_order_id="exch_live_1")
        report = _make_report(
            "exch_live_1",
            OrderStatus.OPEN,
            exchange_order_id="exch_live_1",
        )
        store.on_execution_report(report)
        record = store.get("clord_1")
        assert record is not None
        assert record.status == OrderStatus.OPEN
        assert record.exchange_order_id == "exch_live_1"

    def test_get_open_orders_excludes_terminal(self) -> None:
        store = InMemoryOrderStateStore()
        for cid, status in [("c1", OrderStatus.OPEN), ("c2", OrderStatus.FILLED), ("c3", OrderStatus.CANCELED)]:
            intent = _make_intent(intent_id=f"int_{cid}")
            store.on_intent_sent(intent, cid)
            store.on_execution_report(_make_report(cid, status))
        open_orders = store.get_open_orders("m1")
        assert len(open_orders) == 1
        assert open_orders[0].client_order_id == "c1"

    def test_get_pair_orders(self) -> None:
        store = InMemoryOrderStateStore()
        for cid in ("c1", "c2"):
            intent = _make_intent(intent_id=f"int_{cid}", pair_id="pair_X")
            store.on_intent_sent(intent, cid)
        pair_orders = store.get_pair_orders("pair_X")
        assert {o.client_order_id for o in pair_orders} == {"c1", "c2"}

    def test_get_pair_orders_empty_for_unknown_pair(self) -> None:
        store = InMemoryOrderStateStore()
        assert store.get_pair_orders("unknown_pair") == []

    def test_terminal_status_flip_marks_mismatch(self) -> None:
        """If a record is already FILLED and then CANCELED arrives, flag mismatch."""
        store = InMemoryOrderStateStore()
        intent = _make_intent()
        store.on_intent_sent(intent, "clord_1")
        store.on_execution_report(_make_report("clord_1", OrderStatus.FILLED, filled_size="10", avg_price="0.55"))
        store.on_execution_report(_make_report("clord_1", OrderStatus.CANCELED))
        record = store.get("clord_1")
        assert record is not None
        assert record.mismatch is True
