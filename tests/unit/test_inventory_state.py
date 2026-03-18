"""Unit tests for state.inventory_state.InMemoryInventoryStore."""

from decimal import Decimal

import pytest

from core.enums import OrderStatus, Side
from domain.models import ExecutionReport, MarketMetadata
from market.registry import InMemoryMarketRegistry
from state.inventory_state import InMemoryInventoryStore


def _make_market(market_id: str = "m1") -> MarketMetadata:
    return MarketMetadata(
        market_id=market_id,
        condition_id=f"cond_{market_id}",
        up_token_id="up_token",
        down_token_id="dn_token",
        start_ts_ms=1000,
        end_ts_ms=1_000_000,
        tick_size=Decimal("0.01"),
        fee_rate_bps=Decimal("0"),
        min_order_size=Decimal("1"),
        status="active",
    )


def _fill(
    client_order_id: str,
    market_id: str,
    token_id: str,
    side: Side,
    filled_size: str,
    avg_price: str,
) -> ExecutionReport:
    return ExecutionReport(
        client_order_id=client_order_id,
        pair_id=None,
        market_id=market_id,
        token_id=token_id,
        side=side,
        status=OrderStatus.FILLED,
        filled_size=Decimal(filled_size),
        avg_price=Decimal(avg_price),
        exchange_order_id=None,
        event_ts_ms=1000,
    )


class TestInMemoryInventoryStore:
    def _setup(self, register: bool = True) -> tuple[InMemoryInventoryStore, InMemoryMarketRegistry]:
        registry = InMemoryMarketRegistry()
        if register:
            registry.upsert(_make_market())
        store = InMemoryInventoryStore(registry=registry)
        return store, registry

    def test_initial_inventory_empty(self) -> None:
        store, _ = self._setup()
        assert store.get_inventory("m1") == []

    def test_buy_fill_creates_position(self) -> None:
        store, _ = self._setup()
        store.on_fill(_fill("c1", "m1", "up_token", Side.BUY, "10", "0.55"))
        inventory = store.get_inventory("m1")
        assert len(inventory) == 1
        assert inventory[0].token_id == "up_token"
        assert inventory[0].net_size == Decimal("10")

    def test_sell_fill_reduces_position(self) -> None:
        store, _ = self._setup()
        store.on_fill(_fill("c1", "m1", "up_token", Side.BUY, "10", "0.55"))
        store.on_fill(_fill("c2", "m1", "up_token", Side.SELL, "6", "0.57"))
        inventory = store.get_inventory("m1")
        assert len(inventory) == 1
        assert inventory[0].net_size == Decimal("4")

    def test_sell_fill_removes_fully_consumed_position(self) -> None:
        store, _ = self._setup()
        store.on_fill(_fill("c1", "m1", "up_token", Side.BUY, "10", "0.55"))
        store.on_fill(_fill("c2", "m1", "up_token", Side.SELL, "10", "0.57"))
        inventory = store.get_inventory("m1")
        assert inventory == []

    def test_duplicate_fill_is_idempotent(self) -> None:
        """Replaying the same ExecutionReport must not double-count inventory."""
        store, _ = self._setup()
        report = _fill("c1", "m1", "up_token", Side.BUY, "10", "0.55")
        store.on_fill(report)
        store.on_fill(report)  # duplicate
        inventory = store.get_inventory("m1")
        assert inventory[0].net_size == Decimal("10")

    def test_fill_with_no_avg_price_is_ignored(self) -> None:
        store, _ = self._setup()
        report = ExecutionReport(
            client_order_id="c1",
            pair_id=None,
            market_id="m1",
            token_id="up_token",
            side=Side.BUY,
            status=OrderStatus.FILLED,
            filled_size=Decimal("10"),
            avg_price=None,  # missing price
            exchange_order_id=None,
            event_ts_ms=1000,
        )
        store.on_fill(report)
        assert store.get_inventory("m1") == []

    def test_get_pair_cost_none_if_market_not_registered(self) -> None:
        store, _ = self._setup(register=False)
        store.on_fill(_fill("c1", "m1", "up_token", Side.BUY, "10", "0.55"))
        assert store.get_pair_cost("m1") is None

    def test_get_pair_cost_none_if_one_leg_missing(self) -> None:
        store, _ = self._setup()
        store.on_fill(_fill("c1", "m1", "up_token", Side.BUY, "10", "0.55"))
        assert store.get_pair_cost("m1") is None

    def test_get_pair_cost_returns_sum_of_avg_costs(self) -> None:
        store, _ = self._setup()
        store.on_fill(_fill("c1", "m1", "up_token", Side.BUY, "10", "0.55"))
        store.on_fill(_fill("c2", "m1", "dn_token", Side.BUY, "10", "0.43"))
        cost = store.get_pair_cost("m1")
        assert cost is not None
        assert cost == Decimal("0.98")

    def test_unhedged_exposure_zero_when_balanced(self) -> None:
        store, _ = self._setup()
        store.on_fill(_fill("c1", "m1", "up_token", Side.BUY, "10", "0.55"))
        store.on_fill(_fill("c2", "m1", "dn_token", Side.BUY, "10", "0.43"))
        assert store.get_unhedged_exposure("m1") == Decimal("0")

    def test_unhedged_exposure_nonzero_when_imbalanced(self) -> None:
        store, _ = self._setup()
        store.on_fill(_fill("c1", "m1", "up_token", Side.BUY, "10", "0.55"))
        store.on_fill(_fill("c2", "m1", "dn_token", Side.BUY, "6", "0.43"))
        assert store.get_unhedged_exposure("m1") == Decimal("4")

    def test_unhedged_exposure_zero_for_unknown_market(self) -> None:
        store, _ = self._setup(register=False)
        assert store.get_unhedged_exposure("m1") == Decimal("0")
