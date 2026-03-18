"""Unit tests for state.book_state.InMemoryBookStateStore."""

from decimal import Decimal

import pytest

from domain.events import MarketBookTopEvent
from domain.models import MarketMetadata, OutcomeBookTop
from market.registry import InMemoryMarketRegistry
from state.book_state import InMemoryBookStateStore


def _make_market(market_id: str, up_token: str, down_token: str) -> MarketMetadata:
    return MarketMetadata(
        market_id=market_id,
        condition_id=f"cond_{market_id}",
        up_token_id=up_token,
        down_token_id=down_token,
        start_ts_ms=1000,
        end_ts_ms=1_000_000,
        tick_size=Decimal("0.01"),
        fee_rate_bps=Decimal("0"),
        min_order_size=Decimal("1"),
        status="active",
    )


def _make_top(token_id: str, bid: str, ask: str) -> OutcomeBookTop:
    return OutcomeBookTop(
        token_id=token_id,
        best_bid_px=Decimal(bid),
        best_bid_sz=Decimal("10"),
        best_ask_px=Decimal(ask),
        best_ask_sz=Decimal("10"),
        last_update_ts_ms=1000,
    )


class TestInMemoryBookStateStore:
    def _setup(self) -> tuple[InMemoryBookStateStore, InMemoryMarketRegistry]:
        registry = InMemoryMarketRegistry()
        store = InMemoryBookStateStore(registry=registry)
        return store, registry

    def test_get_top_initially_none(self) -> None:
        store, _ = self._setup()
        assert store.get_top("m1", "up_token") is None

    def test_apply_event_and_get_top(self) -> None:
        store, _ = self._setup()
        top = _make_top("up_token", "0.55", "0.57")
        store.apply_market_event(MarketBookTopEvent(market_id="m1", top=top))
        result = store.get_top("m1", "up_token")
        assert result is not None
        assert result.best_bid_px == Decimal("0.55")

    def test_apply_event_overwrites(self) -> None:
        store, _ = self._setup()
        top1 = _make_top("up_token", "0.55", "0.57")
        top2 = _make_top("up_token", "0.60", "0.62")
        store.apply_market_event(MarketBookTopEvent(market_id="m1", top=top1))
        store.apply_market_event(MarketBookTopEvent(market_id="m1", top=top2))
        result = store.get_top("m1", "up_token")
        assert result is not None
        assert result.best_bid_px == Decimal("0.60")

    def test_apply_unknown_event_raises(self) -> None:
        store, _ = self._setup()
        with pytest.raises(TypeError):
            store.apply_market_event("not_an_event")  # type: ignore[arg-type]

    def test_get_pair_top_none_when_registry_missing(self) -> None:
        store, _ = self._setup()
        # registry has no market registered
        assert store.get_pair_top("m1") is None

    def test_get_pair_top_none_when_one_leg_missing(self) -> None:
        store, registry = self._setup()
        market = _make_market("m1", "up_token", "dn_token")
        registry.upsert(market)
        # only UP side has data
        top_up = _make_top("up_token", "0.55", "0.57")
        store.apply_market_event(MarketBookTopEvent(market_id="m1", top=top_up))
        assert store.get_pair_top("m1") is None

    def test_get_pair_top_returns_both_legs(self) -> None:
        store, registry = self._setup()
        market = _make_market("m1", "up_token", "dn_token")
        registry.upsert(market)
        top_up = _make_top("up_token", "0.55", "0.57")
        top_dn = _make_top("dn_token", "0.43", "0.45")
        store.apply_market_event(MarketBookTopEvent(market_id="m1", top=top_up))
        store.apply_market_event(MarketBookTopEvent(market_id="m1", top=top_dn))
        pair = store.get_pair_top("m1")
        assert pair is not None
        assert pair.up.token_id == "up_token"
        assert pair.down.token_id == "dn_token"

    def test_pair_top_sum_best_ask(self) -> None:
        store, registry = self._setup()
        market = _make_market("m1", "up_token", "dn_token")
        registry.upsert(market)
        top_up = _make_top("up_token", "0.55", "0.57")
        top_dn = _make_top("dn_token", "0.43", "0.45")
        store.apply_market_event(MarketBookTopEvent(market_id="m1", top=top_up))
        store.apply_market_event(MarketBookTopEvent(market_id="m1", top=top_dn))
        pair = store.get_pair_top("m1")
        assert pair is not None
        # 0.57 + 0.45 = 1.02
        assert pair.sum_best_ask == Decimal("1.02")

    def test_pair_top_sum_best_bid(self) -> None:
        store, registry = self._setup()
        market = _make_market("m1", "up_token", "dn_token")
        registry.upsert(market)
        top_up = _make_top("up_token", "0.55", "0.57")
        top_dn = _make_top("dn_token", "0.43", "0.45")
        store.apply_market_event(MarketBookTopEvent(market_id="m1", top=top_up))
        store.apply_market_event(MarketBookTopEvent(market_id="m1", top=top_dn))
        pair = store.get_pair_top("m1")
        assert pair is not None
        # 0.55 + 0.43 = 0.98
        assert pair.sum_best_bid == Decimal("0.98")
