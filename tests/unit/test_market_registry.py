"""Unit tests for market.registry.InMemoryMarketRegistry."""

from decimal import Decimal

import pytest

from domain.models import MarketMetadata
from market.registry import InMemoryMarketRegistry


def _make_market(
    market_id: str,
    start_ts_ms: int,
    end_ts_ms: int,
    status: str = "active",
) -> MarketMetadata:
    return MarketMetadata(
        market_id=market_id,
        condition_id=f"cond_{market_id}",
        up_token_id=f"up_{market_id}",
        down_token_id=f"dn_{market_id}",
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
        tick_size=Decimal("0.01"),
        fee_rate_bps=Decimal("0"),
        min_order_size=Decimal("1"),
        status=status,
    )


class TestInMemoryMarketRegistry:
    def test_upsert_and_get(self) -> None:
        registry = InMemoryMarketRegistry()
        market = _make_market("m1", 1000, 2000)
        registry.upsert(market)
        assert registry.get("m1") is market

    def test_get_unknown_returns_none(self) -> None:
        registry = InMemoryMarketRegistry()
        assert registry.get("missing") is None

    def test_upsert_overwrites(self) -> None:
        registry = InMemoryMarketRegistry()
        m1 = _make_market("m1", 1000, 2000)
        m1b = _make_market("m1", 1000, 3000)
        registry.upsert(m1)
        registry.upsert(m1b)
        assert registry.get("m1") is m1b

    def test_get_active_returns_current(self) -> None:
        registry = InMemoryMarketRegistry()
        registry.upsert(_make_market("m1", 1000, 2000))
        assert registry.get_active(1500) is not None
        assert registry.get_active(1500).market_id == "m1"  # type: ignore[union-attr]

    def test_get_active_excludes_future(self) -> None:
        registry = InMemoryMarketRegistry()
        registry.upsert(_make_market("m1", 2000, 3000))
        assert registry.get_active(1000) is None

    def test_get_active_excludes_past(self) -> None:
        registry = InMemoryMarketRegistry()
        registry.upsert(_make_market("m1", 1000, 2000))
        assert registry.get_active(2000) is None  # end is exclusive

    def test_get_next_returns_soonest_future(self) -> None:
        registry = InMemoryMarketRegistry()
        registry.upsert(_make_market("m1", 3000, 4000))
        registry.upsert(_make_market("m2", 5000, 6000))
        nxt = registry.get_next(1000)
        assert nxt is not None
        assert nxt.market_id == "m1"

    def test_get_next_returns_none_if_no_future(self) -> None:
        registry = InMemoryMarketRegistry()
        registry.upsert(_make_market("m1", 1000, 2000))
        assert registry.get_next(5000) is None

    def test_all_markets_sorted(self) -> None:
        registry = InMemoryMarketRegistry()
        registry.upsert(_make_market("m2", 3000, 4000))
        registry.upsert(_make_market("m1", 1000, 2000))
        ids = [m.market_id for m in registry.all_markets()]
        assert ids == ["m1", "m2"]

    def test_all_markets_empty(self) -> None:
        registry = InMemoryMarketRegistry()
        assert registry.all_markets() == []
