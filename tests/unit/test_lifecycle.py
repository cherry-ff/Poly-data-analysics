"""Unit tests for market.lifecycle.LifecycleManager."""

from decimal import Decimal

import pytest

from app.config import LifecycleConfig
from core.enums import MarketPhase
from domain.models import MarketMetadata
from market.lifecycle import LifecycleManager
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


def _make_lm() -> tuple[LifecycleManager, InMemoryMarketRegistry]:
    registry = InMemoryMarketRegistry()
    config = LifecycleConfig(
        prewarm_ms=60_000,
        fast_close_ms=30_000,
        final_seconds_ms=5_000,
    )
    return LifecycleManager(registry=registry, config=config), registry


class TestLifecycleManager:
    def test_initial_phase_is_discovered(self) -> None:
        lm, registry = _make_lm()
        market = _make_market("m1", 100_000, 1_000_000)
        registry.upsert(market)
        lm.on_market_upsert(market)
        assert lm.get_phase("m1") == MarketPhase.DISCOVERED

    def test_upsert_twice_does_not_reset(self) -> None:
        lm, registry = _make_lm()
        market = _make_market("m1", 100_000, 1_000_000)
        registry.upsert(market)
        lm.on_market_upsert(market)
        lm.on_market_upsert(market)
        assert lm.get_phase("m1") == MarketPhase.DISCOVERED

    def test_transitions_to_prewarm(self) -> None:
        lm, registry = _make_lm()
        start = 100_000
        end = start + 15 * 60_000
        market = _make_market("m1", start, end)
        registry.upsert(market)
        lm.on_market_upsert(market)

        # 30 seconds before start -> within prewarm window (60s)
        transitions = lm.on_time_tick(start - 30_000)
        phases = {t.new_phase for t in transitions}
        assert MarketPhase.PREWARM in phases
        assert lm.get_phase("m1") == MarketPhase.PREWARM

    def test_transitions_to_active(self) -> None:
        lm, registry = _make_lm()
        start = 100_000
        end = start + 15 * 60_000
        market = _make_market("m1", start, end)
        registry.upsert(market)
        lm.on_market_upsert(market)

        # just after start and well before fast_close window
        transitions = lm.on_time_tick(start + 1_000)
        phases = {t.new_phase for t in transitions}
        assert MarketPhase.ACTIVE in phases

    def test_transitions_to_fast_close(self) -> None:
        lm, registry = _make_lm()
        start = 100_000
        end = start + 15 * 60_000
        market = _make_market("m1", start, end)
        registry.upsert(market)
        lm.on_market_upsert(market)

        # 20 seconds before end (within fast_close=30s, outside final=5s)
        transitions = lm.on_time_tick(end - 20_000)
        phases = {t.new_phase for t in transitions}
        assert MarketPhase.FAST_CLOSE in phases

    def test_transitions_to_final_seconds(self) -> None:
        lm, registry = _make_lm()
        start = 100_000
        end = start + 15 * 60_000
        market = _make_market("m1", start, end)
        registry.upsert(market)
        lm.on_market_upsert(market)

        # 3 seconds before end (within final_seconds=5s)
        transitions = lm.on_time_tick(end - 3_000)
        phases = {t.new_phase for t in transitions}
        assert MarketPhase.FINAL_SECONDS in phases

    def test_transitions_to_closed_wait(self) -> None:
        lm, registry = _make_lm()
        start = 100_000
        end = start + 15 * 60_000
        market = _make_market("m1", start, end)
        registry.upsert(market)
        lm.on_market_upsert(market)

        transitions = lm.on_time_tick(end + 1_000)
        phases = {t.new_phase for t in transitions}
        assert MarketPhase.CLOSED_WAIT_RESOLUTION in phases

    def test_resolved_status(self) -> None:
        lm, registry = _make_lm()
        start = 100_000
        end = start + 15 * 60_000
        market = _make_market("m1", start, end, status="resolved")
        registry.upsert(market)
        lm.on_market_upsert(market)

        transitions = lm.on_time_tick(end + 1_000)
        phases = {t.new_phase for t in transitions}
        assert MarketPhase.RESOLVED in phases

    def test_no_transition_when_already_correct_phase(self) -> None:
        lm, registry = _make_lm()
        start = 100_000
        end = start + 15 * 60_000
        market = _make_market("m1", start, end)
        registry.upsert(market)
        lm.on_market_upsert(market)

        # first tick drives to active
        lm.on_time_tick(start + 1_000)
        # second tick at same window -> no new transitions
        transitions = lm.on_time_tick(start + 2_000)
        assert transitions == []

    def test_transition_record_contains_market_id(self) -> None:
        lm, registry = _make_lm()
        start = 100_000
        end = start + 15 * 60_000
        market = _make_market("m1", start, end)
        registry.upsert(market)
        lm.on_market_upsert(market)

        transitions = lm.on_time_tick(start + 1_000)
        assert all(t.market_id == "m1" for t in transitions)
