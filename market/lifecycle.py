from __future__ import annotations

from app.config import LifecycleConfig
from core.enums import MarketPhase
from domain.models import LifecycleTransition, MarketMetadata
from market.registry import InMemoryMarketRegistry


class LifecycleManager:
    def __init__(
        self,
        registry: InMemoryMarketRegistry,
        config: LifecycleConfig,
    ) -> None:
        self._registry = registry
        self._config = config
        self._phases: dict[str, MarketPhase] = {}

    def on_market_upsert(self, market: MarketMetadata) -> None:
        current = self._phases.get(market.market_id)
        if current is None:
            self._phases[market.market_id] = MarketPhase.DISCOVERED

    def on_time_tick(self, now_ms: int) -> list[LifecycleTransition]:
        transitions: list[LifecycleTransition] = []
        for market in self._registry.all_markets():
            new_phase = self._phase_for_market(market, now_ms)
            previous_phase = self._phases.get(market.market_id, MarketPhase.DISCOVERED)
            if previous_phase != new_phase:
                self._phases[market.market_id] = new_phase
                transitions.append(
                    LifecycleTransition(
                        market_id=market.market_id,
                        previous_phase=previous_phase,
                        new_phase=new_phase,
                        ts_ms=now_ms,
                    )
                )
        return transitions

    def get_phase(self, market_id: str) -> MarketPhase:
        return self._phases.get(market_id, MarketPhase.DISCOVERED)

    def _phase_for_market(self, market: MarketMetadata, now_ms: int) -> MarketPhase:
        status = market.status.lower()
        if status == "archived":
            return MarketPhase.ARCHIVED
        if status == "resolved":
            return MarketPhase.RESOLVED

        if now_ms < market.start_ts_ms - self._config.prewarm_ms:
            return MarketPhase.DISCOVERED
        if now_ms < market.start_ts_ms:
            return MarketPhase.PREWARM
        if now_ms >= market.end_ts_ms:
            return MarketPhase.CLOSED_WAIT_RESOLUTION

        time_to_close_ms = market.end_ts_ms - now_ms
        if time_to_close_ms <= self._config.final_seconds_ms:
            return MarketPhase.FINAL_SECONDS
        if time_to_close_ms <= self._config.fast_close_ms:
            return MarketPhase.FAST_CLOSE
        return MarketPhase.ACTIVE
