from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from domain.models import MarketMetadata


class InMemoryMarketRegistry:
    def __init__(self) -> None:
        self._markets: dict[str, MarketMetadata] = {}

    def upsert(self, market: MarketMetadata) -> None:
        self._markets[market.market_id] = market

    def update_tick_size(
        self,
        market_id: str,
        tick_size: Decimal,
    ) -> MarketMetadata | None:
        market = self._markets.get(market_id)
        if market is None:
            return None
        updated = replace(market, tick_size=tick_size)
        self._markets[market_id] = updated
        return updated

    def update_status(self, market_id: str, status: str) -> MarketMetadata | None:
        market = self._markets.get(market_id)
        if market is None:
            return None
        updated = replace(market, status=status)
        self._markets[market_id] = updated
        return updated

    def get(self, market_id: str) -> MarketMetadata | None:
        return self._markets.get(market_id)

    def get_active(self, now_ms: int) -> MarketMetadata | None:
        active = [
            market
            for market in self._markets.values()
            if market.start_ts_ms <= now_ms < market.end_ts_ms
        ]
        if not active:
            return None
        return sorted(active, key=lambda market: (market.end_ts_ms, market.start_ts_ms))[0]

    def get_next(self, now_ms: int) -> MarketMetadata | None:
        upcoming = [
            market for market in self._markets.values() if market.start_ts_ms > now_ms
        ]
        if not upcoming:
            return None
        return sorted(upcoming, key=lambda market: market.start_ts_ms)[0]

    def all_markets(self) -> list[MarketMetadata]:
        return sorted(
            self._markets.values(),
            key=lambda market: (market.start_ts_ms, market.end_ts_ms, market.market_id),
        )
