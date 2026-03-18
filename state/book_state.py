from __future__ import annotations

from collections import defaultdict

from domain.events import MarketBookTopEvent
from domain.models import OutcomeBookTop, PairBookTop
from market.registry import InMemoryMarketRegistry


class InMemoryBookStateStore:
    def __init__(self, registry: InMemoryMarketRegistry) -> None:
        self._registry = registry
        self._tops: dict[str, dict[str, OutcomeBookTop]] = defaultdict(dict)

    def apply_market_event(self, event: object) -> None:
        if not isinstance(event, MarketBookTopEvent):
            raise TypeError(f"unsupported market event: {type(event)!r}")
        self._tops[event.market_id][event.top.token_id] = event.top

    def get_top(self, market_id: str, token_id: str) -> OutcomeBookTop | None:
        return self._tops.get(market_id, {}).get(token_id)

    def get_pair_top(self, market_id: str) -> PairBookTop | None:
        market = self._registry.get(market_id)
        if market is None:
            return None

        up = self.get_top(market_id, market.up_token_id)
        down = self.get_top(market_id, market.down_token_id)
        if up is None or down is None:
            return None

        return PairBookTop(market_id=market_id, up=up, down=down)
