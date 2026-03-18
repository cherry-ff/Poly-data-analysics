from __future__ import annotations

from decimal import Decimal
from typing import Any, Iterable

from app.config import PolymarketFeedConfig
from core.clock import WallClock
from core.event_bus import InMemoryEventBus
from domain.events import (
    MarketBookTopEvent,
    MarketDiscoveredEvent,
    PolymarketDepthEvent,
    MarketResolvedEvent,
    MarketTickSizeChangeEvent,
)
from domain.models import BookLevel, MarketMetadata, OutcomeBookTop, PolymarketDepthSnapshot
from feeds.base import JsonWebSocketFeed


class PolymarketMarketFeed(JsonWebSocketFeed):
    def __init__(
        self,
        config: PolymarketFeedConfig,
        event_bus: InMemoryEventBus,
        clock: WallClock,
        proxy_url: str | None = None,
    ) -> None:
        self._config = config
        self._market_asset_ids: set[str] = set(config.market_assets_ids)
        self._token_to_market: dict[str, str] = {}
        self._market_ref_to_market: dict[str, str] = {}
        self._top_by_token: dict[str, OutcomeBookTop] = {}
        super().__init__(
            name="polymarket_market",
            ws_url=config.market_ws_url,
            event_bus=event_bus,
            clock=clock,
            subscriptions=[],
            heartbeat_interval_ms=config.heartbeat_interval_ms,
            heartbeat_payload="PING",
            connect_timeout_ms=config.connect_timeout_ms,
            idle_timeout_ms=config.market_idle_timeout_ms,
            retry_initial_delay_ms=config.retry_initial_delay_ms,
            retry_max_delay_ms=config.retry_max_delay_ms,
            retry_backoff=config.retry_backoff,
            proxy_url=proxy_url,
        )

    def startup_skip_reason(self, gamma_base_url: str = "") -> str | None:
        if self._market_asset_ids:
            return None
        if gamma_base_url.strip():
            return None
        return "no market asset ids configured and gamma discovery is unavailable"

    async def subscribe(self) -> None:
        if self._ws is None:
            return
        for payload in self._current_subscriptions():
            await self._ws.send_json(payload)

    def register_market(self, market: MarketMetadata) -> None:
        self._market_ref_to_market[market.market_id] = market.market_id
        self._market_ref_to_market[market.condition_id] = market.market_id
        self._token_to_market[market.up_token_id] = market.market_id
        self._token_to_market[market.down_token_id] = market.market_id
        self._market_asset_ids.update({market.up_token_id, market.down_token_id})

    async def ensure_assets(self, asset_ids: Iterable[str]) -> None:
        new_ids = [asset_id for asset_id in asset_ids if asset_id and asset_id not in self._market_asset_ids]
        if not new_ids:
            return
        self._market_asset_ids.update(new_ids)
        if self._ws is None:
            return
        await self._ws.send_json(
            self._subscription_payload(
                new_ids,
                include_operation=True,
            )
        )
        return None

    async def handle_text(self, raw_text: str) -> None:
        payload = self._loads(raw_text)
        if isinstance(payload, str):
            return
        recv_ts_ms = self._clock.now_ms()
        for item in self._iter_payloads(payload):
            await self._emit_events(item, recv_ts_ms)

    async def _emit_events(self, payload: dict[str, Any], recv_ts_ms: int) -> None:
        event_type = str(
            payload.get("event_type") or payload.get("event") or payload.get("type") or ""
        ).lower()

        if event_type in {"book", "price_change", "best_bid_ask"}:
            emitted = False
            event = self._to_book_event(payload, recv_ts_ms, event_type=event_type)
            if event is not None:
                await self._publish("feeds.polymarket.market.book_top", event)
                emitted = True
            depth_event = self._to_depth_event(payload, recv_ts_ms, event_type)
            if depth_event is not None:
                await self._publish("feeds.polymarket.market.depth", depth_event)
                emitted = True
            if emitted:
                self._mark_data_activity(recv_ts_ms)
            return

        if event_type == "new_market":
            market_id = self._extract_market_id(payload)
            if market_id:
                await self._publish(
                    "feeds.polymarket.market.new_market",
                    MarketDiscoveredEvent(market_id=market_id, discovered_ts_ms=recv_ts_ms),
                )
                self._mark_data_activity(recv_ts_ms)
            return

        if event_type == "tick_size_change":
            market_id = self._extract_market_id(payload)
            tick_size = payload.get("tick_size") or payload.get("minimum_tick_size")
            if market_id and tick_size is not None:
                await self._publish(
                    "feeds.polymarket.market.tick_size",
                    MarketTickSizeChangeEvent(
                        market_id=market_id,
                        tick_size=Decimal(str(tick_size)),
                        event_ts_ms=recv_ts_ms,
                    ),
                )
                self._mark_data_activity(recv_ts_ms)
            return

        if event_type == "market_resolved":
            market_id = self._extract_market_id(payload)
            if market_id:
                await self._publish(
                    "feeds.polymarket.market.resolved",
                    MarketResolvedEvent(
                        market_id=market_id,
                        resolved_ts_ms=recv_ts_ms,
                    ),
                )
                self._mark_data_activity(recv_ts_ms)

    def _to_book_event(
        self,
        payload: dict[str, Any],
        recv_ts_ms: int,
        *,
        event_type: str = "",
    ) -> MarketBookTopEvent | None:
        market_id = self._extract_market_id(payload)
        token_id_value = self._first_present(
            payload.get("token_id"),
            payload.get("tokenId"),
            payload.get("asset_id"),
            payload.get("assetId"),
        )
        token_id = str(token_id_value or "")
        if not market_id or not token_id:
            return None

        previous = self._top_by_token.get(token_id)
        bids = self._levels(payload.get("bids"), descending=True)
        asks = self._levels(payload.get("asks"), descending=False)
        best_bid = (bids[0].price, bids[0].size) if bids else None
        best_ask = (asks[0].price, asks[0].size) if asks else None

        bid_px = self._to_decimal(
            self._first_present(
                payload.get("bid"),
                payload.get("best_bid"),
                payload.get("bestBid"),
            )
        )
        ask_px = self._to_decimal(
            self._first_present(
                payload.get("ask"),
                payload.get("best_ask"),
                payload.get("bestAsk"),
            )
        )
        bid_sz = self._to_decimal(
            self._first_present(
                payload.get("bid_size"),
                payload.get("best_bid_size"),
                payload.get("bestBidSize"),
            )
        )
        ask_sz = self._to_decimal(
            self._first_present(
                payload.get("ask_size"),
                payload.get("best_ask_size"),
                payload.get("bestAskSize"),
            )
        )

        if best_bid is not None:
            bid_px, bid_sz = best_bid
        elif isinstance(payload.get("bids"), list) and event_type == "book":
            bid_px = bid_px if bid_px is not None else Decimal("0")
            bid_sz = bid_sz if bid_sz is not None else Decimal("0")
        if best_ask is not None:
            ask_px, ask_sz = best_ask
        elif isinstance(payload.get("asks"), list) and event_type == "book":
            ask_px = ask_px if ask_px is not None else Decimal("1")
            ask_sz = ask_sz if ask_sz is not None else Decimal("0")

        if previous is not None:
            if bid_px is None:
                bid_px = previous.best_bid_px
            if ask_px is None:
                ask_px = previous.best_ask_px
            if bid_sz is None:
                bid_sz = previous.best_bid_sz
            if ask_sz is None:
                ask_sz = previous.best_ask_sz

        if bid_px is None or ask_px is None:
            return None

        top = OutcomeBookTop(
            token_id=token_id,
            best_bid_px=bid_px,
            best_bid_sz=bid_sz or Decimal("0"),
            best_ask_px=ask_px,
            best_ask_sz=ask_sz or Decimal("0"),
            last_update_ts_ms=recv_ts_ms,
        )
        self._top_by_token[token_id] = top
        return MarketBookTopEvent(market_id=market_id, top=top)

    def _to_depth_event(
        self,
        payload: dict[str, Any],
        recv_ts_ms: int,
        event_type: str,
    ) -> PolymarketDepthEvent | None:
        if event_type != "book":
            return None

        market_id = self._extract_market_id(payload)
        token_id = str(
            payload.get("token_id")
            or payload.get("tokenId")
            or payload.get("asset_id")
            or payload.get("assetId")
            or ""
        )
        if not market_id or not token_id:
            return None

        bids = self._levels(payload.get("bids"), descending=True)
        asks = self._levels(payload.get("asks"), descending=False)
        if not bids and not asks:
            return None

        snapshot = PolymarketDepthSnapshot(
            market_id=market_id,
            token_id=token_id,
            event_type=event_type,
            last_update_ts_ms=recv_ts_ms,
            bids=bids,
            asks=asks,
        )
        return PolymarketDepthEvent(snapshot=snapshot)

    @staticmethod
    def _iter_payloads(payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list):
                return [item for item in data if isinstance(item, dict)]
            if isinstance(data, dict):
                return [data]
            return [payload]
        return []

    @staticmethod
    def build_subscriptions(config: PolymarketFeedConfig) -> list[dict[str, Any]]:
        if not config.market_assets_ids:
            return []
        return [
            PolymarketMarketFeed._subscription_payload_for_config(
                config,
                list(config.market_assets_ids),
                include_operation=False,
            )
        ]

    def _current_subscriptions(self) -> list[dict[str, Any]]:
        subscriptions: list[dict[str, Any]] = []
        if self._market_asset_ids:
            subscriptions.append(
                self._subscription_payload(
                    sorted(self._market_asset_ids),
                    include_operation=False,
                )
            )
        return subscriptions

    def _subscription_payload(
        self,
        asset_ids: list[str],
        *,
        include_operation: bool,
    ) -> dict[str, Any]:
        return self._subscription_payload_for_config(
            self._config,
            asset_ids,
            include_operation=include_operation,
        )

    @staticmethod
    def _subscription_payload_for_config(
        config: PolymarketFeedConfig,
        asset_ids: list[str],
        *,
        include_operation: bool,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "type": "market",
            "assets_ids": asset_ids,
        }
        if config.market_initial_dump:
            payload["initial_dump"] = True
        if config.market_custom_features_enabled:
            payload["custom_feature_enabled"] = True
        if include_operation:
            payload["operation"] = "subscribe"
        return payload

    def _extract_market_id(self, payload: dict[str, Any]) -> str | None:
        value = payload.get("market_id") or payload.get("market") or payload.get("id")
        if isinstance(value, dict):
            value = value.get("id") or value.get("market_id")
        if value not in (None, ""):
            value_str = str(value)
            return self._market_ref_to_market.get(value_str, value_str)

        token_id = (
            payload.get("token_id")
            or payload.get("tokenId")
            or payload.get("asset_id")
            or payload.get("assetId")
        )
        if token_id in (None, ""):
            return None
        return self._token_to_market.get(str(token_id))

    @staticmethod
    def _levels(levels: Any, *, descending: bool) -> tuple[BookLevel, ...]:
        if not isinstance(levels, list):
            return ()

        normalized: list[BookLevel] = []
        for level in levels:
            if isinstance(level, dict):
                price = level.get("price") or level.get("px")
                size = level.get("size") or level.get("sz")
            elif isinstance(level, (list, tuple)) and len(level) >= 2:
                price, size = level[0], level[1]
            else:
                continue
            if price is None:
                continue
            normalized.append(
                BookLevel(
                    price=Decimal(str(price)),
                    size=Decimal(str(size or "0")),
                )
            )
        normalized.sort(key=lambda level: level.price, reverse=descending)
        return tuple(normalized[:5])

    @staticmethod
    def _to_decimal(value: Any) -> Decimal | None:
        if value is None:
            return None
        return Decimal(str(value))

    @staticmethod
    def _first_present(*values: Any) -> Any:
        for value in values:
            if value not in (None, ""):
                return value
        return None
