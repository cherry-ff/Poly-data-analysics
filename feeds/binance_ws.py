from __future__ import annotations

from contextlib import suppress
from decimal import Decimal
from typing import Any

from app.config import BinanceFeedConfig
from core.clock import WallClock
from core.event_bus import InMemoryEventBus
from domain.events import BinanceDepthEvent, BinanceTickEvent
from domain.models import BinanceDepthSnapshot, BinanceTick, BookLevel
from feeds.base import JsonWebSocketFeed


class BinanceBookTickerFeed(JsonWebSocketFeed):
    def __init__(
        self,
        config: BinanceFeedConfig,
        event_bus: InMemoryEventBus,
        clock: WallClock,
        proxy_url: str | None = None,
    ) -> None:
        subscriptions = None
        heartbeat_interval_ms = None
        heartbeat_payload = None
        if _is_okx_ws_url(config.ws_url):
            subscriptions = [
                {
                    "op": "subscribe",
                    "args": [
                        {
                            "channel": "tickers",
                            "instId": config.symbol,
                        }
                    ],
                }
            ]
            heartbeat_interval_ms = 25_000
            heartbeat_payload = "ping"
        super().__init__(
            name="binance_book_ticker",
            ws_url=config.ws_url,
            event_bus=event_bus,
            clock=clock,
            subscriptions=subscriptions,
            heartbeat_interval_ms=heartbeat_interval_ms,
            heartbeat_payload=heartbeat_payload,
            connect_timeout_ms=config.connect_timeout_ms,
            idle_timeout_ms=config.idle_timeout_ms,
            retry_initial_delay_ms=config.retry_initial_delay_ms,
            retry_max_delay_ms=config.retry_max_delay_ms,
            retry_backoff=config.retry_backoff,
            proxy_url=proxy_url,
        )
        self._config = config

    async def handle_text(self, raw_text: str) -> None:
        payload = self._loads(raw_text)
        tick = self._normalize(payload, self._clock.now_ms())
        if tick is None:
            return
        self._mark_data_activity(tick.recv_ts_ms)
        await self._publish("feeds.binance.tick", BinanceTickEvent(tick=tick))

    def _normalize(self, payload: Any, recv_ts_ms: int) -> BinanceTick | None:
        if not isinstance(payload, dict):
            return None
        if payload.get("event") in {"subscribe", "unsubscribe"}:
            return None

        data = payload.get("data")
        if isinstance(data, list):
            if not data:
                return None
            item = data[0]
            if not isinstance(item, dict):
                return None
            payload = item
        if isinstance(data, dict):
            payload = data

        if "result" in payload:
            return None

        if "bidPx" in payload or "askPx" in payload:
            symbol = str(payload.get("instId", self._config.symbol))
            best_bid = self._to_decimal(payload.get("bidPx"))
            best_ask = self._to_decimal(payload.get("askPx"))
            last_price = self._to_decimal(payload.get("last"))
            event_ts_ms = int(payload.get("ts") or recv_ts_ms)
        else:
            symbol = str(payload.get("s", self._config.symbol))
            best_bid = self._to_decimal(payload.get("b"))
            best_ask = self._to_decimal(payload.get("a"))
            last_price = self._to_decimal(
                payload.get("c") or payload.get("p") or payload.get("lastPrice")
            )
            event_ts_ms = int(payload.get("E") or payload.get("T") or recv_ts_ms)

        if best_bid is None or best_ask is None:
            return None
        if last_price is None:
            last_price = (best_bid + best_ask) / Decimal("2")

        return BinanceTick(
            symbol=symbol,
            event_ts_ms=event_ts_ms,
            recv_ts_ms=recv_ts_ms,
            last_price=last_price,
            best_bid=best_bid,
            best_ask=best_ask,
        )

    @staticmethod
    def _to_decimal(value: Any) -> Decimal | None:
        if value is None:
            return None
        return Decimal(str(value))


class BinanceDepthFeed(JsonWebSocketFeed):
    def __init__(
        self,
        config: BinanceFeedConfig,
        event_bus: InMemoryEventBus,
        clock: WallClock,
        proxy_url: str | None = None,
    ) -> None:
        subscriptions = None
        heartbeat_interval_ms = None
        heartbeat_payload = None
        if _is_okx_ws_url(config.depth_ws_url):
            subscriptions = [
                {
                    "op": "subscribe",
                    "args": [
                        {
                            "channel": "books5",
                            "instId": config.symbol,
                        }
                    ],
                }
            ]
            heartbeat_interval_ms = 25_000
            heartbeat_payload = "ping"
        super().__init__(
            name="binance_depth",
            ws_url=config.depth_ws_url,
            event_bus=event_bus,
            clock=clock,
            subscriptions=subscriptions,
            heartbeat_interval_ms=heartbeat_interval_ms,
            heartbeat_payload=heartbeat_payload,
            connect_timeout_ms=config.connect_timeout_ms,
            idle_timeout_ms=config.idle_timeout_ms,
            retry_initial_delay_ms=config.retry_initial_delay_ms,
            retry_max_delay_ms=config.retry_max_delay_ms,
            retry_backoff=config.retry_backoff,
            proxy_url=proxy_url,
        )
        self._config = config

    async def handle_text(self, raw_text: str) -> None:
        payload = self._loads(raw_text)
        snapshot = self._normalize(payload, self._clock.now_ms())
        if snapshot is None:
            return
        self._mark_data_activity(snapshot.recv_ts_ms)
        await self._publish(
            "feeds.binance.depth",
            BinanceDepthEvent(snapshot=snapshot),
        )

    def _normalize(self, payload: Any, recv_ts_ms: int) -> BinanceDepthSnapshot | None:
        if not isinstance(payload, dict):
            return None
        if payload.get("event") in {"subscribe", "unsubscribe"}:
            return None

        data = payload.get("data")
        if isinstance(data, list):
            if not data:
                return None
            item = data[0]
            if not isinstance(item, dict):
                return None
            payload = item
        if isinstance(data, dict):
            payload = data

        if "result" in payload:
            return None

        bids = self._normalize_levels(payload.get("bids") or payload.get("b"))
        asks = self._normalize_levels(payload.get("asks") or payload.get("a"))
        if not bids and not asks:
            return None

        event_ts_ms = int(payload.get("ts") or payload.get("E") or payload.get("T") or recv_ts_ms)
        last_update_id = payload.get("lastUpdateId") or payload.get("u") or payload.get("seqId")
        if last_update_id is not None:
            with suppress(ValueError, TypeError):
                last_update_id = int(last_update_id)

        return BinanceDepthSnapshot(
            symbol=str(payload.get("s") or payload.get("instId") or self._config.symbol),
            event_ts_ms=event_ts_ms,
            recv_ts_ms=recv_ts_ms,
            last_update_id=last_update_id,
            bids=bids,
            asks=asks,
        )

    @staticmethod
    def _normalize_levels(levels: Any) -> tuple[BookLevel, ...]:
        if not isinstance(levels, list):
            return ()

        normalized: list[BookLevel] = []
        for level in levels[:5]:
            if isinstance(level, dict):
                price = level.get("price") or level.get("px")
                size = level.get("size") or level.get("qty") or level.get("sz")
            elif isinstance(level, (list, tuple)) and len(level) >= 2:
                price, size = level[0], level[1]
            else:
                continue

            if price in (None, ""):
                continue
            normalized.append(
                BookLevel(
                    price=Decimal(str(price)),
                    size=Decimal(str(size or "0")),
                )
            )
        return tuple(normalized)


def _is_okx_ws_url(ws_url: str) -> bool:
    return "okx.com" in ws_url and "/ws/v5/public" in ws_url
