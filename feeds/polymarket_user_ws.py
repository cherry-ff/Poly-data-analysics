from __future__ import annotations

from decimal import Decimal
from typing import Any, Iterable

from app.config import PolymarketFeedConfig
from core.clock import WallClock
from core.enums import OrderStatus, Side
from core.event_bus import InMemoryEventBus
from domain.events import UserExecutionEvent
from domain.models import ExecutionReport, MarketMetadata
from feeds.base import JsonWebSocketFeed


class PolymarketUserFeed(JsonWebSocketFeed):
    def __init__(
        self,
        config: PolymarketFeedConfig,
        event_bus: InMemoryEventBus,
        clock: WallClock,
        proxy_url: str | None = None,
    ) -> None:
        self._config = config
        self._market_ids: set[str] = set(config.user_market_ids)
        self._condition_to_market: dict[str, str] = {}
        self._token_to_market: dict[str, str] = {}
        super().__init__(
            name="polymarket_user",
            ws_url=config.user_ws_url,
            event_bus=event_bus,
            clock=clock,
            subscriptions=[],
            heartbeat_interval_ms=config.heartbeat_interval_ms,
            heartbeat_payload="PING",
            connect_timeout_ms=config.connect_timeout_ms,
            idle_timeout_ms=config.user_idle_timeout_ms,
            retry_initial_delay_ms=config.retry_initial_delay_ms,
            retry_max_delay_ms=config.retry_max_delay_ms,
            retry_backoff=config.retry_backoff,
            proxy_url=proxy_url,
        )

    def startup_skip_reason(self) -> str | None:
        if not (
            self._config.api_key.strip()
            and self._config.api_secret.strip()
            and self._config.passphrase.strip()
        ):
            return "user auth is incomplete"
        return None

    async def subscribe(self) -> None:
        if self._ws is None:
            return
        for payload in self._current_subscriptions():
            await self._ws.send_json(payload)

    def register_market(self, market: MarketMetadata) -> None:
        self._condition_to_market[market.condition_id] = market.market_id
        self._token_to_market[market.up_token_id] = market.market_id
        self._token_to_market[market.down_token_id] = market.market_id
        self._market_ids.add(market.condition_id)

    async def ensure_markets(self, market_ids: Iterable[str]) -> None:
        new_ids = [market_id for market_id in market_ids if market_id and market_id not in self._market_ids]
        if not new_ids:
            return
        self._market_ids.update(new_ids)
        if self._ws is None:
            return
        await self._ws.send_json(self._subscription_payload(sorted(self._market_ids)))

    async def handle_text(self, raw_text: str) -> None:
        payload = self._loads(raw_text)
        if isinstance(payload, str):
            return
        recv_ts_ms = self._clock.now_ms()
        for item in self._iter_payloads(payload):
            report = self._normalize(item, recv_ts_ms)
            if report is None:
                continue
            self._mark_data_activity(recv_ts_ms)
            await self._publish(
                "feeds.polymarket.user.execution",
                UserExecutionEvent(report=report),
            )

    def _normalize(self, payload: Any, recv_ts_ms: int) -> ExecutionReport | None:
        if not isinstance(payload, dict):
            return None

        event_type = str(payload.get("event_type") or "").lower()
        client_order_id = (
            payload.get("client_order_id")
            or payload.get("clientOrderId")
            or payload.get("cid")
        )
        token_id = self._coerce_id(
            payload.get("token_id")
            or payload.get("tokenId")
            or payload.get("asset_id")
            or payload.get("assetId")
        )
        market_id = self._coerce_id(
            payload.get("market_id") or payload.get("market") or payload.get("id")
        )
        exchange_order_id = self._exchange_order_id(payload)
        if not client_order_id and exchange_order_id is not None:
            client_order_id = exchange_order_id
        if market_id in self._condition_to_market:
            market_id = self._condition_to_market[market_id]
        elif token_id in self._token_to_market:
            market_id = self._token_to_market[token_id]
        if not client_order_id or not token_id or not market_id:
            return None

        return ExecutionReport(
            client_order_id=str(client_order_id),
            pair_id=self._pair_id(payload),
            market_id=str(market_id),
            token_id=str(token_id),
            side=self._side(payload.get("side")),
            status=self._status(payload, event_type),
            filled_size=Decimal(
                str(
                    payload.get("filled_size")
                    or payload.get("filled")
                    or payload.get("matched_size")
                    or payload.get("size_matched")
                    or "0"
                )
            ),
            avg_price=self._avg_price(payload.get("avg_price") or payload.get("price")),
            exchange_order_id=exchange_order_id,
            event_ts_ms=self._event_ts_ms(payload, recv_ts_ms),
        )

    @staticmethod
    def _status(payload: dict[str, Any], event_type: str) -> OrderStatus:
        value = payload.get("status")
        normalized = str(value or "UNKNOWN").upper()
        mapping = {
            "OPEN": OrderStatus.OPEN,
            "LIVE": OrderStatus.OPEN,
            "PLACED": OrderStatus.OPEN,
            "POSTED": OrderStatus.OPEN,
            "PARTIAL": OrderStatus.PARTIAL,
            "PARTIALLY_FILLED": OrderStatus.PARTIAL,
            "PARTIALLY_MATCHED": OrderStatus.PARTIAL,
            "FILLED": OrderStatus.FILLED,
            "MATCHED": OrderStatus.FILLED,
            "EXECUTED": OrderStatus.FILLED,
            "CANCELED": OrderStatus.CANCELED,
            "CANCELLED": OrderStatus.CANCELED,
            "REJECTED": OrderStatus.REJECTED,
        }
        if normalized in mapping:
            return mapping[normalized]

        if event_type == "order":
            order_type = str(payload.get("type") or "").upper()
            if order_type == "PLACEMENT":
                return OrderStatus.OPEN
            if order_type == "CANCELLATION":
                return OrderStatus.CANCELED
            if order_type == "UPDATE":
                matched = PolymarketUserFeed._decimal_or_zero(
                    payload.get("size_matched") or payload.get("matched_size")
                )
                size = PolymarketUserFeed._decimal_or_zero(
                    payload.get("size") or payload.get("original_size")
                )
                if size > 0 and matched >= size:
                    return OrderStatus.FILLED
                if matched > 0:
                    return OrderStatus.PARTIAL
                return OrderStatus.OPEN

        try:
            return OrderStatus[normalized]
        except KeyError:
            return OrderStatus.UNKNOWN

    @staticmethod
    def _side(value: Any) -> Side:
        normalized = str(value or "BUY").upper()
        if normalized in {"BUY", "BID"}:
            return Side.BUY
        if normalized in {"SELL", "ASK"}:
            return Side.SELL
        try:
            return Side[normalized]
        except KeyError:
            return Side.BUY

    @staticmethod
    def _avg_price(value: Any) -> Decimal | None:
        if value in (None, ""):
            return None
        return Decimal(str(value))

    @staticmethod
    def _exchange_order_id(payload: dict[str, Any]) -> str | None:
        value = (
            payload.get("exchange_order_id")
            or payload.get("order_id")
            or payload.get("orderID")
            or payload.get("id")
        )
        if value is None:
            return None
        return str(value)

    @staticmethod
    def _pair_id(payload: dict[str, Any]) -> str | None:
        value = payload.get("pair_id")
        if value is None:
            return None
        return str(value)

    @staticmethod
    def _coerce_id(value: Any) -> str | None:
        if isinstance(value, dict):
            value = value.get("id") or value.get("market_id") or value.get("token_id")
        if value in (None, ""):
            return None
        return str(value)

    @staticmethod
    def _event_ts_ms(payload: dict[str, Any], recv_ts_ms: int) -> int:
        for value in (
            payload.get("event_ts_ms"),
            payload.get("timestamp"),
            payload.get("match_time"),
            payload.get("created_at"),
            payload.get("updated_at"),
        ):
            if value in (None, ""):
                continue
            if isinstance(value, (int, float)):
                ts_ms = int(value)
                return ts_ms * 1000 if ts_ms < 10_000_000_000 else ts_ms
            if isinstance(value, str) and value.isdigit():
                ts_ms = int(value)
                return ts_ms * 1000 if ts_ms < 10_000_000_000 else ts_ms
        return recv_ts_ms

    @staticmethod
    def _decimal_or_zero(value: Any) -> Decimal:
        if value in (None, ""):
            return Decimal("0")
        return Decimal(str(value))

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
        if not (config.api_key and config.api_secret and config.passphrase):
            return []
        if not config.user_market_ids:
            return []
        return [
            PolymarketUserFeed._subscription_payload_for_config(
                config,
                list(config.user_market_ids),
            )
        ]

    def _current_subscriptions(self) -> list[dict[str, Any]]:
        if not self._market_ids:
            return []
        return [self._subscription_payload(sorted(self._market_ids))]

    def _subscription_payload(self, market_ids: list[str]) -> dict[str, Any]:
        return self._subscription_payload_for_config(self._config, market_ids)

    @staticmethod
    def _subscription_payload_for_config(
        config: PolymarketFeedConfig,
        market_ids: list[str],
    ) -> dict[str, Any]:
        return {
            "type": "user",
            "auth": {
                "apiKey": config.api_key,
                "secret": config.api_secret,
                "passphrase": config.passphrase,
            },
            "markets": market_ids,
        }
