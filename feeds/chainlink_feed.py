from __future__ import annotations

import asyncio
from datetime import datetime
import json
import logging
from collections import deque
from decimal import Decimal
from typing import Any

from app.config import ChainlinkFeedConfig
from core.clock import WallClock
from core.event_bus import InMemoryEventBus
from domain.events import ChainlinkTickEvent
from domain.models import ChainlinkTick
from feeds.base import (
    BaseFeed,
    FeedConfigurationError,
    FeedDependencyError,
    _require_aiohttp,
)

logger = logging.getLogger(__name__)


class ChainlinkPollingFeed(BaseFeed):
    def __init__(
        self,
        config: ChainlinkFeedConfig,
        event_bus: InMemoryEventBus,
        clock: WallClock,
        proxy_url: str | None = None,
    ) -> None:
        super().__init__(name="chainlink_polling", event_bus=event_bus, clock=clock)
        self._config = config
        self._proxy_url = proxy_url or None
        self._recent_keys: set[tuple[int, Decimal, Decimal | None, Decimal | None]] = set()
        self._recent_key_order: deque[
            tuple[int, Decimal, Decimal | None, Decimal | None]
        ] = deque()
        self._max_recent_keys = 4096
        self._session_started_ts_ms: int | None = None

    def startup_skip_reason(self) -> str | None:
        if self._config.endpoint.strip():
            return None
        if self._config.api_url.strip() and self._config.feed_id.strip():
            return None
        return "endpoint/feed_id are empty"

    async def run(self) -> None:
        if self.startup_skip_reason() is not None:
            raise FeedConfigurationError("chainlink endpoint/feed_id are empty")

        aiohttp = _require_aiohttp()
        timeout = aiohttp.ClientTimeout(total=self._config.request_timeout_ms / 1000)
        self._running = True
        delay_ms = self._config.retry_initial_delay_ms
        try:
            while self.is_running:
                try:
                    connector = aiohttp.TCPConnector(ssl=False) if self._proxy_url else None
                    async with aiohttp.ClientSession(
                        timeout=timeout,
                        trust_env=True,
                        connector=connector,
                    ) as session:
                        self._session_started_ts_ms = self._clock.now_ms()
                        self._last_data_activity_ts_ms = None
                        while self.is_running:
                            self._raise_if_silent()
                            await self._poll_once(session)
                            self._raise_if_silent()
                            delay_ms = self._config.retry_initial_delay_ms
                            if not self.is_running:
                                break
                            await self._sleep_ms(self._config.poll_interval_ms)
                except asyncio.CancelledError:
                    raise
                except (FeedConfigurationError, FeedDependencyError):
                    raise
                except Exception as exc:
                    if not self.is_running:
                        break
                    logger.warning(
                        "%s polling failed; retrying in %dms: %s(%r)",
                        self.name,
                        delay_ms,
                        type(exc).__name__,
                        exc,
                    )
                    await self._sleep_ms(delay_ms)
                    delay_ms = min(
                        max(int(delay_ms * self._config.retry_backoff), delay_ms),
                        self._config.retry_max_delay_ms,
                    )
        finally:
            self._session_started_ts_ms = None
            self._running = False

    async def _poll_once(self, session: Any) -> None:
        recv_ts_ms = self._clock.now_ms()
        payload = await self._request_payload(session)
        published_any = False
        for tick in self._normalize_ticks(payload, recv_ts_ms):
            key = (tick.oracle_ts_ms, tick.price, tick.bid, tick.ask)
            if key in self._recent_keys:
                continue
            self._remember_key(key)
            await self._publish("feeds.chainlink.tick", ChainlinkTickEvent(tick=tick))
            published_any = True
        if published_any:
            self._mark_data_activity(recv_ts_ms)

    def _remember_key(
        self,
        key: tuple[int, Decimal, Decimal | None, Decimal | None],
    ) -> None:
        self._recent_keys.add(key)
        self._recent_key_order.append(key)
        while len(self._recent_key_order) > self._max_recent_keys:
            old = self._recent_key_order.popleft()
            self._recent_keys.discard(old)

    def _raise_if_silent(self) -> None:
        timeout_ms = self._config.silent_reconnect_after_ms
        if timeout_ms <= 0:
            return
        now_ms = self._clock.now_ms()
        baseline = self._last_data_activity_ts_ms
        if baseline is None:
            baseline = self._session_started_ts_ms
        if baseline is None:
            return
        if now_ms - baseline > timeout_ms:
            raise TimeoutError(
                f"{self.name} produced no fresh ticks for {timeout_ms}ms"
            )

    async def _request_payload(self, session: Any) -> Any:
        if self._config.endpoint.strip():
            async with session.get(self._config.endpoint, proxy=self._proxy_url) as response:
                response.raise_for_status()
                return await response.json()

        params = {
            "query": self._config.query_name,
            "variables": json.dumps({"feedId": self._config.feed_id}),
        }
        async with session.get(
            self._config.api_url,
            params=params,
            proxy=self._proxy_url,
        ) as response:
            if response.status >= 400:
                body = await response.text()
                raise RuntimeError(
                    f"chainlink query failed status={response.status} body={body[:200]!r}"
                )
            response.raise_for_status()
            payload = await response.json()
        if isinstance(payload, dict) and payload.get("errors"):
            raise RuntimeError(f"chainlink query returned errors: {payload['errors']!r}")
        return payload

    def _normalize_ticks(self, payload: Any, recv_ts_ms: int) -> list[ChainlinkTick]:
        query_ticks = self._normalize_query_timescale(payload, recv_ts_ms)
        if query_ticks:
            return query_ticks

        if not isinstance(payload, dict):
            return []

        data = payload.get("data")
        if isinstance(data, dict):
            payload = data

        price = payload.get("price")
        if price is None:
            answer = payload.get("answer")
            if answer is not None:
                price = Decimal(str(answer))
                decimals = payload.get("decimals")
                if decimals is not None:
                    price = price / (Decimal("10") ** int(decimals))
        if price is None:
            return []

        oracle_ts_ms = int(
            payload.get("updatedAt")
            or payload.get("timestamp")
            or payload.get("oracle_ts_ms")
            or recv_ts_ms
        )
        if oracle_ts_ms < 10_000_000_000:
            oracle_ts_ms *= 1000
        if (
            self._config.stale_after_ms > 0
            and recv_ts_ms - oracle_ts_ms > self._config.stale_after_ms
        ):
            return []
        return [
            ChainlinkTick(
                feed=str(payload.get("feed") or self._config.feed_name),
                oracle_ts_ms=oracle_ts_ms,
                recv_ts_ms=recv_ts_ms,
                price=Decimal(str(price)),
                round_id=str(payload.get("roundId") or payload.get("round_id") or ""),
            )
        ]

    def _normalize_query_timescale(
        self,
        payload: Any,
        recv_ts_ms: int,
    ) -> list[ChainlinkTick]:
        if not isinstance(payload, dict):
            return []

        data = payload.get("data")
        if not isinstance(data, dict):
            return []
        reports = data.get("liveStreamReports")
        if not isinstance(reports, dict):
            return []
        nodes = reports.get("nodes")
        if not isinstance(nodes, list) or not nodes:
            return []

        scale = Decimal(str(self._config.price_scale))
        if scale <= 0:
            raise FeedConfigurationError("chainlink price_scale must be positive")

        ticks: list[ChainlinkTick] = []
        for node in sorted(nodes, key=self._node_sort_key):
            if not isinstance(node, dict):
                continue
            oracle_ts_ms = self._parse_node_timestamp_ms(
                node.get("validFromTimestamp") or node.get("timestamp")
            )
            if oracle_ts_ms is None:
                continue

            raw_price = node.get("price")
            if raw_price in (None, ""):
                continue

            price = Decimal(str(raw_price)) / scale
            bid = self._scaled_decimal(node.get("bid"), scale)
            ask = self._scaled_decimal(node.get("ask"), scale)
            if (
                self._config.stale_after_ms > 0
                and recv_ts_ms - oracle_ts_ms > self._config.stale_after_ms
            ):
                continue

            round_id = (
                node.get("observationHash")
                or node.get("configDigest")
                or node.get("feedId")
                or self._config.feed_id
            )
            ticks.append(
                ChainlinkTick(
                    feed=self._config.feed_name,
                    oracle_ts_ms=oracle_ts_ms,
                    recv_ts_ms=recv_ts_ms,
                    price=price,
                    round_id=str(round_id),
                    bid=bid,
                    ask=ask,
                )
            )
        return ticks

    @staticmethod
    def _node_sort_key(node: Any) -> tuple[int, str]:
        if not isinstance(node, dict):
            return (0, "")
        ts_int = ChainlinkPollingFeed._parse_node_timestamp_ms(
            node.get("validFromTimestamp") or node.get("timestamp")
        )
        if ts_int is None:
            ts_int = 0
        ref = (
            node.get("observationHash")
            or node.get("configDigest")
            or node.get("feedId")
            or ""
        )
        return (ts_int, str(ref))

    @staticmethod
    def _parse_node_timestamp_ms(value: Any) -> int | None:
        if value in (None, ""):
            return None
        if isinstance(value, (int, float)):
            ts_int = int(value)
            return ts_int * 1000 if ts_int < 10_000_000_000 else ts_int

        text = str(value).strip()
        if not text:
            return None
        try:
            ts_int = int(text)
        except ValueError:
            try:
                parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            except ValueError:
                return None
            return int(parsed.timestamp() * 1000)
        return ts_int * 1000 if ts_int < 10_000_000_000 else ts_int

    @staticmethod
    def _scaled_decimal(value: Any, scale: Decimal) -> Decimal | None:
        if value in (None, ""):
            return None
        return Decimal(str(value)) / scale

    async def _sleep_ms(self, delay_ms: int) -> None:
        await asyncio.sleep(delay_ms / 1000)
