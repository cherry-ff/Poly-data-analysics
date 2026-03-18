from __future__ import annotations

import asyncio
import json
import logging
from contextlib import suppress
from typing import Any

from core.clock import WallClock
from core.event_bus import InMemoryEventBus

logger = logging.getLogger(__name__)


def _require_aiohttp():
    try:
        import aiohttp  # type: ignore
    except ImportError as exc:
        raise FeedDependencyError("aiohttp is required to run network feeds") from exc
    return aiohttp


class FeedConfigurationError(ValueError):
    pass


class FeedDependencyError(RuntimeError):
    pass


class BaseFeed:
    def __init__(
        self,
        name: str,
        event_bus: InMemoryEventBus,
        clock: WallClock,
    ) -> None:
        self._name = name
        self._event_bus = event_bus
        self._clock = clock
        self._running = False
        self._last_data_activity_ts_ms: int | None = None

    @property
    def name(self) -> str:
        return self._name

    @property
    def is_running(self) -> bool:
        return self._running

    async def connect(self) -> None:
        return None

    async def subscribe(self) -> None:
        return None

    async def run(self) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        self._running = False

    async def _publish(self, topic: str, payload: object) -> None:
        await self._event_bus.publish(topic, payload)

    def _mark_data_activity(self, ts_ms: int | None = None) -> None:
        self._last_data_activity_ts_ms = ts_ms if ts_ms is not None else self._clock.now_ms()


class JsonWebSocketFeed(BaseFeed):
    def __init__(
        self,
        name: str,
        ws_url: str,
        event_bus: InMemoryEventBus,
        clock: WallClock,
        subscriptions: list[dict[str, Any]] | None = None,
        heartbeat_interval_ms: int | None = None,
        heartbeat_payload: str | dict[str, Any] | None = None,
        connect_timeout_ms: int = 5000,
        idle_timeout_ms: int | None = None,
        retry_initial_delay_ms: int = 500,
        retry_max_delay_ms: int = 5000,
        retry_backoff: float = 2.0,
        proxy_url: str | None = None,
    ) -> None:
        super().__init__(name=name, event_bus=event_bus, clock=clock)
        self._ws_url = ws_url
        self._subscriptions = subscriptions or []
        self._heartbeat_interval_ms = heartbeat_interval_ms
        self._heartbeat_payload = heartbeat_payload
        self._connect_timeout_ms = connect_timeout_ms
        self._idle_timeout_ms = idle_timeout_ms if idle_timeout_ms and idle_timeout_ms > 0 else None
        self._retry_initial_delay_ms = retry_initial_delay_ms
        self._retry_max_delay_ms = retry_max_delay_ms
        self._retry_backoff = retry_backoff
        self._proxy_url = proxy_url or None
        self._session = None
        self._ws = None
        self._heartbeat_task = None
        self._connected_at_ts_ms: int | None = None

    async def connect(self) -> None:
        if not self._ws_url:
            raise FeedConfigurationError(f"{self.name} ws_url is empty")
        aiohttp = _require_aiohttp()
        self._session = aiohttp.ClientSession(trust_env=True)
        self._ws = await self._session.ws_connect(
            self._ws_url,
            timeout=self._connect_timeout_ms / 1000,
            proxy=self._proxy_url,
        )

    async def subscribe(self) -> None:
        if self._ws is None:
            return
        for payload in self._subscriptions:
            await self._ws.send_json(payload)

    async def run(self) -> None:
        try:
            self._running = True
            delay_ms = self._retry_initial_delay_ms
            while self.is_running:
                try:
                    await self._run_once()
                    delay_ms = self._retry_initial_delay_ms
                    if not self.is_running:
                        break
                    logger.warning(
                        "%s websocket closed; reconnecting in %dms",
                        self.name,
                        delay_ms,
                    )
                except asyncio.CancelledError:
                    raise
                except (FeedConfigurationError, FeedDependencyError):
                    raise
                except Exception as exc:
                    if not self.is_running:
                        break
                    logger.warning(
                        "%s websocket failed; reconnecting in %dms: %s",
                        self.name,
                        delay_ms,
                        exc,
                    )
                await self._close_transport()
                if not self.is_running:
                    break
                await self._sleep_ms(delay_ms)
                delay_ms = min(
                    max(int(delay_ms * self._retry_backoff), delay_ms),
                    self._retry_max_delay_ms,
                )
        finally:
            await self._close_transport()
            self._running = False

    async def _run_once(self) -> None:
        aiohttp = _require_aiohttp()
        await self.connect()
        await self.subscribe()
        self._start_heartbeat()
        self._connected_at_ts_ms = self._clock.now_ms()
        self._last_data_activity_ts_ms = None

        assert self._ws is not None
        while self.is_running:
            try:
                message = await self._ws.receive(timeout=self._receive_timeout_seconds())
            except asyncio.TimeoutError as exc:
                raise TimeoutError(
                    f"{self.name} websocket produced no data for {self._idle_timeout_ms}ms"
                ) from exc
            if message.type == aiohttp.WSMsgType.TEXT:
                await self.handle_text(message.data)
                self._raise_if_data_stalled()
                continue
            if message.type == aiohttp.WSMsgType.ERROR:
                raise RuntimeError(f"{self.name} websocket error")
            if message.type in {
                aiohttp.WSMsgType.CLOSED,
                aiohttp.WSMsgType.CLOSING,
                aiohttp.WSMsgType.CLOSE,
            }:
                break
            self._raise_if_data_stalled()

        if self.is_running:
            raise ConnectionError(f"{self.name} websocket closed")

    async def handle_text(self, raw_text: str) -> None:
        raise NotImplementedError

    async def close(self) -> None:
        self._running = False
        await self._close_transport()
        await super().close()

    async def _close_transport(self) -> None:
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await self._heartbeat_task
            self._heartbeat_task = None
        if self._ws is not None:
            with suppress(Exception):
                await self._ws.close()
            self._ws = None
        if self._session is not None:
            with suppress(Exception):
                await self._session.close()
            self._session = None
        self._connected_at_ts_ms = None

    @staticmethod
    def _loads(raw_text: str) -> Any:
        try:
            return json.loads(raw_text)
        except json.JSONDecodeError:
            return raw_text

    def _start_heartbeat(self) -> None:
        if self._heartbeat_interval_ms is None or self._heartbeat_payload is None:
            return
        if self._ws is None:
            return
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(),
            name=f"{self.name}:heartbeat",
        )

    async def _heartbeat_loop(self) -> None:
        while self.is_running and self._ws is not None:
            await asyncio.sleep(self._heartbeat_interval_ms / 1000)
            if not self.is_running or self._ws is None:
                return
            if isinstance(self._heartbeat_payload, str):
                await self._ws.send_str(self._heartbeat_payload)
            else:
                await self._ws.send_json(self._heartbeat_payload)

    async def _sleep_ms(self, delay_ms: int) -> None:
        await asyncio.sleep(delay_ms / 1000)

    def _receive_timeout_seconds(self) -> float | None:
        if self._idle_timeout_ms is None:
            return None
        return max(self._idle_timeout_ms / 1000, 0.001)

    def _raise_if_data_stalled(self, now_ms: int | None = None) -> None:
        if self._idle_timeout_ms is None:
            return
        now = now_ms if now_ms is not None else self._clock.now_ms()
        baseline = self._last_data_activity_ts_ms
        if baseline is None:
            baseline = self._connected_at_ts_ms
        if baseline is None:
            return
        if now - baseline > self._idle_timeout_ms:
            raise TimeoutError(
                f"{self.name} websocket produced no data for {self._idle_timeout_ms}ms"
            )
