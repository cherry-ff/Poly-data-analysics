from __future__ import annotations

import asyncio

from decimal import Decimal

from app.config import ChainlinkFeedConfig
from core.clock import WallClock
from core.event_bus import InMemoryEventBus
from feeds.base import FeedConfigurationError, FeedDependencyError, JsonWebSocketFeed
from feeds.chainlink_feed import ChainlinkPollingFeed


class _ReconnectTestFeed(JsonWebSocketFeed):
    def __init__(self) -> None:
        super().__init__(
            name="test_ws",
            ws_url="wss://example.invalid/ws",
            event_bus=InMemoryEventBus(),
            clock=WallClock(),
            retry_initial_delay_ms=5,
            retry_max_delay_ms=20,
        )
        self.attempts = 0
        self.sleep_delays: list[int] = []

    async def handle_text(self, raw_text: str) -> None:
        return None

    async def _run_once(self) -> None:
        self.attempts += 1
        if self.attempts == 1:
            raise ConnectionError("transient network failure")
        await self.close()

    async def _sleep_ms(self, delay_ms: int) -> None:
        self.sleep_delays.append(delay_ms)


class _ConfigErrorFeed(JsonWebSocketFeed):
    def __init__(self) -> None:
        super().__init__(
            name="bad_ws",
            ws_url="",
            event_bus=InMemoryEventBus(),
            clock=WallClock(),
        )

    async def handle_text(self, raw_text: str) -> None:
        return None


class _DependencyErrorFeed(JsonWebSocketFeed):
    def __init__(self) -> None:
        super().__init__(
            name="dep_ws",
            ws_url="wss://example.invalid/ws",
            event_bus=InMemoryEventBus(),
            clock=WallClock(),
        )
        self.attempts = 0

    async def handle_text(self, raw_text: str) -> None:
        return None

    async def _run_once(self) -> None:
        self.attempts += 1
        raise FeedDependencyError("aiohttp is required to run network feeds")


class _RetryingChainlinkFeed(ChainlinkPollingFeed):
    def __init__(self) -> None:
        super().__init__(
            config=ChainlinkFeedConfig(
                enabled=True,
                endpoint="https://example.invalid/feed",
                poll_interval_ms=10,
                retry_initial_delay_ms=7,
                retry_max_delay_ms=20,
            ),
            event_bus=InMemoryEventBus(),
            clock=WallClock(),
        )
        self.attempts = 0
        self.sleep_delays: list[int] = []

    async def _poll_once(self, session: object) -> None:
        self.attempts += 1
        if self.attempts == 1:
            raise RuntimeError("temporary http 502")
        await self.close()

    async def _sleep_ms(self, delay_ms: int) -> None:
        self.sleep_delays.append(delay_ms)


class _FakeClock:
    def __init__(self, *timestamps: int) -> None:
        self._timestamps = list(timestamps) or [0]
        self._last = self._timestamps[-1]

    def now_ms(self) -> int:
        if self._timestamps:
            self._last = self._timestamps.pop(0)
        return self._last


class _IdleStateFeed(JsonWebSocketFeed):
    def __init__(self, clock: _FakeClock) -> None:
        super().__init__(
            name="idle_ws",
            ws_url="wss://example.invalid/ws",
            event_bus=InMemoryEventBus(),
            clock=clock,
            idle_timeout_ms=10,
        )

    async def handle_text(self, raw_text: str) -> None:
        return None


def test_json_websocket_feed_retries_after_transient_failure() -> None:
    feed = _ReconnectTestFeed()

    asyncio.run(feed.run())

    assert feed.attempts == 2
    assert feed.sleep_delays == [5]


def test_json_websocket_feed_raises_configuration_error_without_url() -> None:
    feed = _ConfigErrorFeed()

    try:
        asyncio.run(feed.run())
    except FeedConfigurationError as exc:
        assert "ws_url is empty" in str(exc)
    else:
        raise AssertionError("expected FeedConfigurationError")


def test_json_websocket_feed_does_not_retry_dependency_errors() -> None:
    feed = _DependencyErrorFeed()

    try:
        asyncio.run(feed.run())
    except FeedDependencyError as exc:
        assert "aiohttp is required" in str(exc)
    else:
        raise AssertionError("expected FeedDependencyError")

    assert feed.attempts == 1


def test_chainlink_feed_filters_stale_ticks() -> None:
    feed = ChainlinkPollingFeed(
        config=ChainlinkFeedConfig(
            enabled=True,
            endpoint="https://example.invalid/feed",
            stale_after_ms=1000,
        ),
        event_bus=InMemoryEventBus(),
        clock=WallClock(),
    )

    ticks = feed._normalize_ticks(
        {
            "price": "50000",
            "updatedAt": 1_699_999_998,
        },
        recv_ts_ms=1_700_000_000_000,
    )

    assert ticks == []


def test_chainlink_feed_retries_after_transient_failure() -> None:
    feed = _RetryingChainlinkFeed()

    asyncio.run(feed.run())

    assert feed.attempts == 2
    assert feed.sleep_delays == [7]


def test_json_websocket_feed_detects_data_silence_from_connect_time() -> None:
    feed = _IdleStateFeed(clock=_FakeClock(11))
    feed._connected_at_ts_ms = 0

    try:
        feed._raise_if_data_stalled()
    except TimeoutError as exc:
        assert "produced no data" in str(exc)
    else:
        raise AssertionError("expected TimeoutError")


def test_json_websocket_feed_uses_latest_data_activity_for_silence_watchdog() -> None:
    feed = _IdleStateFeed(clock=_FakeClock(12, 16))
    feed._connected_at_ts_ms = 0
    feed._mark_data_activity(5)
    feed._raise_if_data_stalled()

    try:
        feed._raise_if_data_stalled()
    except TimeoutError as exc:
        assert "produced no data" in str(exc)
    else:
        raise AssertionError("expected TimeoutError")


def test_chainlink_feed_detects_silent_stall_after_no_fresh_ticks() -> None:
    feed = ChainlinkPollingFeed(
        config=ChainlinkFeedConfig(
            enabled=True,
            endpoint="https://example.invalid/feed",
            silent_reconnect_after_ms=20,
        ),
        event_bus=InMemoryEventBus(),
        clock=_FakeClock(21),
    )
    feed._session_started_ts_ms = 0

    try:
        feed._raise_if_silent()
    except TimeoutError as exc:
        assert "produced no fresh ticks" in str(exc)
    else:
        raise AssertionError("expected TimeoutError")


def test_chainlink_feed_silence_watchdog_respects_recent_fresh_tick() -> None:
    feed = ChainlinkPollingFeed(
        config=ChainlinkFeedConfig(
            enabled=True,
            endpoint="https://example.invalid/feed",
            silent_reconnect_after_ms=20,
        ),
        event_bus=InMemoryEventBus(),
        clock=_FakeClock(24, 31),
    )
    feed._session_started_ts_ms = 0
    feed._mark_data_activity(10)
    feed._raise_if_silent()

    try:
        feed._raise_if_silent()
    except TimeoutError as exc:
        assert "produced no fresh ticks" in str(exc)
    else:
        raise AssertionError("expected TimeoutError")


def test_chainlink_feed_normalizes_query_timescale_payload() -> None:
    feed = ChainlinkPollingFeed(
        config=ChainlinkFeedConfig(
            enabled=True,
            api_url="https://data.chain.link/api/query-timescale",
            feed_id="feed",
            price_scale="1e18",
            stale_after_ms=5000,
        ),
        event_bus=InMemoryEventBus(),
        clock=WallClock(),
    )

    ticks = feed._normalize_ticks(
        {
            "data": {
                "liveStreamReports": {
                    "nodes": [
                        {
                            "validFromTimestamp": 1_700_000_001,
                            "price": "67307510000000000000000",
                            "feedId": "feed",
                        },
                        {
                            "validFromTimestamp": 1_700_000_000,
                            "price": "67307000000000000000000",
                            "feedId": "feed",
                        },
                    ]
                }
            }
        },
        recv_ts_ms=1_700_000_001_500,
    )

    assert len(ticks) == 2
    assert ticks[0].oracle_ts_ms == 1_700_000_000_000
    assert ticks[0].price == Decimal("67307")
    assert ticks[1].oracle_ts_ms == 1_700_000_001_000
    assert ticks[1].price == Decimal("67307.51")


def test_chainlink_feed_normalizes_query_timescale_iso_timestamps() -> None:
    feed = ChainlinkPollingFeed(
        config=ChainlinkFeedConfig(
            enabled=True,
            api_url="https://data.chain.link/api/query-timescale",
            feed_id="feed",
            price_scale="1e18",
            stale_after_ms=5_000,
        ),
        event_bus=InMemoryEventBus(),
        clock=WallClock(),
    )

    ticks = feed._normalize_ticks(
        {
            "data": {
                "liveStreamReports": {
                    "nodes": [
                        {
                            "validFromTimestamp": "2023-11-14T22:13:21+00:00",
                            "price": "67307510000000000000000",
                            "feedId": "feed",
                        },
                        {
                            "validFromTimestamp": "2023-11-14T22:13:20+00:00",
                            "price": "67307000000000000000000",
                            "feedId": "feed",
                        },
                    ]
                }
            }
        },
        recv_ts_ms=1_700_000_001_500,
    )

    assert len(ticks) == 2
    assert ticks[0].oracle_ts_ms == 1_700_000_000_000
    assert ticks[1].oracle_ts_ms == 1_700_000_001_000


def test_chainlink_feed_normalizes_bid_ask_and_dedup_key_components() -> None:
    feed = ChainlinkPollingFeed(
        config=ChainlinkFeedConfig(
            enabled=True,
            api_url="https://data.chain.link/api/query-timescale",
            feed_id="feed",
            price_scale="1e18",
            stale_after_ms=20_000,
        ),
        event_bus=InMemoryEventBus(),
        clock=WallClock(),
    )

    ticks = feed._normalize_ticks(
        {
            "data": {
                "liveStreamReports": {
                    "nodes": [
                        {
                            "validFromTimestamp": 1_700_000_001,
                            "price": "67307510000000000000000",
                            "bid": "67307490000000000000000",
                            "ask": "67307530000000000000000",
                            "observationHash": "obs_1",
                        },
                        {
                            "validFromTimestamp": 1_700_000_001,
                            "price": "67307510000000000000000",
                            "bid": "67307490000000000000000",
                            "ask": "67307530000000000000000",
                            "observationHash": "obs_1",
                        },
                    ]
                }
            }
        },
        recv_ts_ms=1_700_000_005_000,
    )

    assert len(ticks) == 2
    assert ticks[0].bid == Decimal("67307.49")
    assert ticks[0].ask == Decimal("67307.53")

    key = (ticks[0].oracle_ts_ms, ticks[0].price, ticks[0].bid, ticks[0].ask)
    feed._remember_key(key)
    assert key in feed._recent_keys
