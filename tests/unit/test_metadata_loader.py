from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any

from market.metadata_loader import MarketMetadataLoader

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None


def _payload(market_id: str, up_token: str = "up", down_token: str = "down") -> dict[str, object]:
    return {
        "id": market_id,
        "conditionId": f"cond_{market_id}",
        "clobTokenIds": [up_token, down_token],
        "startDate": "2026-03-08T00:00:00Z",
        "endDate": "2026-03-08T00:15:00Z",
        "minimum_tick_size": "0.01",
        "feeRateBps": "10",
        "minimum_order_size": "1",
        "status": "active",
        "reference_price": "50000",
    }


def test_metadata_loader_uses_cache_until_ttl_expires() -> None:
    calls: list[str] = []

    async def fetcher(market_id: str) -> dict[str, object]:
        calls.append(market_id)
        return _payload(market_id, up_token=f"{market_id}_up", down_token=f"{market_id}_down")

    loader = MarketMetadataLoader(fetcher=fetcher, cache_ttl_ms=1_000)

    first = asyncio.run(loader.load_market("m1"))
    second = asyncio.run(loader.load_market("m1"))
    loader._cache["m1"] = (first, loader._now_ms() - 2_000)
    third = asyncio.run(loader.load_market("m1"))

    assert first.market_id == "m1"
    assert second.market_id == "m1"
    assert third.market_id == "m1"
    assert third.up_token_id == "m1_up"
    assert calls == ["m1", "m1"]


def test_metadata_loader_returns_stale_cache_on_fetch_error_when_enabled() -> None:
    async def ok_fetcher(market_id: str) -> dict[str, object]:
        return _payload(market_id)

    loader = MarketMetadataLoader(
        fetcher=ok_fetcher,
        cache_ttl_ms=1,
        return_stale_on_error=True,
    )
    cached = asyncio.run(loader.load_market("m2"))
    loader._cache["m2"] = (cached, loader._now_ms() - 2_000)

    async def failing_fetcher(market_id: str) -> dict[str, object]:
        raise RuntimeError(f"boom:{market_id}")

    loader._fetcher = failing_fetcher
    recovered = asyncio.run(loader.load_market("m2"))

    assert recovered is cached


def test_metadata_loader_raises_without_stale_fallback() -> None:
    async def ok_fetcher(market_id: str) -> dict[str, object]:
        return _payload(market_id)

    loader = MarketMetadataLoader(
        fetcher=ok_fetcher,
        cache_ttl_ms=1,
        return_stale_on_error=False,
    )
    cached = asyncio.run(loader.load_market("m3"))
    loader._cache["m3"] = (cached, loader._now_ms() - 2_000)

    async def failing_fetcher(market_id: str) -> dict[str, object]:
        raise RuntimeError(f"boom:{market_id}")

    loader._fetcher = failing_fetcher

    try:
        asyncio.run(loader.load_market("m3"))
    except RuntimeError as exc:
        assert "boom:m3" in str(exc)
    else:
        raise AssertionError("expected RuntimeError")


class _DiscoveryLoader(MarketMetadataLoader):
    def __init__(self, responses: dict[str, Any]) -> None:
        super().__init__(gamma_base_url="https://gamma-api.polymarket.com")
        self._responses = responses

    async def _fetch_json(  # type: ignore[override]
        self,
        url: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        key = url
        if params is not None and url.endswith("/markets"):
            key = f"{url}?offset={params['offset']}"
        return self._responses[key]


def test_metadata_loader_discovers_btc_15m_markets_and_maps_yes_no_tokens() -> None:
    now = datetime.now(timezone.utc) + timedelta(days=1)
    start = now.replace(hour=19, minute=0, second=0, microsecond=0)
    end = start + timedelta(minutes=15)

    loader = _DiscoveryLoader(
        {
            "https://gamma-api.polymarket.com/tags/slug/bitcoin": {"id": "1"},
            "https://gamma-api.polymarket.com/markets?offset=0": [
                {
                    "id": "market_a",
                    "conditionId": "condition_a",
                    "question": "March 13, Will BTC be up or down? 2:00 PM - 2:15 PM ET",
                    "description": "BTC 15 minute market",
                    "startDate": start.isoformat().replace("+00:00", "Z"),
                    "endDate": end.isoformat().replace("+00:00", "Z"),
                    "clobTokenIds": '["tok_no","tok_yes"]',
                    "outcomes": '["No","Yes"]',
                    "minimum_tick_size": "0.01",
                    "feeRateBps": "10",
                    "minimum_order_size": "1",
                    "status": "active",
                }
            ],
        }
    )

    markets = asyncio.run(loader.discover_markets(max_markets=1, max_pages=1))

    assert len(markets) == 1
    market = markets[0]
    assert market.market_id == "market_a"
    assert market.condition_id == "condition_a"
    assert market.up_token_id == "tok_yes"
    assert market.down_token_id == "tok_no"
    assert market.end_ts_ms > market.start_ts_ms


def test_metadata_loader_discovery_prefers_active_market_and_next_upcoming() -> None:
    now = datetime.now(timezone.utc)

    def _iso(dt: datetime) -> str:
        return dt.isoformat().replace("+00:00", "Z")

    loader = _DiscoveryLoader(
        {
            "https://gamma-api.polymarket.com/tags/slug/bitcoin": {"id": "1"},
            "https://gamma-api.polymarket.com/markets?offset=0": [
                {
                    "id": "m_upcoming_far",
                    "conditionId": "condition_upcoming_far",
                    "question": "BTC market far",
                    "description": "BTC 15 minute market",
                    "startDate": _iso(now + timedelta(minutes=20)),
                    "endDate": _iso(now + timedelta(minutes=35)),
                    "clobTokenIds": ["far_yes", "far_no"],
                    "outcomes": ["Yes", "No"],
                    "minimum_tick_size": "0.01",
                    "feeRateBps": "10",
                    "minimum_order_size": "1",
                    "status": "active",
                },
                {
                    "id": "m_active",
                    "conditionId": "condition_active",
                    "question": "BTC market active",
                    "description": "BTC 15 minute market",
                    "startDate": _iso(now - timedelta(minutes=5)),
                    "endDate": _iso(now + timedelta(minutes=10)),
                    "clobTokenIds": ["active_yes", "active_no"],
                    "outcomes": ["Yes", "No"],
                    "minimum_tick_size": "0.01",
                    "feeRateBps": "10",
                    "minimum_order_size": "1",
                    "status": "active",
                },
                {
                    "id": "m_upcoming_near",
                    "conditionId": "condition_upcoming_near",
                    "question": "BTC market near",
                    "description": "BTC 15 minute market",
                    "startDate": _iso(now + timedelta(minutes=10)),
                    "endDate": _iso(now + timedelta(minutes=25)),
                    "clobTokenIds": ["near_yes", "near_no"],
                    "outcomes": ["Yes", "No"],
                    "minimum_tick_size": "0.01",
                    "feeRateBps": "10",
                    "minimum_order_size": "1",
                    "status": "active",
                },
            ],
        }
    )

    markets = asyncio.run(
        loader.discover_markets(
            max_markets=2,
            max_pages=1,
        )
    )

    assert [market.market_id for market in markets] == ["m_active", "m_upcoming_near"]


def test_metadata_loader_discovery_excludes_non_15_minute_btc_market() -> None:
    now = datetime.now(timezone.utc)

    def _iso(dt: datetime) -> str:
        return dt.isoformat().replace("+00:00", "Z")

    loader = _DiscoveryLoader(
        {
            "https://gamma-api.polymarket.com/tags/slug/bitcoin": {"id": "1"},
            "https://gamma-api.polymarket.com/markets?offset=0": [
                {
                    "id": "m_daily",
                    "conditionId": "condition_daily",
                    "question": "Will BTC close above $100,000 tomorrow?",
                    "description": "BTC market",
                    "startDate": _iso(now + timedelta(minutes=5)),
                    "endDate": _iso(now + timedelta(days=1)),
                    "clobTokenIds": ["daily_yes", "daily_no"],
                    "outcomes": ["Yes", "No"],
                    "minimum_tick_size": "0.01",
                    "feeRateBps": "10",
                    "minimum_order_size": "1",
                    "status": "active",
                },
                {
                    "id": "m_15m",
                    "conditionId": "condition_15m",
                    "question": "BTC market active",
                    "description": "BTC 15 minute market",
                    "startDate": _iso(now + timedelta(minutes=10)),
                    "endDate": _iso(now + timedelta(minutes=25)),
                    "clobTokenIds": ["fifteen_yes", "fifteen_no"],
                    "outcomes": ["Yes", "No"],
                    "minimum_tick_size": "0.01",
                    "feeRateBps": "10",
                    "minimum_order_size": "1",
                    "status": "active",
                },
            ],
        }
    )

    markets = asyncio.run(loader.discover_markets(max_markets=2, max_pages=1))

    assert [market.market_id for market in markets] == ["m_15m"]


def test_metadata_loader_load_market_rejects_non_target_market_when_filter_enabled() -> None:
    now = datetime.now(timezone.utc)

    async def fetcher(market_id: str) -> dict[str, object]:
        return {
            "id": market_id,
            "conditionId": f"cond_{market_id}",
            "question": "Will BTC close above $100,000 tomorrow?",
            "description": "BTC market",
            "clobTokenIds": ["tok_yes", "tok_no"],
            "outcomes": ["Yes", "No"],
            "startDate": (now + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
            "endDate": (now + timedelta(days=1)).isoformat().replace("+00:00", "Z"),
            "minimum_tick_size": "0.01",
            "feeRateBps": "10",
            "minimum_order_size": "1",
            "status": "active",
        }

    loader = MarketMetadataLoader(
        fetcher=fetcher,
        market_filter_enabled=True,
        filter_keywords=("btc", "bitcoin"),
        filter_min_duration_minutes=12,
        filter_max_duration_minutes=20,
    )

    try:
        asyncio.run(loader.load_market("not_15m"))
    except LookupError as exc:
        assert "does not match configured BTC 15-minute filters" in str(exc)
    else:
        raise AssertionError("expected LookupError")


def test_metadata_loader_repairs_unreasonable_payload_times_from_question() -> None:
    tz = ZoneInfo("America/New_York") if ZoneInfo is not None else timezone(timedelta(hours=-5))
    start_et = (datetime.now(tz) + timedelta(days=1)).replace(
        hour=14,
        minute=0,
        second=0,
        microsecond=0,
    )
    end_et = start_et + timedelta(minutes=15)
    start_utc = start_et.astimezone(timezone.utc)
    end_utc = end_et.astimezone(timezone.utc)
    question = (
        f"{start_et.strftime('%B')} {start_et.day}, "
        f"Will BTC be up or down? {start_et.strftime('%I:%M %p')} - "
        f"{end_et.strftime('%I:%M %p')} ET"
    )

    market = MarketMetadataLoader.normalize_market_payload(
        {
            "id": "market_fix",
            "conditionId": "condition_fix",
            "question": question,
            "clobTokenIds": ["tok_yes", "tok_no"],
            "outcomes": ["Yes", "No"],
            "startDate": (start_utc - timedelta(days=2)).isoformat().replace("+00:00", "Z"),
            "endDate": end_utc.isoformat().replace("+00:00", "Z"),
            "minimum_tick_size": "0.01",
            "feeRateBps": "10",
            "minimum_order_size": "1",
            "status": "active",
        }
    )

    assert market.start_ts_ms == int(start_utc.timestamp() * 1000)
    assert market.end_ts_ms == int(end_utc.timestamp() * 1000)
    assert market.end_ts_ms - market.start_ts_ms == 15 * 60_000


def test_metadata_loader_ignores_timestamp_like_start_price_as_reference_price() -> None:
    market = MarketMetadataLoader.normalize_market_payload(
        {
            "id": "market_ts",
            "conditionId": "condition_ts",
            "clobTokenIds": ["tok_yes", "tok_no"],
            "outcomes": ["Yes", "No"],
            "startDate": "2026-03-08T00:00:00Z",
            "endDate": "2026-03-08T00:15:00Z",
            "minimum_tick_size": "0.01",
            "feeRateBps": "10",
            "minimum_order_size": "1",
            "status": "active",
            "startPrice": "1772974800",
        }
    )

    assert market.reference_price is None
