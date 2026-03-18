from __future__ import annotations

import asyncio
from decimal import Decimal

from domain.models import MarketMetadata
from pricing.binance_open_price import BinanceMinuteOpenPriceService


def _make_market(start_ts_ms: int) -> MarketMetadata:
    return MarketMetadata(
        market_id="m_open",
        condition_id="cond_open",
        up_token_id="up_tok",
        down_token_id="dn_tok",
        start_ts_ms=start_ts_ms,
        end_ts_ms=start_ts_ms + 900_000,
        tick_size=Decimal("0.01"),
        fee_rate_bps=Decimal("10"),
        min_order_size=Decimal("1"),
        status="active",
        reference_price=None,
    )


def test_binance_minute_open_service_aligns_market_start_to_minute_boundary() -> None:
    requests: list[tuple[str, dict[str, str] | None]] = []

    async def _fetcher(url: str, params: dict[str, str] | None) -> object:
        requests.append((url, params))
        assert params is not None
        start_time = int(params["startTime"])
        return [[start_time, "50000"]]

    market = _make_market(1_002_345)
    service = BinanceMinuteOpenPriceService(
        symbol="BTCUSDT",
        fetcher=_fetcher,
    )

    price = asyncio.run(service.ensure_market_open_price(market))

    assert price == Decimal("50000")
    assert requests == [
        (
            "https://api.binance.com/api/v3/klines",
            {
                "symbol": "BTCUSDT",
                "interval": "1m",
                "startTime": "960000",
                "limit": "1",
            },
        )
    ]


def test_binance_minute_open_service_caches_by_market_and_minute() -> None:
    calls = 0

    async def _fetcher(url: str, params: dict[str, str] | None) -> object:
        del url, params
        nonlocal calls
        calls += 1
        return [[960000, "50000"]]

    market = _make_market(1_002_345)
    service = BinanceMinuteOpenPriceService(
        symbol="BTCUSDT",
        fetcher=_fetcher,
    )

    first = asyncio.run(service.ensure_market_open_price(market))
    second = asyncio.run(service.ensure_market_open_price(market))

    assert first == Decimal("50000")
    assert second == Decimal("50000")
    assert calls == 1
