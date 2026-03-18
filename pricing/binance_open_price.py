from __future__ import annotations

import asyncio
import time
from decimal import Decimal
from typing import Any, Awaitable, Callable

from domain.models import MarketMetadata


Fetcher = Callable[[str, dict[str, Any] | None], Awaitable[Any]]


class BinanceMinuteOpenPriceService:
    _RETRY_COOLDOWN_MS = 1_000

    def __init__(
        self,
        *,
        symbol: str,
        rest_base_url: str = "https://api.binance.com",
        request_timeout_ms: int = 3_000,
        proxy_url: str | None = None,
        fetcher: Fetcher | None = None,
    ) -> None:
        self._symbol = symbol
        self._rest_base_url = rest_base_url.rstrip("/")
        self._request_timeout_ms = request_timeout_ms
        self._proxy_url = proxy_url or None
        self._fetcher = fetcher
        self._minute_cache: dict[int, Decimal] = {}
        self._market_cache: dict[str, Decimal] = {}
        self._pending: dict[int, asyncio.Task[Decimal | None]] = {}
        self._last_attempt_ts_ms: dict[int, int] = {}

    def cached_market_open_price(self, market_id: str) -> Decimal | None:
        return self._market_cache.get(market_id)

    async def ensure_market_open_price(
        self,
        market: MarketMetadata,
    ) -> Decimal | None:
        cached = self._market_cache.get(market.market_id)
        if cached is not None:
            return cached

        minute_start_ms = self.minute_start_ts_ms(market.start_ts_ms)
        price = await self.minute_open_price(minute_start_ms)
        if price is not None:
            self._market_cache[market.market_id] = price
        return price

    async def minute_open_price(self, minute_start_ms: int) -> Decimal | None:
        cached = self._minute_cache.get(minute_start_ms)
        if cached is not None:
            return cached

        pending = self._pending.get(minute_start_ms)
        if pending is not None:
            return await pending

        now_ms = self._now_ms()
        last_attempt_ts_ms = self._last_attempt_ts_ms.get(minute_start_ms)
        if (
            last_attempt_ts_ms is not None
            and now_ms - last_attempt_ts_ms < self._RETRY_COOLDOWN_MS
        ):
            return None

        self._last_attempt_ts_ms[minute_start_ms] = now_ms
        task = asyncio.create_task(
            self._fetch_minute_open_price(minute_start_ms),
            name=f"binance-minute-open:{minute_start_ms}",
        )
        self._pending[minute_start_ms] = task
        try:
            price = await task
        finally:
            self._pending.pop(minute_start_ms, None)

        if price is not None:
            self._minute_cache[minute_start_ms] = price
        return price

    @staticmethod
    def minute_start_ts_ms(ts_ms: int) -> int:
        if ts_ms <= 0:
            return 0
        return ts_ms - (ts_ms % 60_000)

    async def _fetch_minute_open_price(self, minute_start_ms: int) -> Decimal | None:
        if minute_start_ms <= 0:
            return None

        if _is_okx_rest_base_url(self._rest_base_url):
            return await self._fetch_okx_minute_open_price(minute_start_ms)

        payload = await self._fetch_json(
            f"{self._rest_base_url}/api/v3/klines",
            params={
                "symbol": self._symbol,
                "interval": "1m",
                "startTime": str(minute_start_ms),
                "limit": "1",
            },
        )
        if not isinstance(payload, list) or not payload:
            return None

        candle = payload[0]
        if not isinstance(candle, list) or len(candle) < 2:
            return None

        open_time = candle[0]
        if int(open_time) != minute_start_ms:
            return None

        return Decimal(str(candle[1]))

    async def _fetch_okx_minute_open_price(self, minute_start_ms: int) -> Decimal | None:
        params = {
            "instId": self._symbol,
            "bar": "1m",
            "limit": "100",
        }
        for endpoint in (
            "/api/v5/market/candles",
            "/api/v5/market/history-candles",
        ):
            payload = await self._fetch_json(
                f"{self._rest_base_url}{endpoint}",
                params=params,
            )
            data = payload.get("data") if isinstance(payload, dict) else None
            if not isinstance(data, list):
                continue
            for candle in data:
                if not isinstance(candle, list) or len(candle) < 2:
                    continue
                if int(candle[0]) != minute_start_ms:
                    continue
                return Decimal(str(candle[1]))
        return None

    async def _fetch_json(
        self,
        url: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        if self._fetcher is not None:
            return await self._fetcher(url, params)

        try:
            import aiohttp  # type: ignore
        except ImportError as exc:
            raise RuntimeError("aiohttp is required to fetch Binance minute prices") from exc

        timeout = aiohttp.ClientTimeout(total=self._request_timeout_ms / 1000)
        connector = aiohttp.TCPConnector(ssl=False) if self._proxy_url else None
        async with aiohttp.ClientSession(
            timeout=timeout,
            trust_env=True,
            connector=connector,
        ) as session:
            async with session.get(
                url,
                params=params,
                proxy=self._proxy_url,
            ) as response:
                response.raise_for_status()
                return await response.json()

    @staticmethod
    def _now_ms() -> int:
        return time.monotonic_ns() // 1_000_000


def _is_okx_rest_base_url(rest_base_url: str) -> bool:
    return "okx.com" in rest_base_url
