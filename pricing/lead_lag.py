from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from domain.models import BinanceTick, ChainlinkTick


class LeadLagEngine(Protocol):
    def on_binance_tick(self, tick: BinanceTick) -> None: ...

    def on_chainlink_tick(self, tick: ChainlinkTick) -> None: ...

    def current_basis(self) -> Decimal | None: ...

    def directional_bias(self, market_id: str, now_ms: int) -> Decimal | None: ...

    def binance_mid_at_or_before(self, ts_ms: int, max_age_ms: int | None = None) -> Decimal | None: ...

    def chainlink_price_at_or_before(self, ts_ms: int, max_age_ms: int | None = None) -> Decimal | None: ...

    def binance_mid_at_or_after(self, ts_ms: int, max_delay_ms: int | None = None) -> Decimal | None: ...

    def chainlink_price_at_or_after(self, ts_ms: int, max_delay_ms: int | None = None) -> Decimal | None: ...


@dataclass(slots=True)
class _PricePoint:
    ts_ms: int
    price: Decimal


class SimpleLeadLagEngine:
    def __init__(
        self,
        *,
        binance_stale_ms: int = 1_500,
        chainlink_stale_ms: int = 2_500,
        max_abs_bias: Decimal = Decimal("0.0800"),
        history_max_age_ms: int = 1_800_000,
    ) -> None:
        self._binance_stale_ms = binance_stale_ms
        self._chainlink_stale_ms = chainlink_stale_ms
        self._max_abs_bias = max_abs_bias
        self._history_max_age_ms = history_max_age_ms
        self._last_binance_mid: Decimal | None = None
        self._prev_binance_mid: Decimal | None = None
        self._last_binance_ts_ms: int | None = None
        self._last_chainlink_price: Decimal | None = None
        self._last_chainlink_ts_ms: int | None = None
        self._binance_history: deque[_PricePoint] = deque()
        self._chainlink_history: deque[_PricePoint] = deque()

    def on_binance_tick(self, tick: BinanceTick) -> None:
        self._prev_binance_mid = self._last_binance_mid
        self._last_binance_mid = (tick.best_bid + tick.best_ask) / Decimal("2")
        self._last_binance_ts_ms = tick.event_ts_ms or tick.recv_ts_ms
        self._binance_history.append(
            _PricePoint(ts_ms=self._last_binance_ts_ms, price=self._last_binance_mid)
        )
        self._prune_history(self._binance_history, self._last_binance_ts_ms)

    def on_chainlink_tick(self, tick: ChainlinkTick) -> None:
        self._last_chainlink_price = tick.price
        self._last_chainlink_ts_ms = tick.oracle_ts_ms or tick.recv_ts_ms
        self._chainlink_history.append(
            _PricePoint(ts_ms=self._last_chainlink_ts_ms, price=self._last_chainlink_price)
        )
        self._prune_history(self._chainlink_history, self._last_chainlink_ts_ms)

    def current_basis(self) -> Decimal | None:
        if self._last_binance_mid is None or self._last_chainlink_price is None:
            return None
        return self._last_binance_mid - self._last_chainlink_price

    def directional_bias(self, market_id: str, now_ms: int) -> Decimal | None:
        del market_id

        binance = self.current_binance_mid(now_ms)
        if binance is None or binance <= 0:
            return None

        relative_basis = Decimal("0")
        chainlink = self.current_chainlink_price(now_ms)
        if chainlink is not None and chainlink > 0:
            relative_basis = (binance - chainlink) / chainlink

        momentum = Decimal("0")
        if self._prev_binance_mid is not None and self._prev_binance_mid > 0:
            momentum = (binance - self._prev_binance_mid) / self._prev_binance_mid

        bias = relative_basis * Decimal("0.65") + momentum * Decimal("0.35")
        return self._clip(bias, -self._max_abs_bias, self._max_abs_bias)

    def current_binance_mid(self, now_ms: int | None = None) -> Decimal | None:
        if self._last_binance_mid is None:
            return None
        if (
            now_ms is not None
            and self._last_binance_ts_ms is not None
            and now_ms - self._last_binance_ts_ms > self._binance_stale_ms
        ):
            return None
        return self._last_binance_mid

    def current_chainlink_price(self, now_ms: int | None = None) -> Decimal | None:
        if self._last_chainlink_price is None:
            return None
        if (
            now_ms is not None
            and self._last_chainlink_ts_ms is not None
            and now_ms - self._last_chainlink_ts_ms > self._chainlink_stale_ms
        ):
            return None
        return self._last_chainlink_price

    def binance_mid_at_or_before(self, ts_ms: int, max_age_ms: int | None = None) -> Decimal | None:
        return self._price_at_or_before(self._binance_history, ts_ms, max_age_ms)

    def chainlink_price_at_or_before(self, ts_ms: int, max_age_ms: int | None = None) -> Decimal | None:
        return self._price_at_or_before(self._chainlink_history, ts_ms, max_age_ms)

    def binance_mid_at_or_after(self, ts_ms: int, max_delay_ms: int | None = None) -> Decimal | None:
        return self._price_at_or_after(self._binance_history, ts_ms, max_delay_ms)

    def chainlink_price_at_or_after(self, ts_ms: int, max_delay_ms: int | None = None) -> Decimal | None:
        return self._price_at_or_after(self._chainlink_history, ts_ms, max_delay_ms)

    @staticmethod
    def _clip(value: Decimal, low: Decimal, high: Decimal) -> Decimal:
        if value < low:
            return low
        if value > high:
            return high
        return value

    def _prune_history(self, history: deque[_PricePoint], now_ms: int) -> None:
        while history and now_ms - history[0].ts_ms > self._history_max_age_ms:
            history.popleft()

    @staticmethod
    def _price_at_or_before(
        history: deque[_PricePoint],
        ts_ms: int,
        max_age_ms: int | None,
    ) -> Decimal | None:
        for point in reversed(history):
            if point.ts_ms > ts_ms:
                continue
            if max_age_ms is not None and ts_ms - point.ts_ms > max_age_ms:
                return None
            return point.price
        return None

    @staticmethod
    def _price_at_or_after(
        history: deque[_PricePoint],
        ts_ms: int,
        max_delay_ms: int | None,
    ) -> Decimal | None:
        for point in history:
            if point.ts_ms < ts_ms:
                continue
            if max_delay_ms is not None and point.ts_ms - ts_ms > max_delay_ms:
                return None
            return point.price
        return None
