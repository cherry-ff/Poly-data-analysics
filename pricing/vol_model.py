from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from math import exp, log, sqrt
from typing import Protocol

from domain.models import BinanceTick


@dataclass(slots=True)
class _ReturnSample:
    ts_ms: int
    dt_ms: int
    inst_var: float


class VolModel(Protocol):
    def on_binance_tick(self, tick: BinanceTick) -> None: ...

    def sigma_short(self, now_ms: int, horizon_s: float | None = None) -> Decimal | None: ...


class EwmaVolModel:
    _WINDOW_MS: tuple[int, ...] = (
        5_000,
        30_000,
        120_000,
        300_000,
        900_000,
    )

    def __init__(
        self,
        *,
        half_life_ms: int = 15_000,
        max_age_ms: int = 1_800_000,
        stale_after_ms: int = 2_000,
        min_sigma: Decimal = Decimal("0.00005"),
        max_sigma: Decimal = Decimal("0.00500"),
    ) -> None:
        self._half_life_ms = half_life_ms
        self._max_age_ms = max_age_ms
        self._stale_after_ms = stale_after_ms
        self._min_sigma = min_sigma
        self._max_sigma = max_sigma
        self._samples: deque[_ReturnSample] = deque()
        self._ewma_var: float | None = None
        self._last_mid: Decimal | None = None
        self._last_ts_ms: int | None = None

    def on_binance_tick(self, tick: BinanceTick) -> None:
        mid = (tick.best_bid + tick.best_ask) / Decimal("2")
        ts_ms = tick.event_ts_ms or tick.recv_ts_ms

        if mid <= 0:
            return

        if self._last_mid is None or self._last_ts_ms is None:
            self._last_mid = mid
            self._last_ts_ms = ts_ms
            return

        dt_ms = ts_ms - self._last_ts_ms
        if dt_ms <= 0:
            self._last_mid = mid
            self._last_ts_ms = ts_ms
            return

        log_return = log(float(mid / self._last_mid))
        inst_var = (log_return * log_return) / max(dt_ms / 1000, 1e-9)
        alpha = 1 - exp(-log(2) * dt_ms / self._half_life_ms)

        if self._ewma_var is None:
            self._ewma_var = inst_var
        else:
            self._ewma_var = alpha * inst_var + (1 - alpha) * self._ewma_var

        self._samples.append(_ReturnSample(ts_ms=ts_ms, dt_ms=dt_ms, inst_var=inst_var))
        self._last_mid = mid
        self._last_ts_ms = ts_ms
        self._prune(ts_ms)

    def sigma_short(self, now_ms: int, horizon_s: float | None = None) -> Decimal | None:
        if self._last_ts_ms is None or now_ms - self._last_ts_ms > self._stale_after_ms:
            return None

        target_window_ms = self._target_window_ms(horizon_s)
        window_sigmas = {
            window_ms: self._window_sigma(now_ms, window_ms)
            for window_ms in self._WINDOW_MS
        }
        components: list[tuple[float, float]] = []
        for window_ms, sigma in window_sigmas.items():
            if sigma is None:
                continue
            weight = self._window_weight(window_ms, target_window_ms)
            components.append((sigma, weight))

        if self._ewma_var is not None:
            components.append((sqrt(self._ewma_var), 0.75))

        if not components:
            return None

        weight_sum = sum(weight for _, weight in components)
        blended = sum(sigma * weight for sigma, weight in components) / weight_sum

        long_anchor = window_sigmas.get(900_000) or window_sigmas.get(300_000) or window_sigmas.get(120_000)
        if long_anchor is not None:
            long_weight = self._long_anchor_weight(target_window_ms)
            blended = ((1 - long_weight) * blended) + (long_weight * long_anchor)

        bounded = max(float(self._min_sigma), min(blended, float(self._max_sigma)))
        return Decimal(f"{bounded:.8f}")

    def latest_mid(self) -> Decimal | None:
        return self._last_mid

    def _window_sigma(self, now_ms: int, window_ms: int) -> float | None:
        weighted_var = 0.0
        weight_sum = 0.0

        for sample in reversed(self._samples):
            age_ms = now_ms - sample.ts_ms
            if age_ms > window_ms:
                break
            weighted_var += sample.inst_var * sample.dt_ms
            weight_sum += sample.dt_ms

        if weight_sum <= 0:
            return None
        return sqrt(weighted_var / weight_sum)

    def _prune(self, now_ms: int) -> None:
        while self._samples and now_ms - self._samples[0].ts_ms > self._max_age_ms:
            self._samples.popleft()

    @staticmethod
    def _target_window_ms(horizon_s: float | None) -> int:
        if horizon_s is None or horizon_s <= 0:
            return 120_000
        return max(15_000, min(int(horizon_s * 1000), 900_000))

    @staticmethod
    def _window_weight(window_ms: int, target_window_ms: int) -> float:
        distance = abs(log(window_ms / max(target_window_ms, 1)))
        return exp(-distance)

    @staticmethod
    def _long_anchor_weight(target_window_ms: int) -> float:
        if target_window_ms >= 600_000:
            return 0.40
        if target_window_ms >= 180_000:
            return 0.28
        return 0.16
