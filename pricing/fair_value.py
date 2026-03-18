from __future__ import annotations

from decimal import Decimal
from math import erf, log, sqrt
from typing import Protocol

from domain.models import MarketMetadata, TheoSnapshot
from pricing.lead_lag import SimpleLeadLagEngine
from pricing.vol_model import EwmaVolModel
from state.inventory_state import InMemoryInventoryStore


class FairValueEngine(Protocol):
    def compute(self, market: MarketMetadata, now_ms: int) -> TheoSnapshot | None: ...


class BinaryOptionFairValueEngine:
    _BINANCE_OPEN_ANCHOR_MAX_AGE_MS = 5_000
    _BINANCE_OPEN_ANCHOR_MAX_DELAY_MS = 5_000
    _CHAINLINK_OPEN_ANCHOR_MAX_AGE_MS = 30_000
    _CHAINLINK_OPEN_ANCHOR_MAX_DELAY_MS = 30_000

    def __init__(
        self,
        *,
        vol_model: EwmaVolModel,
        lead_lag: SimpleLeadLagEngine,
        inventory_state: InMemoryInventoryStore,
        fair_value_mode: str = "hybrid",
        min_probability: Decimal = Decimal("0.0010"),
        full_set_buffer: Decimal = Decimal("0.0040"),
    ) -> None:
        self._vol_model = vol_model
        self._lead_lag = lead_lag
        self._inventory_state = inventory_state
        self._fair_value_mode = fair_value_mode
        self._min_probability = min_probability
        self._full_set_buffer = full_set_buffer
        self._latest: dict[str, TheoSnapshot] = {}
        self._reference_price_cache: dict[str, Decimal] = {}

    def seed_reference_price(self, market_id: str, price: Decimal) -> None:
        if price > 0:
            self._reference_price_cache[market_id] = price

    def compute(self, market: MarketMetadata, now_ms: int) -> TheoSnapshot | None:
        time_to_close_s = max(market.end_ts_ms - now_ms, 0) / 1000
        sigma = self._vol_model.sigma_short(now_ms, time_to_close_s)
        spot = self._spot_price(now_ms)
        if sigma is None or spot is None or spot <= 0:
            return None

        reference_price = self._reference_price(market, now_ms, spot)
        if reference_price is None or reference_price <= 0:
            return None

        directional_bias = self._directional_bias(market.market_id, now_ms)
        theo_up = self._probability_up(
            spot=spot,
            reference_price=reference_price,
            sigma_short=sigma,
            time_to_close_s=time_to_close_s,
            directional_bias=directional_bias,
        )
        theo_down = Decimal("1") - theo_up
        pair_cost = self._inventory_state.get_pair_cost(market.market_id)
        target_full_set_cost = pair_cost
        if target_full_set_cost is None:
            fee_buffer = market.fee_rate_bps / Decimal("10000")
            target_full_set_cost = Decimal("1") - fee_buffer - self._full_set_buffer

        snapshot = TheoSnapshot(
            market_id=market.market_id,
            ts_ms=now_ms,
            sigma_short=sigma,
            theo_up=theo_up,
            theo_down=theo_down,
            directional_bias=directional_bias,
            target_full_set_cost=target_full_set_cost,
        )
        self._latest[market.market_id] = snapshot
        return snapshot

    def latest(self, market_id: str) -> TheoSnapshot | None:
        return self._latest.get(market_id)

    def _spot_price(self, now_ms: int) -> Decimal | None:
        if self._fair_value_mode == "binance_only":
            return self._lead_lag.current_binance_mid(now_ms)
        return self._lead_lag.current_binance_mid(now_ms) or self._lead_lag.current_chainlink_price(now_ms)

    def _reference_price(
        self,
        market: MarketMetadata,
        now_ms: int,
        spot: Decimal,
    ) -> Decimal | None:
        cached = self._reference_price_cache.get(market.market_id)
        if cached is not None:
            return cached

        market_has_started = now_ms >= market.start_ts_ms
        official_anchor = (
            market.reference_price
            if self._fair_value_mode != "binance_only"
            else None
        )
        if official_anchor is not None and official_anchor > 0:
            if market_has_started:
                self._reference_price_cache[market.market_id] = official_anchor
            return official_anchor

        if market_has_started:
            return None

        locked_anchor = self._locked_reference_price(market)
        if locked_anchor is not None and locked_anchor > 0:
            return locked_anchor

        if self._fair_value_mode == "binance_only":
            return self._lead_lag.current_binance_mid(now_ms)
        return self._lead_lag.current_chainlink_price(now_ms) or spot

    def _locked_reference_price(self, market: MarketMetadata) -> Decimal | None:
        return (
            self._lead_lag.binance_mid_at_or_before(
                market.start_ts_ms,
                max_age_ms=self._BINANCE_OPEN_ANCHOR_MAX_AGE_MS,
            )
            or self._lead_lag.binance_mid_at_or_after(
                market.start_ts_ms,
                max_delay_ms=self._BINANCE_OPEN_ANCHOR_MAX_DELAY_MS,
            )
            or (
                None
                if self._fair_value_mode == "binance_only"
                else self._lead_lag.chainlink_price_at_or_before(
                    market.start_ts_ms,
                    max_age_ms=self._CHAINLINK_OPEN_ANCHOR_MAX_AGE_MS,
                )
            )
            or (
                None
                if self._fair_value_mode == "binance_only"
                else self._lead_lag.chainlink_price_at_or_after(
                    market.start_ts_ms,
                    max_delay_ms=self._CHAINLINK_OPEN_ANCHOR_MAX_DELAY_MS,
                )
            )
        )

    def _directional_bias(self, market_id: str, now_ms: int) -> Decimal:
        if self._fair_value_mode == "binance_only":
            return Decimal("0")
        return self._lead_lag.directional_bias(market_id, now_ms) or Decimal("0")

    def _probability_up(
        self,
        *,
        spot: Decimal,
        reference_price: Decimal,
        sigma_short: Decimal,
        time_to_close_s: float,
        directional_bias: Decimal,
    ) -> Decimal:
        if time_to_close_s <= 0:
            if spot >= reference_price:
                return Decimal("1")
            return Decimal("0")

        adjusted_spot = float(spot * (Decimal("1") + directional_bias))
        strike = float(reference_price)
        sigma_total = float(sigma_short) * sqrt(time_to_close_s)

        if adjusted_spot <= 0 or strike <= 0 or sigma_total <= 1e-9:
            if adjusted_spot >= strike:
                return Decimal("1")
            return Decimal("0")

        log_moneyness = log(adjusted_spot / strike)
        score = (log_moneyness - 0.5 * (sigma_total * sigma_total)) / sigma_total
        probability = 0.5 * (1 + erf(score / sqrt(2)))
        clipped = min(1 - float(self._min_probability), max(float(self._min_probability), probability))
        return Decimal(f"{clipped:.6f}")
