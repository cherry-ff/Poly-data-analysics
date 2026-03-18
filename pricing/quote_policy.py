from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Protocol

from core.enums import MarketPhase
from domain.models import MarketMetadata, QuotePlan, TheoSnapshot
from market.lifecycle import LifecycleManager
from pricing.fair_value import BinaryOptionFairValueEngine
from state.book_state import InMemoryBookStateStore
from state.inventory_state import InMemoryInventoryStore


class QuotePolicy(Protocol):
    def build(self, market: MarketMetadata, now_ms: int) -> QuotePlan | None: ...


class MakerQuotePolicy:
    def __init__(
        self,
        *,
        fair_value: BinaryOptionFairValueEngine,
        book_state: InMemoryBookStateStore,
        inventory_state: InMemoryInventoryStore,
        lifecycle_manager: LifecycleManager,
        min_price: Decimal = Decimal("0.01"),
        max_price: Decimal = Decimal("0.99"),
    ) -> None:
        self._fair_value = fair_value
        self._book_state = book_state
        self._inventory_state = inventory_state
        self._lifecycle_manager = lifecycle_manager
        self._min_price = min_price
        self._max_price = max_price
        self._latest: dict[str, QuotePlan] = {}

    def build(
        self,
        market: MarketMetadata,
        now_ms: int,
        theo: TheoSnapshot | None = None,
    ) -> QuotePlan | None:
        phase = self._lifecycle_manager.get_phase(market.market_id)
        if phase in {
            MarketPhase.FINAL_SECONDS,
            MarketPhase.CLOSED_WAIT_RESOLUTION,
            MarketPhase.RESOLVED,
            MarketPhase.ARCHIVED,
        }:
            return None

        pair_top = self._book_state.get_pair_top(market.market_id)
        if pair_top is None:
            return None

        snapshot = theo or self._fair_value.compute(market, now_ms)
        if snapshot is None:
            return None

        up_shift, down_shift = self._inventory_shift(market)
        half_width = self._phase_half_width(phase, market.tick_size)
        up_bid = self._bid_price(
            theo_px=snapshot.theo_up - up_shift,
            best_bid=pair_top.up.best_bid_px,
            best_ask=pair_top.up.best_ask_px,
            tick=market.tick_size,
            half_width=half_width,
        )
        up_ask = self._ask_price(
            theo_px=snapshot.theo_up - up_shift,
            best_bid=pair_top.up.best_bid_px,
            best_ask=pair_top.up.best_ask_px,
            tick=market.tick_size,
            half_width=half_width,
        )
        down_bid = self._bid_price(
            theo_px=snapshot.theo_down - down_shift,
            best_bid=pair_top.down.best_bid_px,
            best_ask=pair_top.down.best_ask_px,
            tick=market.tick_size,
            half_width=half_width,
        )
        down_ask = self._ask_price(
            theo_px=snapshot.theo_down - down_shift,
            best_bid=pair_top.down.best_bid_px,
            best_ask=pair_top.down.best_ask_px,
            tick=market.tick_size,
            half_width=half_width,
        )

        if phase == MarketPhase.FAST_CLOSE:
            imbalance = self._imbalance(market)
            if imbalance > 0:
                up_bid = None
                down_ask = None
            elif imbalance < 0:
                down_bid = None
                up_ask = None

        up_bid, down_bid = self._cap_pair_bid_sum(
            up_bid=up_bid,
            down_bid=down_bid,
            tick=market.tick_size,
            target=snapshot.target_full_set_cost,
        )

        plan = QuotePlan(
            market_id=market.market_id,
            ts_ms=now_ms,
            up_bid_px=up_bid,
            up_ask_px=up_ask,
            down_bid_px=down_bid,
            down_ask_px=down_ask,
            reason=f"maker_{phase.value.lower()}",
        )
        self._latest[market.market_id] = plan
        return plan

    def latest(self, market_id: str) -> QuotePlan | None:
        return self._latest.get(market_id)

    def _phase_half_width(self, phase: MarketPhase, tick_size: Decimal) -> Decimal:
        multiplier = {
            MarketPhase.PREWARM: Decimal("3"),
            MarketPhase.ACTIVE: Decimal("2"),
            MarketPhase.FAST_CLOSE: Decimal("4"),
        }.get(phase, Decimal("3"))
        return tick_size * multiplier

    def _inventory_shift(self, market: MarketMetadata) -> tuple[Decimal, Decimal]:
        imbalance = self._imbalance(market)
        if imbalance == 0:
            return Decimal("0"), Decimal("0")

        lot_size = market.min_order_size if market.min_order_size > 0 else Decimal("1")
        steps = min(abs(imbalance) / lot_size, Decimal("4"))
        shift = market.tick_size * steps
        if imbalance > 0:
            return shift, -shift
        return -shift, shift

    def _imbalance(self, market: MarketMetadata) -> Decimal:
        positions = {
            position.token_id: position
            for position in self._inventory_state.get_inventory(market.market_id)
        }
        up_size = positions.get(market.up_token_id)
        down_size = positions.get(market.down_token_id)
        return (up_size.net_size if up_size is not None else Decimal("0")) - (
            down_size.net_size if down_size is not None else Decimal("0")
        )

    def _bid_price(
        self,
        *,
        theo_px: Decimal,
        best_bid: Decimal,
        best_ask: Decimal,
        tick: Decimal,
        half_width: Decimal,
    ) -> Decimal | None:
        ceiling = min(self._max_price, best_ask - tick)
        if ceiling < self._min_price:
            return None
        raw = min(theo_px - half_width, ceiling)
        raw = max(raw, self._min_price)
        price = self._round_down(raw, tick)
        if price > ceiling:
            price = self._round_down(ceiling, tick)
        if price < self._min_price:
            return None
        return price

    def _ask_price(
        self,
        *,
        theo_px: Decimal,
        best_bid: Decimal,
        best_ask: Decimal,
        tick: Decimal,
        half_width: Decimal,
    ) -> Decimal | None:
        floor = max(self._min_price, best_bid + tick)
        if floor > self._max_price:
            return None
        raw = max(theo_px + half_width, floor)
        raw = min(raw, self._max_price)
        price = self._round_up(raw, tick)
        if price < floor:
            price = self._round_up(floor, tick)
        if price > self._max_price:
            return None
        return price

    def _cap_pair_bid_sum(
        self,
        *,
        up_bid: Decimal | None,
        down_bid: Decimal | None,
        tick: Decimal,
        target: Decimal,
    ) -> tuple[Decimal | None, Decimal | None]:
        if up_bid is None or down_bid is None:
            return up_bid, down_bid

        bid_sum = up_bid + down_bid
        if bid_sum <= target:
            return up_bid, down_bid

        excess = bid_sum - target
        reduction = self._round_up(excess / Decimal("2"), tick)
        capped_up = self._round_down(max(self._min_price, up_bid - reduction), tick)
        capped_down = self._round_down(max(self._min_price, down_bid - reduction), tick)
        return capped_up, capped_down

    @staticmethod
    def _round_down(value: Decimal, tick: Decimal) -> Decimal:
        return (value / tick).to_integral_value(rounding=ROUND_FLOOR) * tick

    @staticmethod
    def _round_up(value: Decimal, tick: Decimal) -> Decimal:
        return (value / tick).to_integral_value(rounding=ROUND_CEILING) * tick
