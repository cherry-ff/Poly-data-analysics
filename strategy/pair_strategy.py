from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal

from core.enums import OrderRole, Side, TimeInForce
from core.ids import SequentialIdGenerator
from domain.models import MarketMetadata, OrderIntent, OrderRecord
from market.lifecycle import LifecycleManager
from market.registry import InMemoryMarketRegistry
from pricing.quote_policy import MakerQuotePolicy
from state.order_state import InMemoryOrderStateStore
from strategy.phase_policy import PhasePolicy

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PairStrategyConfig:
    """Tunable parameters for PairStrategy."""

    quote_size: Decimal = field(default_factory=lambda: Decimal("10"))
    # Maximum open maker quotes per (token, side) slot.
    max_quote_depth: int = 1
    # If an existing order is within this many ticks of the target price,
    # treat it as "good enough" and skip generating a new intent.
    reprice_tolerance_ticks: int = 1


def _with_pair(intent: OrderIntent, pair_id: str) -> OrderIntent:
    """Return a copy of *intent* with *pair_id* filled in."""
    return OrderIntent(
        intent_id=intent.intent_id,
        pair_id=pair_id,
        market_id=intent.market_id,
        token_id=intent.token_id,
        side=intent.side,
        price=intent.price,
        size=intent.size,
        tif=intent.tif,
        post_only=intent.post_only,
        role=intent.role,
        reason=intent.reason,
    )


class PairStrategy:
    """Convert a ``QuotePlan`` into maker ``OrderIntent`` objects.

    Flow on each main-loop tick::

        on_tick(market_id, now_ms)
            -> quote_policy.build()   -> QuotePlan
            -> compare open orders    -> decide which sides need quoting
            -> return list[OrderIntent]

    The caller (AppRuntime / main loop) submits returned intents via
    ``ExecutionRouter.submit()``.  Cancellation of stale quotes is handled
    separately by ``CancelManager.cancel_stale_quotes()``.
    """

    def __init__(
        self,
        *,
        quote_policy: MakerQuotePolicy,
        order_state: InMemoryOrderStateStore,
        registry: InMemoryMarketRegistry,
        lifecycle: LifecycleManager,
        id_generator: SequentialIdGenerator,
        phase_policy: PhasePolicy | None = None,
        config: PairStrategyConfig | None = None,
    ) -> None:
        self._quote_policy = quote_policy
        self._order_state = order_state
        self._registry = registry
        self._lifecycle = lifecycle
        self._ids = id_generator
        self._phase_policy = phase_policy or PhasePolicy()
        self._config = config or PairStrategyConfig()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def on_tick(self, market_id: str, now_ms: int) -> list[OrderIntent]:
        """Return new maker intents for *market_id* at time *now_ms*.

        Returns an empty list when the current phase does not allow quoting,
        when the quote policy has no valid plan, or when existing open orders
        already adequately cover the target prices.
        """
        market = self._registry.get(market_id)
        if market is None:
            return []

        phase = self._lifecycle.get_phase(market_id)
        if not self._phase_policy.allow_new_quotes(phase):
            return []

        plan = self._quote_policy.build(market, now_ms)
        if plan is None:
            return []

        open_orders = self._order_state.get_open_orders(market_id)
        maker_quotes: list[OrderRecord] = [
            o for o in open_orders if o.role == OrderRole.MAKER_QUOTE
        ]

        intents: list[OrderIntent] = []

        # ---- bid legs -------------------------------------------------------
        # UP bid + DOWN bid share the same pair_id when both are generated;
        # this lets cancel_manager and inventory logic track them as a full set.
        up_bid = self._maybe_quote(
            market=market,
            token_id=market.up_token_id,
            side=Side.BUY,
            target_price=plan.up_bid_px,
            existing=maker_quotes,
        )
        down_bid = self._maybe_quote(
            market=market,
            token_id=market.down_token_id,
            side=Side.BUY,
            target_price=plan.down_bid_px,
            existing=maker_quotes,
        )

        if up_bid is not None and down_bid is not None:
            pair_id = self._ids.next_pair_id()
            intents.append(_with_pair(up_bid, pair_id))
            intents.append(_with_pair(down_bid, pair_id))
        else:
            if up_bid is not None:
                intents.append(up_bid)
            if down_bid is not None:
                intents.append(down_bid)

        # ---- ask legs -------------------------------------------------------
        # Ask-side quotes have no structural pair linkage.
        up_ask = self._maybe_quote(
            market=market,
            token_id=market.up_token_id,
            side=Side.SELL,
            target_price=plan.up_ask_px,
            existing=maker_quotes,
        )
        down_ask = self._maybe_quote(
            market=market,
            token_id=market.down_token_id,
            side=Side.SELL,
            target_price=plan.down_ask_px,
            existing=maker_quotes,
        )
        if up_ask is not None:
            intents.append(up_ask)
        if down_ask is not None:
            intents.append(down_ask)

        if intents:
            logger.debug(
                "[pair_strategy] market=%s phase=%s plan=%s intents=%d",
                market_id,
                phase.value,
                plan.reason,
                len(intents),
            )
        return intents

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _maybe_quote(
        self,
        *,
        market: MarketMetadata,
        token_id: str,
        side: Side,
        target_price: Decimal | None,
        existing: list[OrderRecord],
    ) -> OrderIntent | None:
        """Return a new intent for this side/token if one is needed.

        Returns ``None`` if:
        - *target_price* is ``None`` (quote policy has no price for this side)
        - an existing order is already close enough to the target price
        - the slot is at maximum depth (stale orders will be cleared by
          ``CancelManager`` before the next tick creates replacements)
        """
        if target_price is None:
            return None

        tolerance = market.tick_size * self._config.reprice_tolerance_ticks
        same_slot = [
            o for o in existing if o.token_id == token_id and o.side == side
        ]

        # If any existing order is adequately priced, skip.
        if any(abs(o.price - target_price) <= tolerance for o in same_slot):
            return None

        # If already at max depth (stale-priced), wait for CancelManager.
        if len(same_slot) >= self._config.max_quote_depth:
            return None

        return OrderIntent(
            intent_id=self._ids.next_intent_id(),
            pair_id=None,
            market_id=market.market_id,
            token_id=token_id,
            side=side,
            price=target_price,
            size=self._config.quote_size,
            tif=TimeInForce.GTC,
            post_only=True,
            role=OrderRole.MAKER_QUOTE,
            reason=f"pair_maker_{side.value.lower()}",
        )
