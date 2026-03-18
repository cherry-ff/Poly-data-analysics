from __future__ import annotations

import logging
from dataclasses import dataclass, field
from decimal import Decimal

from core.enums import OrderRole, Side, TimeInForce
from core.ids import SequentialIdGenerator
from domain.models import ExecutionReport, MarketMetadata, OrderIntent
from market.lifecycle import LifecycleManager
from market.registry import InMemoryMarketRegistry
from state.book_state import InMemoryBookStateStore
from state.inventory_state import InMemoryInventoryStore
from state.order_state import InMemoryOrderStateStore
from strategy.phase_policy import PhasePolicy

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class RecoveryStrategyConfig:
    """Tunable parameters for RecoveryStrategy."""

    # Minimum unhedged exposure (token units) that triggers recovery.
    min_recovery_threshold: Decimal = field(default_factory=lambda: Decimal("1"))
    # Maximum size of a single recovery order.
    max_recovery_size: Decimal = field(default_factory=lambda: Decimal("500"))
    # Maximum number of concurrent open recovery orders per market.
    max_concurrent_recovery: int = 3


class RecoveryStrategy:
    """Generate recovery ``OrderIntent`` objects for unhedged single-leg inventory.

    Called in two contexts:

    1. ``on_fill(report)`` — immediately after an execution report indicates a
       fill.  If the fill creates or widens an unhedged position the strategy
       returns one or more aggressive recovery intents (FOK) to close the gap
       quickly.

    2. ``on_timer(market_id, now_ms)`` — periodic sweep.  If unhedged exposure
       remains above threshold and there are no pending recovery orders, return
       passive recovery intents (GTC, not post-only) to work the position down.

    The caller (AppRuntime / main loop) submits returned intents via
    ``ExecutionRouter.submit()``.
    """

    def __init__(
        self,
        *,
        inventory_state: InMemoryInventoryStore,
        order_state: InMemoryOrderStateStore,
        book_state: InMemoryBookStateStore,
        registry: InMemoryMarketRegistry,
        lifecycle: LifecycleManager,
        id_generator: SequentialIdGenerator,
        phase_policy: PhasePolicy | None = None,
        config: RecoveryStrategyConfig | None = None,
    ) -> None:
        self._inventory = inventory_state
        self._order_state = order_state
        self._book_state = book_state
        self._registry = registry
        self._lifecycle = lifecycle
        self._ids = id_generator
        self._phase_policy = phase_policy or PhasePolicy()
        self._config = config or RecoveryStrategyConfig()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def on_fill(self, report: ExecutionReport) -> list[OrderIntent]:
        """React to a fill event; return aggressive recovery intents if unhedged.

        Safe to call for partial fills and non-fill events (returns ``[]``).
        """
        if report.filled_size <= Decimal("0"):
            return []

        market = self._registry.get(report.market_id)
        if market is None:
            return []

        phase = self._lifecycle.get_phase(report.market_id)
        if not self._phase_policy.allow_recovery(phase):
            return []

        unhedged = self._inventory.get_unhedged_exposure(report.market_id)
        if unhedged < self._config.min_recovery_threshold:
            return []

        intents = self._build_recovery_intents(
            market=market,
            tif=TimeInForce.FOK,  # aggressive: fill-or-kill
            reason="recovery_on_fill",
        )
        if intents:
            logger.info(
                "[recovery] on_fill market=%s unhedged=%s -> %d intent(s)",
                report.market_id,
                unhedged,
                len(intents),
            )
        return intents

    def on_timer(self, market_id: str, now_ms: int) -> list[OrderIntent]:  # noqa: ARG002
        """Periodic sweep: return passive recovery intents if unhedged and no pending recovery.

        *now_ms* is accepted for future use (e.g. time-to-close throttling)
        but is not currently used internally.
        """
        market = self._registry.get(market_id)
        if market is None:
            return []

        phase = self._lifecycle.get_phase(market_id)
        if not self._phase_policy.allow_recovery(phase):
            return []

        unhedged = self._inventory.get_unhedged_exposure(market_id)
        if unhedged < self._config.min_recovery_threshold:
            return []

        # Don't pile on if recovery orders are already working.
        open_orders = self._order_state.get_open_orders(market_id)
        pending_recovery = [o for o in open_orders if o.role == OrderRole.RECOVERY]
        if len(pending_recovery) >= self._config.max_concurrent_recovery:
            return []

        intents = self._build_recovery_intents(
            market=market,
            tif=TimeInForce.GTC,  # passive: rest in book
            reason="recovery_on_timer",
        )
        if intents:
            logger.info(
                "[recovery] on_timer market=%s unhedged=%s -> %d intent(s)",
                market_id,
                unhedged,
                len(intents),
            )
        return intents

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _build_recovery_intents(
        self,
        *,
        market: MarketMetadata,
        tif: TimeInForce,
        reason: str,
    ) -> list[OrderIntent]:
        """Identify the under-inventoried token and return a BUY intent for it.

        Returns an empty list when:
        - inventory is balanced
        - the recovery size is below the market minimum
        - the book has no valid ask price for the target token
        """
        positions = {
            pos.token_id: pos
            for pos in self._inventory.get_inventory(market.market_id)
        }
        up_net = positions[market.up_token_id].net_size if market.up_token_id in positions else Decimal("0")
        down_net = positions[market.down_token_id].net_size if market.down_token_id in positions else Decimal("0")

        if up_net > down_net:
            token_id = market.down_token_id
            missing = up_net - down_net
        elif down_net > up_net:
            token_id = market.up_token_id
            missing = down_net - up_net
        else:
            return []  # already balanced

        recovery_size = min(missing, self._config.max_recovery_size)
        if recovery_size < market.min_order_size:
            return []

        book_top = self._book_state.get_top(market.market_id, token_id)
        if book_top is None:
            logger.debug(
                "[recovery] no book top for market=%s token=%s; skipping",
                market.market_id,
                token_id,
            )
            return []

        price = book_top.best_ask_px
        if price <= Decimal("0") or price >= Decimal("1"):
            return []

        return [
            OrderIntent(
                intent_id=self._ids.next_intent_id(),
                pair_id=None,
                market_id=market.market_id,
                token_id=token_id,
                side=Side.BUY,
                price=price,
                size=recovery_size,
                tif=tif,
                post_only=False,
                role=OrderRole.RECOVERY,
                reason=reason,
            )
        ]
