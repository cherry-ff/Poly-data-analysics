from __future__ import annotations

import logging
from decimal import Decimal

from core.enums import OrderRole
from execution.intents import CancelIntent
from execution.polymarket_gateway import GatewayError, PolymarketGateway
from state.order_state import InMemoryOrderStateStore

logger = logging.getLogger(__name__)


class CancelManager:
    """Manages stale-quote cancellation and full market cancel-all.

    Responsibilities:
    - ``cancel_stale_quotes``: scan open MAKER_QUOTE orders older than
      ``max_age_ms`` and send cancel requests.  Called each lifecycle loop tick.
    - ``cancel_all_for_market``: emergency cancel-all for a given market
      (e.g. on FAST_CLOSE or system shutdown).

    The cancel manager sends cancels directly through the gateway without
    going through the router's risk engine — cancels are always allowed.
    The actual terminal state is confirmed later via user WS execution reports.

    Design note (from summary.md §8.4):
        "fair value 快速漂移时，优先级应是：撤旧单 -> 校验状态 -> 挂新单"
    This manager implements the first step of that pattern.
    """

    def __init__(
        self,
        order_state: InMemoryOrderStateStore,
        gateway: PolymarketGateway,
        max_maker_quote_age_ms: int = 30_000,
    ) -> None:
        self._order_state = order_state
        self._gateway = gateway
        self._max_age_ms = max_maker_quote_age_ms

    async def cancel_stale_quotes(self, market_id: str, now_ms: int) -> int:
        """Cancel MAKER_QUOTE orders older than ``max_maker_quote_age_ms``.

        Returns the number of cancel requests dispatched.
        """
        open_orders = self._order_state.get_open_orders(market_id)
        stale = [
            order
            for order in open_orders
            if order.role == OrderRole.MAKER_QUOTE
            and order.last_event_ts_ms > 0  # has been acknowledged at least once
            and (now_ms - order.last_event_ts_ms) > self._max_age_ms
        ]

        dispatched = 0
        for order in stale:
            intent = CancelIntent(
                client_order_id=order.client_order_id,
                market_id=market_id,
                reason=f"stale_quote: age={(now_ms - order.last_event_ts_ms)}ms",
            )
            if await self._send_cancel(intent):
                dispatched += 1

        if dispatched:
            logger.info(
                "[cancel_manager] cancelled %d stale maker quotes for market=%s",
                dispatched,
                market_id,
            )
        return dispatched

    async def cancel_all_for_market(self, market_id: str, reason: str) -> int:
        """Cancel ALL open orders for a market regardless of role or age.

        Used for:
        - FAST_CLOSE / FINAL_SECONDS phase transitions
        - Emergency shutdown
        - Market resolved event

        Returns the number of cancel requests dispatched.
        """
        open_orders = self._order_state.get_open_orders(market_id)
        dispatched = 0
        for order in open_orders:
            intent = CancelIntent(
                client_order_id=order.client_order_id,
                market_id=market_id,
                reason=reason,
            )
            if await self._send_cancel(intent):
                dispatched += 1

        if dispatched:
            logger.info(
                "[cancel_manager] cancel_all for market=%s reason=%s count=%d",
                market_id,
                reason,
                dispatched,
            )
        return dispatched

    async def cancel_by_pair(self, pair_id: str, reason: str) -> int:
        """Cancel all orders belonging to a specific pair_id.

        Useful when a pair needs to be unwound (one leg filled, other not yet).
        """
        pair_orders = self._order_state.get_pair_orders(pair_id)
        open_orders = [
            o for o in pair_orders
            if not _is_terminal(o.status.value)
        ]
        dispatched = 0
        for order in open_orders:
            intent = CancelIntent(
                client_order_id=order.client_order_id,
                market_id=order.market_id,
                reason=reason,
            )
            if await self._send_cancel(intent):
                dispatched += 1
        return dispatched

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------

    async def _send_cancel(self, intent: CancelIntent) -> bool:
        order = self._order_state.get(intent.client_order_id)
        exchange_order_id = (
            order.exchange_order_id if order is not None else intent.client_order_id
        )
        try:
            await self._gateway.cancel(exchange_order_id)
            logger.debug(
                "[cancel_manager] sent cancel cid=%s exch=%s reason=%s",
                intent.client_order_id,
                exchange_order_id,
                intent.reason,
            )
            return True
        except GatewayError as exc:
            logger.error(
                "[cancel_manager] cancel failed cid=%s exch=%s: %s",
                intent.client_order_id,
                exchange_order_id,
                exc,
            )
            return False


def _is_terminal(status: str) -> bool:
    return status in {"FILLED", "CANCELED", "REJECTED"}
