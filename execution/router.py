from __future__ import annotations

import logging

from core.ids import SequentialIdGenerator
from domain.models import MarketMetadata, OrderIntent
from execution.intents import CancelIntent, ReplaceIntent
from execution.order_builder import OrderBuilder
from execution.polymarket_gateway import GatewayError, PolymarketGateway
from market.registry import InMemoryMarketRegistry
from risk.engine import RiskDecision, RiskEngine
from state.order_state import InMemoryOrderStateStore

logger = logging.getLogger(__name__)


class ExecutionRouter:
    """Routes order intents through the full execution pipeline:

        OrderIntent
            -> RiskEngine.evaluate()   (synchronous, hot-path)
            -> OrderBuilder.build_place()
            -> PolymarketGateway.place()
            -> InMemoryOrderStateStore.on_intent_sent()

    For cancels:
        CancelIntent
            -> PolymarketGateway.cancel()
            -> (no state update here; execution report from user WS will confirm)

    The router is the **only** component that calls the gateway.  All other
    components produce intents; this router is the single choke-point where
    risk is evaluated and orders are submitted.
    """

    def __init__(
        self,
        risk_engine: RiskEngine,
        order_builder: OrderBuilder,
        gateway: PolymarketGateway,
        order_state: InMemoryOrderStateStore,
        registry: InMemoryMarketRegistry,
        id_generator: SequentialIdGenerator,
    ) -> None:
        self._risk = risk_engine
        self._builder = order_builder
        self._gateway = gateway
        self._order_state = order_state
        self._registry = registry
        self._ids = id_generator

    # ------------------------------------------------------------------
    # Place
    # ------------------------------------------------------------------

    async def submit(self, intent: OrderIntent, now_ms: int) -> RiskDecision:
        """Evaluate risk, build payload, send to gateway, update local state.

        Returns the ``RiskDecision``.  If the decision is not allowed the
        gateway is not called.  On gateway failure a hard-reject decision is
        returned with the error reason.
        """
        decision = self._risk.evaluate(intent, now_ms)
        if not decision.allowed:
            logger.debug(
                "[router] REJECT intent=%s reason=%s", intent.intent_id, decision.reason
            )
            return decision

        market = self._registry.get(intent.market_id)
        if market is None:
            return RiskDecision(
                allowed=False,
                reason=f"router: market {intent.market_id} not in registry at submit time",
                severity="hard",
            )

        cid = self._ids.next_client_order_id()
        payload = self._builder.build_place(intent, market, cid)

        try:
            exchange_id = await self._gateway.place(payload)
        except GatewayError as exc:
            logger.error("[router] gateway PLACE error: %s", exc)
            return RiskDecision(
                allowed=False,
                reason=f"gateway_error: {exc}",
                severity="hard",
            )

        self._order_state.on_intent_sent(intent, cid, exchange_order_id=exchange_id)
        logger.info(
            "[router] PLACED intent=%s cid=%s exch=%s price=%s size=%s",
            intent.intent_id,
            cid,
            exchange_id,
            payload.get("price"),
            payload.get("size"),
        )

        if decision.severity == "soft":
            logger.warning("[router] soft risk warn on placed order: %s", decision.reason)

        return decision

    # ------------------------------------------------------------------
    # Cancel
    # ------------------------------------------------------------------

    async def cancel(self, cancel_intent: CancelIntent) -> bool:
        """Send a cancel request to the gateway.

        Returns True if the cancel was dispatched (or dry-run logged).
        The execution report from user WS will later confirm the terminal status.
        """
        order = self._order_state.get(cancel_intent.client_order_id)
        exchange_order_id = (
            order.exchange_order_id if order is not None else cancel_intent.client_order_id
        )
        try:
            await self._gateway.cancel(exchange_order_id)
            logger.info(
                "[router] CANCEL cid=%s exch=%s reason=%s",
                cancel_intent.client_order_id,
                exchange_order_id,
                cancel_intent.reason,
            )
            return True
        except GatewayError as exc:
            logger.error(
                "[router] gateway CANCEL error cid=%s exch=%s: %s",
                cancel_intent.client_order_id,
                exchange_order_id,
                exc,
            )
            return False

    # ------------------------------------------------------------------
    # Replace (best-effort)
    # ------------------------------------------------------------------

    async def replace(self, replace_intent: ReplaceIntent, now_ms: int) -> RiskDecision:
        """Cancel old order then place new one.

        The cancel is sent first.  If it fails we still attempt the new
        placement (the old order may have already been terminal on the
        exchange).  The new intent is risk-checked independently.
        """
        cancel = CancelIntent(
            client_order_id=replace_intent.cancel_client_order_id,
            market_id=replace_intent.new_intent.market_id,
            reason=f"replace:{replace_intent.reason}",
        )
        await self.cancel(cancel)
        return await self.submit(replace_intent.new_intent, now_ms)
