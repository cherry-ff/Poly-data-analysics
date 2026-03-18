from __future__ import annotations

from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Any

from core.enums import Side, TimeInForce
from domain.models import MarketMetadata, OrderIntent


# ---------------------------------------------------------------------------
# Tick-rounding helpers
# ---------------------------------------------------------------------------

def round_to_tick(
    price: Decimal,
    tick_size: Decimal,
    side: Side,
    post_only: bool,
) -> Decimal:
    """Round ``price`` to the nearest valid tick for ``tick_size``.

    Rounding direction is chosen conservatively to avoid crossing the spread
    for post-only (maker) orders:

    - BUY post_only  → round DOWN  (don't accidentally lift the ask)
    - SELL post_only → round UP    (don't accidentally hit the bid)
    - BUY taker      → round UP    (ensure the order hits)
    - SELL taker     → round DOWN  (ensure the order hits)

    Prices are clamped to the valid range [tick_size, 1 - tick_size].
    """
    if tick_size <= Decimal("0"):
        raise ValueError(f"tick_size must be positive, got {tick_size}")

    if post_only:
        rounding = ROUND_DOWN if side == Side.BUY else ROUND_UP
    else:
        rounding = ROUND_UP if side == Side.BUY else ROUND_DOWN

    rounded = (price / tick_size).quantize(Decimal("1"), rounding=rounding) * tick_size

    # Clamp to valid Polymarket range
    min_price = tick_size
    max_price = Decimal("1") - tick_size
    return max(min_price, min(max_price, rounded))


# ---------------------------------------------------------------------------
# TIF mapping
# ---------------------------------------------------------------------------

_TIF_MAP: dict[TimeInForce, str] = {
    TimeInForce.GTC: "GTC",
    TimeInForce.GTD: "GTD",
    TimeInForce.FOK: "FOK",
    TimeInForce.FAK: "FAK",
}


# ---------------------------------------------------------------------------
# OrderBuilder
# ---------------------------------------------------------------------------

class OrderBuilder:
    """Converts internal domain objects to Polymarket CLOB REST payloads.

    This class is responsible for:
    - Tick rounding prices to the current market's tick_size
    - Snapshotting fee_rate_bps from the market at build time
    - Assembling the correct ``side``, ``time_in_force``, and ``post_only``
      flags according to the intent

    It does NOT sign requests or make network calls.  Signing / HTTP is
    handled by ``PolymarketGateway``.
    """

    def build_place(
        self,
        intent: OrderIntent,
        market: MarketMetadata,
        client_order_id: str,
    ) -> dict[str, Any]:
        """Return a Polymarket CLOB order placement payload.

        The caller is responsible for passing the ``client_order_id`` that
        was already registered in ``OrderStateStore``.
        """
        rounded_price = round_to_tick(
            price=intent.price,
            tick_size=market.tick_size,
            side=intent.side,
            post_only=intent.post_only,
        )

        return {
            "market_id": market.market_id,
            "token_id": intent.token_id,
            "side": intent.side.value,
            "price": str(rounded_price),
            "size": str(intent.size),
            "time_in_force": _TIF_MAP.get(intent.tif, "GTC"),
            "post_only": intent.post_only,
            "client_order_id": client_order_id,
            "fee_rate_bps": str(market.fee_rate_bps),
        }

    def build_cancel(self, client_order_id: str) -> dict[str, Any]:
        """Return a Polymarket CLOB order cancellation payload."""
        return {"client_order_id": client_order_id}
