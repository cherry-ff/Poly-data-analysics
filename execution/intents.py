from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from domain.models import OrderIntent


class IntentAction(str, Enum):
    """The three fundamental actions the execution layer understands."""

    PLACE = "PLACE"
    CANCEL = "CANCEL"
    REPLACE = "REPLACE"  # atomic cancel + place (best-effort; not all venues guarantee this)


@dataclass(slots=True)
class CancelIntent:
    """Request to cancel an open order by client_order_id."""

    client_order_id: str
    market_id: str
    reason: str


@dataclass(slots=True)
class ReplaceIntent:
    """Best-effort replace: cancel an old order and place a new one.

    The router submits the cancel first.  If the cancel is confirmed (or the
    order is already terminal), it then places the new order.  If the cancel
    is still pending, the router queues the new order until confirmation.
    """

    cancel_client_order_id: str
    new_intent: OrderIntent
    reason: str
