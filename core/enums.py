from __future__ import annotations

from enum import Enum


class MarketPhase(str, Enum):
    DISCOVERED = "DISCOVERED"
    PREWARM = "PREWARM"
    ACTIVE = "ACTIVE"
    FAST_CLOSE = "FAST_CLOSE"
    FINAL_SECONDS = "FINAL_SECONDS"
    CLOSED_WAIT_RESOLUTION = "CLOSED_WAIT_RESOLUTION"
    RESOLVED = "RESOLVED"
    ARCHIVED = "ARCHIVED"


class OrderStatus(str, Enum):
    PENDING_SUBMIT = "PENDING_SUBMIT"
    OPEN = "OPEN"
    PARTIAL = "PARTIAL"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    UNKNOWN = "UNKNOWN"


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class TimeInForce(str, Enum):
    GTC = "GTC"
    GTD = "GTD"
    FOK = "FOK"
    FAK = "FAK"


class OrderRole(str, Enum):
    UNKNOWN = "UNKNOWN"
    MAKER_QUOTE = "MAKER_QUOTE"
    SELECTIVE_TAKER = "SELECTIVE_TAKER"
    RECOVERY = "RECOVERY"
    UNWIND = "UNWIND"
