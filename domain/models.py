from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from core.enums import MarketPhase, OrderRole, OrderStatus, Side, TimeInForce


@dataclass(slots=True)
class MarketMetadata:
    market_id: str
    condition_id: str
    up_token_id: str
    down_token_id: str
    start_ts_ms: int
    end_ts_ms: int
    tick_size: Decimal
    fee_rate_bps: Decimal
    min_order_size: Decimal
    status: str
    reference_price: Decimal | None = None


@dataclass(slots=True)
class BinanceTick:
    symbol: str
    event_ts_ms: int
    recv_ts_ms: int
    last_price: Decimal
    best_bid: Decimal
    best_ask: Decimal


@dataclass(slots=True)
class ChainlinkTick:
    feed: str
    oracle_ts_ms: int
    recv_ts_ms: int
    price: Decimal
    round_id: str
    bid: Decimal | None = None
    ask: Decimal | None = None


@dataclass(slots=True)
class BookLevel:
    price: Decimal
    size: Decimal


@dataclass(slots=True)
class BinanceDepthSnapshot:
    symbol: str
    event_ts_ms: int
    recv_ts_ms: int
    last_update_id: int | None
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]


@dataclass(slots=True)
class PolymarketDepthSnapshot:
    market_id: str
    token_id: str
    event_type: str
    last_update_ts_ms: int
    bids: tuple[BookLevel, ...]
    asks: tuple[BookLevel, ...]


@dataclass(slots=True)
class OutcomeBookTop:
    token_id: str
    best_bid_px: Decimal
    best_bid_sz: Decimal
    best_ask_px: Decimal
    best_ask_sz: Decimal
    last_update_ts_ms: int


@dataclass(slots=True)
class PairBookTop:
    market_id: str
    up: OutcomeBookTop
    down: OutcomeBookTop

    @property
    def sum_best_bid(self) -> Decimal:
        return self.up.best_bid_px + self.down.best_bid_px

    @property
    def sum_best_ask(self) -> Decimal:
        return self.up.best_ask_px + self.down.best_ask_px


@dataclass(slots=True)
class TheoSnapshot:
    market_id: str
    ts_ms: int
    sigma_short: Decimal
    theo_up: Decimal
    theo_down: Decimal
    directional_bias: Decimal
    target_full_set_cost: Decimal


@dataclass(slots=True)
class QuotePlan:
    market_id: str
    ts_ms: int
    up_bid_px: Decimal | None
    up_ask_px: Decimal | None
    down_bid_px: Decimal | None
    down_ask_px: Decimal | None
    reason: str


@dataclass(slots=True)
class OrderIntent:
    intent_id: str
    pair_id: str | None
    market_id: str
    token_id: str
    side: Side
    price: Decimal
    size: Decimal
    tif: TimeInForce
    post_only: bool
    role: OrderRole
    reason: str


@dataclass(slots=True)
class ExecutionReport:
    client_order_id: str
    pair_id: str | None
    market_id: str
    token_id: str
    side: Side
    status: OrderStatus
    filled_size: Decimal
    avg_price: Decimal | None
    exchange_order_id: str | None
    event_ts_ms: int


@dataclass(slots=True)
class OrderRecord:
    client_order_id: str
    pair_id: str | None
    market_id: str
    token_id: str
    side: Side
    price: Decimal
    size: Decimal
    role: OrderRole
    status: OrderStatus
    filled_size: Decimal = Decimal("0")
    avg_price: Decimal | None = None
    exchange_order_id: str | None = None
    last_event_ts_ms: int = 0
    mismatch: bool = False


@dataclass(slots=True)
class InventoryLot:
    lot_id: str
    market_id: str
    token_id: str
    side: Side
    avg_cost: Decimal
    size: Decimal
    opened_ts_ms: int
    source: str


@dataclass(slots=True)
class InventoryPosition:
    market_id: str
    token_id: str
    net_size: Decimal
    avg_cost: Decimal


@dataclass(slots=True)
class LifecycleTransition:
    market_id: str
    previous_phase: MarketPhase
    new_phase: MarketPhase
    ts_ms: int
