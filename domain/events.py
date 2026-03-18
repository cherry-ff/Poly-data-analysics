from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from domain.models import (
    BinanceDepthSnapshot,
    BinanceTick,
    ChainlinkTick,
    ExecutionReport,
    LifecycleTransition,
    MarketMetadata,
    OrderIntent,
    OutcomeBookTop,
    PolymarketDepthSnapshot,
    QuotePlan,
    TheoSnapshot,
)


@dataclass(slots=True)
class BinanceTickEvent:
    tick: BinanceTick


@dataclass(slots=True)
class BinanceDepthEvent:
    snapshot: BinanceDepthSnapshot


@dataclass(slots=True)
class ChainlinkTickEvent:
    tick: ChainlinkTick


@dataclass(slots=True)
class MarketDiscoveredEvent:
    market_id: str
    discovered_ts_ms: int


@dataclass(slots=True)
class MarketMetadataEvent:
    market: MarketMetadata


@dataclass(slots=True)
class MarketBookTopEvent:
    market_id: str
    top: OutcomeBookTop


@dataclass(slots=True)
class PolymarketDepthEvent:
    snapshot: PolymarketDepthSnapshot


@dataclass(slots=True)
class MarketTickSizeChangeEvent:
    market_id: str
    tick_size: Decimal
    event_ts_ms: int


@dataclass(slots=True)
class MarketResolvedEvent:
    market_id: str
    resolved_ts_ms: int


@dataclass(slots=True)
class LifecycleTransitionEvent:
    transition: LifecycleTransition


@dataclass(slots=True)
class TheoSnapshotEvent:
    snapshot: TheoSnapshot


@dataclass(slots=True)
class QuotePlanEvent:
    plan: QuotePlan


@dataclass(slots=True)
class OrderIntentsEvent:
    source: str
    market_id: str
    intents: list[OrderIntent]


@dataclass(slots=True)
class UserExecutionEvent:
    report: ExecutionReport
