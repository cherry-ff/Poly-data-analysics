from __future__ import annotations

from dataclasses import fields, is_dataclass
from decimal import Decimal
from enum import Enum
from types import UnionType
from typing import Any, Union, get_args, get_origin, get_type_hints

from core.enums import MarketPhase, OrderRole, OrderStatus, Side, TimeInForce
from domain.events import (
    BinanceDepthEvent,
    BinanceTickEvent,
    ChainlinkTickEvent,
    LifecycleTransitionEvent,
    MarketBookTopEvent,
    MarketDiscoveredEvent,
    MarketMetadataEvent,
    PolymarketDepthEvent,
    MarketResolvedEvent,
    MarketTickSizeChangeEvent,
    OrderIntentsEvent,
    QuotePlanEvent,
    TheoSnapshotEvent,
    UserExecutionEvent,
)
from domain.models import (
    BinanceDepthSnapshot,
    BinanceTick,
    BookLevel,
    ChainlinkTick,
    ExecutionReport,
    InventoryLot,
    InventoryPosition,
    LifecycleTransition,
    MarketMetadata,
    OrderIntent,
    OrderRecord,
    OutcomeBookTop,
    PairBookTop,
    PolymarketDepthSnapshot,
    QuotePlan,
    TheoSnapshot,
)
from replay.player import TypeRegistry


_ALL_REPLAY_TYPES = (
    MarketMetadata,
    BinanceTick,
    BinanceDepthSnapshot,
    BookLevel,
    ChainlinkTick,
    OutcomeBookTop,
    PairBookTop,
    PolymarketDepthSnapshot,
    TheoSnapshot,
    QuotePlan,
    OrderIntent,
    ExecutionReport,
    OrderRecord,
    InventoryLot,
    InventoryPosition,
    LifecycleTransition,
    BinanceDepthEvent,
    BinanceTickEvent,
    ChainlinkTickEvent,
    MarketDiscoveredEvent,
    MarketMetadataEvent,
    MarketBookTopEvent,
    PolymarketDepthEvent,
    MarketTickSizeChangeEvent,
    MarketResolvedEvent,
    LifecycleTransitionEvent,
    TheoSnapshotEvent,
    QuotePlanEvent,
    OrderIntentsEvent,
    UserExecutionEvent,
)


def build_default_registry() -> TypeRegistry:
    """Register all current domain models/events used by recorder JSONL."""
    registry = TypeRegistry()
    for cls in _ALL_REPLAY_TYPES:
        registry.register(cls.__name__, lambda payload, cls=cls: _from_dict(cls, payload))
    return registry


def _from_dict(cls: type[Any], payload: dict[str, Any]) -> Any:
    if not is_dataclass(cls):
        return payload

    hints = get_type_hints(cls)
    kwargs: dict[str, Any] = {}
    for field in fields(cls):
        value = payload.get(field.name)
        kwargs[field.name] = _coerce(hints.get(field.name, field.type), value)
    return cls(**kwargs)


def _coerce(tp: Any, value: Any) -> Any:
    if value is None:
        return None

    origin = get_origin(tp)
    args = get_args(tp)

    if origin in {list, tuple, set}:
        item_type = args[0] if args else Any
        items = [_coerce(item_type, item) for item in value]
        if origin is tuple:
            return tuple(items)
        if origin is set:
            return set(items)
        return items

    if origin is dict:
        key_type = args[0] if len(args) > 0 else Any
        value_type = args[1] if len(args) > 1 else Any
        return {
            _coerce(key_type, key): _coerce(value_type, item)
            for key, item in value.items()
        }

    if origin in {UnionType, Union}:
        non_none = [arg for arg in args if arg is not type(None)]
        if len(non_none) == 1:
            return _coerce(non_none[0], value)
        for candidate in non_none:
            try:
                return _coerce(candidate, value)
            except Exception:
                continue
        return value

    if tp is Decimal:
        return Decimal(str(value))

    if tp in {int, float, str, bool}:
        return tp(value)

    if isinstance(tp, type) and issubclass(tp, Enum):
        return tp(value)

    if isinstance(tp, type) and is_dataclass(tp):
        if not isinstance(value, dict):
            raise TypeError(f"expected dict for dataclass {tp!r}, got {type(value)!r}")
        return _from_dict(tp, value)

    return value
