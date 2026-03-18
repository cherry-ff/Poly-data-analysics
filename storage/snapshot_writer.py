from __future__ import annotations

import dataclasses
import json
import pathlib
import time
from decimal import Decimal
from enum import Enum
from typing import Any

from market.registry import InMemoryMarketRegistry
from state.book_state import InMemoryBookStateStore
from state.inventory_state import InMemoryInventoryStore
from state.order_state import InMemoryOrderStateStore


class _PolyEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, Enum):
            return obj.value
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            return dataclasses.asdict(obj)  # type: ignore[arg-type]
        return super().default(obj)


class SnapshotWriter:
    """Writes periodic hot-state snapshots to disk (cold path).

    Snapshot file layout:
        <output_dir>/snapshot_<now_ms>.json

    Each snapshot captures the current state of all known markets:
    book top, open order counts, inventory positions, pair cost,
    and unhedged exposure.  Only public state-store APIs are used so
    that this class never needs to access private members of other modules.
    """

    def __init__(
        self,
        output_dir: str | pathlib.Path,
        registry: InMemoryMarketRegistry,
        book_state: InMemoryBookStateStore,
        order_state: InMemoryOrderStateStore,
        inventory_state: InMemoryInventoryStore,
    ) -> None:
        self._dir = pathlib.Path(output_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._registry = registry
        self._book = book_state
        self._orders = order_state
        self._inventory = inventory_state

    async def write_snapshot(self, now_ms: int) -> None:
        """Collect and persist a hot-state snapshot for all known markets."""
        snapshot = self._collect(now_ms)
        path = self._dir / f"snapshot_{now_ms}.json"
        path.write_text(json.dumps(snapshot, cls=_PolyEncoder, indent=2), encoding="utf-8")

    def _collect(self, now_ms: int) -> dict[str, Any]:
        markets_snapshot: dict[str, Any] = {}
        for market in self._registry.all_markets():
            mid = market.market_id
            pair_top = self._book.get_pair_top(mid)
            open_orders = self._orders.get_open_orders(mid)
            inventory = self._inventory.get_inventory(mid)
            pair_cost = self._inventory.get_pair_cost(mid)
            unhedged = self._inventory.get_unhedged_exposure(mid)

            markets_snapshot[mid] = {
                "pair_top": pair_top,
                "open_orders_count": len(open_orders),
                "inventory": inventory,
                "pair_cost": str(pair_cost) if pair_cost is not None else None,
                "unhedged_exposure": str(unhedged),
            }

        return {
            "schema_version": 1,
            "now_ms": now_ms,
            "written_at_ns": time.time_ns(),
            "markets": markets_snapshot,
        }
