from __future__ import annotations

from collections import defaultdict
from decimal import Decimal

from core.enums import Side
from domain.models import ExecutionReport, InventoryLot, InventoryPosition
from market.registry import InMemoryMarketRegistry


class InMemoryInventoryStore:
    def __init__(self, registry: InMemoryMarketRegistry) -> None:
        self._registry = registry
        self._lots: dict[str, dict[str, list[InventoryLot]]] = defaultdict(
            lambda: defaultdict(list)
        )
        self._seen_fills: dict[str, Decimal] = {}
        self._lot_counter = 0

    def on_fill(self, report: ExecutionReport) -> None:
        if report.avg_price is None:
            return

        previous_filled = self._seen_fills.get(report.client_order_id, Decimal("0"))
        delta_filled = report.filled_size - previous_filled
        if delta_filled <= 0:
            self._seen_fills[report.client_order_id] = max(previous_filled, report.filled_size)
            return

        self._seen_fills[report.client_order_id] = report.filled_size
        if report.side == Side.BUY:
            self._append_lot(report, delta_filled)
            return

        self._reduce_lots(report.market_id, report.token_id, delta_filled)

    def get_inventory(self, market_id: str) -> list[InventoryPosition]:
        positions: list[InventoryPosition] = []
        for token_id, lots in self._lots.get(market_id, {}).items():
            net_size = sum(lot.size for lot in lots)
            if net_size <= 0:
                continue
            total_cost = sum(lot.size * lot.avg_cost for lot in lots)
            positions.append(
                InventoryPosition(
                    market_id=market_id,
                    token_id=token_id,
                    net_size=net_size,
                    avg_cost=total_cost / net_size,
                )
            )
        return positions

    def get_pair_cost(self, market_id: str) -> Decimal | None:
        market = self._registry.get(market_id)
        if market is None:
            return None

        positions = {position.token_id: position for position in self.get_inventory(market_id)}
        up = positions.get(market.up_token_id)
        down = positions.get(market.down_token_id)
        if up is None or down is None:
            return None
        return up.avg_cost + down.avg_cost

    def get_unhedged_exposure(self, market_id: str) -> Decimal:
        market = self._registry.get(market_id)
        if market is None:
            return Decimal("0")

        positions = {position.token_id: position for position in self.get_inventory(market_id)}
        up_size = positions.get(
            market.up_token_id,
            InventoryPosition(market_id, market.up_token_id, Decimal("0"), Decimal("0")),
        ).net_size
        down_size = positions.get(
            market.down_token_id,
            InventoryPosition(market_id, market.down_token_id, Decimal("0"), Decimal("0")),
        ).net_size
        return abs(up_size - down_size)

    def _append_lot(self, report: ExecutionReport, size: Decimal) -> None:
        self._lot_counter += 1
        self._lots[report.market_id][report.token_id].append(
            InventoryLot(
                lot_id=f"lot_{self._lot_counter}",
                market_id=report.market_id,
                token_id=report.token_id,
                side=report.side,
                avg_cost=report.avg_price or Decimal("0"),
                size=size,
                opened_ts_ms=report.event_ts_ms,
                source="fill",
            )
        )

    def _reduce_lots(self, market_id: str, token_id: str, size: Decimal) -> None:
        remaining = size
        lots = self._lots.get(market_id, {}).get(token_id, [])
        while remaining > 0 and lots:
            head = lots[0]
            if head.size <= remaining:
                remaining -= head.size
                lots.pop(0)
                continue
            head.size -= remaining
            remaining = Decimal("0")
