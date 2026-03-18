from __future__ import annotations

from collections import defaultdict
from decimal import Decimal

from core.enums import OrderRole, OrderStatus
from domain.models import ExecutionReport, OrderIntent, OrderRecord


class InMemoryOrderStateStore:
    def __init__(self) -> None:
        self._orders: dict[str, OrderRecord] = {}
        self._pair_index: dict[str, set[str]] = defaultdict(set)
        self._exchange_index: dict[str, str] = {}

    def on_intent_sent(
        self,
        intent: OrderIntent,
        client_order_id: str,
        exchange_order_id: str | None = None,
    ) -> None:
        record = OrderRecord(
            client_order_id=client_order_id,
            pair_id=intent.pair_id,
            market_id=intent.market_id,
            token_id=intent.token_id,
            side=intent.side,
            price=intent.price,
            size=intent.size,
            role=intent.role,
            status=OrderStatus.PENDING_SUBMIT,
            exchange_order_id=exchange_order_id,
        )
        self._orders[client_order_id] = record
        if intent.pair_id is not None:
            self._pair_index[intent.pair_id].add(client_order_id)
        if exchange_order_id is not None:
            self._exchange_index[exchange_order_id] = client_order_id

    def on_execution_report(self, report: ExecutionReport) -> None:
        record = self._orders.get(report.client_order_id)
        if record is None and report.exchange_order_id is not None:
            mapped_client_id = self._exchange_index.get(report.exchange_order_id)
            if mapped_client_id is not None:
                record = self._orders.get(mapped_client_id)
        if record is None:
            self._orders[report.client_order_id] = OrderRecord(
                client_order_id=report.client_order_id,
                pair_id=report.pair_id,
                market_id=report.market_id,
                token_id=report.token_id,
                side=report.side,
                price=report.avg_price or Decimal("0"),
                size=report.filled_size,
                role=self._infer_role(report),
                status=report.status,
                filled_size=report.filled_size,
                avg_price=report.avg_price,
                exchange_order_id=report.exchange_order_id,
                last_event_ts_ms=report.event_ts_ms,
                mismatch=True,
            )
            if report.pair_id is not None:
                self._pair_index[report.pair_id].add(report.client_order_id)
            if report.exchange_order_id is not None:
                self._exchange_index[report.exchange_order_id] = report.client_order_id
            return

        if (
            record.exchange_order_id is not None
            and report.exchange_order_id is not None
            and record.exchange_order_id != report.exchange_order_id
        ):
            record.mismatch = True

        if self._is_terminal(record.status) and record.status != report.status:
            record.mismatch = True

        record.status = report.status
        record.filled_size = report.filled_size
        record.avg_price = report.avg_price
        record.exchange_order_id = report.exchange_order_id or record.exchange_order_id
        record.last_event_ts_ms = report.event_ts_ms
        if record.exchange_order_id is not None:
            self._exchange_index[record.exchange_order_id] = record.client_order_id

    def get_open_orders(self, market_id: str) -> list[OrderRecord]:
        return [
            order
            for order in self._orders.values()
            if order.market_id == market_id and not self._is_terminal(order.status)
        ]

    def get_pair_orders(self, pair_id: str) -> list[OrderRecord]:
        client_ids = self._pair_index.get(pair_id, set())
        return [self._orders[client_id] for client_id in sorted(client_ids)]

    def get(self, client_order_id: str) -> OrderRecord | None:
        return self._orders.get(client_order_id)

    def get_by_exchange_order_id(self, exchange_order_id: str) -> OrderRecord | None:
        client_order_id = self._exchange_index.get(exchange_order_id)
        if client_order_id is None:
            return None
        return self._orders.get(client_order_id)

    @staticmethod
    def _is_terminal(status: OrderStatus) -> bool:
        return status in {
            OrderStatus.FILLED,
            OrderStatus.CANCELED,
            OrderStatus.REJECTED,
        }

    @staticmethod
    def _infer_role(report: ExecutionReport) -> OrderRole:
        return OrderRole.UNKNOWN
