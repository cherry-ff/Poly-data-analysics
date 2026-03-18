from __future__ import annotations

import time


class SequentialIdGenerator:
    def __init__(self, prefix: str = "poly15") -> None:
        self._prefix = prefix
        self._counter = 0

    def _next(self, kind: str) -> str:
        self._counter += 1
        now_ms = time.time_ns() // 1_000_000
        return f"{self._prefix}_{kind}_{now_ms}_{self._counter}"

    def next_intent_id(self) -> str:
        return self._next("intent")

    def next_pair_id(self) -> str:
        return self._next("pair")

    def next_client_order_id(self) -> str:
        return self._next("clord")
