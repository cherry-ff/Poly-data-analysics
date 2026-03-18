from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator


class InMemoryEventBus:
    def __init__(self) -> None:
        self._subscribers: dict[str, list[asyncio.Queue[object]]] = defaultdict(list)

    async def publish(self, topic: str, payload: object) -> None:
        for queue in list(self._subscribers[topic]):
            await queue.put(payload)

    def subscribe(self, topic: str) -> AsyncIterator[object]:
        queue: asyncio.Queue[object] = asyncio.Queue()
        self._subscribers[topic].append(queue)

        async def iterator() -> AsyncIterator[object]:
            try:
                while True:
                    yield await queue.get()
            finally:
                self._subscribers[topic].remove(queue)

        return iterator()
