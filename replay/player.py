from __future__ import annotations

import heapq
import json
import pathlib
from collections.abc import AsyncIterator, Callable, Iterator
from decimal import Decimal
from typing import Any


# ---------------------------------------------------------------------------
# Type reconstruction registry
# ---------------------------------------------------------------------------

class TypeRegistry:
    """Maps class names recorded in JSONL to factory callables.

    Factories receive the raw ``payload`` dict (with ``__class__`` stripped)
    and return the reconstructed domain object or the raw dict if no factory
    is registered.

    Agents A/B/C should register their domain types before playback starts.
    Agent D registers nothing by default so that the player remains
    independent of other layers.
    """

    def __init__(self) -> None:
        self._factories: dict[str, Callable[[dict[str, Any]], Any]] = {}

    def register(self, type_name: str, factory: Callable[[dict[str, Any]], Any]) -> None:
        self._factories[type_name] = factory

    def reconstruct(self, type_name: str, payload: dict[str, Any]) -> Any:
        """Return a reconstructed object, or the raw dict if no factory exists."""
        factory = self._factories.get(type_name)
        if factory is None:
            return payload
        cleaned = {k: v for k, v in payload.items() if k != "__class__"}
        return factory(cleaned)


_DEFAULT_REGISTRY = TypeRegistry()


# ---------------------------------------------------------------------------
# Record helpers
# ---------------------------------------------------------------------------

def _iter_jsonl(path: pathlib.Path) -> Iterator[dict[str, Any]]:
    """Yield parsed records from a single JSONL file."""
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue  # skip malformed lines silently


def _merge_sorted(
    iterators: list[Iterator[dict[str, Any]]],
) -> Iterator[dict[str, Any]]:
    """Merge N already-sorted iterators by recv_ts_ms (stable, min-heap)."""
    heap: list[tuple[int, int, dict[str, Any], Iterator[dict[str, Any]]]] = []

    for idx, it in enumerate(iterators):
        record = next(it, None)
        if record is not None:
            ts = record.get("recv_ts_ms", 0)
            heapq.heappush(heap, (ts, idx, record, it))

    while heap:
        ts, idx, record, it = heapq.heappop(heap)
        yield record
        nxt = next(it, None)
        if nxt is not None:
            nxt_ts = nxt.get("recv_ts_ms", 0)
            heapq.heappush(heap, (nxt_ts, idx, nxt, it))


# ---------------------------------------------------------------------------
# ReplayPlayer
# ---------------------------------------------------------------------------

class ReplayPlayer:
    """Three-stream JSONL replay player.

    Reads one or more JSONL files produced by ``AsyncRecorder``, merges them
    by ``recv_ts_ms``, and emits records to a caller-supplied handler.

    The handler signature is::

        async def handler(topic: str, type_name: str, payload: Any) -> None: ...

    ``payload`` is either a reconstructed domain object (if a factory is
    registered in ``registry``) or the raw ``dict`` from the JSONL file.

    Usage::

        player = ReplayPlayer(registry=my_registry)
        await player.run(
            paths=["data/binance_tick.jsonl", "data/polymarket_market.jsonl"],
            handler=my_handler,
        )
    """

    def __init__(self, registry: TypeRegistry | None = None) -> None:
        self._registry = registry or _DEFAULT_REGISTRY

    async def run(
        self,
        paths: list[str | pathlib.Path],
        handler: Callable[[str, str, Any], Any],
    ) -> None:
        """Replay all records in time order, calling ``handler`` for each."""
        iterators = [_iter_jsonl(pathlib.Path(p)) for p in paths]
        for record in _merge_sorted(iterators):
            topic = record.get("topic", "unknown")
            type_name = record.get("type", "unknown")
            raw_payload = record.get("payload", {})
            payload = self._registry.reconstruct(type_name, raw_payload)
            result = handler(topic, type_name, payload)
            # support both sync and async handlers
            if hasattr(result, "__await__"):
                await result

    def iter_records(
        self,
        paths: list[str | pathlib.Path],
    ) -> Iterator[tuple[str, str, dict[str, Any]]]:
        """Synchronous iterator yielding ``(topic, type_name, raw_payload)`` tuples.

        Useful for offline analysis scripts that don't need async.
        """
        iterators = [_iter_jsonl(pathlib.Path(p)) for p in paths]
        for record in _merge_sorted(iterators):
            topic = record.get("topic", "unknown")
            type_name = record.get("type", "unknown")
            raw_payload = record.get("payload", {})
            yield topic, type_name, raw_payload
