from __future__ import annotations

import asyncio
import dataclasses
import json
import pathlib
import queue
import threading
import time
from collections import defaultdict
from decimal import Decimal
from enum import Enum
from typing import Any


class _PolyEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal, Enum, and dataclass objects."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, Enum):
            return obj.value
        if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            d = dataclasses.asdict(obj)  # type: ignore[arg-type]
            d["__class__"] = type(obj).__name__
            return d
        return super().default(obj)


_STOP = object()


def _segment_dir_for_relative_path(
    base_dir: pathlib.Path,
    relative_path: str,
) -> pathlib.Path:
    relative = pathlib.Path(relative_path)
    topic_stem = relative.stem
    parts = relative.parts
    if len(parts) >= 3 and parts[0] == "markets":
        return base_dir / "sealed" / "markets" / parts[1] / topic_stem
    if len(parts) >= 2 and parts[0] == "global":
        return base_dir / "sealed" / "global" / topic_stem
    return base_dir / "sealed" / "other" / topic_stem


def _discover_last_segment_index(segment_dir: pathlib.Path) -> int:
    if not segment_dir.exists():
        return 0
    highest = 0
    for path in segment_dir.glob("*.jsonl"):
        try:
            highest = max(highest, int(path.stem))
        except ValueError:
            continue
    return highest


def _next_segment_path_for_relative_path(
    base_dir: pathlib.Path,
    relative_path: str,
    segment_counters: dict[str, int],
) -> pathlib.Path:
    segment_dir = _segment_dir_for_relative_path(base_dir, relative_path)
    segment_key = segment_dir.relative_to(base_dir).as_posix()
    last_index = segment_counters.get(segment_key)
    if last_index is None:
        last_index = _discover_last_segment_index(segment_dir)
    next_index = last_index + 1
    segment_counters[segment_key] = next_index
    return segment_dir / f"{next_index:020d}.jsonl"


def flush_live_records_to_sealed(output_dir: str | pathlib.Path) -> dict[str, int]:
    """Rotate every on-disk live JSONL file into the sealed layout.

    This is intended for maintenance/backfill flows while the collector is stopped.
    It scans the live `global/*.jsonl` and `markets/*/*.jsonl` files on disk,
    moves non-empty files into the next sealed segment path, and removes empty
    live files.
    """

    base_dir = pathlib.Path(output_dir).resolve()
    segment_counters: dict[str, int] = {}
    rotated_file_count = 0
    rotated_bytes = 0
    deleted_empty_live_file_count = 0

    live_paths = sorted((base_dir / "global").glob("*.jsonl"))
    live_paths.extend(sorted((base_dir / "markets").glob("*/*.jsonl")))

    for path in live_paths:
        if not path.is_file():
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        if stat.st_size <= 0:
            path.unlink(missing_ok=True)
            deleted_empty_live_file_count += 1
            continue

        relative_path = path.relative_to(base_dir).as_posix()
        segment_path = _next_segment_path_for_relative_path(
            base_dir,
            relative_path,
            segment_counters,
        )
        segment_path.parent.mkdir(parents=True, exist_ok=True)
        path.rename(segment_path)
        rotated_file_count += 1
        rotated_bytes += stat.st_size

    return {
        "rotated_file_count": rotated_file_count,
        "rotated_bytes": rotated_bytes,
        "deleted_empty_live_file_count": deleted_empty_live_file_count,
    }


@dataclasses.dataclass(slots=True)
class _StreamState:
    opened_at_monotonic: float


class AsyncRecorder:
    """Async JSONL event recorder for cold-path storage.

    Events are written under a market-aware directory layout:

    - ``<output_dir>/global/<topic>.jsonl`` for shared feeds like Binance/Chainlink
    - ``<output_dir>/markets/<market_id>/<topic>.jsonl`` for market-scoped events

    Record schema:
        {"topic": str, "recv_ts_ms": int, "type": str, "market_id": str | null, "payload": {...}}

    The recorder runs a dedicated background thread and periodically flushes
    batches to disk so that the event loop never blocks on file I/O.
    Backpressure is handled by dropping records when the queue is full and
    counting the drops.
    """

    def __init__(
        self,
        output_dir: str | pathlib.Path,
        max_queue_size: int = 10_000,
        flush_interval_ms: int = 1_000,
        flush_batch_size: int = 1_000,
        rotate_interval_ms: int = 300_000,
        rotate_max_file_size_bytes: int = 16 * 1024 * 1024,
    ) -> None:
        self._dir = pathlib.Path(output_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._queue: queue.Queue[dict[str, Any] | object] = queue.Queue(maxsize=max_queue_size)
        self._flush_interval_s = flush_interval_ms / 1000
        self._flush_batch_size = flush_batch_size
        self._rotate_interval_s = rotate_interval_ms / 1000 if rotate_interval_ms > 0 else 0.0
        self._rotate_max_file_size_bytes = max(0, rotate_max_file_size_bytes)
        self._handles: dict[str, Any] = {}
        self._stream_states: dict[str, _StreamState] = {}
        self._segment_counters: dict[str, int] = {}
        self._thread: threading.Thread | None = None
        self._dropped: int = 0
        self._written: int = 0
        self._flushes: int = 0

    async def start(self) -> None:
        """Start the background writer thread."""
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._writer_loop,
            name="recorder_writer",
            daemon=True,
        )
        self._thread.start()

    async def stop(self) -> None:
        """Flush pending records and close all file handles."""
        if self._thread is None:
            return
        try:
            self._queue.put_nowait(_STOP)
        except queue.Full:
            self._queue.put(_STOP)
        thread = self._thread
        self._thread = None
        await asyncio.to_thread(thread.join)
        for fh in self._handles.values():
            fh.close()
        self._handles.clear()

    async def write_event(self, topic: str, payload: object) -> None:
        """Enqueue an event for async writing.  Never blocks; drops if queue full."""
        market_id = self._extract_market_id(payload)
        record: dict[str, Any] = {
            "topic": topic,
            "recv_ts_ms": time.time_ns() // 1_000_000,
            "type": type(payload).__name__,
            "market_id": market_id,
            "payload": payload,
        }
        try:
            self._queue.put_nowait(record)
        except queue.Full:
            self._dropped += 1

    @property
    def dropped_count(self) -> int:
        """Number of events dropped due to queue backpressure."""
        return self._dropped

    @property
    def written_count(self) -> int:
        return self._written

    @property
    def flush_count(self) -> int:
        return self._flushes

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------

    def _writer_loop(self) -> None:
        pending: list[dict[str, Any]] = []
        next_flush_at = time.monotonic() + self._flush_interval_s
        while True:
            timeout_s = max(0.0, next_flush_at - time.monotonic())
            try:
                record = self._queue.get(timeout=timeout_s)
            except queue.Empty:
                record = None

            if record is _STOP:
                self._flush_pending(pending)
                self._rotate_due_streams(force_all=True)
                break

            if isinstance(record, dict):
                pending.append(record)
                if len(pending) >= self._flush_batch_size:
                    self._flush_pending(pending)
                    next_flush_at = time.monotonic() + self._flush_interval_s
                    continue

            if time.monotonic() >= next_flush_at:
                self._flush_pending(pending)
                next_flush_at = time.monotonic() + self._flush_interval_s

    def _flush_pending(self, pending: list[dict[str, Any]]) -> None:
        if not pending:
            return
        by_path: dict[str, list[str]] = defaultdict(list)
        for record in pending:
            relative_path = self._relative_path_for_record(record)
            by_path[relative_path].append(json.dumps(record, cls=_PolyEncoder) + "\n")

        for relative_path, lines in by_path.items():
            fh = self._get_handle(relative_path)
            fh.writelines(lines)
            fh.flush()

        self._written += len(pending)
        self._flushes += 1
        pending.clear()
        self._rotate_due_streams()

    def _get_handle(self, relative_path: str) -> Any:
        path = self._dir / relative_path
        existing = self._handles.get(relative_path)
        if existing is not None and not path.exists():
            existing.close()
            self._handles.pop(relative_path, None)
            self._stream_states.pop(relative_path, None)
        if relative_path not in self._handles:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._handles[relative_path] = open(path, "a", encoding="utf-8")  # noqa: WPS515
            self._stream_states[relative_path] = _StreamState(
                opened_at_monotonic=time.monotonic(),
            )
        return self._handles[relative_path]

    def _relative_path_for_record(self, record: dict[str, Any]) -> str:
        topic = str(record["topic"])
        market_id = record.get("market_id")
        safe_topic = self._sanitize_segment(topic) + ".jsonl"
        if topic == "feeds.polymarket.market.new_market":
            return f"global/{safe_topic}"
        if isinstance(market_id, str) and self._is_market_scoped_id(market_id):
            safe_market_id = self._sanitize_segment(market_id)
            return f"markets/{safe_market_id}/{safe_topic}"
        return f"global/{safe_topic}"

    def _extract_market_id(self, payload: object) -> str | None:
        return self._extract_market_id_recursive(payload, depth=0)

    def _extract_market_id_recursive(self, value: object, *, depth: int) -> str | None:
        if depth > 4 or value is None:
            return None

        if dataclasses.is_dataclass(value) and not isinstance(value, type):
            if hasattr(value, "market_id"):
                market_id = getattr(value, "market_id")
                if market_id not in (None, ""):
                    return str(market_id)
            for attr in ("market", "snapshot", "plan", "transition", "report"):
                if hasattr(value, attr):
                    found = self._extract_market_id_recursive(
                        getattr(value, attr),
                        depth=depth + 1,
                    )
                    if found:
                        return found
            return None

        if isinstance(value, dict):
            market_id = value.get("market_id")
            if market_id not in (None, ""):
                return str(market_id)
            for key in ("market", "snapshot", "plan", "transition", "report"):
                if key in value:
                    found = self._extract_market_id_recursive(
                        value[key],
                        depth=depth + 1,
                    )
                    if found:
                        return found
            return None

        return None

    @staticmethod
    def _sanitize_segment(value: str) -> str:
        return "".join(c if c.isalnum() or c in "-_" else "_" for c in value)

    @staticmethod
    def _is_market_scoped_id(value: str) -> bool:
        stripped = value.strip()
        return bool(stripped) and stripped.isdigit()

    def _rotate_due_streams(self, *, force_all: bool = False) -> None:
        if not self._handles:
            return

        now = time.monotonic()
        to_rotate: list[str] = []
        for relative_path in list(self._handles):
            path = self._dir / relative_path
            if not path.exists():
                continue
            try:
                stat = path.stat()
            except OSError:
                continue
            if stat.st_size <= 0:
                continue

            should_rotate = force_all
            if not should_rotate and self._rotate_max_file_size_bytes > 0:
                should_rotate = stat.st_size >= self._rotate_max_file_size_bytes
            if not should_rotate and self._rotate_interval_s > 0:
                stream_state = self._stream_states.get(relative_path)
                if stream_state is not None:
                    should_rotate = (
                        now - stream_state.opened_at_monotonic >= self._rotate_interval_s
                    )
            if should_rotate:
                to_rotate.append(relative_path)

        for relative_path in to_rotate:
            self._rotate_stream(relative_path)

    def _rotate_stream(self, relative_path: str) -> None:
        fh = self._handles.pop(relative_path, None)
        self._stream_states.pop(relative_path, None)
        if fh is None:
            return

        path = self._dir / relative_path
        try:
            fh.flush()
        finally:
            fh.close()

        if not path.exists():
            return
        try:
            stat = path.stat()
        except OSError:
            return
        if stat.st_size <= 0:
            return

        segment_path = self._next_segment_path(relative_path)
        segment_path.parent.mkdir(parents=True, exist_ok=True)
        path.rename(segment_path)

    def _next_segment_path(self, relative_path: str) -> pathlib.Path:
        return _next_segment_path_for_relative_path(
            self._dir,
            relative_path,
            self._segment_counters,
        )

    @staticmethod
    def _discover_last_segment_index(segment_dir: pathlib.Path) -> int:
        return _discover_last_segment_index(segment_dir)
