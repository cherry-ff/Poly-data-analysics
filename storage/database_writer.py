from __future__ import annotations

import asyncio
import dataclasses
import json
import pathlib
import queue
import sqlite3
import threading
import time
from decimal import Decimal
from enum import Enum
from typing import Any


class _PolyEncoder(json.JSONEncoder):
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


class ThreadedDatabaseWriter:
    """Background SQLite writer with periodic batch flushes.

    Hot-path callers only enqueue records. A dedicated thread owns the SQLite
    connection and flushes buffered records every ``flush_interval_ms`` or when
    ``flush_batch_size`` is reached.
    """

    def __init__(
        self,
        db_path: str | pathlib.Path,
        *,
        max_queue_size: int = 50_000,
        flush_interval_ms: int = 1_000,
        flush_batch_size: int = 1_000,
    ) -> None:
        self._path = pathlib.Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._queue: queue.Queue[dict[str, Any] | object] = queue.Queue(
            maxsize=max_queue_size
        )
        self._flush_interval_s = flush_interval_ms / 1000
        self._flush_batch_size = flush_batch_size
        self._thread: threading.Thread | None = None
        self._dropped = 0
        self._written = 0
        self._flushes = 0

    async def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._worker_main,
            name="poly15_db_writer",
            daemon=True,
        )
        self._thread.start()

    async def stop(self) -> None:
        if self._thread is None:
            return
        try:
            self._queue.put_nowait(_STOP)
        except queue.Full:
            # Ensure shutdown proceeds even under sustained backpressure.
            self._queue.put(_STOP)
        thread = self._thread
        self._thread = None
        await asyncio.to_thread(thread.join)

    async def write_event(self, topic: str, payload: object) -> None:
        record: dict[str, Any] = {
            "topic": topic,
            "recv_ts_ms": time.time_ns() // 1_000_000,
            "type": type(payload).__name__,
            "payload": payload,
            "written_at_ns": time.time_ns(),
        }
        try:
            self._queue.put_nowait(record)
        except queue.Full:
            self._dropped += 1

    @property
    def dropped_count(self) -> int:
        return self._dropped

    @property
    def written_count(self) -> int:
        return self._written

    @property
    def flush_count(self) -> int:
        return self._flushes

    def _worker_main(self) -> None:
        conn = sqlite3.connect(self._path)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            self._ensure_schema(conn)
            pending: list[tuple[str, int, str, str, int]] = []
            next_flush_at = time.monotonic() + self._flush_interval_s

            while True:
                timeout_s = max(0.0, next_flush_at - time.monotonic())
                try:
                    item = self._queue.get(timeout=timeout_s)
                except queue.Empty:
                    item = None

                if item is _STOP:
                    self._flush(conn, pending)
                    break

                if isinstance(item, dict):
                    pending.append(
                        (
                            str(item["topic"]),
                            int(item["recv_ts_ms"]),
                            str(item["type"]),
                            json.dumps(item["payload"], cls=_PolyEncoder),
                            int(item["written_at_ns"]),
                        )
                    )
                    if len(pending) >= self._flush_batch_size:
                        self._flush(conn, pending)
                        next_flush_at = time.monotonic() + self._flush_interval_s
                        continue

                if time.monotonic() >= next_flush_at:
                    self._flush(conn, pending)
                    next_flush_at = time.monotonic() + self._flush_interval_s
        finally:
            conn.close()

    @staticmethod
    def _ensure_schema(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS event_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT NOT NULL,
                recv_ts_ms INTEGER NOT NULL,
                type TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                written_at_ns INTEGER NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_event_records_topic_recv
            ON event_records(topic, recv_ts_ms)
            """
        )
        conn.commit()

    def _flush(
        self,
        conn: sqlite3.Connection,
        pending: list[tuple[str, int, str, str, int]],
    ) -> None:
        if not pending:
            return
        conn.executemany(
            """
            INSERT INTO event_records (
                topic,
                recv_ts_ms,
                type,
                payload_json,
                written_at_ns
            ) VALUES (?, ?, ?, ?, ?)
            """,
            pending,
        )
        conn.commit()
        self._written += len(pending)
        self._flushes += 1
        pending.clear()
