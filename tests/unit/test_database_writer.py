from __future__ import annotations

import asyncio
import sqlite3
import tempfile
from pathlib import Path

from storage.database_writer import ThreadedDatabaseWriter


async def _wait_for(predicate, *, timeout_s: float = 1.5) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met before timeout")


async def _run_threaded_database_writer_flushes_records() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "events.sqlite3"
        writer = ThreadedDatabaseWriter(
            db_path=db_path,
            max_queue_size=100,
            flush_interval_ms=50,
            flush_batch_size=2,
        )
        await writer.start()
        try:
            await writer.write_event("feeds.binance.tick", {"price": "50000"})
            await writer.write_event("feeds.chainlink.tick", {"price": "49990"})

            def _has_rows() -> bool:
                if not db_path.exists():
                    return False
                try:
                    with sqlite3.connect(db_path) as conn:
                        row = conn.execute("SELECT COUNT(*) FROM event_records").fetchone()
                except sqlite3.OperationalError:
                    return False
                return bool(row and row[0] >= 2)

            await _wait_for(_has_rows)
        finally:
            await writer.stop()

        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                "SELECT topic, type, payload_json FROM event_records ORDER BY id"
            ).fetchall()

        assert len(rows) >= 2
        assert rows[0][0] == "feeds.binance.tick"
        assert rows[1][0] == "feeds.chainlink.tick"
        assert writer.written_count >= 2
        assert writer.flush_count >= 1


def test_threaded_database_writer_flushes_records() -> None:
    asyncio.run(_run_threaded_database_writer_flushes_records())
