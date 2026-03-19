from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

from storage.recorder import AsyncRecorder, flush_live_records_to_sealed


async def _wait_for(predicate, *, timeout_s: float = 1.5) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met before timeout")


async def _run_recorder_flushes_jsonl_in_background_thread() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        recorder = AsyncRecorder(
            output_dir=output_dir,
            max_queue_size=100,
            flush_interval_ms=50,
            flush_batch_size=2,
        )
        await recorder.start()
        try:
            await recorder.write_event("feeds.binance.tick", {"price": "50000"})
            await recorder.write_event("feeds.binance.tick", {"price": "50010"})

            live_target = output_dir / "global" / "feeds_binance_tick.jsonl"
            await _wait_for(
                lambda: live_target.exists() and live_target.read_text(encoding="utf-8").count("\n") >= 2
            )
        finally:
            await recorder.stop()

        sealed_files = sorted((output_dir / "sealed" / "global" / "feeds_binance_tick").glob("*.jsonl"))
        assert sealed_files
        contents = sealed_files[-1].read_text(encoding="utf-8")
        assert '"topic": "feeds.binance.tick"' in contents
        assert '"market_id": null' in contents
        assert not (output_dir / "global" / "feeds_binance_tick.jsonl").exists()
        assert recorder.written_count >= 2
        assert recorder.flush_count >= 1


def test_recorder_flushes_jsonl_in_background_thread() -> None:
    asyncio.run(_run_recorder_flushes_jsonl_in_background_thread())


async def _run_recorder_routes_new_market_refs_to_global() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        recorder = AsyncRecorder(
            output_dir=output_dir,
            max_queue_size=100,
            flush_interval_ms=50,
            flush_batch_size=1,
        )
        await recorder.start()
        try:
            await recorder.write_event(
                "feeds.polymarket.market.new_market",
                {"market_id": "0xabc123"},
            )
            live_target = output_dir / "global" / "feeds_polymarket_market_new_market.jsonl"
            await _wait_for(lambda: live_target.exists())
        finally:
            await recorder.stop()

        sealed_files = sorted(
            (output_dir / "sealed" / "global" / "feeds_polymarket_market_new_market").glob("*.jsonl")
        )
        assert sealed_files
        assert not (output_dir / "global" / "feeds_polymarket_market_new_market.jsonl").exists()
        assert not (output_dir / "markets" / "0xabc123").exists()


def test_recorder_routes_new_market_refs_to_global() -> None:
    asyncio.run(_run_recorder_routes_new_market_refs_to_global())


async def _run_recorder_rotates_live_files_into_sealed_segments() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        recorder = AsyncRecorder(
            output_dir=output_dir,
            max_queue_size=100,
            flush_interval_ms=50,
            flush_batch_size=1,
            rotate_interval_ms=60_000,
            rotate_max_file_size_bytes=1,
        )
        await recorder.start()
        sealed_dir = output_dir / "sealed" / "global" / "feeds_binance_tick"
        try:
            await recorder.write_event("feeds.binance.tick", {"price": "50000"})
            await _wait_for(lambda: len(list(sealed_dir.glob("*.jsonl"))) >= 1)

            await recorder.write_event("feeds.binance.tick", {"price": "50010"})
            await _wait_for(lambda: len(list(sealed_dir.glob("*.jsonl"))) >= 2)
        finally:
            await recorder.stop()

        assert not (output_dir / "global" / "feeds_binance_tick.jsonl").exists()
        assert len(list(sealed_dir.glob("*.jsonl"))) >= 2


def test_recorder_rotates_live_files_into_sealed_segments() -> None:
    asyncio.run(_run_recorder_rotates_live_files_into_sealed_segments())


def test_flush_live_records_to_sealed_rotates_existing_disk_files() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        output_dir = Path(tmpdir)
        live_global = output_dir / "global" / "feeds_binance_tick.jsonl"
        live_market = output_dir / "markets" / "1604309" / "pricing_theo.jsonl"
        existing_sealed = (
            output_dir / "sealed" / "global" / "feeds_binance_tick" / "00000000000000000001.jsonl"
        )
        existing_sealed.parent.mkdir(parents=True, exist_ok=True)
        existing_sealed.write_text('{"existing": true}\n', encoding="utf-8")
        live_global.parent.mkdir(parents=True, exist_ok=True)
        live_market.parent.mkdir(parents=True, exist_ok=True)
        live_global.write_text('{"payload":{"tick":{"recv_ts_ms":1000}}}\n', encoding="utf-8")
        live_market.write_text('{"payload":{"snapshot":{"market_id":"1604309"}}}\n', encoding="utf-8")

        result = flush_live_records_to_sealed(output_dir)

        sealed_global = (
            output_dir / "sealed" / "global" / "feeds_binance_tick" / "00000000000000000002.jsonl"
        )
        sealed_market = (
            output_dir / "sealed" / "markets" / "1604309" / "pricing_theo" / "00000000000000000001.jsonl"
        )
        assert result["rotated_file_count"] == 2
        assert result["deleted_empty_live_file_count"] == 0
        assert not live_global.exists()
        assert not live_market.exists()
        assert sealed_global.exists()
        assert sealed_market.exists()
