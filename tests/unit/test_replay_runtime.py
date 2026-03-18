from __future__ import annotations

import asyncio
import json
import tempfile
from decimal import Decimal
from pathlib import Path

from app.bootstrap import AppBootstrapper
from app.config import (
    ExecutionConfig,
    ObservabilityConfig,
    RuntimeConfig,
    StorageConfig,
)
from app.runtime import AppRuntime
from domain.events import BinanceTickEvent, MarketBookTopEvent, MarketMetadataEvent
from domain.models import BinanceTick, MarketMetadata, OutcomeBookTop
from replay.runtime_runner import ReplayRuntimeRunner


def _make_market(now_ms: int, market_id: str = "m_replay") -> MarketMetadata:
    return MarketMetadata(
        market_id=market_id,
        condition_id=f"cond_{market_id}",
        up_token_id=f"{market_id}_up",
        down_token_id=f"{market_id}_down",
        start_ts_ms=now_ms - 60_000,
        end_ts_ms=now_ms + 120_000,
        tick_size=Decimal("0.01"),
        fee_rate_bps=Decimal("10"),
        min_order_size=Decimal("1"),
        status="active",
        reference_price=Decimal("50000"),
    )


def _make_book_event(
    market_id: str,
    token_id: str,
    bid: str,
    ask: str,
    ts_ms: int,
) -> MarketBookTopEvent:
    return MarketBookTopEvent(
        market_id=market_id,
        top=OutcomeBookTop(
            token_id=token_id,
            best_bid_px=Decimal(bid),
            best_bid_sz=Decimal("100"),
            best_ask_px=Decimal(ask),
            best_ask_sz=Decimal("100"),
            last_update_ts_ms=ts_ms,
        ),
    )


def _make_binance_tick(price: str, ts_ms: int) -> BinanceTickEvent:
    price_dec = Decimal(price)
    return BinanceTickEvent(
        tick=BinanceTick(
            symbol="BTCUSDT",
            event_ts_ms=ts_ms,
            recv_ts_ms=ts_ms + 2,
            last_price=price_dec,
            best_bid=price_dec - Decimal("5"),
            best_ask=price_dec + Decimal("5"),
        )
    )


async def _record_runtime_slice(records_dir: Path) -> list[Path]:
    config = RuntimeConfig(
        loop_interval_ms=25,
        execution=ExecutionConfig(gateway_dry_run=True),
        storage=StorageConfig(
            recorder_enabled=True,
            recorder_output_dir=str(records_dir),
            snapshot_enabled=False,
        ),
        observability=ObservabilityConfig(metrics_log_interval_ms=0),
    )
    bootstrapper = AppBootstrapper(config)
    context = await bootstrapper.start()
    runtime = AppRuntime(context)
    now_ms = context.clock.now_ms()
    market = _make_market(now_ms)

    await runtime.start()
    try:
        await context.event_bus.publish("market.metadata", MarketMetadataEvent(market=market))
        await asyncio.sleep(0.05)
        await context.event_bus.publish(
            "feeds.polymarket.market.book_top",
            _make_book_event(market.market_id, market.up_token_id, "0.49", "0.51", now_ms),
        )
        await context.event_bus.publish(
            "feeds.polymarket.market.book_top",
            _make_book_event(market.market_id, market.down_token_id, "0.47", "0.49", now_ms),
        )
        await asyncio.sleep(0.05)
        await context.event_bus.publish(
            "feeds.binance.tick",
            _make_binance_tick("50000", now_ms),
        )
        await asyncio.sleep(0.05)
        await context.event_bus.publish(
            "feeds.binance.tick",
            _make_binance_tick("50025", now_ms + 250),
        )
        await asyncio.sleep(0.1)
    finally:
        await runtime.stop()
        await bootstrapper.stop()

    return sorted(records_dir.rglob("*.jsonl"))


async def _run_replay_runtime_end_to_end() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        records_dir = base_dir / "records"
        report_path = base_dir / "report.json"
        record_files = await _record_runtime_slice(records_dir)
        assert record_files

        runner = ReplayRuntimeRunner(
            RuntimeConfig(
                loop_interval_ms=25,
                execution=ExecutionConfig(gateway_dry_run=True),
                observability=ObservabilityConfig(metrics_log_interval_ms=0),
            )
        )
        report = await runner.run([records_dir], report_path=report_path)

        assert report.input_records >= 4
        assert report.replayed_topics["market.metadata"] == 1
        assert report.replayed_topics["feeds.binance.tick"] == 2
        assert report.replayed_topics["feeds.polymarket.market.book_top"] == 2
        assert report.output_topics["pricing.quote_plan"] >= 1
        assert report.output_topics["strategy.order_intents"] >= 1
        assert report.markets == ["m_replay"]
        assert report.open_orders["m_replay"] >= 2
        assert report.unhedged_exposure["m_replay"] == "0"
        assert report.metrics["gauges"]
        assert report_path.exists()

        persisted = json.loads(report_path.read_text(encoding="utf-8"))
        assert persisted["markets"] == ["m_replay"]
        assert persisted["output_topics"]["pricing.quote_plan"] >= 1


def test_replay_runtime_runner_rebuilds_outputs_from_recordings() -> None:
    asyncio.run(_run_replay_runtime_end_to_end())
