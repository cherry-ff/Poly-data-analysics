"""Integration-style tests for runtime sidecars: recorder/snapshot/metrics/alerts."""

from __future__ import annotations

import asyncio
import io
import logging
import sqlite3
import tempfile
from decimal import Decimal
from pathlib import Path

from app.bootstrap import AppBootstrapper
from app.config import (
    ChainlinkFeedConfig,
    ExecutionConfig,
    MetadataConfig,
    ObservabilityConfig,
    PolymarketFeedConfig,
    RuntimeConfig,
    StorageConfig,
)
from app.runtime import AppRuntime
from domain.events import (
    BinanceDepthEvent,
    BinanceTickEvent,
    MarketBookTopEvent,
    MarketDiscoveredEvent,
    MarketMetadataEvent,
    PolymarketDepthEvent,
)
from domain.models import (
    BinanceDepthSnapshot,
    BinanceTick,
    BookLevel,
    MarketMetadata,
    OutcomeBookTop,
    PolymarketDepthSnapshot,
)


def _make_market(now_ms: int, market_id: str = "m1") -> MarketMetadata:
    return MarketMetadata(
        market_id=market_id,
        condition_id=f"cond_{market_id}",
        up_token_id="up_tok",
        down_token_id="dn_tok",
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
        )
    )


def _make_binance_tick(price: str, ts_ms: int) -> BinanceTickEvent:
    price_dec = Decimal(price)
    return BinanceTickEvent(
        tick=BinanceTick(
            symbol="BTCUSDT",
            event_ts_ms=ts_ms,
            recv_ts_ms=ts_ms + 3,
            last_price=price_dec,
            best_bid=price_dec - Decimal("5"),
            best_ask=price_dec + Decimal("5"),
        )
    )


def _make_binance_depth(ts_ms: int) -> BinanceDepthEvent:
    return BinanceDepthEvent(
        snapshot=BinanceDepthSnapshot(
            symbol="BTCUSDT",
            event_ts_ms=ts_ms,
            recv_ts_ms=ts_ms + 4,
            last_update_id=42,
            bids=(
                BookLevel(price=Decimal("49995"), size=Decimal("1.2")),
                BookLevel(price=Decimal("49994"), size=Decimal("1.0")),
            ),
            asks=(
                BookLevel(price=Decimal("50005"), size=Decimal("1.1")),
                BookLevel(price=Decimal("50006"), size=Decimal("0.9")),
            ),
        )
    )


def _make_polymarket_depth(market_id: str, token_id: str, ts_ms: int) -> PolymarketDepthEvent:
    return PolymarketDepthEvent(
        snapshot=PolymarketDepthSnapshot(
            market_id=market_id,
            token_id=token_id,
            event_type="book",
            last_update_ts_ms=ts_ms,
            bids=(
                BookLevel(price=Decimal("0.49"), size=Decimal("100")),
                BookLevel(price=Decimal("0.48"), size=Decimal("90")),
            ),
            asks=(
                BookLevel(price=Decimal("0.51"), size=Decimal("110")),
                BookLevel(price=Decimal("0.52"), size=Decimal("120")),
            ),
        )
    )


async def _wait_for(predicate, *, timeout_s: float = 1.5) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met before timeout")


async def _run_runtime_recorder_and_snapshot_sidecars() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        base_dir = Path(tmpdir)
        records_dir = base_dir / "records"
        db_path = base_dir / "events.sqlite3"
        snapshots_dir = base_dir / "snapshots"
        config = RuntimeConfig(
            loop_interval_ms=25,
            execution=ExecutionConfig(gateway_dry_run=True),
            storage=StorageConfig(
                recorder_enabled=True,
                recorder_output_dir=str(records_dir),
                recorder_flush_interval_ms=50,
                recorder_flush_batch_size=10,
                db_enabled=True,
                db_path=str(db_path),
                db_flush_interval_ms=50,
                db_flush_batch_size=10,
                snapshot_enabled=True,
                snapshot_output_dir=str(snapshots_dir),
                snapshot_interval_ms=50,
            ),
            observability=ObservabilityConfig(metrics_log_interval_ms=0),
        )
        bootstrapper = AppBootstrapper(config)
        context = await bootstrapper.start()
        runtime = AppRuntime(context)
        now_ms = context.clock.now_ms()
        market = _make_market(now_ms, market_id="1001")

        await runtime.start()
        try:
            await context.event_bus.publish(
                "market.metadata",
                MarketMetadataEvent(market=market),
            )
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
                _make_binance_tick("50020", now_ms + 250),
            )
            await context.event_bus.publish(
                "feeds.binance.depth",
                _make_binance_depth(now_ms + 300),
            )
            await context.event_bus.publish(
                "feeds.polymarket.market.depth",
                _make_polymarket_depth(market.market_id, market.up_token_id, now_ms + 325),
            )

            await _wait_for(
                lambda: any(records_dir.rglob("*.jsonl"))
                and any(snapshots_dir.glob("snapshot_*.json")),
            )
            await _wait_for(
                lambda: db_path.exists(),
            )
            await _wait_for(
                lambda: len(context.order_state.get_open_orders(market.market_id)) >= 2,
            )
        finally:
            await runtime.stop()
            await bootstrapper.stop()

        record_files = list(records_dir.rglob("*.jsonl"))
        snapshot_files = list(snapshots_dir.glob("snapshot_*.json"))
        assert record_files
        assert snapshot_files
        assert db_path.exists()

        merged_records = "\n".join(path.read_text(encoding="utf-8") for path in record_files)
        assert '"topic": "feeds.binance.tick"' in merged_records
        assert '"topic": "feeds.binance.depth"' in merged_records
        assert '"topic": "feeds.polymarket.market.depth"' in merged_records
        assert '"topic": "pricing.quote_plan"' in merged_records
        assert context.observability.metrics.snapshot()["counters"]["snapshot_written_count"] >= 1
        assert list((records_dir / "sealed" / "global" / "feeds_binance_tick").glob("*.jsonl"))
        assert list((records_dir / "sealed" / "markets" / market.market_id / "market_metadata").glob("*.jsonl"))
        assert list(
            (records_dir / "sealed" / "markets" / market.market_id / "feeds_polymarket_market_depth").glob("*.jsonl")
        )
        with sqlite3.connect(db_path) as conn:
            rows = conn.execute(
                "SELECT COUNT(*) FROM event_records WHERE topic IN (?, ?, ?)",
                (
                    "feeds.binance.tick",
                    "feeds.binance.depth",
                    "feeds.polymarket.market.depth",
                ),
            ).fetchone()
        assert rows is not None
        assert rows[0] >= 3


def test_runtime_recorder_and_snapshot_sidecars() -> None:
    asyncio.run(_run_runtime_recorder_and_snapshot_sidecars())


async def _run_runtime_metrics_and_alerts_sidecars() -> None:
    config = RuntimeConfig(
        loop_interval_ms=25,
        execution=ExecutionConfig(gateway_dry_run=True),
        observability=ObservabilityConfig(metrics_log_interval_ms=0, alerts_max_history=20),
    )
    bootstrapper = AppBootstrapper(config)
    context = await bootstrapper.start()
    runtime = AppRuntime(context)
    now_ms = context.clock.now_ms()

    async def failing_load_market(market_id: str):
        raise RuntimeError(f"boom:{market_id}")

    context.metadata_loader.load_market = failing_load_market  # type: ignore[method-assign]

    await runtime.start()
    try:
        await context.event_bus.publish(
            "feeds.binance.tick",
            _make_binance_tick("50000", now_ms),
        )
        await context.event_bus.publish(
            "feeds.polymarket.market.new_market",
            MarketDiscoveredEvent(market_id="123456", discovered_ts_ms=now_ms),
        )

        await _wait_for(
            lambda: context.observability.alerts.p1_count() >= 1,
        )
    finally:
        await runtime.stop()
        await bootstrapper.stop()

    metrics = context.observability.metrics.snapshot()
    latency_key = next(
        key for key in metrics["gauges"] if key.startswith("binance_feed_latency_ms")
    )
    error_key = next(
        key for key in metrics["counters"] if key.startswith("metadata_load_error_count")
    )
    assert metrics["gauges"][latency_key] == 3
    assert metrics["counters"][error_key] == 1
    alerts = context.observability.alerts.recent()
    assert alerts
    assert alerts[-1].title == "metadata load failed"


def test_runtime_metrics_and_alerts_sidecars() -> None:
    asyncio.run(_run_runtime_metrics_and_alerts_sidecars())


async def _run_runtime_refreshes_discovery_on_non_gamma_market_refs() -> None:
    config = RuntimeConfig(
        loop_interval_ms=25,
        execution=ExecutionConfig(gateway_dry_run=True),
        observability=ObservabilityConfig(metrics_log_interval_ms=0, alerts_max_history=20),
    )
    bootstrapper = AppBootstrapper(config)
    context = await bootstrapper.start()
    runtime = AppRuntime(context)
    now_ms = context.clock.now_ms()
    market = _make_market(now_ms + 60_000, market_id="m_ws_hint")
    market.condition_id = "condition_ws_hint"

    async def discover_markets(**_: object):
        return [market]

    context.metadata_loader.discover_markets = discover_markets  # type: ignore[method-assign]

    await runtime.start()
    try:
        await context.event_bus.publish(
            "feeds.polymarket.market.new_market",
            MarketDiscoveredEvent(market_id="0xabc123", discovered_ts_ms=now_ms),
        )
        await _wait_for(lambda: context.market_registry.get("m_ws_hint") is not None)
    finally:
        await runtime.stop()
        await bootstrapper.stop()

    metrics = context.observability.metrics.snapshot()
    hint_key = next(
        key
        for key in metrics["counters"]
        if key.startswith("market_discovery_ws_hint_count")
    )
    refresh_key = next(
        key
        for key in metrics["counters"]
        if key.startswith("market_discovery_refresh_count")
    )
    assert metrics["counters"][hint_key] == 1
    assert metrics["counters"][refresh_key] >= 1
    assert context.market_registry.get("m_ws_hint") is not None
    assert context.observability.alerts.recent() == []


def test_runtime_refreshes_discovery_on_non_gamma_market_refs() -> None:
    asyncio.run(_run_runtime_refreshes_discovery_on_non_gamma_market_refs())


async def _run_runtime_startup_diagnostics_logs_feed_health() -> None:
    config = RuntimeConfig(
        loop_interval_ms=25,
        execution=ExecutionConfig(gateway_dry_run=True),
        observability=ObservabilityConfig(
            metrics_log_interval_ms=0,
            startup_diagnostics_enabled=True,
            startup_diagnostics_log_interval_ms=40,
            startup_diagnostics_window_ms=120,
            startup_diagnostics_first_events=1,
        ),
    )
    bootstrapper = AppBootstrapper(config)
    context = await bootstrapper.start()
    runtime = AppRuntime(context)
    now_ms = context.clock.now_ms()

    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    runtime_logger = logging.getLogger("app.runtime")
    previous_level = runtime_logger.level
    runtime_logger.setLevel(logging.INFO)
    runtime_logger.addHandler(handler)

    await runtime.start()
    try:
        await context.event_bus.publish(
            "feeds.binance.tick",
            _make_binance_tick("50000", now_ms),
        )
        await _wait_for(
            lambda: "first events | feed=binance" in stream.getvalue()
            and "feed health |" in stream.getvalue(),
        )
    finally:
        await runtime.stop()
        await bootstrapper.stop()
        runtime_logger.removeHandler(handler)
        runtime_logger.setLevel(previous_level)

    logs = stream.getvalue()
    assert "first events | feed=binance" in logs
    assert "feed health |" in logs
    assert "binance[count=1" in logs


def test_runtime_startup_diagnostics_logs_feed_health() -> None:
    asyncio.run(_run_runtime_startup_diagnostics_logs_feed_health())


async def _run_runtime_logs_feed_task_failures() -> None:
    config = RuntimeConfig(
        loop_interval_ms=25,
        chainlink=ChainlinkFeedConfig(enabled=True, endpoint="", api_url="", feed_id=""),
        execution=ExecutionConfig(gateway_dry_run=True),
        observability=ObservabilityConfig(
            metrics_log_interval_ms=0,
            startup_diagnostics_enabled=True,
            startup_diagnostics_log_interval_ms=40,
            startup_diagnostics_window_ms=120,
            startup_diagnostics_first_events=0,
        ),
    )
    bootstrapper = AppBootstrapper(config)
    context = await bootstrapper.start()
    runtime = AppRuntime(context)

    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(message)s"))
    runtime_logger = logging.getLogger("app.runtime")
    previous_level = runtime_logger.level
    runtime_logger.setLevel(logging.INFO)
    runtime_logger.addHandler(handler)

    await runtime.start()
    try:
        await _wait_for(
            lambda: "skipping chainlink feed startup: endpoint/feed_id are empty" in stream.getvalue()
            and "chainlink[error=skipped: endpoint/feed_id are empty]" in stream.getvalue(),
        )
    finally:
        await runtime.stop()
        await bootstrapper.stop()
        runtime_logger.removeHandler(handler)
        runtime_logger.setLevel(previous_level)

    logs = stream.getvalue()
    assert "skipping chainlink feed startup: endpoint/feed_id are empty" in logs
    assert "chainlink[error=skipped: endpoint/feed_id are empty]" in logs


def test_runtime_logs_feed_task_failures() -> None:
    asyncio.run(_run_runtime_logs_feed_task_failures())


async def _run_runtime_market_discovery_cycle_registers_metadata() -> None:
    config = RuntimeConfig(
        loop_interval_ms=25,
        execution=ExecutionConfig(gateway_dry_run=True),
        polymarket=PolymarketFeedConfig(
            market_enabled=False,
            user_enabled=False,
        ),
        metadata=MetadataConfig(
            gamma_base_url="https://gamma-api.polymarket.com",
            discovery_enabled=True,
        ),
        observability=ObservabilityConfig(metrics_log_interval_ms=0),
    )
    bootstrapper = AppBootstrapper(config)
    context = await bootstrapper.start()
    runtime = AppRuntime(context)
    now_ms = context.clock.now_ms()
    market = _make_market(now_ms, market_id="m_discovery")
    market.condition_id = "condition_discovery"

    async def discover_markets(**_: object):
        return [market]

    context.metadata_loader.discover_markets = discover_markets  # type: ignore[method-assign]

    await runtime.start()
    try:
        await runtime._run_market_discovery_cycle()
        await _wait_for(lambda: context.market_registry.get("m_discovery") is not None)
    finally:
        await runtime.stop()
        await bootstrapper.stop()

    assert context.market_registry.get("m_discovery") is not None
    assert (
        context.feeds.polymarket_market._extract_market_id({"asset_id": market.up_token_id})
        == "m_discovery"
    )
    report = context.feeds.polymarket_user._normalize(
        {
            "event_type": "order",
            "type": "UPDATE",
            "id": "0xorder",
            "market": "condition_discovery",
            "asset_id": market.up_token_id,
            "side": "BUY",
            "size": "10",
            "size_matched": "2",
            "price": "0.52",
            "timestamp": now_ms,
        },
        recv_ts_ms=now_ms + 5,
    )
    assert report is not None
    assert report.market_id == "m_discovery"


def test_runtime_market_discovery_cycle_registers_metadata() -> None:
    asyncio.run(_run_runtime_market_discovery_cycle_registers_metadata())
