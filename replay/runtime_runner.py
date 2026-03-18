from __future__ import annotations

import asyncio
import json
from collections import Counter
from contextlib import suppress
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

from app.bootstrap import AppBootstrapper
from app.config import RuntimeConfig
from app.runtime import AppRuntime
from replay.player import ReplayPlayer
from replay.registry import build_default_registry


DEFAULT_REPLAY_TOPICS: frozenset[str] = frozenset(
    {
        "market.metadata",
        "feeds.binance.tick",
        "feeds.binance.depth",
        "feeds.chainlink.tick",
        "feeds.polymarket.market.book_top",
        "feeds.polymarket.market.depth",
        "feeds.polymarket.market.tick_size",
        "feeds.polymarket.market.resolved",
        "feeds.polymarket.user.execution",
    }
)

OUTPUT_TOPICS: tuple[str, ...] = (
    "pricing.theo",
    "pricing.quote_plan",
    "strategy.order_intents",
    "market.lifecycle.transition",
)


@dataclass(slots=True)
class ReplayReport:
    input_records: int
    replayed_records: int
    skipped_records: int
    input_topics: dict[str, int]
    replayed_topics: dict[str, int]
    skipped_topics: dict[str, int]
    output_topics: dict[str, int]
    markets: list[str]
    open_orders: dict[str, int]
    pair_costs: dict[str, str | None]
    unhedged_exposure: dict[str, str]
    alerts: list[dict[str, Any]]
    metrics: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "input_records": self.input_records,
            "replayed_records": self.replayed_records,
            "skipped_records": self.skipped_records,
            "input_topics": self.input_topics,
            "replayed_topics": self.replayed_topics,
            "skipped_topics": self.skipped_topics,
            "output_topics": self.output_topics,
            "markets": self.markets,
            "open_orders": self.open_orders,
            "pair_costs": self.pair_costs,
            "unhedged_exposure": self.unhedged_exposure,
            "alerts": self.alerts,
            "metrics": self.metrics,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, sort_keys=True)


class ReplayRuntimeRunner:
    """Replay JSONL recordings through the existing runtime and summarize outputs."""

    def __init__(
        self,
        config: RuntimeConfig | None = None,
        *,
        replay_topics: frozenset[str] = DEFAULT_REPLAY_TOPICS,
    ) -> None:
        self._config = _build_replay_safe_config(config or RuntimeConfig.from_env())
        self._replay_topics = replay_topics

    async def run(
        self,
        paths: list[str | Path],
        *,
        report_path: str | Path | None = None,
    ) -> ReplayReport:
        expanded_paths = _expand_paths(paths)
        if not expanded_paths:
            raise ValueError("no replay files found")

        bootstrapper = AppBootstrapper(self._config)
        context = await bootstrapper.start()
        runtime = AppRuntime(context)
        output_counts: Counter[str] = Counter()
        counters = _ReplayCounters()
        collector_tasks: list[asyncio.Task[None]] = []

        async def collect(topic: str) -> None:
            async for _ in context.event_bus.subscribe(topic):
                output_counts[topic] += 1

        for topic in OUTPUT_TOPICS:
            collector_tasks.append(asyncio.create_task(collect(topic), name=f"replay:{topic}"))
        await asyncio.sleep(0)

        await runtime.start()
        try:
            player = ReplayPlayer(registry=build_default_registry())

            async def handler(topic: str, _: str, payload: Any) -> None:
                counters.input_records += 1
                counters.input_topics[topic] += 1
                if topic not in self._replay_topics:
                    counters.skipped_records += 1
                    counters.skipped_topics[topic] += 1
                    return
                counters.replayed_records += 1
                counters.replayed_topics[topic] += 1
                await context.event_bus.publish(topic, payload)

            await player.run(expanded_paths, handler)
            await _drain_runtime(self._config.loop_interval_ms)
            report = _build_report(context, counters, output_counts)
            if report_path is not None:
                Path(report_path).write_text(report.to_json(), encoding="utf-8")
            return report
        finally:
            for task in collector_tasks:
                task.cancel()
            for task in collector_tasks:
                with suppress(asyncio.CancelledError):
                    await task
            await runtime.stop()
            await bootstrapper.stop()


@dataclass(slots=True)
class _ReplayCounters:
    input_records: int = 0
    replayed_records: int = 0
    skipped_records: int = 0
    input_topics: Counter[str] = field(default_factory=Counter)
    replayed_topics: Counter[str] = field(default_factory=Counter)
    skipped_topics: Counter[str] = field(default_factory=Counter)


def _build_replay_safe_config(config: RuntimeConfig) -> RuntimeConfig:
    return replace(
        config,
        binance=replace(config.binance, enabled=False),
        chainlink=replace(config.chainlink, enabled=False),
        polymarket=replace(config.polymarket, market_enabled=False, user_enabled=False),
        storage=replace(config.storage, recorder_enabled=False, snapshot_enabled=False),
        observability=replace(config.observability, metrics_log_interval_ms=0),
        execution=replace(config.execution, gateway_dry_run=True),
    )


def _expand_paths(paths: list[str | Path]) -> list[Path]:
    expanded: list[Path] = []
    for raw_path in paths:
        path = Path(raw_path)
        if path.is_dir():
            expanded.extend(sorted(path.rglob("*.jsonl")))
        elif path.exists():
            expanded.append(path)
    seen: set[Path] = set()
    unique_paths: list[Path] = []
    for path in expanded:
        resolved = path.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        unique_paths.append(path)
    return unique_paths


def _build_report(
    context,
    counters: _ReplayCounters,
    output_counts: Counter[str],
) -> ReplayReport:
    markets = sorted(market.market_id for market in context.market_registry.all_markets())
    open_orders = {
        market_id: len(context.order_state.get_open_orders(market_id))
        for market_id in markets
    }
    pair_costs = {
        market_id: _decimal_to_str(context.inventory_state.get_pair_cost(market_id))
        for market_id in markets
    }
    unhedged_exposure = {
        market_id: str(context.inventory_state.get_unhedged_exposure(market_id))
        for market_id in markets
    }
    alerts = [
        {
            "level": alert.level,
            "title": alert.title,
            "detail": alert.detail,
            "ts_ms": alert.ts_ms,
        }
        for alert in context.observability.alerts.recent(limit=100)
    ]
    return ReplayReport(
        input_records=counters.input_records,
        replayed_records=counters.replayed_records,
        skipped_records=counters.skipped_records,
        input_topics=dict(sorted(counters.input_topics.items())),
        replayed_topics=dict(sorted(counters.replayed_topics.items())),
        skipped_topics=dict(sorted(counters.skipped_topics.items())),
        output_topics=dict(sorted(output_counts.items())),
        markets=markets,
        open_orders=open_orders,
        pair_costs=pair_costs,
        unhedged_exposure=unhedged_exposure,
        alerts=alerts,
        metrics=context.observability.metrics.snapshot(),
    )


async def _drain_runtime(loop_interval_ms: int) -> None:
    sleep_s = max(loop_interval_ms, 10) / 1000
    for _ in range(4):
        await asyncio.sleep(sleep_s)


def _decimal_to_str(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)
