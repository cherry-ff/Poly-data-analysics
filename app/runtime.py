from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from contextlib import suppress
from typing import Coroutine

from app.bootstrap import RuntimeContext
from core.enums import MarketPhase, OrderStatus
from domain.events import (
    BinanceDepthEvent,
    BinanceTickEvent,
    ChainlinkTickEvent,
    LifecycleTransitionEvent,
    MarketBookTopEvent,
    MarketDiscoveredEvent,
    MarketMetadataEvent,
    PolymarketDepthEvent,
    MarketResolvedEvent,
    MarketTickSizeChangeEvent,
    OrderIntentsEvent,
    QuotePlanEvent,
    TheoSnapshotEvent,
    UserExecutionEvent,
)
from domain.models import MarketMetadata, OrderIntent
from observability.alerts import AlertLevel

logger = logging.getLogger(__name__)


class AppRuntime:
    def __init__(self, context: RuntimeContext) -> None:
        self._context = context
        self._tasks: list[asyncio.Task[None]] = []
        self._started = False
        self._startup_diag_started_ms: int | None = None
        self._ignored_market_discovery_refs: set[str] = set()
        self._market_discovery_refresh_lock = asyncio.Lock()
        self._last_market_discovery_refresh_ts_ms: int | None = None
        self._quote_block_reason_by_market: dict[str, str] = {}
        self._feed_diag = {
            "binance": {"enabled": context.config.binance.enabled, "count": 0, "first_ts_ms": None, "last_ts_ms": None, "last_latency_ms": None, "last_detail": "", "error": None},
            "binance_depth": {"enabled": context.config.binance.enabled and context.config.binance.depth_enabled, "count": 0, "first_ts_ms": None, "last_ts_ms": None, "last_latency_ms": None, "last_detail": "", "error": None},
            "chainlink": {"enabled": context.config.chainlink.enabled, "count": 0, "first_ts_ms": None, "last_ts_ms": None, "last_latency_ms": None, "last_detail": "", "error": None},
            "polymarket_market": {"enabled": context.config.polymarket.market_enabled, "count": 0, "first_ts_ms": None, "last_ts_ms": None, "last_latency_ms": None, "last_detail": "", "error": None},
            "polymarket_user": {"enabled": context.config.polymarket.user_enabled, "count": 0, "first_ts_ms": None, "last_ts_ms": None, "last_latency_ms": None, "last_detail": "", "error": None},
        }

    async def start(self) -> None:
        if self._started:
            return

        self._started = True
        self._startup_diag_started_ms = self._context.clock.now_ms()
        if self._context.storage.recorder is not None:
            await self._context.storage.recorder.start()
        if self._context.storage.db_writer is not None:
            await self._context.storage.db_writer.start()

        self._spawn("lifecycle_loop", self._lifecycle_loop())
        if self._context.storage.snapshot_writer is not None:
            self._spawn("snapshot_loop", self._snapshot_loop())
        if self._context.config.observability.metrics_log_interval_ms > 0:
            self._spawn("metrics_log_loop", self._metrics_log_loop())
        if self._context.config.observability.startup_diagnostics_enabled:
            self._spawn("startup_diagnostics_loop", self._startup_diagnostics_loop())
        self._spawn("consume_binance_ticks", self._consume_binance_ticks())
        self._spawn("consume_binance_depth", self._consume_binance_depth())
        self._spawn("consume_chainlink_ticks", self._consume_chainlink_ticks())
        self._spawn("consume_market_metadata", self._consume_market_metadata())
        self._spawn("consume_market_discovery", self._consume_market_discovery())
        self._spawn("consume_market_books", self._consume_market_books())
        self._spawn("consume_market_depth", self._consume_market_depth())
        self._spawn("consume_market_tick_size", self._consume_market_tick_size())
        self._spawn("consume_market_resolved", self._consume_market_resolved())
        self._spawn("consume_strategy_intents", self._consume_strategy_intents())
        self._spawn("consume_user_execution", self._consume_user_execution())
        if (
            self._context.config.metadata.discovery_enabled
            and self._context.config.metadata.gamma_base_url
            and (
                self._context.config.polymarket.market_enabled
                or self._context.config.polymarket.user_enabled
            )
        ):
            self._spawn("market_discovery_loop", self._market_discovery_loop())

        # Let consumer tasks subscribe before any producer/feed can publish.
        await asyncio.sleep(0)

        if self._context.config.binance.enabled:
            self._spawn("feed_binance", self._context.feeds.binance.run())
            if self._context.config.binance.depth_enabled:
                self._spawn("feed_binance_depth", self._context.feeds.binance_depth.run())
        if self._context.config.chainlink.enabled:
            reason = self._context.feeds.chainlink.startup_skip_reason()
            if reason is None:
                self._spawn("feed_chainlink", self._context.feeds.chainlink.run())
            else:
                self._skip_feed("chainlink", reason)
        if self._context.config.polymarket.market_enabled:
            reason = self._context.feeds.polymarket_market.startup_skip_reason(
                self._context.config.metadata.gamma_base_url
                if self._context.config.metadata.discovery_enabled
                else ""
            )
            if reason is None:
                self._spawn(
                    "feed_polymarket_market",
                    self._context.feeds.polymarket_market.run(),
                )
            else:
                self._skip_feed("polymarket_market", reason)
        if self._context.config.polymarket.user_enabled:
            reason = self._context.feeds.polymarket_user.startup_skip_reason()
            if reason is None:
                self._spawn(
                    "feed_polymarket_user",
                    self._context.feeds.polymarket_user.run(),
                )
            else:
                self._skip_feed("polymarket_user", reason)

        await asyncio.sleep(0)

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()

        for task in self._tasks:
            with suppress(asyncio.CancelledError, Exception):
                await task

        self._tasks.clear()
        if self._context.storage.recorder is not None:
            await self._context.storage.recorder.stop()
        if self._context.storage.db_writer is not None:
            await self._context.storage.db_writer.stop()
        self._started = False

    def _spawn(self, name: str, coro: Coroutine[object, object, None]) -> None:
        task = asyncio.create_task(coro, name=f"poly15:{name}")
        task.add_done_callback(lambda completed, task_name=name: self._on_task_done(task_name, completed))
        self._tasks.append(task)

    def _on_task_done(self, name: str, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            return
        if exc is None:
            return

        feed_name = self._feed_name_from_task(name)
        if feed_name is not None:
            state = self._feed_diag[feed_name]
            state["enabled"] = True
            state["error"] = f"{type(exc).__name__}: {exc}"

        logger.error("background task failed | task=%s", name, exc_info=exc)

    def _skip_feed(self, feed_name: str, reason: str) -> None:
        state = self._feed_diag[feed_name]
        state["enabled"] = True
        state["error"] = f"skipped: {reason}"
        logger.warning("skipping %s feed startup: %s", feed_name, reason)

    @staticmethod
    def _feed_name_from_task(name: str) -> str | None:
        mapping = {
            "feed_binance": "binance",
            "feed_binance_depth": "binance_depth",
            "feed_chainlink": "chainlink",
            "feed_polymarket_market": "polymarket_market",
            "feed_polymarket_user": "polymarket_user",
        }
        return mapping.get(name)

    async def _lifecycle_loop(self) -> None:
        sleep_s = max(self._context.config.loop_interval_ms, 1) / 1000
        while True:
            now_ms = self._context.clock.now_ms()
            transitions = self._context.lifecycle_manager.on_time_tick(now_ms)
            for transition in transitions:
                await self._publish_event(
                    "market.lifecycle.transition",
                    LifecycleTransitionEvent(transition=transition),
                )
                await self._refresh_market_outputs(
                    transition.market_id,
                    now_ms,
                )
            for market in self._context.market_registry.all_markets():
                await self._context.execution.cancel_manager.cancel_stale_quotes(
                    market.market_id,
                    now_ms,
                )
                phase = self._context.lifecycle_manager.get_phase(market.market_id)
                unhedged = float(
                    self._context.inventory_state.get_unhedged_exposure(market.market_id)
                )
                self._context.observability.metrics.gauge(
                    "close_phase_unhedged_exposure",
                    unhedged,
                    market_id=market.market_id,
                    phase=phase.value,
                )
                recovery_intents = self._context.strategy.recovery_strategy.on_timer(
                    market.market_id,
                    now_ms,
                )
                await self._publish_intents(
                    source="recovery_timer",
                    market_id=market.market_id,
                    intents=recovery_intents,
                )
            await asyncio.sleep(sleep_s)

    async def _consume_market_metadata(self) -> None:
        async for payload in self._context.event_bus.subscribe("market.metadata"):
            if not isinstance(payload, MarketMetadataEvent):
                continue
            await self._record_event("market.metadata", payload)
            self._context.feeds.polymarket_market.register_market(payload.market)
            self._context.feeds.polymarket_user.register_market(payload.market)
            self._context.market_registry.upsert(payload.market)
            self._context.lifecycle_manager.on_market_upsert(payload.market)
            await self._ensure_binance_open_anchor(
                payload.market,
                self._context.clock.now_ms(),
            )
            await self._refresh_market_outputs(payload.market.market_id, self._context.clock.now_ms())

    async def _consume_binance_ticks(self) -> None:
        async for payload in self._context.event_bus.subscribe("feeds.binance.tick"):
            if not isinstance(payload, BinanceTickEvent):
                continue
            await self._record_event("feeds.binance.tick", payload)
            latency_ms = max(payload.tick.recv_ts_ms - payload.tick.event_ts_ms, 0)
            self._context.observability.metrics.gauge(
                "binance_feed_latency_ms",
                latency_ms,
                symbol=payload.tick.symbol,
            )
            self._note_feed_event(
                "binance",
                recv_ts_ms=payload.tick.recv_ts_ms,
                latency_ms=latency_ms,
                detail=(
                    f"symbol={payload.tick.symbol} "
                    f"bid={payload.tick.best_bid} ask={payload.tick.best_ask}"
                ),
            )
            self._context.risk.engine.on_feed_heartbeat("binance", payload.tick.recv_ts_ms)
            self._context.pricing.vol_model.on_binance_tick(payload.tick)
            self._context.pricing.lead_lag.on_binance_tick(payload.tick)
            await self._refresh_pricing_markets(payload.tick.recv_ts_ms)

    async def _consume_binance_depth(self) -> None:
        async for payload in self._context.event_bus.subscribe("feeds.binance.depth"):
            if not isinstance(payload, BinanceDepthEvent):
                continue
            await self._record_event("feeds.binance.depth", payload)
            depth_age_ms = max(payload.snapshot.recv_ts_ms - payload.snapshot.event_ts_ms, 0)
            self._context.observability.metrics.gauge(
                "binance_depth_latency_ms",
                depth_age_ms,
                symbol=payload.snapshot.symbol,
            )
            self._note_feed_event(
                "binance_depth",
                recv_ts_ms=payload.snapshot.recv_ts_ms,
                latency_ms=depth_age_ms,
                detail=(
                    f"symbol={payload.snapshot.symbol} "
                    f"bids={len(payload.snapshot.bids)} asks={len(payload.snapshot.asks)}"
                ),
            )

    async def _consume_chainlink_ticks(self) -> None:
        async for payload in self._context.event_bus.subscribe("feeds.chainlink.tick"):
            if not isinstance(payload, ChainlinkTickEvent):
                continue
            await self._record_event("feeds.chainlink.tick", payload)
            oracle_age_ms = max(payload.tick.recv_ts_ms - payload.tick.oracle_ts_ms, 0)
            self._context.observability.metrics.gauge(
                "chainlink_oracle_age_ms",
                oracle_age_ms,
                feed=payload.tick.feed,
            )
            self._note_feed_event(
                "chainlink",
                recv_ts_ms=payload.tick.recv_ts_ms,
                latency_ms=oracle_age_ms,
                detail=f"feed={payload.tick.feed} price={payload.tick.price}",
            )
            self._context.risk.engine.on_feed_heartbeat("chainlink", payload.tick.recv_ts_ms)
            self._context.pricing.lead_lag.on_chainlink_tick(payload.tick)
            await self._refresh_pricing_markets(payload.tick.recv_ts_ms)

    async def _consume_market_discovery(self) -> None:
        async for payload in self._context.event_bus.subscribe(
            "feeds.polymarket.market.new_market"
        ):
            if not isinstance(payload, MarketDiscoveredEvent):
                continue
            await self._record_event("feeds.polymarket.market.new_market", payload)
            if not self._looks_like_gamma_market_id(payload.market_id):
                self._context.observability.metrics.incr(
                    "market_discovery_ws_hint_count",
                    ref=payload.market_id[:24],
                )
                if payload.market_id not in self._ignored_market_discovery_refs:
                    self._ignored_market_discovery_refs.add(payload.market_id)
                    logger.info(
                        "received websocket market discovery ref that is not a gamma market id; triggering gamma refresh: %s",
                        payload.market_id,
                    )
                await self._refresh_market_discovery(reason="ws_hint")
                continue
            try:
                market = await self._context.metadata_loader.load_market(payload.market_id)
            except Exception as exc:
                self._context.observability.metrics.incr(
                    "metadata_load_error_count",
                    market_id=payload.market_id,
                )
                await self._context.observability.alerts.emit(
                    AlertLevel.P1,
                    "metadata load failed",
                    f"market_id={payload.market_id} error={exc}",
                )
                continue
            await self._publish_event(
                "market.metadata",
                MarketMetadataEvent(market=market),
            )

    async def _market_discovery_loop(self) -> None:
        interval_s = self._context.config.metadata.discovery_interval_ms / 1000
        await self._refresh_market_discovery(reason="startup", min_interval_ms=0)
        while True:
            await asyncio.sleep(interval_s)
            await self._refresh_market_discovery(reason="interval", min_interval_ms=0)

    async def _refresh_market_discovery(
        self,
        *,
        reason: str,
        min_interval_ms: int = 1_000,
    ) -> None:
        if (
            not self._context.config.metadata.discovery_enabled
            or not self._context.config.metadata.gamma_base_url
        ):
            return

        now_ms = self._context.clock.now_ms()
        last_refresh_ts_ms = self._last_market_discovery_refresh_ts_ms
        if (
            min_interval_ms > 0
            and last_refresh_ts_ms is not None
            and now_ms - last_refresh_ts_ms < min_interval_ms
        ):
            self._context.observability.metrics.incr(
                "market_discovery_refresh_skipped_count",
                reason=reason,
            )
            return

        async with self._market_discovery_refresh_lock:
            now_ms = self._context.clock.now_ms()
            last_refresh_ts_ms = self._last_market_discovery_refresh_ts_ms
            if (
                min_interval_ms > 0
                and last_refresh_ts_ms is not None
                and now_ms - last_refresh_ts_ms < min_interval_ms
            ):
                self._context.observability.metrics.incr(
                    "market_discovery_refresh_skipped_count",
                    reason=reason,
                )
                return

            self._last_market_discovery_refresh_ts_ms = now_ms
            self._context.observability.metrics.incr(
                "market_discovery_refresh_count",
                reason=reason,
            )
            await self._run_market_discovery_cycle()

    async def _run_market_discovery_cycle(self) -> None:
        try:
            markets = await self._context.metadata_loader.discover_markets(
                tag_slug=self._context.config.metadata.discovery_tag_slug,
                keywords=self._context.config.metadata.discovery_keywords,
                exclude_keywords=self._context.config.metadata.discovery_exclude_keywords,
                min_duration_minutes=self._context.config.metadata.discovery_min_duration_minutes,
                max_duration_minutes=self._context.config.metadata.discovery_max_duration_minutes,
                max_markets=self._context.config.metadata.discovery_max_markets,
                page_limit=self._context.config.metadata.discovery_page_limit,
                max_pages=self._context.config.metadata.discovery_max_pages,
            )
        except Exception as exc:
            self._context.observability.metrics.incr("market_discovery_error_count")
            await self._context.observability.alerts.emit(
                AlertLevel.P2,
                "market discovery failed",
                str(exc),
            )
            return

        if not markets:
            return

        asset_ids: list[str] = []
        condition_ids: list[str] = []
        for market in markets:
            await self._publish_event(
                "market.metadata",
                MarketMetadataEvent(market=market),
            )
            asset_ids.extend((market.up_token_id, market.down_token_id))
            condition_ids.append(market.condition_id)

        if self._context.config.polymarket.market_enabled:
            await self._context.feeds.polymarket_market.ensure_assets(asset_ids)
        if self._context.config.polymarket.user_enabled:
            await self._context.feeds.polymarket_user.ensure_markets(condition_ids)

    async def _consume_market_books(self) -> None:
        async for payload in self._context.event_bus.subscribe(
            "feeds.polymarket.market.book_top"
        ):
            if not isinstance(payload, MarketBookTopEvent):
                continue
            await self._record_event("feeds.polymarket.market.book_top", payload)
            ws_age_ms = max(self._context.clock.now_ms() - payload.top.last_update_ts_ms, 0)
            self._context.observability.metrics.gauge(
                "polymarket_market_ws_age_ms",
                ws_age_ms,
                market_id=payload.market_id,
                token_id=payload.top.token_id,
            )
            self._note_feed_event(
                "polymarket_market",
                recv_ts_ms=payload.top.last_update_ts_ms,
                latency_ms=ws_age_ms,
                detail=(
                    f"market_id={payload.market_id} token_id={payload.top.token_id} "
                    f"bid={payload.top.best_bid_px} ask={payload.top.best_ask_px}"
                ),
            )
            self._context.risk.engine.on_feed_heartbeat(
                "polymarket_market",
                payload.top.last_update_ts_ms,
            )
            self._context.book_state.apply_market_event(payload)
            await self._refresh_market_outputs(payload.market_id, self._context.clock.now_ms())

    async def _consume_market_depth(self) -> None:
        async for payload in self._context.event_bus.subscribe(
            "feeds.polymarket.market.depth"
        ):
            if not isinstance(payload, PolymarketDepthEvent):
                continue
            await self._record_event("feeds.polymarket.market.depth", payload)
            depth_age_ms = max(
                self._context.clock.now_ms() - payload.snapshot.last_update_ts_ms,
                0,
            )
            self._context.observability.metrics.gauge(
                "polymarket_market_depth_age_ms",
                depth_age_ms,
                market_id=payload.snapshot.market_id,
                token_id=payload.snapshot.token_id,
            )

    async def _consume_market_tick_size(self) -> None:
        async for payload in self._context.event_bus.subscribe(
            "feeds.polymarket.market.tick_size"
        ):
            if not isinstance(payload, MarketTickSizeChangeEvent):
                continue
            await self._record_event("feeds.polymarket.market.tick_size", payload)
            self._context.market_registry.update_tick_size(
                payload.market_id,
                payload.tick_size,
            )
            await self._refresh_market_outputs(payload.market_id, self._context.clock.now_ms())

    async def _consume_market_resolved(self) -> None:
        async for payload in self._context.event_bus.subscribe(
            "feeds.polymarket.market.resolved"
        ):
            if not isinstance(payload, MarketResolvedEvent):
                continue
            await self._record_event("feeds.polymarket.market.resolved", payload)
            self._context.market_registry.update_status(payload.market_id, "resolved")
            await self._context.observability.alerts.emit(
                AlertLevel.P2,
                "market resolved",
                f"market_id={payload.market_id}",
            )
            await self._context.execution.cancel_manager.cancel_all_for_market(
                payload.market_id,
                reason="market_resolved",
            )

    async def _consume_user_execution(self) -> None:
        async for payload in self._context.event_bus.subscribe(
            "feeds.polymarket.user.execution"
        ):
            if not isinstance(payload, UserExecutionEvent):
                continue
            await self._record_event("feeds.polymarket.user.execution", payload)
            ws_age_ms = max(self._context.clock.now_ms() - payload.report.event_ts_ms, 0)
            self._context.observability.metrics.gauge(
                "polymarket_user_ws_age_ms",
                ws_age_ms,
                market_id=payload.report.market_id,
            )
            self._note_feed_event(
                "polymarket_user",
                recv_ts_ms=payload.report.event_ts_ms,
                latency_ms=ws_age_ms,
                detail=(
                    f"market_id={payload.report.market_id} "
                    f"status={payload.report.status.value} filled={payload.report.filled_size}"
                ),
            )
            self._context.risk.engine.on_feed_heartbeat(
                "polymarket_user",
                payload.report.event_ts_ms,
            )
            self._context.order_state.on_execution_report(payload.report)
            record = self._lookup_order_record(payload.report.client_order_id)
            if record is not None and record.mismatch:
                self._context.observability.metrics.incr(
                    "ghost_fill_count",
                    market_id=payload.report.market_id,
                )
                await self._context.observability.alerts.emit(
                    AlertLevel.P1,
                    "order state mismatch",
                    (
                        f"market_id={payload.report.market_id} "
                        f"client_order_id={record.client_order_id}"
                    ),
                )
            if payload.report.status in {OrderStatus.PARTIAL, OrderStatus.FILLED}:
                self._context.inventory_state.on_fill(payload.report)
            recovery_intents = self._context.strategy.recovery_strategy.on_fill(payload.report)
            await self._publish_intents(
                source="recovery_fill",
                market_id=payload.report.market_id,
                intents=recovery_intents,
            )
            await self._refresh_market_outputs(
                payload.report.market_id,
                payload.report.event_ts_ms,
                emit_pair_strategy=not bool(recovery_intents),
            )

    async def _consume_strategy_intents(self) -> None:
        async for payload in self._context.event_bus.subscribe("strategy.order_intents"):
            if not isinstance(payload, OrderIntentsEvent):
                continue
            await self._record_event("strategy.order_intents", payload)
            now_ms = self._context.clock.now_ms()
            for intent in payload.intents:
                started = self._context.clock.now_ms()
                decision = await self._context.execution.router.submit(intent, now_ms)
                elapsed = self._context.clock.now_ms() - started
                self._context.observability.metrics.timing(
                    "router_submit_latency_ms",
                    float(elapsed),
                    source=payload.source,
                    role=intent.role.value,
                )
                if decision.allowed:
                    self._context.observability.metrics.incr(
                        "order_submitted_count",
                        source=payload.source,
                        role=intent.role.value,
                    )
                else:
                    self._context.observability.metrics.incr(
                        "order_rejected_count",
                        source=payload.source,
                        severity=decision.severity,
                    )
                    if decision.reason.startswith("gateway_error"):
                        await self._context.observability.alerts.emit(
                            AlertLevel.P1,
                            "gateway submit failed",
                            decision.reason,
                        )

    async def _refresh_pricing_markets(self, now_ms: int) -> None:
        for market in self._context.market_registry.all_markets():
            phase = self._context.lifecycle_manager.get_phase(market.market_id)
            if phase in {
                MarketPhase.PREWARM,
                MarketPhase.ACTIVE,
                MarketPhase.FAST_CLOSE,
                MarketPhase.FINAL_SECONDS,
            }:
                await self._refresh_market_outputs(market.market_id, now_ms)

    async def _refresh_market_outputs(
        self,
        market_id: str,
        now_ms: int,
        *,
        emit_pair_strategy: bool = True,
    ) -> None:
        market = self._context.market_registry.get(market_id)
        if market is None:
            return

        phase = self._context.lifecycle_manager.get_phase(market.market_id)
        if phase in {
            MarketPhase.CLOSED_WAIT_RESOLUTION,
            MarketPhase.RESOLVED,
            MarketPhase.ARCHIVED,
        }:
            return

        await self._ensure_binance_open_anchor(market, now_ms)

        snapshot = self._context.pricing.fair_value.compute(market, now_ms)
        if snapshot is not None:
            await self._publish_event(
                "pricing.theo",
                TheoSnapshotEvent(snapshot=snapshot),
            )
            pair_top = self._context.book_state.get_pair_top(market.market_id)
            if pair_top is not None:
                mid_up = (pair_top.up.best_bid_px + pair_top.up.best_ask_px) / Decimal("2")
                if mid_up > 0:
                    fair_value_vs_mid_bps = ((snapshot.theo_up - mid_up) / mid_up) * Decimal("10000")
                    self._context.observability.metrics.gauge(
                        "fair_value_vs_mid_bps",
                        float(fair_value_vs_mid_bps),
                        market_id=market.market_id,
                    )

        quote_plan = self._context.pricing.quote_policy.build(
            market,
            now_ms,
            theo=snapshot,
        )
        if quote_plan is not None:
            await self._publish_event(
                "pricing.quote_plan",
                QuotePlanEvent(plan=quote_plan),
            )
            pair_bid_sum = Decimal("0")
            if quote_plan.up_bid_px is not None and quote_plan.down_bid_px is not None:
                pair_bid_sum = quote_plan.up_bid_px + quote_plan.down_bid_px
            self._context.observability.metrics.gauge(
                "pair_cost_vs_one_bps",
                float((pair_bid_sum - Decimal("1")) * Decimal("10000")),
                market_id=market.market_id,
            )
            if emit_pair_strategy and self._should_emit_pair_strategy(market.market_id, now_ms):
                pair_intents = self._context.strategy.pair_strategy.on_tick(
                    market.market_id,
                    now_ms,
                )
                await self._publish_intents(
                    source="pair_strategy",
                    market_id=market.market_id,
                    intents=pair_intents,
                )

    async def _ensure_binance_open_anchor(
        self,
        market: MarketMetadata,
        now_ms: int,
    ) -> None:
        if not self._context.config.binance.enabled:
            return

        minute_start_ms = self._context.pricing.binance_open_price.minute_start_ts_ms(
            market.start_ts_ms
        )
        if minute_start_ms <= 0 or now_ms < minute_start_ms:
            return

        price = self._context.pricing.binance_open_price.cached_market_open_price(
            market.market_id
        )
        if price is None:
            try:
                price = await self._context.pricing.binance_open_price.ensure_market_open_price(
                    market
                )
            except Exception as exc:
                self._context.observability.metrics.incr(
                    "binance_open_anchor_error_count",
                    market_id=market.market_id,
                )
                logger.warning(
                    "failed to fetch Binance minute open price for market=%s start_ts_ms=%s: %s",
                    market.market_id,
                    market.start_ts_ms,
                    exc,
                )
                return

        if price is None or price <= 0:
            return

        self._context.pricing.fair_value.seed_reference_price(
            market.market_id,
            price,
        )

    def _should_emit_pair_strategy(self, market_id: str, now_ms: int) -> bool:
        reason = self._pair_strategy_block_reason(now_ms)
        if reason is None:
            self._quote_block_reason_by_market.pop(market_id, None)
            return True

        previous_reason = self._quote_block_reason_by_market.get(market_id)
        if previous_reason != reason:
            self._quote_block_reason_by_market[market_id] = reason
            logger.info(
                "pair strategy blocked for market=%s reason=%s",
                market_id,
                reason,
            )
        self._context.observability.metrics.incr(
            "pair_strategy_blocked_count",
            market_id=market_id,
            reason=reason,
        )
        return False

    def _pair_strategy_block_reason(self, now_ms: int) -> str | None:
        risk_cfg = self._context.risk.engine.config
        if not self._is_feed_fresh("binance", now_ms, risk_cfg.max_binance_staleness_ms):
            return "binance_unhealthy"
        if self._context.config.polymarket.market_enabled and not self._is_feed_fresh(
            "polymarket_market",
            now_ms,
            risk_cfg.max_polymarket_staleness_ms,
        ):
            return "polymarket_market_unhealthy"
        if self._context.config.chainlink.enabled and not self._is_feed_fresh(
            "chainlink",
            now_ms,
            self._context.config.chainlink.stale_after_ms,
        ):
            return "chainlink_unhealthy"
        if self._context.config.polymarket.user_enabled and not self._is_feed_fresh(
            "polymarket_user",
            now_ms,
            risk_cfg.max_polymarket_staleness_ms,
        ):
            return "polymarket_user_unhealthy"
        return None

    def _is_feed_fresh(self, feed_name: str, now_ms: int, max_age_ms: int) -> bool:
        state = self._feed_diag[feed_name]
        if state["error"] is not None:
            return False
        last_ts_ms = state["last_ts_ms"]
        if last_ts_ms is None:
            return False
        return now_ms - int(last_ts_ms) <= max_age_ms

    async def _publish_intents(
        self,
        *,
        source: str,
        market_id: str,
        intents: list[OrderIntent],
    ) -> None:
        if not intents:
            return
        await self._publish_event(
            "strategy.order_intents",
            OrderIntentsEvent(
                source=source,
                market_id=market_id,
                intents=intents,
            ),
        )

    async def _publish_event(self, topic: str, payload: object) -> None:
        await self._record_event(topic, payload)
        await self._context.event_bus.publish(topic, payload)

    async def _record_event(self, topic: str, payload: object) -> None:
        recorder = self._context.storage.recorder
        if recorder is not None:
            await recorder.write_event(topic, payload)
        db_writer = self._context.storage.db_writer
        if db_writer is not None:
            await db_writer.write_event(topic, payload)

    async def _snapshot_loop(self) -> None:
        snapshot_writer = self._context.storage.snapshot_writer
        if snapshot_writer is None:
            return
        interval_s = self._context.config.storage.snapshot_interval_ms / 1000
        while True:
            await asyncio.sleep(interval_s)
            now_ms = self._context.clock.now_ms()
            await snapshot_writer.write_snapshot(now_ms)
            self._context.observability.metrics.incr("snapshot_written_count")

    async def _metrics_log_loop(self) -> None:
        interval_ms = self._context.config.observability.metrics_log_interval_ms
        if interval_ms <= 0:
            return
        while True:
            await asyncio.sleep(interval_ms / 1000)
            self._context.observability.metrics.gauge(
                "recorder_dropped_count",
                float(self._context.storage.recorder.dropped_count)
                if self._context.storage.recorder is not None
                else 0.0,
            )
            self._context.observability.metrics.gauge(
                "recorder_written_count",
                float(self._context.storage.recorder.written_count)
                if self._context.storage.recorder is not None
                else 0.0,
            )
            self._context.observability.metrics.gauge(
                "recorder_flush_count",
                float(self._context.storage.recorder.flush_count)
                if self._context.storage.recorder is not None
                else 0.0,
            )
            self._context.observability.metrics.gauge(
                "db_writer_dropped_count",
                float(self._context.storage.db_writer.dropped_count)
                if self._context.storage.db_writer is not None
                else 0.0,
            )
            self._context.observability.metrics.gauge(
                "db_writer_written_count",
                float(self._context.storage.db_writer.written_count)
                if self._context.storage.db_writer is not None
                else 0.0,
            )
            self._context.observability.metrics.gauge(
                "db_writer_flush_count",
                float(self._context.storage.db_writer.flush_count)
                if self._context.storage.db_writer is not None
                else 0.0,
            )
            self._context.observability.metrics.log_summary()

    async def _startup_diagnostics_loop(self) -> None:
        config = self._context.config.observability
        interval_ms = config.startup_diagnostics_log_interval_ms
        window_ms = config.startup_diagnostics_window_ms
        if interval_ms <= 0:
            return
        while True:
            await asyncio.sleep(interval_ms / 1000)
            now_ms = self._context.clock.now_ms()
            logger.info("[startup] feed health | %s", self._format_feed_health(now_ms))
            if (
                self._startup_diag_started_ms is not None
                and window_ms > 0
                and now_ms - self._startup_diag_started_ms >= window_ms
            ):
                logger.info(
                    "[startup] diagnostics window ended after %dms; detailed startup feed logs are now disabled",
                    window_ms,
                )
                return

    def _note_feed_event(
        self,
        feed_name: str,
        *,
        recv_ts_ms: int,
        latency_ms: int | None,
        detail: str,
    ) -> None:
        state = self._feed_diag[feed_name]
        state["enabled"] = True
        count = int(state["count"]) + 1
        state["count"] = count
        if state["first_ts_ms"] is None:
            state["first_ts_ms"] = recv_ts_ms
        state["last_ts_ms"] = recv_ts_ms
        state["last_latency_ms"] = latency_ms
        state["last_detail"] = detail
        state["error"] = None

        first_events = self._context.config.observability.startup_diagnostics_first_events
        if self._context.config.observability.startup_diagnostics_enabled and count <= first_events:
            latency_text = f"{latency_ms}ms" if latency_ms is not None else "n/a"
            logger.info(
                "[startup] first events | feed=%s event=%d latency=%s detail=%s",
                feed_name,
                count,
                latency_text,
                detail,
            )

    def _format_feed_health(self, now_ms: int) -> str:
        parts: list[str] = []
        for feed_name in (
            "binance",
            "binance_depth",
            "chainlink",
            "polymarket_market",
            "polymarket_user",
        ):
            state = self._feed_diag[feed_name]
            if not state["enabled"]:
                parts.append(f"{feed_name}=disabled")
                continue
            count = int(state["count"])
            error = state["error"]
            if error is not None and count == 0:
                parts.append(f"{feed_name}[error={error}]")
                continue
            if count == 0:
                parts.append(f"{feed_name}=waiting")
                continue
            first_ts_ms = int(state["first_ts_ms"] or now_ms)
            last_ts_ms = int(state["last_ts_ms"] or now_ms)
            age_ms = max(now_ms - last_ts_ms, 0)
            span_ms = max(last_ts_ms - first_ts_ms, 1)
            rate_per_s = count * 1000 / span_ms if count > 1 else 0.0
            latency_ms = state["last_latency_ms"]
            latency_text = f"{latency_ms}ms" if latency_ms is not None else "n/a"
            parts.append(
                f"{feed_name}[count={count} rate={rate_per_s:.1f}/s age={age_ms}ms latency={latency_text}]"
            )
        return " | ".join(parts)

    def _lookup_order_record(self, client_order_id: str):
        record = self._context.order_state.get(client_order_id)
        if record is not None:
            return record
        return self._context.order_state.get_by_exchange_order_id(client_order_id)

    @staticmethod
    def _looks_like_gamma_market_id(value: str) -> bool:
        return value.isdigit()
