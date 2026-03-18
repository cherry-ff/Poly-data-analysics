"""Integration-style tests for runtime -> strategy -> execution wiring."""

from __future__ import annotations

import asyncio
from decimal import Decimal

from app.bootstrap import AppBootstrapper
from app.config import (
    BinanceFeedConfig,
    ChainlinkFeedConfig,
    ExecutionConfig,
    PolymarketFeedConfig,
    RuntimeConfig,
)
from app.runtime import AppRuntime
from core.enums import OrderRole, OrderStatus, Side, TimeInForce
from domain.events import (
    BinanceTickEvent,
    MarketBookTopEvent,
    MarketMetadataEvent,
    OrderIntentsEvent,
)
from domain.models import BinanceTick, MarketMetadata, OrderIntent, OutcomeBookTop


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
        ),
    )


def _make_binance_tick(price: str, ts_ms: int) -> BinanceTickEvent:
    price_dec = Decimal(price)
    return BinanceTickEvent(
        tick=BinanceTick(
            symbol="BTCUSDT",
            event_ts_ms=ts_ms,
            recv_ts_ms=ts_ms,
            last_price=price_dec,
            best_bid=price_dec - Decimal("5"),
            best_ask=price_dec + Decimal("5"),
        )
    )


async def _wait_for(predicate, *, timeout_s: float = 1.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout_s
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("condition not met before timeout")


async def _run_runtime_routes_strategy_order_intents() -> None:
    config = RuntimeConfig(
        loop_interval_ms=25,
        execution=ExecutionConfig(gateway_dry_run=True),
    )
    bootstrapper = AppBootstrapper(config)
    context = await bootstrapper.start()
    runtime = AppRuntime(context)
    now_ms = context.clock.now_ms()
    market = _make_market(now_ms)

    context.market_registry.upsert(market)
    context.lifecycle_manager.on_market_upsert(market)
    context.lifecycle_manager.on_time_tick(now_ms)

    intent = OrderIntent(
        intent_id="int_test",
        pair_id="pair_test",
        market_id=market.market_id,
        token_id=market.up_token_id,
        side=Side.BUY,
        price=Decimal("0.48"),
        size=Decimal("10"),
        tif=TimeInForce.GTC,
        post_only=True,
        role=OrderRole.MAKER_QUOTE,
        reason="runtime_test",
    )

    await runtime.start()
    try:
        await context.event_bus.publish(
            "strategy.order_intents",
            OrderIntentsEvent(
                source="unit_test",
                market_id=market.market_id,
                intents=[intent],
            ),
        )

        await _wait_for(
            lambda: len(context.order_state.get_open_orders(market.market_id)) == 1
        )

        orders = context.order_state.get_open_orders(market.market_id)
        assert len(orders) == 1
        assert orders[0].role == OrderRole.MAKER_QUOTE
        assert orders[0].price == Decimal("0.48")
        assert orders[0].status == OrderStatus.PENDING_SUBMIT
    finally:
        await runtime.stop()
        await bootstrapper.stop()


def test_runtime_routes_strategy_order_intents_into_execution_router() -> None:
    asyncio.run(_run_runtime_routes_strategy_order_intents())


async def _run_runtime_end_to_end_pricing_produces_submitted_quotes() -> None:
    config = RuntimeConfig(
        loop_interval_ms=25,
        execution=ExecutionConfig(gateway_dry_run=True),
    )
    bootstrapper = AppBootstrapper(config)
    context = await bootstrapper.start()
    runtime = AppRuntime(context)
    now_ms = context.clock.now_ms()
    market = _make_market(now_ms, market_id="m_quotes")

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

        await _wait_for(
            lambda: len(context.order_state.get_open_orders(market.market_id)) >= 2,
            timeout_s=1.5,
        )

        orders = context.order_state.get_open_orders(market.market_id)
        assert orders
        assert all(order.role == OrderRole.MAKER_QUOTE for order in orders)
        assert {order.token_id for order in orders}.issubset(
            {market.up_token_id, market.down_token_id}
        )
    finally:
        await runtime.stop()
        await bootstrapper.stop()


def test_runtime_end_to_end_pricing_produces_submitted_quotes() -> None:
    asyncio.run(_run_runtime_end_to_end_pricing_produces_submitted_quotes())


async def _run_runtime_uses_binance_minute_open_anchor_for_theo() -> None:
    config = RuntimeConfig(
        loop_interval_ms=25,
        binance=BinanceFeedConfig(enabled=True, depth_enabled=False),
        execution=ExecutionConfig(gateway_dry_run=True),
    )
    bootstrapper = AppBootstrapper(config)
    context = await bootstrapper.start()
    runtime = AppRuntime(context)
    now_ms = context.clock.now_ms()
    market = MarketMetadata(
        market_id="m_binance_anchor",
        condition_id="cond_m_binance_anchor",
        up_token_id="up_tok",
        down_token_id="dn_tok",
        start_ts_ms=now_ms,
        end_ts_ms=now_ms + 120_000,
        tick_size=Decimal("0.01"),
        fee_rate_bps=Decimal("10"),
        min_order_size=Decimal("1"),
        status="active",
        reference_price=None,
    )

    async def _noop_feed() -> None:
        await asyncio.Event().wait()

    calls: list[dict[str, str] | None] = []

    async def _fetcher(url: str, params: dict[str, str] | None) -> object:
        del url
        calls.append(params)
        assert params is not None
        return [[int(params["startTime"]), "50000"]]

    context.feeds.binance.run = _noop_feed  # type: ignore[method-assign]
    context.pricing.binance_open_price._fetcher = _fetcher

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
            _make_binance_tick("50000", now_ms + 250),
        )
        await context.event_bus.publish(
            "feeds.binance.tick",
            _make_binance_tick("50020", now_ms + 500),
        )

        await _wait_for(
            lambda: context.pricing.fair_value.latest(market.market_id) is not None,
            timeout_s=1.5,
        )

        snapshot = context.pricing.fair_value.latest(market.market_id)
        assert snapshot is not None
        assert snapshot.theo_up > Decimal("0.5")
        assert context.pricing.fair_value._reference_price_cache[market.market_id] == Decimal("50000")
        assert calls
    finally:
        await runtime.stop()
        await bootstrapper.stop()


def test_runtime_uses_binance_minute_open_anchor_for_theo() -> None:
    asyncio.run(_run_runtime_uses_binance_minute_open_anchor_for_theo())


async def _run_runtime_blocks_quotes_when_required_feeds_are_unhealthy() -> None:
    config = RuntimeConfig(
        loop_interval_ms=25,
        chainlink=ChainlinkFeedConfig(enabled=True),
        polymarket=PolymarketFeedConfig(
            user_enabled=True,
            api_key="k",
            api_secret="s",
            passphrase="p",
        ),
        execution=ExecutionConfig(gateway_dry_run=True),
    )
    bootstrapper = AppBootstrapper(config)
    context = await bootstrapper.start()
    runtime = AppRuntime(context)
    now_ms = context.clock.now_ms()
    market = _make_market(now_ms, market_id="m_blocked")

    async def _noop_feed() -> None:
        await asyncio.Event().wait()

    context.feeds.chainlink.run = _noop_feed  # type: ignore[method-assign]
    context.feeds.polymarket_user.run = _noop_feed  # type: ignore[method-assign]

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
        await asyncio.sleep(0.1)
    finally:
        await runtime.stop()
        await bootstrapper.stop()

    assert context.order_state.get_open_orders(market.market_id) == []
    metrics = context.observability.metrics.snapshot()
    blocked_keys = [
        key for key in metrics["counters"] if key.startswith("pair_strategy_blocked_count")
    ]
    assert blocked_keys


def test_runtime_blocks_quotes_when_required_feeds_are_unhealthy() -> None:
    asyncio.run(_run_runtime_blocks_quotes_when_required_feeds_are_unhealthy())
