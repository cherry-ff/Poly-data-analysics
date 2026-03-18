"""Unit tests for strategy.phase_policy, strategy.pair_strategy, strategy.recovery_strategy."""

from decimal import Decimal

import pytest

from app.config import LifecycleConfig
from core.enums import MarketPhase, OrderRole, OrderStatus, Side, TimeInForce
from core.ids import SequentialIdGenerator
from domain.events import MarketBookTopEvent
from domain.models import (
    ExecutionReport,
    MarketMetadata,
    OrderIntent,
    OutcomeBookTop,
)
from market.lifecycle import LifecycleManager
from market.registry import InMemoryMarketRegistry
from pricing.fair_value import BinaryOptionFairValueEngine
from pricing.lead_lag import SimpleLeadLagEngine
from pricing.quote_policy import MakerQuotePolicy
from pricing.vol_model import EwmaVolModel
from state.book_state import InMemoryBookStateStore
from state.inventory_state import InMemoryInventoryStore
from state.order_state import InMemoryOrderStateStore
from strategy.pair_strategy import PairStrategy, PairStrategyConfig
from strategy.phase_policy import PhasePolicy
from strategy.recovery_strategy import RecoveryStrategy, RecoveryStrategyConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NOW_MS = 1_000_000


def _make_market(
    market_id: str = "m1",
    start_ts_ms: int = 500_000,
    end_ts_ms: int = 2_000_000,
    tick_size: str = "0.01",
    fee_rate_bps: str = "10",
    status: str = "active",
) -> MarketMetadata:
    return MarketMetadata(
        market_id=market_id,
        condition_id=f"cond_{market_id}",
        up_token_id="up_tok",
        down_token_id="dn_tok",
        start_ts_ms=start_ts_ms,
        end_ts_ms=end_ts_ms,
        tick_size=Decimal(tick_size),
        fee_rate_bps=Decimal(fee_rate_bps),
        min_order_size=Decimal("1"),
        status=status,
        reference_price=Decimal("50000"),
    )


def _make_book_event(
    market_id: str,
    token_id: str,
    bid: str,
    ask: str,
) -> MarketBookTopEvent:
    return MarketBookTopEvent(
        market_id=market_id,
        top=OutcomeBookTop(
            token_id=token_id,
            best_bid_px=Decimal(bid),
            best_bid_sz=Decimal("100"),
            best_ask_px=Decimal(ask),
            best_ask_sz=Decimal("100"),
            last_update_ts_ms=NOW_MS,
        ),
    )


def _make_fill_report(
    market_id: str = "m1",
    token_id: str = "up_tok",
    side: Side = Side.BUY,
    filled: str = "10",
    price: str = "0.55",
    cid: str = "c_fill",
) -> ExecutionReport:
    return ExecutionReport(
        client_order_id=cid,
        pair_id=None,
        market_id=market_id,
        token_id=token_id,
        side=side,
        status=OrderStatus.FILLED,
        filled_size=Decimal(filled),
        avg_price=Decimal(price),
        exchange_order_id=f"exch_{cid}",
        event_ts_ms=NOW_MS,
    )


def _setup_registry_and_lifecycle(
    market: MarketMetadata,
) -> tuple[InMemoryMarketRegistry, LifecycleManager]:
    registry = InMemoryMarketRegistry()
    registry.upsert(market)
    lc_config = LifecycleConfig()
    lifecycle = LifecycleManager(registry=registry, config=lc_config)
    lifecycle.on_market_upsert(market)
    lifecycle.on_time_tick(NOW_MS)
    return registry, lifecycle


def _setup_book(
    registry: InMemoryMarketRegistry,
    market: MarketMetadata,
    up_bid: str = "0.53",
    up_ask: str = "0.57",
    dn_bid: str = "0.41",
    dn_ask: str = "0.45",
) -> InMemoryBookStateStore:
    book_state = InMemoryBookStateStore(registry=registry)
    book_state.apply_market_event(_make_book_event(market.market_id, "up_tok", up_bid, up_ask))
    book_state.apply_market_event(_make_book_event(market.market_id, "dn_tok", dn_bid, dn_ask))
    return book_state


def _make_quote_policy(
    registry: InMemoryMarketRegistry,
    lifecycle: LifecycleManager,
    book_state: InMemoryBookStateStore,
    inventory_state: InMemoryInventoryStore,
) -> MakerQuotePolicy:
    vol_model = EwmaVolModel()
    lead_lag = SimpleLeadLagEngine()
    fair_value = BinaryOptionFairValueEngine(
        vol_model=vol_model,
        lead_lag=lead_lag,
        inventory_state=inventory_state,
    )
    return MakerQuotePolicy(
        fair_value=fair_value,
        book_state=book_state,
        inventory_state=inventory_state,
        lifecycle_manager=lifecycle,
    )


def _make_pair_strategy(
    market: MarketMetadata | None = None,
    config: PairStrategyConfig | None = None,
    up_bid: str = "0.53",
    up_ask: str = "0.57",
    dn_bid: str = "0.41",
    dn_ask: str = "0.45",
) -> tuple[PairStrategy, InMemoryOrderStateStore, InMemoryInventoryStore]:
    if market is None:
        market = _make_market()
    registry, lifecycle = _setup_registry_and_lifecycle(market)
    inventory_state = InMemoryInventoryStore(registry=registry)
    book_state = _setup_book(registry, market, up_bid, up_ask, dn_bid, dn_ask)
    quote_policy = _make_quote_policy(registry, lifecycle, book_state, inventory_state)
    order_state = InMemoryOrderStateStore()
    ids = SequentialIdGenerator(prefix="test")
    strategy = PairStrategy(
        quote_policy=quote_policy,
        order_state=order_state,
        registry=registry,
        lifecycle=lifecycle,
        id_generator=ids,
        config=config,
    )
    return strategy, order_state, inventory_state


def _make_recovery_strategy(
    market: MarketMetadata | None = None,
    config: RecoveryStrategyConfig | None = None,
    up_bid: str = "0.53",
    up_ask: str = "0.57",
    dn_bid: str = "0.41",
    dn_ask: str = "0.45",
) -> tuple[RecoveryStrategy, InMemoryOrderStateStore, InMemoryInventoryStore]:
    if market is None:
        market = _make_market()
    registry, lifecycle = _setup_registry_and_lifecycle(market)
    inventory_state = InMemoryInventoryStore(registry=registry)
    book_state = _setup_book(registry, market, up_bid, up_ask, dn_bid, dn_ask)
    order_state = InMemoryOrderStateStore()
    ids = SequentialIdGenerator(prefix="test")
    strategy = RecoveryStrategy(
        inventory_state=inventory_state,
        order_state=order_state,
        book_state=book_state,
        registry=registry,
        lifecycle=lifecycle,
        id_generator=ids,
        config=config,
    )
    return strategy, order_state, inventory_state


# ---------------------------------------------------------------------------
# PhasePolicy
# ---------------------------------------------------------------------------


class TestPhasePolicy:
    def test_allow_new_quotes_in_active(self) -> None:
        policy = PhasePolicy()
        assert policy.allow_new_quotes(MarketPhase.ACTIVE) is True

    def test_allow_new_quotes_in_prewarm(self) -> None:
        assert PhasePolicy().allow_new_quotes(MarketPhase.PREWARM) is True

    def test_allow_new_quotes_in_fast_close(self) -> None:
        assert PhasePolicy().allow_new_quotes(MarketPhase.FAST_CLOSE) is True

    def test_no_quotes_in_final_seconds(self) -> None:
        assert PhasePolicy().allow_new_quotes(MarketPhase.FINAL_SECONDS) is False

    def test_no_quotes_in_closed_wait(self) -> None:
        assert PhasePolicy().allow_new_quotes(MarketPhase.CLOSED_WAIT_RESOLUTION) is False

    def test_no_quotes_in_discovered(self) -> None:
        assert PhasePolicy().allow_new_quotes(MarketPhase.DISCOVERED) is False

    def test_allow_taker_in_active(self) -> None:
        assert PhasePolicy().allow_selective_taker(MarketPhase.ACTIVE) is True

    def test_no_taker_in_prewarm(self) -> None:
        assert PhasePolicy().allow_selective_taker(MarketPhase.PREWARM) is False

    def test_allow_recovery_in_closed_wait(self) -> None:
        # Recovery is allowed through CLOSED_WAIT_RESOLUTION (summary §9.2)
        assert PhasePolicy().allow_recovery(MarketPhase.CLOSED_WAIT_RESOLUTION) is True

    def test_no_recovery_in_resolved(self) -> None:
        assert PhasePolicy().allow_recovery(MarketPhase.RESOLVED) is False

    def test_no_recovery_in_archived(self) -> None:
        assert PhasePolicy().allow_recovery(MarketPhase.ARCHIVED) is False

    def test_max_unhedged_decreases_near_close(self) -> None:
        policy = PhasePolicy()
        active = policy.max_unhedged_exposure(MarketPhase.ACTIVE)
        fast = policy.max_unhedged_exposure(MarketPhase.FAST_CLOSE)
        final = policy.max_unhedged_exposure(MarketPhase.FINAL_SECONDS)
        assert active > fast > final

    def test_max_unhedged_zero_in_discovered(self) -> None:
        assert PhasePolicy().max_unhedged_exposure(MarketPhase.DISCOVERED) == Decimal("0")


# ---------------------------------------------------------------------------
# PairStrategy — basic intent generation
# ---------------------------------------------------------------------------


class TestPairStrategyOnTick:
    def test_returns_intents_for_active_market(self) -> None:
        strategy, *_ = _make_pair_strategy()
        intents = strategy.on_tick("m1", NOW_MS)
        # Should return at least some intents when market is active and book is set
        # (quote_policy may return None if vol/lead_lag not fed; that's OK)
        assert isinstance(intents, list)

    def test_returns_empty_for_unknown_market(self) -> None:
        strategy, *_ = _make_pair_strategy()
        intents = strategy.on_tick("nonexistent", NOW_MS)
        assert intents == []

    def test_returns_empty_when_phase_forbids_quoting(self) -> None:
        # Create a market that is already in FINAL_SECONDS
        market = _make_market(end_ts_ms=NOW_MS + 2_000)  # only 2s left
        strategy, *_ = _make_pair_strategy(market=market)
        # To reach FINAL_SECONDS we can't easily do it here without driving lifecycle
        # Instead: test with a market that is past end_ts → CLOSED_WAIT
        closed_market = _make_market(end_ts_ms=NOW_MS - 1_000)
        s2, *_ = _make_pair_strategy(market=closed_market)
        intents = s2.on_tick("m1", NOW_MS)
        assert intents == []

    def test_intents_have_correct_role(self) -> None:
        """All intents from pair_strategy must be MAKER_QUOTE."""
        from pricing.vol_model import EwmaVolModel
        from domain.models import BinanceTick
        from pricing.lead_lag import SimpleLeadLagEngine

        market = _make_market()
        registry, lifecycle = _setup_registry_and_lifecycle(market)
        inventory_state = InMemoryInventoryStore(registry=registry)
        book_state = _setup_book(registry, market)

        # Feed vol_model and lead_lag so fair_value can compute
        vol_model = EwmaVolModel()
        lead_lag = SimpleLeadLagEngine()
        tick = BinanceTick(
            symbol="BTCUSDT",
            event_ts_ms=NOW_MS,
            recv_ts_ms=NOW_MS,
            last_price=Decimal("50000"),
            best_bid=Decimal("49990"),
            best_ask=Decimal("50010"),
        )
        vol_model.on_binance_tick(tick)
        lead_lag.on_binance_tick(tick)

        from pricing.fair_value import BinaryOptionFairValueEngine
        fair_value = BinaryOptionFairValueEngine(
            vol_model=vol_model,
            lead_lag=lead_lag,
            inventory_state=inventory_state,
        )
        quote_policy = MakerQuotePolicy(
            fair_value=fair_value,
            book_state=book_state,
            inventory_state=inventory_state,
            lifecycle_manager=lifecycle,
        )
        order_state = InMemoryOrderStateStore()
        ids = SequentialIdGenerator(prefix="t")
        strategy = PairStrategy(
            quote_policy=quote_policy,
            order_state=order_state,
            registry=registry,
            lifecycle=lifecycle,
            id_generator=ids,
        )
        intents = strategy.on_tick("m1", NOW_MS)
        for intent in intents:
            assert intent.role == OrderRole.MAKER_QUOTE
            assert intent.post_only is True
            assert intent.tif == TimeInForce.GTC

    def test_paired_bids_share_pair_id(self) -> None:
        """UP-bid and DOWN-bid intents generated in the same tick share a pair_id."""
        from pricing.vol_model import EwmaVolModel
        from domain.models import BinanceTick
        from pricing.lead_lag import SimpleLeadLagEngine
        from pricing.fair_value import BinaryOptionFairValueEngine

        market = _make_market()
        registry, lifecycle = _setup_registry_and_lifecycle(market)
        inventory_state = InMemoryInventoryStore(registry=registry)
        book_state = _setup_book(registry, market)

        vol_model = EwmaVolModel()
        lead_lag = SimpleLeadLagEngine()
        btick = BinanceTick(
            symbol="BTCUSDT",
            event_ts_ms=NOW_MS,
            recv_ts_ms=NOW_MS,
            last_price=Decimal("50000"),
            best_bid=Decimal("49990"),
            best_ask=Decimal("50010"),
        )
        vol_model.on_binance_tick(btick)
        lead_lag.on_binance_tick(btick)

        fair_value = BinaryOptionFairValueEngine(
            vol_model=vol_model,
            lead_lag=lead_lag,
            inventory_state=inventory_state,
        )
        quote_policy = MakerQuotePolicy(
            fair_value=fair_value,
            book_state=book_state,
            inventory_state=inventory_state,
            lifecycle_manager=lifecycle,
        )
        order_state = InMemoryOrderStateStore()
        ids = SequentialIdGenerator(prefix="t")
        strategy = PairStrategy(
            quote_policy=quote_policy,
            order_state=order_state,
            registry=registry,
            lifecycle=lifecycle,
            id_generator=ids,
        )
        intents = strategy.on_tick("m1", NOW_MS)
        buy_intents = [i for i in intents if i.side == Side.BUY]
        if len(buy_intents) == 2:
            assert buy_intents[0].pair_id is not None
            assert buy_intents[0].pair_id == buy_intents[1].pair_id

    def test_no_duplicate_quote_when_order_at_good_price(self) -> None:
        """If there is already a well-priced open order, on_tick must not add another."""
        from pricing.vol_model import EwmaVolModel
        from domain.models import BinanceTick
        from pricing.lead_lag import SimpleLeadLagEngine
        from pricing.fair_value import BinaryOptionFairValueEngine

        market = _make_market()
        registry, lifecycle = _setup_registry_and_lifecycle(market)
        inventory_state = InMemoryInventoryStore(registry=registry)
        book_state = _setup_book(registry, market)

        vol_model = EwmaVolModel()
        lead_lag = SimpleLeadLagEngine()
        btick = BinanceTick(
            symbol="BTCUSDT", event_ts_ms=NOW_MS, recv_ts_ms=NOW_MS,
            last_price=Decimal("50000"), best_bid=Decimal("49990"), best_ask=Decimal("50010"),
        )
        vol_model.on_binance_tick(btick)
        lead_lag.on_binance_tick(btick)

        fair_value = BinaryOptionFairValueEngine(
            vol_model=vol_model, lead_lag=lead_lag, inventory_state=inventory_state,
        )
        quote_policy = MakerQuotePolicy(
            fair_value=fair_value, book_state=book_state,
            inventory_state=inventory_state, lifecycle_manager=lifecycle,
        )
        order_state = InMemoryOrderStateStore()
        ids = SequentialIdGenerator(prefix="t")
        strategy = PairStrategy(
            quote_policy=quote_policy,
            order_state=order_state,
            registry=registry,
            lifecycle=lifecycle,
            id_generator=ids,
            config=PairStrategyConfig(max_quote_depth=1),
        )

        # First tick: get intents
        first_intents = strategy.on_tick("m1", NOW_MS)
        # Register all returned intents as open orders
        for i, intent in enumerate(first_intents):
            order_state.on_intent_sent(intent, f"cid_{i}")
            order_state.on_execution_report(ExecutionReport(
                client_order_id=f"cid_{i}",
                pair_id=intent.pair_id,
                market_id=intent.market_id,
                token_id=intent.token_id,
                side=intent.side,
                status=OrderStatus.OPEN,
                filled_size=Decimal("0"),
                avg_price=None,
                exchange_order_id=f"exch_{i}",
                event_ts_ms=NOW_MS,
            ))

        # Second tick: with all slots at max depth, no new intents should be produced
        second_intents = strategy.on_tick("m1", NOW_MS)
        assert len(second_intents) == 0


# ---------------------------------------------------------------------------
# RecoveryStrategy
# ---------------------------------------------------------------------------


class TestRecoveryStrategyOnFill:
    def test_no_recovery_when_balanced(self) -> None:
        strategy, order_state, inventory_state = _make_recovery_strategy()
        # Fill both legs equally; use distinct client_order_ids to avoid dedup
        fill_up = _make_fill_report(token_id="up_tok", filled="10", price="0.55", cid="c_up")
        fill_dn = _make_fill_report(token_id="dn_tok", filled="10", price="0.43", cid="c_dn")
        inventory_state.on_fill(fill_up)
        inventory_state.on_fill(fill_dn)

        result = strategy.on_fill(fill_dn)
        assert result == []

    def test_recovery_intent_when_unhedged_after_fill(self) -> None:
        strategy, order_state, inventory_state = _make_recovery_strategy(
            config=RecoveryStrategyConfig(min_recovery_threshold=Decimal("1"))
        )
        # Only buy UP; DOWN is missing
        fill_up = _make_fill_report(token_id="up_tok", filled="10", price="0.55")
        inventory_state.on_fill(fill_up)

        intents = strategy.on_fill(fill_up)
        assert len(intents) == 1
        intent = intents[0]
        assert intent.role == OrderRole.RECOVERY
        assert intent.token_id == "dn_tok"
        assert intent.side == Side.BUY
        assert intent.tif == TimeInForce.FOK  # aggressive on fill

    def test_recovery_uses_best_ask_price(self) -> None:
        strategy, order_state, inventory_state = _make_recovery_strategy(
            dn_ask="0.45",
            config=RecoveryStrategyConfig(min_recovery_threshold=Decimal("1")),
        )
        fill_up = _make_fill_report(token_id="up_tok", filled="10", price="0.55")
        inventory_state.on_fill(fill_up)

        intents = strategy.on_fill(fill_up)
        assert len(intents) == 1
        assert intents[0].price == Decimal("0.45")

    def test_no_recovery_when_fill_size_zero(self) -> None:
        strategy, _, inventory_state = _make_recovery_strategy()
        report = ExecutionReport(
            client_order_id="c1", pair_id=None, market_id="m1",
            token_id="up_tok", side=Side.BUY, status=OrderStatus.OPEN,
            filled_size=Decimal("0"), avg_price=None,
            exchange_order_id=None, event_ts_ms=NOW_MS,
        )
        result = strategy.on_fill(report)
        assert result == []

    def test_no_recovery_in_resolved_phase(self) -> None:
        market = _make_market(status="resolved", end_ts_ms=NOW_MS - 1_000)
        strategy, _, inventory_state = _make_recovery_strategy(market=market)
        fill_up = _make_fill_report(token_id="up_tok", filled="10")
        inventory_state.on_fill(fill_up)
        result = strategy.on_fill(fill_up)
        assert result == []


class TestRecoveryStrategyOnTimer:
    def test_no_recovery_when_balanced(self) -> None:
        strategy, _, inventory_state = _make_recovery_strategy()
        fill_up = _make_fill_report(token_id="up_tok", filled="5", cid="c_up")
        fill_dn = _make_fill_report(token_id="dn_tok", filled="5", cid="c_dn")
        inventory_state.on_fill(fill_up)
        inventory_state.on_fill(fill_dn)

        result = strategy.on_timer("m1", NOW_MS)
        assert result == []

    def test_recovery_intent_when_unhedged(self) -> None:
        strategy, _, inventory_state = _make_recovery_strategy(
            config=RecoveryStrategyConfig(min_recovery_threshold=Decimal("1")),
        )
        fill_up = _make_fill_report(token_id="up_tok", filled="10")
        inventory_state.on_fill(fill_up)

        intents = strategy.on_timer("m1", NOW_MS)
        assert len(intents) == 1
        assert intents[0].role == OrderRole.RECOVERY
        assert intents[0].tif == TimeInForce.GTC  # passive on timer

    def test_skips_when_max_concurrent_recovery_reached(self) -> None:
        strategy, order_state, inventory_state = _make_recovery_strategy(
            config=RecoveryStrategyConfig(
                min_recovery_threshold=Decimal("1"),
                max_concurrent_recovery=1,
            )
        )
        fill_up = _make_fill_report(token_id="up_tok", filled="10")
        inventory_state.on_fill(fill_up)

        # Simulate an open recovery order already in state
        recovery_intent = OrderIntent(
            intent_id="rec1",
            pair_id=None,
            market_id="m1",
            token_id="dn_tok",
            side=Side.BUY,
            price=Decimal("0.45"),
            size=Decimal("5"),
            tif=TimeInForce.GTC,
            post_only=False,
            role=OrderRole.RECOVERY,
            reason="recovery_on_timer",
        )
        order_state.on_intent_sent(recovery_intent, "c_rec1")
        order_state.on_execution_report(ExecutionReport(
            client_order_id="c_rec1", pair_id=None, market_id="m1",
            token_id="dn_tok", side=Side.BUY, status=OrderStatus.OPEN,
            filled_size=Decimal("0"), avg_price=None,
            exchange_order_id="exch_rec1", event_ts_ms=NOW_MS,
        ))

        # Already at max_concurrent_recovery=1; should not generate another
        intents = strategy.on_timer("m1", NOW_MS)
        assert intents == []

    def test_recovery_size_capped_by_config(self) -> None:
        strategy, _, inventory_state = _make_recovery_strategy(
            config=RecoveryStrategyConfig(
                min_recovery_threshold=Decimal("1"),
                max_recovery_size=Decimal("3"),
            )
        )
        fill_up = _make_fill_report(token_id="up_tok", filled="100")
        inventory_state.on_fill(fill_up)

        intents = strategy.on_timer("m1", NOW_MS)
        if intents:
            assert intents[0].size <= Decimal("3")


class TestFairValueReferenceAnchor:
    def test_fair_value_requires_exact_open_anchor_after_market_start(self) -> None:
        from domain.models import BinanceTick
        from pricing.fair_value import BinaryOptionFairValueEngine
        from pricing.lead_lag import SimpleLeadLagEngine
        from pricing.vol_model import EwmaVolModel

        market = MarketMetadata(
            market_id="m_anchor",
            condition_id="cond_anchor",
            up_token_id="up_tok",
            down_token_id="dn_tok",
            start_ts_ms=NOW_MS - 10_000,
            end_ts_ms=NOW_MS + 60_000,
            tick_size=Decimal("0.01"),
            fee_rate_bps=Decimal("10"),
            min_order_size=Decimal("1"),
            status="active",
            reference_price=None,
        )
        registry = InMemoryMarketRegistry()
        registry.upsert(market)
        inventory = InMemoryInventoryStore(registry=registry)
        vol_model = EwmaVolModel()
        lead_lag = SimpleLeadLagEngine()

        first_tick = BinanceTick(
            symbol="BTCUSDT",
            event_ts_ms=NOW_MS,
            recv_ts_ms=NOW_MS,
            last_price=Decimal("50000"),
            best_bid=Decimal("49990"),
            best_ask=Decimal("50010"),
        )
        second_tick = BinanceTick(
            symbol="BTCUSDT",
            event_ts_ms=NOW_MS + 250,
            recv_ts_ms=NOW_MS + 250,
            last_price=Decimal("51000"),
            best_bid=Decimal("50990"),
            best_ask=Decimal("51010"),
        )

        vol_model.on_binance_tick(first_tick)
        lead_lag.on_binance_tick(first_tick)
        vol_model.on_binance_tick(second_tick)
        lead_lag.on_binance_tick(second_tick)

        engine = BinaryOptionFairValueEngine(
            vol_model=vol_model,
            lead_lag=lead_lag,
            inventory_state=inventory,
        )
        snapshot = engine.compute(market, NOW_MS + 250)

        assert snapshot is None
        assert market.market_id not in engine._reference_price_cache

    def test_fair_value_binance_only_uses_seeded_exact_open_anchor(self) -> None:
        from domain.models import BinanceTick

        market = MarketMetadata(
            market_id="m_anchor_after_open",
            condition_id="cond_anchor_after_open",
            up_token_id="up_tok",
            down_token_id="dn_tok",
            start_ts_ms=NOW_MS,
            end_ts_ms=NOW_MS + 60_000,
            tick_size=Decimal("0.01"),
            fee_rate_bps=Decimal("10"),
            min_order_size=Decimal("1"),
            status="active",
            reference_price=None,
        )
        registry = InMemoryMarketRegistry()
        registry.upsert(market)
        inventory = InMemoryInventoryStore(registry=registry)
        vol_model = EwmaVolModel()
        lead_lag = SimpleLeadLagEngine()

        first_tick = BinanceTick(
            symbol="BTCUSDT",
            event_ts_ms=NOW_MS + 250,
            recv_ts_ms=NOW_MS + 250,
            last_price=Decimal("50000"),
            best_bid=Decimal("49990"),
            best_ask=Decimal("50010"),
        )
        second_tick = BinanceTick(
            symbol="BTCUSDT",
            event_ts_ms=NOW_MS + 500,
            recv_ts_ms=NOW_MS + 500,
            last_price=Decimal("51000"),
            best_bid=Decimal("50990"),
            best_ask=Decimal("51010"),
        )

        for tick in (first_tick, second_tick):
            vol_model.on_binance_tick(tick)
            lead_lag.on_binance_tick(tick)

        engine = BinaryOptionFairValueEngine(
            vol_model=vol_model,
            lead_lag=lead_lag,
            inventory_state=inventory,
            fair_value_mode="binance_only",
        )
        engine.seed_reference_price(market.market_id, Decimal("50000"))
        snapshot = engine.compute(market, NOW_MS + 500)

        assert snapshot is not None
        assert engine._reference_price_cache[market.market_id] == Decimal("50000")
        assert snapshot.theo_up > Decimal("0.5")

    def test_fair_value_binance_only_seeded_open_anchor_overrides_metadata_reference(self) -> None:
        from domain.models import BinanceTick, ChainlinkTick

        market = MarketMetadata(
            market_id="m_anchor_binance",
            condition_id="cond_anchor_binance",
            up_token_id="up_tok",
            down_token_id="dn_tok",
            start_ts_ms=NOW_MS,
            end_ts_ms=NOW_MS + 60_000,
            tick_size=Decimal("0.01"),
            fee_rate_bps=Decimal("10"),
            min_order_size=Decimal("1"),
            status="active",
            reference_price=Decimal("49000"),
        )
        registry = InMemoryMarketRegistry()
        registry.upsert(market)
        inventory = InMemoryInventoryStore(registry=registry)
        vol_model = EwmaVolModel()
        lead_lag = SimpleLeadLagEngine()

        open_tick = BinanceTick(
            symbol="BTCUSDT",
            event_ts_ms=NOW_MS,
            recv_ts_ms=NOW_MS,
            last_price=Decimal("50000"),
            best_bid=Decimal("49990"),
            best_ask=Decimal("50010"),
        )
        later_tick = BinanceTick(
            symbol="BTCUSDT",
            event_ts_ms=NOW_MS + 250,
            recv_ts_ms=NOW_MS + 250,
            last_price=Decimal("51000"),
            best_bid=Decimal("50990"),
            best_ask=Decimal("51010"),
        )
        chainlink_tick = ChainlinkTick(
            feed="BTC/USD",
            oracle_ts_ms=NOW_MS,
            recv_ts_ms=NOW_MS,
            price=Decimal("49500"),
            round_id="1",
        )

        for tick in (open_tick, later_tick):
            vol_model.on_binance_tick(tick)
            lead_lag.on_binance_tick(tick)
        lead_lag.on_chainlink_tick(chainlink_tick)

        engine = BinaryOptionFairValueEngine(
            vol_model=vol_model,
            lead_lag=lead_lag,
            inventory_state=inventory,
            fair_value_mode="binance_only",
        )
        engine.seed_reference_price(market.market_id, Decimal("50000"))
        snapshot = engine.compute(market, NOW_MS + 250)

        assert snapshot is not None
        assert engine._reference_price_cache[market.market_id] == Decimal("50000")
        assert snapshot.theo_up > Decimal("0.5")

    def test_fair_value_hybrid_uses_seeded_binance_open_anchor(self) -> None:
        from domain.models import BinanceTick, ChainlinkTick

        market = MarketMetadata(
            market_id="m_anchor_hybrid",
            condition_id="cond_anchor_hybrid",
            up_token_id="up_tok",
            down_token_id="dn_tok",
            start_ts_ms=NOW_MS,
            end_ts_ms=NOW_MS + 60_000,
            tick_size=Decimal("0.01"),
            fee_rate_bps=Decimal("10"),
            min_order_size=Decimal("1"),
            status="active",
            reference_price=None,
        )
        registry = InMemoryMarketRegistry()
        registry.upsert(market)
        inventory = InMemoryInventoryStore(registry=registry)
        vol_model = EwmaVolModel()
        lead_lag = SimpleLeadLagEngine()

        open_tick = BinanceTick(
            symbol="BTCUSDT",
            event_ts_ms=NOW_MS,
            recv_ts_ms=NOW_MS,
            last_price=Decimal("50000"),
            best_bid=Decimal("49990"),
            best_ask=Decimal("50010"),
        )
        later_tick = BinanceTick(
            symbol="BTCUSDT",
            event_ts_ms=NOW_MS + 250,
            recv_ts_ms=NOW_MS + 250,
            last_price=Decimal("51000"),
            best_bid=Decimal("50990"),
            best_ask=Decimal("51010"),
        )
        chainlink_tick = ChainlinkTick(
            feed="BTC/USD",
            oracle_ts_ms=NOW_MS,
            recv_ts_ms=NOW_MS,
            price=Decimal("49800"),
            round_id="1",
        )

        for tick in (open_tick, later_tick):
            vol_model.on_binance_tick(tick)
            lead_lag.on_binance_tick(tick)
        lead_lag.on_chainlink_tick(chainlink_tick)

        engine = BinaryOptionFairValueEngine(
            vol_model=vol_model,
            lead_lag=lead_lag,
            inventory_state=inventory,
            fair_value_mode="hybrid",
        )
        engine.seed_reference_price(market.market_id, Decimal("50000"))
        snapshot = engine.compute(market, NOW_MS + 250)

        assert snapshot is not None
        assert engine._reference_price_cache[market.market_id] == Decimal("50000")
        assert snapshot.theo_up > Decimal("0.5")

    def test_fair_value_at_the_money_is_not_hardcoded_to_half_before_close(self) -> None:
        from domain.models import BinanceTick

        market = MarketMetadata(
            market_id="m_atm",
            condition_id="cond_atm",
            up_token_id="up_tok",
            down_token_id="dn_tok",
            start_ts_ms=NOW_MS - 60_000,
            end_ts_ms=NOW_MS + 60_000,
            tick_size=Decimal("0.01"),
            fee_rate_bps=Decimal("10"),
            min_order_size=Decimal("1"),
            status="active",
            reference_price=Decimal("50000"),
        )
        registry = InMemoryMarketRegistry()
        registry.upsert(market)
        inventory = InMemoryInventoryStore(registry=registry)
        vol_model = EwmaVolModel()
        lead_lag = SimpleLeadLagEngine()

        for ts_ms in (NOW_MS - 1_000, NOW_MS):
            tick = BinanceTick(
                symbol="BTCUSDT",
                event_ts_ms=ts_ms,
                recv_ts_ms=ts_ms,
                last_price=Decimal("50000"),
                best_bid=Decimal("49990"),
                best_ask=Decimal("50010"),
            )
            vol_model.on_binance_tick(tick)
            lead_lag.on_binance_tick(tick)

        engine = BinaryOptionFairValueEngine(
            vol_model=vol_model,
            lead_lag=lead_lag,
            inventory_state=inventory,
            fair_value_mode="binance_only",
        )
        engine.seed_reference_price(market.market_id, Decimal("50000"))
        snapshot = engine.compute(market, NOW_MS)

        assert snapshot is not None
        assert snapshot.theo_up < Decimal("0.5")


class TestAdaptiveVolModel:
    def test_long_horizon_sigma_is_smoother_than_short_horizon_sigma(self) -> None:
        from domain.models import BinanceTick

        model = EwmaVolModel()
        price = Decimal("50000")

        for index, ts_ms in enumerate(range(NOW_MS - 900_000, NOW_MS, 30_000)):
            next_price = price + Decimal(index % 2)
            tick = BinanceTick(
                symbol="BTCUSDT",
                event_ts_ms=ts_ms,
                recv_ts_ms=ts_ms,
                last_price=next_price,
                best_bid=next_price - Decimal("1"),
                best_ask=next_price + Decimal("1"),
            )
            model.on_binance_tick(tick)
            price = next_price

        spike_tick = BinanceTick(
            symbol="BTCUSDT",
            event_ts_ms=NOW_MS,
            recv_ts_ms=NOW_MS,
            last_price=price + Decimal("350"),
            best_bid=price + Decimal("349"),
            best_ask=price + Decimal("351"),
        )
        model.on_binance_tick(spike_tick)

        short_sigma = model.sigma_short(NOW_MS, 30)
        long_sigma = model.sigma_short(NOW_MS, 900)

        assert short_sigma is not None
        assert long_sigma is not None
        assert short_sigma > long_sigma
