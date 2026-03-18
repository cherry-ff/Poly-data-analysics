from __future__ import annotations

from dataclasses import dataclass

from app.config import RuntimeConfig
from core.clock import WallClock
from core.event_bus import InMemoryEventBus
from core.ids import SequentialIdGenerator
from observability.alerts import LoggingAlerts
from observability.metrics import InMemoryMetrics
from execution.cancel_manager import CancelManager
from execution.order_builder import OrderBuilder
from execution.polymarket_gateway import PolymarketGateway
from execution.router import ExecutionRouter
from feeds.binance_ws import BinanceBookTickerFeed, BinanceDepthFeed
from feeds.chainlink_feed import ChainlinkPollingFeed
from feeds.polymarket_market_ws import PolymarketMarketFeed
from feeds.polymarket_user_ws import PolymarketUserFeed
from market.lifecycle import LifecycleManager
from market.metadata_loader import MarketMetadataLoader
from market.registry import InMemoryMarketRegistry
from pricing.binance_open_price import BinanceMinuteOpenPriceService
from pricing.fair_value import BinaryOptionFairValueEngine
from pricing.lead_lag import SimpleLeadLagEngine
from pricing.quote_policy import MakerQuotePolicy
from pricing.vol_model import EwmaVolModel
from risk.engine import RiskEngine
from state.book_state import InMemoryBookStateStore
from state.inventory_state import InMemoryInventoryStore
from state.order_state import InMemoryOrderStateStore
from storage.recorder import AsyncRecorder
from storage.database_writer import ThreadedDatabaseWriter
from storage.snapshot_writer import SnapshotWriter
from strategy.pair_strategy import PairStrategy
from strategy.phase_policy import PhasePolicy
from strategy.recovery_strategy import RecoveryStrategy


@dataclass(slots=True)
class FeedRuntime:
    binance: BinanceBookTickerFeed
    binance_depth: BinanceDepthFeed
    chainlink: ChainlinkPollingFeed
    polymarket_market: PolymarketMarketFeed
    polymarket_user: PolymarketUserFeed


@dataclass(slots=True)
class PricingRuntime:
    vol_model: EwmaVolModel
    lead_lag: SimpleLeadLagEngine
    binance_open_price: BinanceMinuteOpenPriceService
    fair_value: BinaryOptionFairValueEngine
    quote_policy: MakerQuotePolicy


@dataclass(slots=True)
class StrategyRuntime:
    phase_policy: PhasePolicy
    pair_strategy: PairStrategy
    recovery_strategy: RecoveryStrategy


@dataclass(slots=True)
class RiskRuntime:
    engine: RiskEngine


@dataclass(slots=True)
class ExecutionRuntime:
    gateway: PolymarketGateway
    order_builder: OrderBuilder
    router: ExecutionRouter
    cancel_manager: CancelManager


@dataclass(slots=True)
class StorageRuntime:
    recorder: AsyncRecorder | None
    db_writer: ThreadedDatabaseWriter | None
    snapshot_writer: SnapshotWriter | None


@dataclass(slots=True)
class ObservabilityRuntime:
    metrics: InMemoryMetrics
    alerts: LoggingAlerts


@dataclass(slots=True)
class RuntimeContext:
    config: RuntimeConfig
    clock: WallClock
    event_bus: InMemoryEventBus
    id_generator: SequentialIdGenerator
    market_registry: InMemoryMarketRegistry
    lifecycle_manager: LifecycleManager
    book_state: InMemoryBookStateStore
    order_state: InMemoryOrderStateStore
    inventory_state: InMemoryInventoryStore
    metadata_loader: MarketMetadataLoader
    feeds: FeedRuntime
    pricing: PricingRuntime
    strategy: StrategyRuntime
    risk: RiskRuntime
    execution: ExecutionRuntime
    storage: StorageRuntime
    observability: ObservabilityRuntime


class AppBootstrapper:
    def __init__(self, config: RuntimeConfig | None = None) -> None:
        self._config = config or RuntimeConfig.from_env()
        self._context: RuntimeContext | None = None

    async def build(self) -> RuntimeContext:
        proxy_url = self._config.proxy.url if self._config.proxy.enabled else None
        clock = WallClock()
        event_bus = InMemoryEventBus()
        id_generator = SequentialIdGenerator()
        market_registry = InMemoryMarketRegistry()
        lifecycle_manager = LifecycleManager(
            registry=market_registry,
            config=self._config.lifecycle,
        )
        metadata_loader = MarketMetadataLoader(
            gamma_base_url=self._config.metadata.gamma_base_url,
            cache_ttl_ms=self._config.metadata.cache_ttl_ms,
            request_timeout_ms=self._config.metadata.request_timeout_ms,
            return_stale_on_error=self._config.metadata.return_stale_on_error,
            proxy_url=proxy_url,
            market_filter_enabled=True,
            filter_keywords=self._config.metadata.discovery_keywords,
            filter_exclude_keywords=self._config.metadata.discovery_exclude_keywords,
            filter_min_duration_minutes=self._config.metadata.discovery_min_duration_minutes,
            filter_max_duration_minutes=self._config.metadata.discovery_max_duration_minutes,
        )
        book_state = InMemoryBookStateStore(registry=market_registry)
        order_state = InMemoryOrderStateStore()
        inventory_state = InMemoryInventoryStore(registry=market_registry)
        vol_model = EwmaVolModel(
            half_life_ms=self._config.pricing.vol_half_life_ms,
            max_age_ms=self._config.pricing.vol_max_age_ms,
            stale_after_ms=self._config.pricing.vol_stale_after_ms,
            min_sigma=self._config.pricing.vol_min_sigma,
            max_sigma=self._config.pricing.vol_max_sigma,
        )
        lead_lag = SimpleLeadLagEngine()
        binance_open_price = BinanceMinuteOpenPriceService(
            symbol=self._config.binance.symbol,
            rest_base_url=self._config.binance.rest_base_url,
            request_timeout_ms=self._config.binance.rest_request_timeout_ms,
            proxy_url=proxy_url,
        )
        fair_value = BinaryOptionFairValueEngine(
            vol_model=vol_model,
            lead_lag=lead_lag,
            inventory_state=inventory_state,
            fair_value_mode=self._config.pricing.fair_value_mode,
            min_probability=self._config.pricing.min_probability,
            full_set_buffer=self._config.pricing.full_set_buffer,
        )
        pricing = PricingRuntime(
            vol_model=vol_model,
            lead_lag=lead_lag,
            binance_open_price=binance_open_price,
            fair_value=fair_value,
            quote_policy=MakerQuotePolicy(
                fair_value=fair_value,
                book_state=book_state,
                inventory_state=inventory_state,
                lifecycle_manager=lifecycle_manager,
            ),
        )
        risk = RiskRuntime(
            engine=RiskEngine(
                registry=market_registry,
                lifecycle=lifecycle_manager,
                order_state=order_state,
                inventory_state=inventory_state,
            )
        )
        gateway = PolymarketGateway(
            base_url=self._config.execution.gateway_base_url,
            api_key=self._config.execution.api_key,
            api_secret=self._config.execution.api_secret,
            passphrase=self._config.execution.passphrase,
            dry_run=self._config.execution.gateway_dry_run,
            chain_id=self._config.execution.chain_id,
            signature_type=self._config.execution.signature_type,
            private_key=self._config.execution.private_key,
            funder_address=self._config.execution.funder_address,
        )
        order_builder = OrderBuilder()
        execution = ExecutionRuntime(
            gateway=gateway,
            order_builder=order_builder,
            router=ExecutionRouter(
                risk_engine=risk.engine,
                order_builder=order_builder,
                gateway=gateway,
                order_state=order_state,
                registry=market_registry,
                id_generator=id_generator,
            ),
            cancel_manager=CancelManager(
                order_state=order_state,
                gateway=gateway,
                max_maker_quote_age_ms=self._config.execution.cancel_stale_quotes_ms,
            ),
        )
        storage = StorageRuntime(
            recorder=AsyncRecorder(
                output_dir=self._config.storage.recorder_output_dir,
                max_queue_size=self._config.storage.recorder_max_queue_size,
                flush_interval_ms=self._config.storage.recorder_flush_interval_ms,
                flush_batch_size=self._config.storage.recorder_flush_batch_size,
                rotate_interval_ms=self._config.storage.recorder_rotate_interval_ms,
                rotate_max_file_size_bytes=(
                    self._config.storage.recorder_rotate_max_file_size_bytes
                ),
            )
            if self._config.storage.recorder_enabled
            else None,
            db_writer=ThreadedDatabaseWriter(
                db_path=self._config.storage.db_path,
                max_queue_size=self._config.storage.db_max_queue_size,
                flush_interval_ms=self._config.storage.db_flush_interval_ms,
                flush_batch_size=self._config.storage.db_flush_batch_size,
            )
            if self._config.storage.db_enabled
            else None,
            snapshot_writer=SnapshotWriter(
                output_dir=self._config.storage.snapshot_output_dir,
                registry=market_registry,
                book_state=book_state,
                order_state=order_state,
                inventory_state=inventory_state,
            )
            if self._config.storage.snapshot_enabled
            else None,
        )
        observability = ObservabilityRuntime(
            metrics=InMemoryMetrics(),
            alerts=LoggingAlerts(
                max_history=self._config.observability.alerts_max_history,
            ),
        )
        phase_policy = PhasePolicy()
        strategy = StrategyRuntime(
            phase_policy=phase_policy,
            pair_strategy=PairStrategy(
                quote_policy=pricing.quote_policy,
                order_state=order_state,
                registry=market_registry,
                lifecycle=lifecycle_manager,
                id_generator=id_generator,
                phase_policy=phase_policy,
            ),
            recovery_strategy=RecoveryStrategy(
                id_generator=id_generator,
                inventory_state=inventory_state,
                order_state=order_state,
                book_state=book_state,
                registry=market_registry,
                lifecycle=lifecycle_manager,
                phase_policy=phase_policy,
            ),
        )
        feeds = FeedRuntime(
            binance=BinanceBookTickerFeed(
                config=self._config.binance,
                event_bus=event_bus,
                clock=clock,
                proxy_url=proxy_url,
            ),
            binance_depth=BinanceDepthFeed(
                config=self._config.binance,
                event_bus=event_bus,
                clock=clock,
                proxy_url=proxy_url,
            ),
            chainlink=ChainlinkPollingFeed(
                config=self._config.chainlink,
                event_bus=event_bus,
                clock=clock,
                proxy_url=proxy_url,
            ),
            polymarket_market=PolymarketMarketFeed(
                config=self._config.polymarket,
                event_bus=event_bus,
                clock=clock,
                proxy_url=proxy_url,
            ),
            polymarket_user=PolymarketUserFeed(
                config=self._config.polymarket,
                event_bus=event_bus,
                clock=clock,
                proxy_url=proxy_url,
            ),
        )

        self._context = RuntimeContext(
            config=self._config,
            clock=clock,
            event_bus=event_bus,
            id_generator=id_generator,
            market_registry=market_registry,
            lifecycle_manager=lifecycle_manager,
            book_state=book_state,
            order_state=order_state,
            inventory_state=inventory_state,
            metadata_loader=metadata_loader,
            feeds=feeds,
            pricing=pricing,
            strategy=strategy,
            risk=risk,
            execution=execution,
            storage=storage,
            observability=observability,
        )
        return self._context

    async def start(self) -> RuntimeContext:
        if self._context is None:
            return await self.build()
        return self._context

    async def stop(self) -> None:
        if self._context is not None:
            await self._context.feeds.binance.close()
            await self._context.feeds.binance_depth.close()
            await self._context.feeds.chainlink.close()
            await self._context.feeds.polymarket_market.close()
            await self._context.feeds.polymarket_user.close()
        self._context = None
