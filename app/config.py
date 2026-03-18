from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from os import getenv

from app.env import load_project_env
from security.private_key import resolve_private_key_from_env


def _env_bool(name: str, default: str = "0") -> bool:
    return getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _env_csv(name: str, default: str = "") -> tuple[str, ...]:
    raw = getenv(name, default).strip()
    if not raw:
        return ()
    return tuple(item.strip() for item in raw.split(",") if item.strip())


@dataclass(slots=True)
class LifecycleConfig:
    prewarm_ms: int = 60_000
    fast_close_ms: int = 30_000
    final_seconds_ms: int = 5_000

    def __post_init__(self) -> None:
        if self.prewarm_ms <= 0:
            raise ValueError("prewarm_ms must be positive")
        if self.fast_close_ms <= 0:
            raise ValueError("fast_close_ms must be positive")
        if self.final_seconds_ms <= 0:
            raise ValueError("final_seconds_ms must be positive")
        if self.final_seconds_ms > self.fast_close_ms:
            raise ValueError("final_seconds_ms must be <= fast_close_ms")


@dataclass(slots=True)
class BinanceFeedConfig:
    enabled: bool = False
    symbol: str = "BTCUSDT"
    ws_url: str = "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"
    depth_enabled: bool = True
    depth_ws_url: str = "wss://stream.binance.com:9443/ws/btcusdt@depth5@100ms"
    rest_base_url: str = "https://api.binance.com"
    rest_request_timeout_ms: int = 3000
    connect_timeout_ms: int = 5000
    idle_timeout_ms: int = 5000
    retry_initial_delay_ms: int = 500
    retry_max_delay_ms: int = 5000
    retry_backoff: float = 2.0

    def __post_init__(self) -> None:
        if self.rest_request_timeout_ms <= 0:
            raise ValueError("rest_request_timeout_ms must be positive")
        if self.connect_timeout_ms <= 0:
            raise ValueError("connect_timeout_ms must be positive")
        if self.idle_timeout_ms < 0:
            raise ValueError("idle_timeout_ms must be non-negative")
        if self.retry_initial_delay_ms <= 0:
            raise ValueError("retry_initial_delay_ms must be positive")
        if self.retry_max_delay_ms < self.retry_initial_delay_ms:
            raise ValueError("retry_max_delay_ms must be >= retry_initial_delay_ms")
        if self.retry_backoff < 1:
            raise ValueError("retry_backoff must be >= 1")


@dataclass(slots=True)
class ChainlinkFeedConfig:
    enabled: bool = False
    feed_name: str = "BTC/USD"
    endpoint: str = ""
    api_url: str = "https://data.chain.link/api/query-timescale"
    query_name: str = "LIVE_STREAM_REPORTS_QUERY"
    feed_id: str = "0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8"
    price_scale: str = "1e18"
    poll_interval_ms: int = 10000
    request_timeout_ms: int = 5000
    stale_after_ms: int = 20000
    silent_reconnect_after_ms: int = 45000
    retry_initial_delay_ms: int = 500
    retry_max_delay_ms: int = 5000
    retry_backoff: float = 2.0

    def __post_init__(self) -> None:
        if self.poll_interval_ms <= 0:
            raise ValueError("poll_interval_ms must be positive")
        if self.request_timeout_ms <= 0:
            raise ValueError("request_timeout_ms must be positive")
        if self.stale_after_ms < 0:
            raise ValueError("stale_after_ms must be non-negative")
        if self.silent_reconnect_after_ms < 0:
            raise ValueError("silent_reconnect_after_ms must be non-negative")
        if self.retry_initial_delay_ms <= 0:
            raise ValueError("retry_initial_delay_ms must be positive")
        if self.retry_max_delay_ms < self.retry_initial_delay_ms:
            raise ValueError("retry_max_delay_ms must be >= retry_initial_delay_ms")
        if self.retry_backoff < 1:
            raise ValueError("retry_backoff must be >= 1")


@dataclass(slots=True)
class PolymarketFeedConfig:
    market_enabled: bool = False
    market_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    market_assets_ids: tuple[str, ...] = ()
    market_custom_features_enabled: bool = True
    market_event_types: tuple[str, ...] = (
        "book",
        "price_change",
        "tick_size_change",
        "market_resolved",
    )
    market_include_new_markets: bool = True
    market_initial_dump: bool = False
    user_enabled: bool = False
    user_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    user_market_ids: tuple[str, ...] = ()
    api_key: str = ""
    api_secret: str = ""
    passphrase: str = ""
    heartbeat_interval_ms: int = 10_000
    connect_timeout_ms: int = 5000
    market_idle_timeout_ms: int = 40_000
    user_idle_timeout_ms: int = 0
    retry_initial_delay_ms: int = 500
    retry_max_delay_ms: int = 5000
    retry_backoff: float = 2.0

    def __post_init__(self) -> None:
        if self.heartbeat_interval_ms <= 0:
            raise ValueError("heartbeat_interval_ms must be positive")
        if self.connect_timeout_ms <= 0:
            raise ValueError("connect_timeout_ms must be positive")
        if self.market_idle_timeout_ms < 0:
            raise ValueError("market_idle_timeout_ms must be non-negative")
        if self.user_idle_timeout_ms < 0:
            raise ValueError("user_idle_timeout_ms must be non-negative")
        if self.retry_initial_delay_ms <= 0:
            raise ValueError("retry_initial_delay_ms must be positive")
        if self.retry_max_delay_ms < self.retry_initial_delay_ms:
            raise ValueError("retry_max_delay_ms must be >= retry_initial_delay_ms")
        if self.retry_backoff < 1:
            raise ValueError("retry_backoff must be >= 1")


@dataclass(slots=True)
class MetadataConfig:
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    discovery_enabled: bool = True
    discovery_interval_ms: int = 5_000
    discovery_max_markets: int = 3
    discovery_page_limit: int = 100
    discovery_max_pages: int = 3
    discovery_tag_slug: str = "bitcoin"
    discovery_keywords: tuple[str, ...] = ("btc", "bitcoin")
    discovery_exclude_keywords: tuple[str, ...] = (
        "weekly",
        "daily",
        "election",
        "approval",
    )
    discovery_min_duration_minutes: int = 12
    discovery_max_duration_minutes: int = 20
    cache_ttl_ms: int = 300_000
    request_timeout_ms: int = 5000
    return_stale_on_error: bool = True

    def __post_init__(self) -> None:
        if self.discovery_interval_ms <= 0:
            raise ValueError("discovery_interval_ms must be positive")
        if self.discovery_max_markets <= 0:
            raise ValueError("discovery_max_markets must be positive")
        if self.discovery_page_limit <= 0:
            raise ValueError("discovery_page_limit must be positive")
        if self.discovery_max_pages <= 0:
            raise ValueError("discovery_max_pages must be positive")
        if self.discovery_min_duration_minutes <= 0:
            raise ValueError("discovery_min_duration_minutes must be positive")
        if self.discovery_max_duration_minutes < self.discovery_min_duration_minutes:
            raise ValueError(
                "discovery_max_duration_minutes must be >= discovery_min_duration_minutes"
            )
        if self.cache_ttl_ms < 0:
            raise ValueError("cache_ttl_ms must be non-negative")
        if self.request_timeout_ms <= 0:
            raise ValueError("request_timeout_ms must be positive")


@dataclass(slots=True)
class ProxyConfig:
    enabled: bool = False
    url: str = ""
    no_proxy: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.enabled and not self.url.strip():
            raise ValueError("proxy url must be set when proxy is enabled")


@dataclass(slots=True)
class StorageConfig:
    recorder_enabled: bool = False
    recorder_output_dir: str = "runtime_data/records"
    recorder_max_queue_size: int = 10_000
    recorder_flush_interval_ms: int = 1_000
    recorder_flush_batch_size: int = 1_000
    recorder_rotate_interval_ms: int = 300_000
    recorder_rotate_max_file_size_bytes: int = 16 * 1024 * 1024
    db_enabled: bool = False
    db_path: str = "runtime_data/events.sqlite3"
    db_max_queue_size: int = 50_000
    db_flush_interval_ms: int = 1_000
    db_flush_batch_size: int = 1_000
    snapshot_enabled: bool = False
    snapshot_output_dir: str = "runtime_data/snapshots"
    snapshot_interval_ms: int = 30_000

    def __post_init__(self) -> None:
        if self.recorder_max_queue_size <= 0:
            raise ValueError("recorder_max_queue_size must be positive")
        if self.recorder_flush_interval_ms <= 0:
            raise ValueError("recorder_flush_interval_ms must be positive")
        if self.recorder_flush_batch_size <= 0:
            raise ValueError("recorder_flush_batch_size must be positive")
        if self.recorder_rotate_interval_ms <= 0:
            raise ValueError("recorder_rotate_interval_ms must be positive")
        if self.recorder_rotate_max_file_size_bytes <= 0:
            raise ValueError("recorder_rotate_max_file_size_bytes must be positive")
        if self.db_max_queue_size <= 0:
            raise ValueError("db_max_queue_size must be positive")
        if self.db_flush_interval_ms <= 0:
            raise ValueError("db_flush_interval_ms must be positive")
        if self.db_flush_batch_size <= 0:
            raise ValueError("db_flush_batch_size must be positive")
        if self.snapshot_interval_ms <= 0:
            raise ValueError("snapshot_interval_ms must be positive")


@dataclass(slots=True)
class ObservabilityConfig:
    metrics_log_interval_ms: int = 60_000
    alerts_max_history: int = 500
    startup_diagnostics_enabled: bool = True
    startup_diagnostics_log_interval_ms: int = 5_000
    startup_diagnostics_window_ms: int = 180_000
    startup_diagnostics_first_events: int = 3

    def __post_init__(self) -> None:
        if self.metrics_log_interval_ms < 0:
            raise ValueError("metrics_log_interval_ms must be non-negative")
        if self.alerts_max_history <= 0:
            raise ValueError("alerts_max_history must be positive")
        if self.startup_diagnostics_log_interval_ms <= 0:
            raise ValueError("startup_diagnostics_log_interval_ms must be positive")
        if self.startup_diagnostics_window_ms < 0:
            raise ValueError("startup_diagnostics_window_ms must be non-negative")
        if self.startup_diagnostics_first_events < 0:
            raise ValueError("startup_diagnostics_first_events must be non-negative")


@dataclass(slots=True)
class PricingConfig:
    fair_value_mode: str = "hybrid"
    min_probability: Decimal = Decimal("0.0010")
    full_set_buffer: Decimal = Decimal("0.0040")
    vol_half_life_ms: int = 15_000
    vol_max_age_ms: int = 1_800_000
    vol_stale_after_ms: int = 2_000
    vol_min_sigma: Decimal = Decimal("0.00005")
    vol_max_sigma: Decimal = Decimal("0.00500")

    def __post_init__(self) -> None:
        if self.fair_value_mode not in {"hybrid", "binance_only"}:
            raise ValueError("fair_value_mode must be one of: hybrid, binance_only")
        if not (Decimal("0") < self.min_probability < Decimal("0.5")):
            raise ValueError("min_probability must be between 0 and 0.5")
        if self.full_set_buffer < 0:
            raise ValueError("full_set_buffer must be non-negative")
        if self.vol_half_life_ms <= 0:
            raise ValueError("vol_half_life_ms must be positive")
        if self.vol_max_age_ms <= 0:
            raise ValueError("vol_max_age_ms must be positive")
        if self.vol_stale_after_ms <= 0:
            raise ValueError("vol_stale_after_ms must be positive")
        if self.vol_min_sigma <= 0:
            raise ValueError("vol_min_sigma must be positive")
        if self.vol_max_sigma < self.vol_min_sigma:
            raise ValueError("vol_max_sigma must be >= vol_min_sigma")


@dataclass(slots=True)
class ExecutionConfig:
    gateway_base_url: str = "https://clob.polymarket.com"
    gateway_dry_run: bool = True
    cancel_stale_quotes_ms: int = 30_000
    chain_id: int = 137
    signature_type: int = 1
    private_key: str = ""
    encrypted_private_key: str = ""
    funder_address: str = ""
    api_key: str = ""
    api_secret: str = ""
    passphrase: str = ""

    def __post_init__(self) -> None:
        if self.cancel_stale_quotes_ms <= 0:
            raise ValueError("cancel_stale_quotes_ms must be positive")
        if self.chain_id <= 0:
            raise ValueError("chain_id must be positive")
        if self.signature_type <= 0:
            raise ValueError("signature_type must be positive")


@dataclass(slots=True)
class RuntimeConfig:
    env: str = "dev"
    loop_interval_ms: int = 10
    primary_symbol: str = "BTCUSDT"
    lifecycle: LifecycleConfig = field(default_factory=LifecycleConfig)
    pricing: PricingConfig = field(default_factory=PricingConfig)
    binance: BinanceFeedConfig = field(default_factory=BinanceFeedConfig)
    chainlink: ChainlinkFeedConfig = field(default_factory=ChainlinkFeedConfig)
    polymarket: PolymarketFeedConfig = field(default_factory=PolymarketFeedConfig)
    metadata: MetadataConfig = field(default_factory=MetadataConfig)
    proxy: ProxyConfig = field(default_factory=ProxyConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    observability: ObservabilityConfig = field(default_factory=ObservabilityConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)

    @classmethod
    def from_env(
        cls,
        *,
        decrypt_password: str | None = None,
        env_path: str | None = None,
        strict_encrypted_private_key: bool = False,
    ) -> "RuntimeConfig":
        load_project_env(env_path)
        encrypted_private_key = getenv(
            "POLY15_PM_ENCRYPTED_PRIVATE_KEY",
            getenv("ENCRYPTED_PRIVATE_KEY", ""),
        ).strip()
        return cls(
            env=getenv("POLY15_ENV", "dev"),
            loop_interval_ms=int(getenv("POLY15_LOOP_INTERVAL_MS", "10")),
            primary_symbol=getenv("POLY15_PRIMARY_SYMBOL", "BTCUSDT"),
            lifecycle=LifecycleConfig(
                prewarm_ms=int(getenv("POLY15_PREWARM_MS", "60000")),
                fast_close_ms=int(getenv("POLY15_FAST_CLOSE_MS", "30000")),
                final_seconds_ms=int(getenv("POLY15_FINAL_SECONDS_MS", "5000")),
            ),
            pricing=PricingConfig(
                fair_value_mode=getenv("POLY15_FAIR_VALUE_MODE", "hybrid").strip().lower(),
                min_probability=Decimal(getenv("POLY15_MIN_PROBABILITY", "0.0010")),
                full_set_buffer=Decimal(getenv("POLY15_FULL_SET_BUFFER", "0.0040")),
                vol_half_life_ms=int(getenv("POLY15_VOL_HALF_LIFE_MS", "15000")),
                vol_max_age_ms=int(getenv("POLY15_VOL_MAX_AGE_MS", "1800000")),
                vol_stale_after_ms=int(getenv("POLY15_VOL_STALE_AFTER_MS", "2000")),
                vol_min_sigma=Decimal(getenv("POLY15_VOL_MIN_SIGMA", "0.00005")),
                vol_max_sigma=Decimal(getenv("POLY15_VOL_MAX_SIGMA", "0.00500")),
            ),
            binance=BinanceFeedConfig(
                enabled=_env_bool("POLY15_BINANCE_ENABLED", "0"),
                symbol=getenv("POLY15_BINANCE_SYMBOL", "BTCUSDT"),
                ws_url=getenv(
                    "POLY15_BINANCE_WS_URL",
                    "wss://stream.binance.com:9443/ws/btcusdt@bookTicker",
                ),
                depth_enabled=_env_bool("POLY15_BINANCE_DEPTH_ENABLED", "1"),
                depth_ws_url=getenv(
                    "POLY15_BINANCE_DEPTH_WS_URL",
                    "wss://stream.binance.com:9443/ws/btcusdt@depth5@100ms",
                ),
                rest_base_url=getenv(
                    "POLY15_BINANCE_REST_BASE_URL",
                    "https://api.binance.com",
                ),
                rest_request_timeout_ms=int(
                    getenv("POLY15_BINANCE_REST_REQUEST_TIMEOUT_MS", "3000")
                ),
                connect_timeout_ms=int(
                    getenv("POLY15_BINANCE_CONNECT_TIMEOUT_MS", "5000")
                ),
                idle_timeout_ms=int(
                    getenv("POLY15_BINANCE_IDLE_TIMEOUT_MS", "5000")
                ),
                retry_initial_delay_ms=int(
                    getenv("POLY15_BINANCE_RETRY_INITIAL_MS", "500")
                ),
                retry_max_delay_ms=int(
                    getenv("POLY15_BINANCE_RETRY_MAX_MS", "5000")
                ),
                retry_backoff=float(
                    getenv("POLY15_BINANCE_RETRY_BACKOFF", "2.0")
                ),
            ),
            chainlink=ChainlinkFeedConfig(
                enabled=_env_bool("POLY15_CHAINLINK_ENABLED", "0"),
                feed_name=getenv("POLY15_CHAINLINK_FEED_NAME", "BTC/USD"),
                endpoint=getenv("POLY15_CHAINLINK_ENDPOINT", ""),
                api_url=getenv(
                    "POLY15_CHAINLINK_API_URL",
                    "https://data.chain.link/api/query-timescale",
                ),
                query_name=getenv(
                    "POLY15_CHAINLINK_QUERY_NAME",
                    "LIVE_STREAM_REPORTS_QUERY",
                ),
                feed_id=getenv(
                    "POLY15_CHAINLINK_FEED_ID",
                    "0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8",
                ),
                price_scale=getenv("POLY15_CHAINLINK_PRICE_SCALE", "1e18"),
                poll_interval_ms=int(getenv("POLY15_CHAINLINK_POLL_MS", "10000")),
                request_timeout_ms=int(
                    getenv("POLY15_CHAINLINK_REQUEST_TIMEOUT_MS", "5000")
                ),
                stale_after_ms=int(
                    getenv("POLY15_CHAINLINK_STALE_AFTER_MS", "20000")
                ),
                silent_reconnect_after_ms=int(
                    getenv("POLY15_CHAINLINK_SILENT_RECONNECT_MS", "45000")
                ),
                retry_initial_delay_ms=int(
                    getenv("POLY15_CHAINLINK_RETRY_INITIAL_MS", "500")
                ),
                retry_max_delay_ms=int(
                    getenv("POLY15_CHAINLINK_RETRY_MAX_MS", "5000")
                ),
                retry_backoff=float(
                    getenv("POLY15_CHAINLINK_RETRY_BACKOFF", "2.0")
                ),
            ),
            polymarket=PolymarketFeedConfig(
                market_enabled=_env_bool("POLY15_PM_MARKET_ENABLED", "0"),
                market_ws_url=getenv(
                    "POLY15_PM_MARKET_WS_URL",
                    "wss://ws-subscriptions-clob.polymarket.com/ws/market",
                ),
                market_assets_ids=_env_csv("POLY15_PM_MARKET_ASSET_IDS", ""),
                market_custom_features_enabled=_env_bool(
                    "POLY15_PM_MARKET_CUSTOM_FEATURES_ENABLED",
                    "1",
                ),
                market_event_types=_env_csv(
                    "POLY15_PM_MARKET_EVENT_TYPES",
                    "book,price_change,tick_size_change,market_resolved",
                ),
                market_include_new_markets=_env_bool(
                    "POLY15_PM_MARKET_INCLUDE_NEW_MARKETS",
                    "1",
                ),
                market_initial_dump=_env_bool("POLY15_PM_MARKET_INITIAL_DUMP", "0"),
                user_enabled=_env_bool("POLY15_PM_USER_ENABLED", "0"),
                user_ws_url=getenv(
                    "POLY15_PM_USER_WS_URL",
                    "wss://ws-subscriptions-clob.polymarket.com/ws/user",
                ),
                user_market_ids=_env_csv("POLY15_PM_USER_MARKET_IDS", ""),
                api_key=getenv("POLY15_PM_API_KEY", ""),
                api_secret=getenv("POLY15_PM_API_SECRET", ""),
                passphrase=getenv("POLY15_PM_PASSPHRASE", ""),
                heartbeat_interval_ms=int(
                    getenv("POLY15_PM_HEARTBEAT_MS", "10000")
                ),
                connect_timeout_ms=int(
                    getenv("POLY15_PM_CONNECT_TIMEOUT_MS", "5000")
                ),
                market_idle_timeout_ms=int(
                    getenv("POLY15_PM_MARKET_IDLE_TIMEOUT_MS", "40000")
                ),
                user_idle_timeout_ms=int(
                    getenv("POLY15_PM_USER_IDLE_TIMEOUT_MS", "0")
                ),
                retry_initial_delay_ms=int(
                    getenv("POLY15_PM_RETRY_INITIAL_MS", "500")
                ),
                retry_max_delay_ms=int(
                    getenv("POLY15_PM_RETRY_MAX_MS", "5000")
                ),
                retry_backoff=float(
                    getenv("POLY15_PM_RETRY_BACKOFF", "2.0")
                ),
            ),
            metadata=MetadataConfig(
                gamma_base_url=getenv(
                    "POLY15_PM_GAMMA_BASE_URL",
                    "https://gamma-api.polymarket.com",
                ),
                discovery_enabled=_env_bool("POLY15_PM_DISCOVERY_ENABLED", "1"),
                discovery_interval_ms=int(
                    getenv("POLY15_PM_DISCOVERY_INTERVAL_MS", "5000")
                ),
                discovery_max_markets=int(
                    getenv("POLY15_PM_DISCOVERY_MAX_MARKETS", "3")
                ),
                discovery_page_limit=int(
                    getenv("POLY15_PM_DISCOVERY_PAGE_LIMIT", "100")
                ),
                discovery_max_pages=int(
                    getenv("POLY15_PM_DISCOVERY_MAX_PAGES", "3")
                ),
                discovery_tag_slug=getenv(
                    "POLY15_PM_DISCOVERY_TAG_SLUG",
                    "bitcoin",
                ),
                discovery_keywords=_env_csv(
                    "POLY15_PM_DISCOVERY_KEYWORDS",
                    "btc,bitcoin",
                ),
                discovery_exclude_keywords=_env_csv(
                    "POLY15_PM_DISCOVERY_EXCLUDE_KEYWORDS",
                    "weekly,daily,election,approval",
                ),
                discovery_min_duration_minutes=int(
                    getenv("POLY15_PM_DISCOVERY_MIN_DURATION_MINUTES", "12")
                ),
                discovery_max_duration_minutes=int(
                    getenv("POLY15_PM_DISCOVERY_MAX_DURATION_MINUTES", "20")
                ),
                cache_ttl_ms=int(
                    getenv("POLY15_PM_GAMMA_CACHE_TTL_MS", "300000")
                ),
                request_timeout_ms=int(
                    getenv("POLY15_PM_GAMMA_REQUEST_TIMEOUT_MS", "5000")
                ),
                return_stale_on_error=_env_bool(
                    "POLY15_PM_GAMMA_RETURN_STALE_ON_ERROR",
                    "1",
                ),
            ),
            proxy=ProxyConfig(
                enabled=_env_bool("POLY15_PROXY_ENABLED", "0"),
                url=getenv("POLY15_PROXY_URL", "").strip(),
                no_proxy=_env_csv("POLY15_PROXY_NO_PROXY", ""),
            ),
            storage=StorageConfig(
                recorder_enabled=_env_bool("POLY15_RECORDER_ENABLED", "0"),
                recorder_output_dir=getenv(
                    "POLY15_RECORDER_OUTPUT_DIR",
                    "runtime_data/records",
                ),
                recorder_max_queue_size=int(
                    getenv("POLY15_RECORDER_MAX_QUEUE_SIZE", "10000")
                ),
                recorder_flush_interval_ms=int(
                    getenv("POLY15_RECORDER_FLUSH_INTERVAL_MS", "1000")
                ),
                recorder_flush_batch_size=int(
                    getenv("POLY15_RECORDER_FLUSH_BATCH_SIZE", "1000")
                ),
                recorder_rotate_interval_ms=int(
                    getenv("POLY15_RECORDER_ROTATE_INTERVAL_MS", "300000")
                ),
                recorder_rotate_max_file_size_bytes=int(
                    getenv("POLY15_RECORDER_ROTATE_MAX_FILE_SIZE_BYTES", str(16 * 1024 * 1024))
                ),
                db_enabled=_env_bool("POLY15_DB_ENABLED", "0"),
                db_path=getenv(
                    "POLY15_DB_PATH",
                    "runtime_data/events.sqlite3",
                ),
                db_max_queue_size=int(
                    getenv("POLY15_DB_MAX_QUEUE_SIZE", "50000")
                ),
                db_flush_interval_ms=int(
                    getenv("POLY15_DB_FLUSH_INTERVAL_MS", "1000")
                ),
                db_flush_batch_size=int(
                    getenv("POLY15_DB_FLUSH_BATCH_SIZE", "1000")
                ),
                snapshot_enabled=_env_bool("POLY15_SNAPSHOT_ENABLED", "0"),
                snapshot_output_dir=getenv(
                    "POLY15_SNAPSHOT_OUTPUT_DIR",
                    "runtime_data/snapshots",
                ),
                snapshot_interval_ms=int(
                    getenv("POLY15_SNAPSHOT_INTERVAL_MS", "30000")
                ),
            ),
            observability=ObservabilityConfig(
                metrics_log_interval_ms=int(
                    getenv("POLY15_METRICS_LOG_INTERVAL_MS", "60000")
                ),
                alerts_max_history=int(
                    getenv("POLY15_ALERTS_MAX_HISTORY", "500")
                ),
                startup_diagnostics_enabled=_env_bool(
                    "POLY15_STARTUP_DIAGNOSTICS_ENABLED",
                    "1",
                ),
                startup_diagnostics_log_interval_ms=int(
                    getenv("POLY15_STARTUP_DIAGNOSTICS_LOG_INTERVAL_MS", "5000")
                ),
                startup_diagnostics_window_ms=int(
                    getenv("POLY15_STARTUP_DIAGNOSTICS_WINDOW_MS", "180000")
                ),
                startup_diagnostics_first_events=int(
                    getenv("POLY15_STARTUP_DIAGNOSTICS_FIRST_EVENTS", "3")
                ),
            ),
            execution=ExecutionConfig(
                gateway_base_url=getenv(
                    "POLY15_EXEC_GATEWAY_BASE_URL",
                    "https://clob.polymarket.com",
                ),
                gateway_dry_run=_env_bool("POLY15_EXEC_DRY_RUN", "1"),
                cancel_stale_quotes_ms=int(
                    getenv("POLY15_CANCEL_STALE_QUOTES_MS", "30000")
                ),
                chain_id=int(getenv("POLY15_PM_CHAIN_ID", "137")),
                signature_type=int(getenv("POLY15_PM_SIGNATURE_TYPE", "1")),
                private_key=resolve_private_key_from_env(
                    decrypt_password=decrypt_password,
                    env_path=env_path,
                    strict_encrypted_private_key=strict_encrypted_private_key,
                ),
                encrypted_private_key=encrypted_private_key,
                funder_address=getenv("POLY15_PM_FUNDER", ""),
                api_key=getenv("POLY15_PM_API_KEY", ""),
                api_secret=getenv("POLY15_PM_API_SECRET", ""),
                passphrase=getenv("POLY15_PM_PASSPHRASE", ""),
            ),
        )
