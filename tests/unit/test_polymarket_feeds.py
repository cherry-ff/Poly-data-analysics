"""Unit tests for Polymarket feed subscription/auth helpers."""

from decimal import Decimal

from app.config import PolymarketFeedConfig
from core.clock import WallClock
from core.enums import OrderStatus
from core.event_bus import InMemoryEventBus
from domain.models import MarketMetadata
from feeds.polymarket_market_ws import PolymarketMarketFeed
from feeds.polymarket_user_ws import PolymarketUserFeed


def _make_config(**overrides: object) -> PolymarketFeedConfig:
    base = {
        "market_assets_ids": ("asset_up", "asset_down"),
        "user_market_ids": ("market_1",),
        "api_key": "k",
        "api_secret": "s",
        "passphrase": "p",
    }
    base.update(overrides)
    return PolymarketFeedConfig(**base)


def test_market_feed_builds_official_subscription_payloads() -> None:
    config = _make_config(
        market_custom_features_enabled=True,
        market_initial_dump=True,
    )
    payloads = PolymarketMarketFeed.build_subscriptions(config)

    assert payloads[0] == {
        "type": "market",
        "assets_ids": ["asset_up", "asset_down"],
        "initial_dump": True,
        "custom_feature_enabled": True,
    }


def test_user_feed_builds_authenticated_subscription_payload() -> None:
    config = _make_config()
    payloads = PolymarketUserFeed.build_subscriptions(config)

    assert payloads == [
        {
            "type": "user",
            "auth": {
                "apiKey": "k",
                "secret": "s",
                "passphrase": "p",
            },
            "markets": ["market_1"],
        }
    ]


def test_user_feed_skips_subscription_without_markets_or_auth() -> None:
    config = _make_config(user_market_ids=(), api_key="", api_secret="", passphrase="")
    assert PolymarketUserFeed.build_subscriptions(config) == []


def test_user_feed_reports_startup_skip_reason_without_auth() -> None:
    feed = PolymarketUserFeed(
        config=_make_config(user_market_ids=(), api_key="", api_secret="", passphrase=""),
        event_bus=InMemoryEventBus(),
        clock=WallClock(),
    )

    assert feed.startup_skip_reason() == "user auth is incomplete"


def test_market_feed_reports_startup_skip_reason_without_assets_and_gamma() -> None:
    feed = PolymarketMarketFeed(
        config=_make_config(market_assets_ids=()),
        event_bus=InMemoryEventBus(),
        clock=WallClock(),
    )

    assert (
        feed.startup_skip_reason("")
        == "no market asset ids configured and gamma discovery is unavailable"
    )


def test_market_feed_registers_market_and_maps_token_to_internal_market_id() -> None:
    feed = PolymarketMarketFeed(
        config=_make_config(market_assets_ids=()),
        event_bus=InMemoryEventBus(),
        clock=WallClock(),
    )
    feed.register_market(
        MarketMetadata(
            market_id="market_internal",
            condition_id="condition_1",
            up_token_id="asset_up",
            down_token_id="asset_down",
            start_ts_ms=1,
            end_ts_ms=2,
            tick_size=Decimal("0.01"),
            fee_rate_bps=Decimal("0"),
            min_order_size=Decimal("1"),
            status="active",
        )
    )

    event = feed._to_book_event(
        {
            "event_type": "best_bid_ask",
            "asset_id": "asset_up",
            "best_bid": "0.48",
            "best_ask": "0.52",
        },
        recv_ts_ms=1000,
    )

    assert event is not None
    assert event.market_id == "market_internal"


def test_market_feed_merges_partial_price_change_with_previous_top() -> None:
    feed = PolymarketMarketFeed(
        config=_make_config(market_assets_ids=()),
        event_bus=InMemoryEventBus(),
        clock=WallClock(),
    )
    feed.register_market(
        MarketMetadata(
            market_id="market_internal",
            condition_id="condition_1",
            up_token_id="asset_up",
            down_token_id="asset_down",
            start_ts_ms=1,
            end_ts_ms=2,
            tick_size=Decimal("0.01"),
            fee_rate_bps=Decimal("0"),
            min_order_size=Decimal("1"),
            status="active",
        )
    )

    seed = feed._to_book_event(
        {
            "event_type": "best_bid_ask",
            "asset_id": "asset_up",
            "best_bid": "0.48",
            "best_bid_size": "120",
            "best_ask": "0.52",
            "best_ask_size": "80",
        },
        recv_ts_ms=1000,
    )
    assert seed is not None

    partial = feed._to_book_event(
        {
            "event_type": "price_change",
            "asset_id": "asset_up",
            "best_bid": "0.49",
        },
        recv_ts_ms=1100,
        event_type="price_change",
    )

    assert partial is not None
    assert partial.market_id == "market_internal"
    assert partial.top.best_bid_px == Decimal("0.49")
    assert partial.top.best_ask_px == Decimal("0.52")
    assert partial.top.best_bid_sz == Decimal("120")
    assert partial.top.best_ask_sz == Decimal("80")


def test_market_feed_preserves_sizes_when_price_change_omits_them() -> None:
    feed = PolymarketMarketFeed(
        config=_make_config(market_assets_ids=()),
        event_bus=InMemoryEventBus(),
        clock=WallClock(),
    )
    feed.register_market(
        MarketMetadata(
            market_id="market_internal",
            condition_id="condition_1",
            up_token_id="asset_up",
            down_token_id="asset_down",
            start_ts_ms=1,
            end_ts_ms=2,
            tick_size=Decimal("0.01"),
            fee_rate_bps=Decimal("0"),
            min_order_size=Decimal("1"),
            status="active",
        )
    )

    seed = feed._to_book_event(
        {
            "event_type": "book",
            "asset_id": "asset_up",
            "bids": [{"price": "0.48", "size": "120"}],
            "asks": [{"price": "0.52", "size": "80"}],
        },
        recv_ts_ms=1000,
        event_type="book",
    )
    assert seed is not None

    partial = feed._to_book_event(
        {
            "event_type": "price_change",
            "asset_id": "asset_up",
            "best_ask": "0.51",
        },
        recv_ts_ms=1100,
        event_type="price_change",
    )

    assert partial is not None
    assert partial.top.best_bid_px == Decimal("0.48")
    assert partial.top.best_ask_px == Decimal("0.51")
    assert partial.top.best_bid_sz == Decimal("120")
    assert partial.top.best_ask_sz == Decimal("80")


def test_user_feed_maps_condition_id_to_internal_market_id() -> None:
    feed = PolymarketUserFeed(
        config=_make_config(user_market_ids=()),
        event_bus=InMemoryEventBus(),
        clock=WallClock(),
    )
    feed.register_market(
        MarketMetadata(
            market_id="market_internal",
            condition_id="condition_1",
            up_token_id="asset_up",
            down_token_id="asset_down",
            start_ts_ms=1,
            end_ts_ms=2,
            tick_size=Decimal("0.01"),
            fee_rate_bps=Decimal("0"),
            min_order_size=Decimal("1"),
            status="active",
        )
    )
    report = feed._normalize(
        {
            "event_type": "order",
            "type": "UPDATE",
            "id": "0xorder",
            "market": "condition_1",
            "asset_id": "asset_up",
            "side": "BUY",
            "size": "10",
            "size_matched": "4",
            "price": "0.52",
            "timestamp": 1_700_000_000_000,
        },
        recv_ts_ms=1_700_000_000_100,
    )

    assert report is not None
    assert report.market_id == "market_internal"


def test_user_feed_normalizes_exchange_id_when_client_id_absent() -> None:
    feed = PolymarketUserFeed(
        config=_make_config(),
        event_bus=InMemoryEventBus(),
        clock=WallClock(),
    )
    report = feed._normalize(
        {
            "event_type": "order",
            "type": "UPDATE",
            "id": "0xorder",
            "market": "market_1",
            "asset_id": "asset_up",
            "side": "BUY",
            "size": "10",
            "size_matched": "4",
            "price": "0.52",
            "timestamp": 1_700_000_000_000,
        },
        recv_ts_ms=1_700_000_000_100,
    )

    assert report is not None
    assert report.client_order_id == "0xorder"
    assert report.exchange_order_id == "0xorder"
    assert report.market_id == "market_1"
    assert report.token_id == "asset_up"
    assert report.filled_size == Decimal("4")
    assert report.status == OrderStatus.PARTIAL
