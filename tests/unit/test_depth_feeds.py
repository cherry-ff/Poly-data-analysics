from __future__ import annotations

from decimal import Decimal

from app.config import BinanceFeedConfig, PolymarketFeedConfig
from core.clock import WallClock
from core.event_bus import InMemoryEventBus
from domain.models import MarketMetadata
from feeds.binance_ws import BinanceDepthFeed
from feeds.polymarket_market_ws import PolymarketMarketFeed


def test_binance_depth_feed_normalizes_top_five_levels() -> None:
    feed = BinanceDepthFeed(
        config=BinanceFeedConfig(enabled=True, depth_enabled=True),
        event_bus=InMemoryEventBus(),
        clock=WallClock(),
    )

    snapshot = feed._normalize(
        {
            "lastUpdateId": 42,
            "bids": [
                ["67500.1", "0.4"],
                ["67500.0", "0.3"],
                ["67499.9", "0.2"],
                ["67499.8", "0.1"],
                ["67499.7", "0.05"],
                ["67499.6", "0.01"],
            ],
            "asks": [
                ["67500.2", "0.5"],
                ["67500.3", "0.6"],
                ["67500.4", "0.7"],
                ["67500.5", "0.8"],
                ["67500.6", "0.9"],
                ["67500.7", "1.0"],
            ],
        },
        recv_ts_ms=1_700_000_000_100,
    )

    assert snapshot is not None
    assert snapshot.last_update_id == 42
    assert len(snapshot.bids) == 5
    assert len(snapshot.asks) == 5
    assert snapshot.bids[0].price == Decimal("67500.1")
    assert snapshot.asks[-1].size == Decimal("0.9")


def test_polymarket_market_feed_normalizes_five_levels_from_book_event() -> None:
    feed = PolymarketMarketFeed(
        config=PolymarketFeedConfig(market_assets_ids=("asset_up",)),
        event_bus=InMemoryEventBus(),
        clock=WallClock(),
    )
    feed.register_market(
        MarketMetadata(
            market_id="market_1",
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

    event = feed._to_depth_event(
        {
            "event_type": "book",
            "asset_id": "asset_up",
            "bids": [
                {"price": "0.49", "size": "100"},
                {"price": "0.48", "size": "90"},
                {"price": "0.47", "size": "80"},
                {"price": "0.46", "size": "70"},
                {"price": "0.45", "size": "60"},
                {"price": "0.44", "size": "50"},
            ],
            "asks": [
                {"price": "0.51", "size": "110"},
                {"price": "0.52", "size": "120"},
                {"price": "0.53", "size": "130"},
                {"price": "0.54", "size": "140"},
                {"price": "0.55", "size": "150"},
                {"price": "0.56", "size": "160"},
            ],
        },
        recv_ts_ms=1_700_000_000_200,
        event_type="book",
    )

    assert event is not None
    assert event.snapshot.market_id == "market_1"
    assert event.snapshot.token_id == "asset_up"
    assert len(event.snapshot.bids) == 5
    assert len(event.snapshot.asks) == 5
    assert event.snapshot.bids[0].price == Decimal("0.49")
    assert event.snapshot.asks[0].price == Decimal("0.51")
    assert event.snapshot.asks[-1].price == Decimal("0.55")


def test_polymarket_market_feed_sorts_unsorted_book_levels_to_best_first() -> None:
    feed = PolymarketMarketFeed(
        config=PolymarketFeedConfig(market_assets_ids=("asset_up",)),
        event_bus=InMemoryEventBus(),
        clock=WallClock(),
    )
    feed.register_market(
        MarketMetadata(
            market_id="market_1",
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

    top_event = feed._to_book_event(
        {
            "event_type": "book",
            "asset_id": "asset_up",
            "bids": [
                {"price": "0.01", "size": "1000"},
                {"price": "0.55", "size": "0"},
                {"price": "0.54", "size": "10"},
            ],
            "asks": [
                {"price": "0.99", "size": "1000"},
                {"price": "0.56", "size": "0"},
                {"price": "0.57", "size": "10"},
            ],
        },
        recv_ts_ms=1_700_000_000_200,
    )

    depth_event = feed._to_depth_event(
        {
            "event_type": "book",
            "asset_id": "asset_up",
            "bids": [
                {"price": "0.01", "size": "1000"},
                {"price": "0.55", "size": "0"},
                {"price": "0.54", "size": "10"},
            ],
            "asks": [
                {"price": "0.99", "size": "1000"},
                {"price": "0.56", "size": "0"},
                {"price": "0.57", "size": "10"},
            ],
        },
        recv_ts_ms=1_700_000_000_200,
        event_type="book",
    )

    assert top_event is not None
    assert top_event.top.best_bid_px == Decimal("0.55")
    assert top_event.top.best_ask_px == Decimal("0.56")

    assert depth_event is not None
    assert depth_event.snapshot.bids[0].price == Decimal("0.55")
    assert depth_event.snapshot.bids[1].price == Decimal("0.54")
    assert depth_event.snapshot.asks[0].price == Decimal("0.56")
    assert depth_event.snapshot.asks[1].price == Decimal("0.57")
