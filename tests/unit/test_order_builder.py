"""Unit tests for execution.order_builder.OrderBuilder and round_to_tick."""

from decimal import Decimal

import pytest

from core.enums import OrderRole, Side, TimeInForce
from domain.models import MarketMetadata, OrderIntent
from execution.order_builder import OrderBuilder, round_to_tick


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_market(tick_size: str = "0.01", fee_rate_bps: str = "10") -> MarketMetadata:
    return MarketMetadata(
        market_id="m1",
        condition_id="cond_m1",
        up_token_id="up_tok",
        down_token_id="dn_tok",
        start_ts_ms=1000,
        end_ts_ms=2_000_000,
        tick_size=Decimal(tick_size),
        fee_rate_bps=Decimal(fee_rate_bps),
        min_order_size=Decimal("1"),
        status="active",
    )


def _make_intent(
    price: str = "0.55",
    size: str = "10",
    side: Side = Side.BUY,
    post_only: bool = True,
    tif: TimeInForce = TimeInForce.GTC,
) -> OrderIntent:
    return OrderIntent(
        intent_id="int_1",
        pair_id="pair_1",
        market_id="m1",
        token_id="up_tok",
        side=side,
        price=Decimal(price),
        size=Decimal(size),
        tif=tif,
        post_only=post_only,
        role=OrderRole.MAKER_QUOTE,
        reason="test",
    )


# ---------------------------------------------------------------------------
# round_to_tick
# ---------------------------------------------------------------------------

class TestRoundToTick:
    def test_buy_post_only_rounds_down(self) -> None:
        # 0.556 with tick 0.01 -> 0.55 (round down, don't overpay)
        result = round_to_tick(Decimal("0.556"), Decimal("0.01"), Side.BUY, post_only=True)
        assert result == Decimal("0.55")

    def test_sell_post_only_rounds_up(self) -> None:
        # 0.554 with tick 0.01 -> 0.56 (round up, don't undersell)
        result = round_to_tick(Decimal("0.554"), Decimal("0.01"), Side.SELL, post_only=True)
        assert result == Decimal("0.56")

    def test_buy_taker_rounds_up(self) -> None:
        # 0.551 with tick 0.01 -> 0.56 (taker needs to hit)
        result = round_to_tick(Decimal("0.551"), Decimal("0.01"), Side.BUY, post_only=False)
        assert result == Decimal("0.56")

    def test_sell_taker_rounds_down(self) -> None:
        # 0.559 with tick 0.01 -> 0.55 (taker needs to hit)
        result = round_to_tick(Decimal("0.559"), Decimal("0.01"), Side.SELL, post_only=False)
        assert result == Decimal("0.55")

    def test_already_on_tick_unchanged(self) -> None:
        result = round_to_tick(Decimal("0.50"), Decimal("0.01"), Side.BUY, post_only=True)
        assert result == Decimal("0.50")

    def test_clamps_to_min_price(self) -> None:
        # Price 0.001 with tick 0.01: can't go below tick_size
        result = round_to_tick(Decimal("0.001"), Decimal("0.01"), Side.BUY, post_only=True)
        assert result == Decimal("0.01")

    def test_clamps_to_max_price(self) -> None:
        # Price 0.999 with tick 0.01: can't go above 0.99
        result = round_to_tick(Decimal("0.999"), Decimal("0.01"), Side.SELL, post_only=True)
        assert result == Decimal("0.99")

    def test_zero_tick_size_raises(self) -> None:
        with pytest.raises(ValueError, match="tick_size must be positive"):
            round_to_tick(Decimal("0.55"), Decimal("0"), Side.BUY, post_only=True)

    def test_fine_tick_size(self) -> None:
        # tick_size = 0.001
        result = round_to_tick(Decimal("0.5555"), Decimal("0.001"), Side.BUY, post_only=True)
        assert result == Decimal("0.555")

    def test_exact_mid_rounds_conservatively(self) -> None:
        # Exactly 0.555 with tick 0.01, BUY post_only -> round DOWN -> 0.55
        result = round_to_tick(Decimal("0.555"), Decimal("0.01"), Side.BUY, post_only=True)
        assert result == Decimal("0.55")


# ---------------------------------------------------------------------------
# OrderBuilder.build_place
# ---------------------------------------------------------------------------

class TestOrderBuilderBuildPlace:
    def setup_method(self) -> None:
        self._builder = OrderBuilder()
        self._market = _make_market()

    def test_basic_payload_structure(self) -> None:
        intent = _make_intent()
        payload = self._builder.build_place(intent, self._market, "cid_1")
        assert payload["market_id"] == "m1"
        assert payload["token_id"] == "up_tok"
        assert payload["side"] == "BUY"
        assert payload["client_order_id"] == "cid_1"
        assert payload["post_only"] is True
        assert payload["time_in_force"] == "GTC"

    def test_price_is_rounded_to_tick(self) -> None:
        intent = _make_intent(price="0.556")  # mid-tick
        payload = self._builder.build_place(intent, self._market, "cid_1")
        # BUY post_only -> round down
        assert payload["price"] == "0.55"

    def test_fee_rate_bps_snapshotted(self) -> None:
        intent = _make_intent()
        payload = self._builder.build_place(intent, self._market, "cid_1")
        assert payload["fee_rate_bps"] == "10"

    def test_tif_fok(self) -> None:
        intent = _make_intent(tif=TimeInForce.FOK, post_only=False)
        payload = self._builder.build_place(intent, self._market, "cid_1")
        assert payload["time_in_force"] == "FOK"

    def test_tif_fak(self) -> None:
        intent = _make_intent(tif=TimeInForce.FAK, post_only=False)
        payload = self._builder.build_place(intent, self._market, "cid_1")
        assert payload["time_in_force"] == "FAK"

    def test_sell_side_payload(self) -> None:
        intent = _make_intent(side=Side.SELL, price="0.45", post_only=True)
        payload = self._builder.build_place(intent, self._market, "cid_2")
        assert payload["side"] == "SELL"
        # SELL post_only -> round up: 0.45 already on tick
        assert payload["price"] == "0.45"

    def test_size_preserved(self) -> None:
        intent = _make_intent(size="250")
        payload = self._builder.build_place(intent, self._market, "cid_3")
        assert payload["size"] == "250"


# ---------------------------------------------------------------------------
# OrderBuilder.build_cancel
# ---------------------------------------------------------------------------

class TestOrderBuilderBuildCancel:
    def test_cancel_payload(self) -> None:
        builder = OrderBuilder()
        payload = builder.build_cancel("cid_99")
        assert payload == {"client_order_id": "cid_99"}
