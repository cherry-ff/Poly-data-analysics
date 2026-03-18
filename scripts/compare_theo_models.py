from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RECORDS_ROOT = PROJECT_ROOT / "runtime_data" / "records"
BUILD_SCRIPT_PATH = PROJECT_ROOT / "scripts" / "build_dashboard_data.py"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from domain.models import BinanceTick, ChainlinkTick, MarketMetadata
from market.registry import InMemoryMarketRegistry
from pricing.fair_value import BinaryOptionFairValueEngine
from pricing.lead_lag import SimpleLeadLagEngine
from pricing.vol_model import EwmaVolModel
from state.inventory_state import InMemoryInventoryStore


def _load_builder_module():
    spec = importlib.util.spec_from_file_location("build_dashboard_data", BUILD_SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load dashboard builder from {BUILD_SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@dataclass(slots=True)
class ErrorMetrics:
    count: int = 0
    abs_error_sum: float = 0.0
    sq_error_sum: float = 0.0
    signed_error_sum: float = 0.0
    theo_variation_sum: float = 0.0
    prev_theo: float | None = None
    latest_theo: float | None = None
    latest_mid: float | None = None

    def observe(self, *, theo: Decimal, mid: float) -> None:
        theo_float = float(theo)
        error = theo_float - mid
        self.count += 1
        self.abs_error_sum += abs(error)
        self.sq_error_sum += error * error
        self.signed_error_sum += error
        if self.prev_theo is not None:
            self.theo_variation_sum += abs(theo_float - self.prev_theo)
        self.prev_theo = theo_float
        self.latest_theo = theo_float
        self.latest_mid = mid

    def to_dict(self) -> dict[str, float | int | None]:
        if self.count == 0:
            return {
                "count": 0,
                "mae": None,
                "rmse": None,
                "mean_signed_error": None,
                "mean_abs_theo_step": None,
                "latest_theo": self.latest_theo,
                "latest_mid": self.latest_mid,
            }
        return {
            "count": self.count,
            "mae": round(self.abs_error_sum / self.count, 6),
            "rmse": round((self.sq_error_sum / self.count) ** 0.5, 6),
            "mean_signed_error": round(self.signed_error_sum / self.count, 6),
            "mean_abs_theo_step": round(self.theo_variation_sum / max(self.count - 1, 1), 6),
            "latest_theo": round(self.latest_theo, 6) if self.latest_theo is not None else None,
            "latest_mid": round(self.latest_mid, 6) if self.latest_mid is not None else None,
        }


@dataclass(slots=True)
class ModelState:
    vol_model: EwmaVolModel
    lead_lag: SimpleLeadLagEngine
    engine: BinaryOptionFairValueEngine
    metrics: ErrorMetrics
    binance_idx: int = 0
    chainlink_idx: int = 0


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare binance-only vs hybrid fair-value models against Polymarket UP mid.",
    )
    parser.add_argument(
        "--records-root",
        default=str(RECORDS_ROOT),
        help="Path to runtime_data/records",
    )
    parser.add_argument(
        "--market-id",
        action="append",
        dest="market_ids",
        default=[],
        help="Specific market id(s) to evaluate. Defaults to all markets with pair-book data.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional path to write JSON comparison report.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    records_root = Path(args.records_root).resolve()
    builder = _load_builder_module()
    global_files, market_files = builder._discover_record_files(records_root)

    selected_market_ids = args.market_ids or sorted(
        market_files.keys(),
        key=builder._safe_int,
    )
    report = {
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "records_root": str(records_root),
        "markets": {},
    }

    for market_id in selected_market_ids:
        files = market_files.get(market_id)
        if files is None:
            continue
        market_report = _compare_market(builder, global_files, market_id, files)
        if market_report is None:
            continue
        report["markets"][market_id] = market_report

    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(json.dumps(report, indent=2))


def _compare_market(builder, global_files: dict[str, Path], market_id: str, files: dict[str, Path]) -> dict[str, Any] | None:
    metadata_raw = builder._load_market_metadata(files.get("market.metadata"), market_id)
    if metadata_raw is None:
        return None

    pair_book = builder._load_pair_book_series(
        files.get("feeds.polymarket.market.book_top"),
        metadata_raw,
        market_id,
    )
    if not pair_book["series"]:
        return None

    latest_ts = pair_book["latest"]["ts"] if pair_book.get("latest") else None
    market_window = builder._build_market_window(metadata_raw, latest_ts)
    market = _to_market_metadata(metadata_raw, market_window)

    binance_points = _load_binance_window(
        builder,
        global_files.get("feeds.binance.tick"),
        market_window.get("start_ts"),
        market_window.get("end_ts"),
    )
    chainlink_points = _load_chainlink_window(
        builder,
        global_files.get("feeds.chainlink.tick"),
        market_window.get("start_ts"),
        market_window.get("end_ts"),
    )
    if not binance_points:
        return None

    models = {
        "binance_only": _make_model_state(market, "binance_only"),
        "hybrid": _make_model_state(market, "hybrid"),
    }
    pair_series = [
        point for point in pair_book["series"]
        if isinstance(point.get("ts"), int) and isinstance(point.get("up_mid"), float)
    ]
    if not pair_series:
        return None

    for point in pair_series:
        now_ms = point["ts"]
        up_mid = point["up_mid"]
        for state in models.values():
            _advance_state(state, binance_points, chainlink_points, now_ms)
            snapshot = state.engine.compute(market, now_ms)
            if snapshot is None:
                continue
            state.metrics.observe(theo=snapshot.theo_up, mid=up_mid)

    model_reports = {
        name: state.metrics.to_dict()
        for name, state in models.items()
    }
    winner = _select_winner(model_reports)
    return {
        "window": market_window,
        "event_count": len(pair_series),
        "models": model_reports,
        "winner": winner,
    }


def _make_model_state(market: MarketMetadata, mode: str) -> ModelState:
    registry = InMemoryMarketRegistry()
    registry.upsert(market)
    inventory_state = InMemoryInventoryStore(registry=registry)
    vol_model = EwmaVolModel()
    lead_lag = SimpleLeadLagEngine()
    engine = BinaryOptionFairValueEngine(
        vol_model=vol_model,
        lead_lag=lead_lag,
        inventory_state=inventory_state,
        fair_value_mode=mode,
    )
    return ModelState(
        vol_model=vol_model,
        lead_lag=lead_lag,
        engine=engine,
        metrics=ErrorMetrics(),
    )


def _advance_state(
    state: ModelState,
    binance_points: list[dict[str, Any]],
    chainlink_points: list[dict[str, Any]],
    now_ms: int,
) -> None:
    while state.binance_idx < len(binance_points) and binance_points[state.binance_idx]["ts"] <= now_ms:
        point = binance_points[state.binance_idx]
        tick = BinanceTick(
            symbol="BTCUSDT",
            event_ts_ms=point["ts"],
            recv_ts_ms=point["ts"],
            last_price=_to_decimal(point["last"] or point["mid"]),
            best_bid=_to_decimal(point["bid"] or point["mid"]),
            best_ask=_to_decimal(point["ask"] or point["mid"]),
        )
        state.vol_model.on_binance_tick(tick)
        state.lead_lag.on_binance_tick(tick)
        state.binance_idx += 1

    while state.chainlink_idx < len(chainlink_points) and chainlink_points[state.chainlink_idx]["ts"] <= now_ms:
        point = chainlink_points[state.chainlink_idx]
        tick = ChainlinkTick(
            feed="BTC/USD",
            oracle_ts_ms=point["ts"],
            recv_ts_ms=point["ts"],
            price=_to_decimal(point["price"]),
            round_id=str(point["ts"]),
            bid=_to_decimal(point["bid"]) if point.get("bid") is not None else None,
            ask=_to_decimal(point["ask"]) if point.get("ask") is not None else None,
        )
        state.lead_lag.on_chainlink_tick(tick)
        state.chainlink_idx += 1


def _to_market_metadata(raw: dict[str, Any], market_window: dict[str, Any]) -> MarketMetadata:
    return MarketMetadata(
        market_id=str(raw["market_id"]),
        condition_id=str(raw["condition_id"]),
        up_token_id=str(raw["up_token_id"]),
        down_token_id=str(raw["down_token_id"]),
        start_ts_ms=int(market_window["start_ts"]),
        end_ts_ms=int(market_window["end_ts"]),
        tick_size=_to_decimal(raw["tick_size"]),
        fee_rate_bps=_to_decimal(raw["fee_rate_bps"]),
        min_order_size=_to_decimal(raw["min_order_size"]),
        status=str(raw["status"]),
        reference_price=_to_decimal(raw["reference_price"]) if raw.get("reference_price") is not None else None,
    )


def _select_winner(model_reports: dict[str, dict[str, Any]]) -> str | None:
    best_name = None
    best_mae = None
    for name, report in model_reports.items():
        mae = report.get("mae")
        if not isinstance(mae, (int, float)):
            continue
        if best_mae is None or mae < best_mae:
            best_mae = mae
            best_name = name
    return best_name


def _to_decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def _load_binance_window(builder, path: Path | None, start_ts: int | None, end_ts: int | None) -> list[dict[str, Any]]:
    return _load_jsonl_window(
        path=path,
        start_ts=start_ts,
        end_ts=end_ts,
        ts_getter=lambda record: builder._safe_int(
            record.get("payload", {}).get("tick", {}).get("recv_ts_ms")
            or record.get("payload", {}).get("tick", {}).get("event_ts_ms")
        ),
        point_builder=lambda record: _build_binance_point(builder, record),
    )


def _load_chainlink_window(builder, path: Path | None, start_ts: int | None, end_ts: int | None) -> list[dict[str, Any]]:
    return _load_jsonl_window(
        path=path,
        start_ts=start_ts,
        end_ts=end_ts,
        ts_getter=lambda record: builder._safe_int(
            record.get("payload", {}).get("tick", {}).get("oracle_ts_ms")
            or record.get("payload", {}).get("tick", {}).get("recv_ts_ms")
        ),
        point_builder=lambda record: _build_chainlink_point(builder, record),
    )


def _build_binance_point(builder, record: dict[str, Any]) -> dict[str, Any] | None:
    tick = record.get("payload", {}).get("tick")
    if not isinstance(tick, dict):
        return None
    bid = builder._safe_float(tick.get("best_bid"))
    ask = builder._safe_float(tick.get("best_ask"))
    return {
        "ts": builder._safe_int(tick.get("recv_ts_ms") or tick.get("event_ts_ms")),
        "last": builder._safe_float(tick.get("last_price")),
        "bid": bid,
        "ask": ask,
        "mid": builder._mid(bid, ask),
    }


def _build_chainlink_point(builder, record: dict[str, Any]) -> dict[str, Any] | None:
    tick = record.get("payload", {}).get("tick")
    if not isinstance(tick, dict):
        return None
    return {
        "ts": builder._safe_int(tick.get("oracle_ts_ms") or tick.get("recv_ts_ms")),
        "price": builder._safe_float(tick.get("price")),
        "bid": builder._safe_float(tick.get("bid")),
        "ask": builder._safe_float(tick.get("ask")),
    }


def _load_jsonl_window(
    *,
    path: Path | None,
    start_ts: int | None,
    end_ts: int | None,
    ts_getter: Callable[[dict[str, Any]], int | None],
    point_builder: Callable[[dict[str, Any]], dict[str, Any] | None],
) -> list[dict[str, Any]]:
    if path is None or not path.exists():
        return []

    points: list[dict[str, Any]] = []
    with path.open("rb") as handle:
        start_offset = 0
        if isinstance(start_ts, int):
            start_offset = _find_record_offset_for_ts(handle, start_ts, ts_getter)

        handle.seek(start_offset)
        while True:
            raw_line = handle.readline()
            if not raw_line:
                break
            line = raw_line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            ts = ts_getter(record)
            if not isinstance(ts, int):
                continue
            if isinstance(start_ts, int) and ts < start_ts:
                continue
            if isinstance(end_ts, int) and ts > end_ts:
                break
            point = point_builder(record)
            if point is not None:
                points.append(point)
    return points


def _find_record_offset_for_ts(
    handle,
    target_ts: int,
    ts_getter: Callable[[dict[str, Any]], int | None],
) -> int:
    handle.seek(0, 2)
    file_size = handle.tell()
    if file_size <= 0:
        return 0

    low = 0
    high = file_size
    candidate = 0
    while low < high:
        mid = (low + high) // 2
        record_meta = _read_record_at_or_after(handle, mid)
        if record_meta is None:
            high = mid
            continue
        record_offset, next_offset, record = record_meta
        ts = ts_getter(record)
        if not isinstance(ts, int):
            low = next_offset
            continue
        if ts < target_ts:
            low = next_offset
            continue
        candidate = record_offset
        high = mid
    return candidate


def _read_record_at_or_after(handle, offset: int) -> tuple[int, int, dict[str, Any]] | None:
    handle.seek(0, 2)
    file_size = handle.tell()
    if offset >= file_size:
        return None

    handle.seek(offset)
    if offset > 0:
        handle.readline()
    while True:
        record_offset = handle.tell()
        raw_line = handle.readline()
        if not raw_line:
            return None
        next_offset = handle.tell()
        line = raw_line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        return record_offset, next_offset, record


if __name__ == "__main__":
    main()
