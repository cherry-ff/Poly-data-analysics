from __future__ import annotations

import argparse
import json
from bisect import bisect_right
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RECORDS_ROOT = PROJECT_ROOT / "runtime_data" / "records"
DEFAULT_OUTPUT_PATH = PROJECT_ROOT / "dashboard" / "data" / "market-dashboard.json"
GLOBAL_MAX_POINTS = 900
MARKET_MAX_POINTS = 720
EXPECTED_MARKET_DURATION_MS = 15 * 60_000
MIN_REASONABLE_MARKET_DURATION_MS = 12 * 60_000
MAX_REASONABLE_MARKET_DURATION_MS = 20 * 60_000


def main() -> None:
    args = _parse_args()
    records_root = Path(args.records_root).resolve()
    output_path = Path(args.output).resolve()
    payload = build_dashboard_payload(records_root)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "records_root": str(records_root),
                "output": str(output_path),
                "market_count": len(payload["market_order"]),
            },
            indent=2,
        )
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a frontend-friendly JSON payload from runtime_data/records.",
    )
    parser.add_argument(
        "--records-root",
        default=str(DEFAULT_RECORDS_ROOT),
        help="Path to runtime_data/records",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help="Output JSON path for the dashboard",
    )
    return parser.parse_args()


def build_dashboard_payload(records_root: Path) -> dict[str, Any]:
    global_files, market_files = _discover_record_files(records_root)

    binance_raw = _load_binance_series(global_files.get("feeds.binance.tick"))
    chainlink_raw = _load_chainlink_series(global_files.get("feeds.chainlink.tick"))
    basis_raw = _align_basis_series(binance_raw["raw"], chainlink_raw["raw"])

    markets: dict[str, dict[str, Any]] = {}
    market_rows: list[tuple[str, dict[str, Any]]] = []

    for market_id, files in sorted(
        market_files.items(),
        key=lambda item: _safe_int(item[0]),
    ):
        metadata = _load_market_metadata(files.get("market.metadata"), market_id)
        market = _build_market_payload(
            market_id=market_id,
            files=files,
            metadata=metadata,
            binance_raw=binance_raw["raw"],
            chainlink_raw=chainlink_raw["raw"],
        )
        if not _market_has_dashboard_data(market):
            continue
        markets[market_id] = market
        market_rows.append((market_id, market))

    market_rows.sort(
        key=lambda item: _market_sort_key(item[0], item[1]),
        reverse=True,
    )
    market_order = [market_id for market_id, _ in market_rows]

    generated_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    return {
        "generated_at": generated_at,
        "records_root": str(records_root),
        "market_order": market_order,
        "global": {
            "binance": _global_payload(binance_raw, GLOBAL_MAX_POINTS),
            "chainlink": _global_payload(chainlink_raw, GLOBAL_MAX_POINTS),
            "basis": {
                "count": len(basis_raw),
                "latest": basis_raw[-1] if basis_raw else None,
                "series": _downsample(basis_raw, GLOBAL_MAX_POINTS),
            },
        },
        "markets": markets,
    }


def _discover_record_files(
    records_root: Path,
) -> tuple[dict[str, list[Path]], dict[str, dict[str, list[Path]]]]:
    global_files: dict[str, list[Path]] = {}
    market_files: dict[str, dict[str, list[Path]]] = {}

    _collect_segmented_global_files(records_root / "sealed" / "global", global_files)
    _collect_segmented_global_files(records_root / "global", global_files)
    _collect_live_global_files(records_root / "global", global_files)

    _collect_segmented_market_files(records_root / "sealed" / "markets", market_files)
    _collect_segmented_market_files(records_root / "markets", market_files)
    _collect_live_market_files(records_root / "markets", market_files)

    if global_files or market_files:
        return global_files, market_files

    legacy_dir = records_root / "_legacy_flat"
    if legacy_dir.exists():
        for path in legacy_dir.glob("*.jsonl"):
            topic = _topic_from_filename(path.name)
            if topic.startswith("feeds.binance") or topic.startswith("feeds.chainlink"):
                _append_stream_path(global_files, topic, path)

        for path in legacy_dir.glob("*.jsonl"):
            topic = _topic_from_filename(path.name)
            for record in _read_jsonl(path):
                market_id = str(record.get("market_id") or record.get("payload", {}).get("market_id") or "")
                if not market_id.isdigit():
                    continue
                market_files.setdefault(market_id, {})
                _append_stream_path(market_files[market_id], topic, path)
        return global_files, market_files

    return global_files, market_files


def _collect_live_global_files(global_dir: Path, global_files: dict[str, list[Path]]) -> None:
    if not global_dir.exists():
        return
    for path in sorted(global_dir.glob("*.jsonl")):
        _append_stream_path(global_files, _topic_from_filename(path.name), path)


def _collect_segmented_global_files(global_dir: Path, global_files: dict[str, list[Path]]) -> None:
    if not global_dir.exists():
        return
    for topic_dir in sorted(global_dir.iterdir()):
        if not topic_dir.is_dir():
            continue
        topic = _topic_from_filename(topic_dir.name)
        for path in sorted(topic_dir.glob("*.jsonl")):
            _append_stream_path(global_files, topic, path)


def _collect_live_market_files(
    markets_dir: Path,
    market_files: dict[str, dict[str, list[Path]]],
) -> None:
    if not markets_dir.exists():
        return
    for market_dir in sorted(markets_dir.iterdir()):
        if not market_dir.is_dir() or not market_dir.name.isdigit():
            continue
        entries = market_files.setdefault(market_dir.name, {})
        for path in sorted(market_dir.glob("*.jsonl")):
            _append_stream_path(entries, _topic_from_filename(path.name), path)
        if not entries:
            market_files.pop(market_dir.name, None)


def _collect_segmented_market_files(
    markets_dir: Path,
    market_files: dict[str, dict[str, list[Path]]],
) -> None:
    if not markets_dir.exists():
        return
    for market_dir in sorted(markets_dir.iterdir()):
        if not market_dir.is_dir() or not market_dir.name.isdigit():
            continue
        entries = market_files.setdefault(market_dir.name, {})
        for topic_dir in sorted(market_dir.iterdir()):
            if not topic_dir.is_dir():
                continue
            topic = _topic_from_filename(topic_dir.name)
            for path in sorted(topic_dir.glob("*.jsonl")):
                _append_stream_path(entries, topic, path)
        if not entries:
            market_files.pop(market_dir.name, None)


def _append_stream_path(mapping: dict[str, list[Path]], topic: str, path: Path) -> None:
    mapping.setdefault(topic, []).append(path)


def _market_sort_key(market_id: str, market: dict[str, Any]) -> tuple[int, int, int, int]:
    metadata = market.get("metadata") or {}
    window = market.get("window") or {}
    summary = market.get("summary") or {}
    start_ts = _safe_int(
        window.get("start_ts")
        or metadata.get("start_ts_ms")
    ) or 0
    end_ts = _safe_int(
        window.get("end_ts")
        or metadata.get("end_ts_ms")
    ) or 0
    observed_end_ts = _safe_int(summary.get("observed_end_ts")) or 0
    market_numeric_id = _safe_int(market_id) or 0
    return (start_ts, end_ts, observed_end_ts, market_numeric_id)


def _build_market_payload(
    *,
    market_id: str,
    files: dict[str, list[Path]],
    metadata: dict[str, Any] | None,
    binance_raw: list[dict[str, Any]],
    chainlink_raw: list[dict[str, Any]],
    market_max_points: int | None = MARKET_MAX_POINTS,
) -> dict[str, Any]:
    lifecycle = _load_lifecycle(files.get("market.lifecycle.transition"), market_id)
    theo = _load_theo_series(files.get("pricing.theo"), market_id)
    quote = _load_quote_series(files.get("pricing.quote_plan"), market_id)
    pair_book = _load_pair_book_series(
        files.get("feeds.polymarket.market.book_top"),
        metadata,
        market_id,
    )
    latest_depth = _load_latest_depth(
        files.get("feeds.polymarket.market.depth"),
        metadata,
        market_id,
    )

    raw_latest_ts = _latest_market_timestamp(
        lifecycle,
        theo["latest"],
        quote["latest"],
        pair_book["latest"],
    )
    market_window = _build_market_window(metadata, raw_latest_ts)
    lifecycle_series = _clip_series_to_window(
        lifecycle["series"],
        market_window["start_ts"],
        market_window["end_ts"],
    )
    theo_series = _clip_series_to_window(
        theo["series"],
        market_window["start_ts"],
        market_window["end_ts"],
    )
    quote_series = _clip_series_to_window(
        quote["series"],
        market_window["start_ts"],
        market_window["end_ts"],
    )
    pair_series = _clip_series_to_window(
        pair_book["series"],
        market_window["start_ts"],
        market_window["end_ts"],
    )
    latest_theo = theo_series[-1] if theo_series else None
    latest_quote = quote_series[-1] if quote_series else None
    latest_pair = pair_series[-1] if pair_series else None
    latest_ts = _latest_market_timestamp(
        {"series": lifecycle_series},
        latest_theo,
        latest_quote,
        latest_pair,
    )
    market_window = _build_market_window(metadata, latest_ts)
    latest_binance = _latest_at_or_before(binance_raw, latest_ts)
    latest_chainlink = _latest_at_or_before(chainlink_raw, latest_ts)
    latest_basis = None
    if latest_binance and latest_chainlink:
        latest_basis = round(latest_binance["mid"] - latest_chainlink["price"], 4)

    summary = {
        "latest_phase": lifecycle["latest_phase"],
        "progress_pct": _progress_pct(market_window),
        "duration_minutes": market_window["duration_minutes"],
        "observed_end_ts": latest_ts,
        "latest_binance_mid": latest_binance["mid"] if latest_binance else None,
        "latest_chainlink_price": latest_chainlink["price"] if latest_chainlink else None,
        "latest_basis": latest_basis,
        "latest_theo_up": latest_theo["theo_up"] if latest_theo else None,
        "latest_theo_down": latest_theo["theo_down"] if latest_theo else None,
        "latest_sigma_short": latest_theo["sigma_short"] if latest_theo else None,
        "latest_target_full_set_cost": latest_theo["target_full_set_cost"] if latest_theo else None,
        "latest_sum_best_bid": latest_pair["sum_best_bid"] if latest_pair else None,
        "latest_sum_best_ask": latest_pair["sum_best_ask"] if latest_pair else None,
        "latest_quote_bid_sum": latest_quote["quote_bid_sum"] if latest_quote else None,
        "latest_quote_ask_sum": latest_quote["quote_ask_sum"] if latest_quote else None,
        "latest_quote_vs_target": _subtract_nullable(
            latest_theo["target_full_set_cost"] if latest_theo else None,
            latest_quote["quote_bid_sum"] if latest_quote else None,
        ),
        "latest_market_edge": _subtract_nullable(
            1.0,
            latest_pair["sum_best_ask"] if latest_pair else None,
        ),
        "event_counts": {
            "lifecycle": len(lifecycle_series),
            "theo": len(theo_series),
            "quote": len(quote_series),
            "pair_book": len(pair_series),
            "depth": latest_depth["count"],
        },
    }

    return {
        "metadata": metadata,
        "window": market_window,
        "summary": summary,
        "lifecycle": lifecycle_series,
        "latest_depth": latest_depth["latest"],
        "series": {
            "theo": _maybe_downsample(theo_series, market_max_points),
            "quote": _maybe_downsample(quote_series, market_max_points),
            "pair_book": _maybe_downsample(pair_series, market_max_points),
        },
    }


def _global_payload(series_bundle: dict[str, Any], max_points: int) -> dict[str, Any]:
    raw = series_bundle["raw"]
    return {
        "count": len(raw),
        "latest": raw[-1] if raw else None,
        "series": _downsample(raw, max_points),
    }


def _load_market_metadata(paths: list[Path] | Path | None, market_id: str) -> dict[str, Any] | None:
    if not _normalize_stream_paths(paths):
        return None

    latest = None
    for record in _iter_stream_records(paths):
        if not _record_market_matches(record, market_id):
            continue
        market = record.get("payload", {}).get("market")
        if not isinstance(market, dict):
            continue
        latest = {
            "market_id": str(market.get("market_id") or ""),
            "condition_id": str(market.get("condition_id") or ""),
            "up_token_id": str(market.get("up_token_id") or ""),
            "down_token_id": str(market.get("down_token_id") or ""),
            "start_ts_ms": _safe_int(market.get("start_ts_ms")),
            "end_ts_ms": _safe_int(market.get("end_ts_ms")),
            "tick_size": _safe_float(market.get("tick_size")),
            "fee_rate_bps": _safe_float(market.get("fee_rate_bps")),
            "min_order_size": _safe_float(market.get("min_order_size")),
            "status": str(market.get("status") or ""),
            "reference_price": _normalized_reference_price(market.get("reference_price")),
            "raw_reference_price": _safe_float(market.get("reference_price")),
        }
    return latest


def _normalized_reference_price(value: Any) -> float | None:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    if 10_000 <= abs(numeric) <= 1_000_000:
        return numeric
    return None


def _load_lifecycle(paths: list[Path] | Path | None, market_id: str) -> dict[str, Any]:
    if not _normalize_stream_paths(paths):
        return {"count": 0, "series": [], "latest_phase": None}

    series: list[dict[str, Any]] = []
    latest_phase = None
    for record in _iter_stream_records(paths):
        if not _record_market_matches(record, market_id):
            continue
        transition = record.get("payload", {}).get("transition")
        if not isinstance(transition, dict):
            continue
        latest_phase = str(transition.get("new_phase") or "")
        series.append(
            {
                "ts": _safe_int(transition.get("ts_ms")),
                "phase": latest_phase,
                "previous_phase": str(transition.get("previous_phase") or ""),
            }
        )
    return {"count": len(series), "series": series, "latest_phase": latest_phase}


def _load_theo_series(paths: list[Path] | Path | None, market_id: str) -> dict[str, Any]:
    if not _normalize_stream_paths(paths):
        return {"count": 0, "series": [], "latest": None}

    series: list[dict[str, Any]] = []
    latest = None
    for record in _iter_stream_records(paths):
        if not _record_market_matches(record, market_id):
            continue
        snapshot = record.get("payload", {}).get("snapshot")
        if not isinstance(snapshot, dict):
            continue
        latest = {
            "ts": _safe_int(snapshot.get("ts_ms")),
            "theo_up": _safe_float(snapshot.get("theo_up")),
            "theo_down": _safe_float(snapshot.get("theo_down")),
            "sigma_short": _safe_float(snapshot.get("sigma_short")),
            "directional_bias": _safe_float(snapshot.get("directional_bias")),
            "target_full_set_cost": _safe_float(snapshot.get("target_full_set_cost")),
        }
        series.append(latest)
    series = _compress_series(
        series,
        identity_keys=[
            "theo_up",
            "theo_down",
            "sigma_short",
            "directional_bias",
            "target_full_set_cost",
        ],
    )
    latest = series[-1] if series else latest
    return {"count": len(series), "series": series, "latest": latest}


def _load_quote_series(paths: list[Path] | Path | None, market_id: str) -> dict[str, Any]:
    if not _normalize_stream_paths(paths):
        return {"count": 0, "series": [], "latest": None}

    series: list[dict[str, Any]] = []
    latest = None
    for record in _iter_stream_records(paths):
        if not _record_market_matches(record, market_id):
            continue
        plan = record.get("payload", {}).get("plan")
        if not isinstance(plan, dict):
            continue
        up_bid = _safe_float(plan.get("up_bid_px"))
        down_bid = _safe_float(plan.get("down_bid_px"))
        up_ask = _safe_float(plan.get("up_ask_px"))
        down_ask = _safe_float(plan.get("down_ask_px"))
        latest = {
            "ts": _safe_int(plan.get("ts_ms")),
            "up_bid": up_bid,
            "up_ask": up_ask,
            "down_bid": down_bid,
            "down_ask": down_ask,
            "quote_bid_sum": _sum_nullable(up_bid, down_bid),
            "quote_ask_sum": _sum_nullable(up_ask, down_ask),
            "reason": str(plan.get("reason") or ""),
        }
        series.append(latest)
    series = _compress_series(
        series,
        identity_keys=[
            "up_bid",
            "up_ask",
            "down_bid",
            "down_ask",
            "quote_bid_sum",
            "quote_ask_sum",
            "reason",
        ],
    )
    latest = series[-1] if series else latest
    return {"count": len(series), "series": series, "latest": latest}


def _load_pair_book_series(
    paths: list[Path] | Path | None,
    metadata: dict[str, Any] | None,
    market_id: str,
) -> dict[str, Any]:
    if not _normalize_stream_paths(paths) or metadata is None:
        return {"count": 0, "series": [], "latest": None}

    up_token_id = metadata.get("up_token_id")
    down_token_id = metadata.get("down_token_id")
    rows_by_ts: dict[int, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))

    for record in _iter_stream_records(paths):
        if not _record_market_matches(record, market_id):
            continue
        top = record.get("payload", {}).get("top")
        if not isinstance(top, dict):
            continue

        token_id = str(top.get("token_id") or "")
        rows_by_ts[_safe_int(top.get("last_update_ts_ms"))][token_id].append(
            {
                "bid": _safe_float(top.get("best_bid_px")),
                "ask": _safe_float(top.get("best_ask_px")),
                "bid_size": _safe_float(top.get("best_bid_sz")),
                "ask_size": _safe_float(top.get("best_ask_sz")),
            }
        )

    series: list[dict[str, Any]] = []
    for ts in sorted(rows_by_ts):
        bucket = rows_by_ts[ts]
        if up_token_id not in bucket or down_token_id not in bucket:
            continue

        up = _best_top_candidate(bucket[up_token_id])
        down = _best_top_candidate(bucket[down_token_id])
        if up is None or down is None:
            continue
        if _is_placeholder_quote(up) or _is_placeholder_quote(down):
            continue

        snapshot = {
            "ts": ts,
            "up_bid": up["bid"],
            "up_ask": up["ask"],
            "up_mid": _mid(up["bid"], up["ask"]),
            "down_bid": down["bid"],
            "down_ask": down["ask"],
            "down_mid": _mid(down["bid"], down["ask"]),
            "sum_best_bid": _sum_nullable(up["bid"], down["bid"]),
            "sum_best_ask": _sum_nullable(up["ask"], down["ask"]),
            "up_bid_size": up["bid_size"],
            "up_ask_size": up["ask_size"],
            "down_bid_size": down["bid_size"],
            "down_ask_size": down["ask_size"],
        }
        if series and _same_pair_snapshot(series[-1], snapshot):
            continue
        series.append(snapshot)

    latest = series[-1] if series else None
    return {"count": len(series), "series": series, "latest": latest}


def _same_pair_snapshot(left: dict[str, Any], right: dict[str, Any]) -> bool:
    for key in (
        "up_bid",
        "up_ask",
        "down_bid",
        "down_ask",
        "sum_best_bid",
        "sum_best_ask",
    ):
        if left.get(key) != right.get(key):
            return False
    return True


def _load_latest_depth(
    paths: list[Path] | Path | None,
    metadata: dict[str, Any] | None,
    market_id: str,
) -> dict[str, Any]:
    if not _normalize_stream_paths(paths) or metadata is None:
        return {"count": 0, "latest": None}

    up_token_id = metadata.get("up_token_id")
    down_token_id = metadata.get("down_token_id")
    latest: dict[str, Any] = {
        "up": {"bids": [], "asks": [], "ts": None},
        "down": {"bids": [], "asks": [], "ts": None},
    }
    count = 0

    for record in _iter_stream_records(paths):
        if not _record_market_matches(record, market_id):
            continue
        snapshot = record.get("payload", {}).get("snapshot")
        if not isinstance(snapshot, dict):
            continue
        token_id = str(snapshot.get("token_id") or "")
        side = "up" if token_id == up_token_id else "down" if token_id == down_token_id else None
        if side is None:
            continue
        count += 1
        latest[side] = {
            "ts": _safe_int(snapshot.get("last_update_ts_ms")),
            "bids": _normalize_levels(snapshot.get("bids")),
            "asks": _normalize_levels(snapshot.get("asks")),
        }

    if count == 0:
        return {"count": 0, "latest": None}
    return {"count": count, "latest": latest}


def _load_binance_series(paths: list[Path] | Path | None) -> dict[str, Any]:
    if not _normalize_stream_paths(paths):
        return {"raw": []}

    raw: list[dict[str, Any]] = []
    for record in _iter_stream_records(paths):
        tick = record.get("payload", {}).get("tick")
        if not isinstance(tick, dict):
            continue
        bid = _safe_float(tick.get("best_bid"))
        ask = _safe_float(tick.get("best_ask"))
        raw.append(
            {
                "ts": _safe_int(tick.get("recv_ts_ms") or tick.get("event_ts_ms")),
                "last": _safe_float(tick.get("last_price")),
                "bid": bid,
                "ask": ask,
                "mid": _mid(bid, ask),
            }
        )
    return {"raw": raw}


def _load_chainlink_series(paths: list[Path] | Path | None) -> dict[str, Any]:
    if not _normalize_stream_paths(paths):
        return {"raw": []}

    raw: list[dict[str, Any]] = []
    for record in _iter_stream_records(paths):
        tick = record.get("payload", {}).get("tick")
        if not isinstance(tick, dict):
            continue
        raw.append(
            {
                "ts": _safe_int(tick.get("oracle_ts_ms") or tick.get("recv_ts_ms")),
                "price": _safe_float(tick.get("price")),
                "bid": _safe_float(tick.get("bid")),
                "ask": _safe_float(tick.get("ask")),
            }
        )
    return {"raw": raw}


def _align_basis_series(
    binance_points: list[dict[str, Any]],
    chainlink_points: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not binance_points or not chainlink_points:
        return []

    binance_ts = [point["ts"] for point in binance_points]
    aligned: list[dict[str, Any]] = []
    for point in chainlink_points:
        idx = bisect_right(binance_ts, point["ts"]) - 1
        if idx < 0:
            continue
        binance = binance_points[idx]
        if binance.get("mid") is None or point.get("price") is None:
            continue
        aligned.append(
            {
                "ts": point["ts"],
                "binance_mid": binance["mid"],
                "chainlink_price": point["price"],
                "basis": round(binance["mid"] - point["price"], 4),
            }
        )
    return aligned


def _latest_market_timestamp(
    lifecycle: dict[str, Any],
    latest_theo: dict[str, Any] | None,
    latest_quote: dict[str, Any] | None,
    latest_pair: dict[str, Any] | None,
) -> int | None:
    candidates = [
        lifecycle["series"][-1]["ts"] if lifecycle["series"] else None,
        latest_theo["ts"] if latest_theo else None,
        latest_quote["ts"] if latest_quote else None,
        latest_pair["ts"] if latest_pair else None,
    ]
    values = [value for value in candidates if isinstance(value, int) and value > 0]
    return max(values) if values else None


def _build_market_window(metadata: dict[str, Any] | None, latest_ts: int | None) -> dict[str, Any]:
    if metadata is None:
        return {
            "start_ts": None,
            "end_ts": latest_ts,
            "duration_minutes": None,
            "latest_ts": latest_ts,
            "has_valid_bounds": False,
        }

    start_ts = metadata.get("start_ts_ms")
    end_ts = metadata.get("end_ts_ms")
    if not _has_reasonable_market_bounds(start_ts, end_ts):
        return {
            "start_ts": None,
            "end_ts": None,
            "duration_minutes": None,
            "latest_ts": latest_ts,
            "has_valid_bounds": False,
        }
    duration_minutes = None
    if isinstance(start_ts, int) and isinstance(end_ts, int) and end_ts > start_ts:
        duration_minutes = round((end_ts - start_ts) / 60_000, 2)
    return {
        "start_ts": start_ts,
        "end_ts": end_ts,
        "duration_minutes": duration_minutes,
        "latest_ts": latest_ts,
        "has_valid_bounds": True,
    }


def _has_reasonable_market_bounds(
    start_ts: int | None,
    end_ts: int | None,
) -> bool:
    if isinstance(start_ts, int) and start_ts <= 0:
        start_ts = None
    if isinstance(end_ts, int) and end_ts <= 0:
        end_ts = None

    if isinstance(start_ts, int) and isinstance(end_ts, int) and end_ts > start_ts:
        duration_ms = end_ts - start_ts
        if MIN_REASONABLE_MARKET_DURATION_MS <= duration_ms <= MAX_REASONABLE_MARKET_DURATION_MS:
            return True
    return False


def _clip_series_to_window(
    series: list[dict[str, Any]],
    start_ts: int | None,
    end_ts: int | None,
) -> list[dict[str, Any]]:
    if not series:
        return []
    if not isinstance(start_ts, int) or not isinstance(end_ts, int) or end_ts <= start_ts:
        return list(series)
    return [
        item
        for item in series
        if isinstance(item.get("ts"), int) and start_ts <= item["ts"] <= end_ts
    ]


def _progress_pct(window: dict[str, Any]) -> float | None:
    start_ts = window.get("start_ts")
    end_ts = window.get("end_ts")
    latest_ts = window.get("latest_ts")
    if not all(isinstance(value, int) for value in (start_ts, end_ts, latest_ts)):
        return None
    if end_ts <= start_ts:
        return None
    progress = (latest_ts - start_ts) / (end_ts - start_ts)
    return round(max(0.0, min(1.0, progress)) * 100, 2)


def _latest_at_or_before(points: list[dict[str, Any]], ts: int | None) -> dict[str, Any] | None:
    if ts is None or not points:
        return points[-1] if points else None
    timestamps = [point["ts"] for point in points]
    idx = bisect_right(timestamps, ts) - 1
    if idx < 0:
        return None
    return points[idx]


def _normalize_levels(levels: Any) -> list[dict[str, float]]:
    if not isinstance(levels, list):
        return []
    normalized: list[dict[str, float]] = []
    for level in levels[:5]:
        if not isinstance(level, dict):
            continue
        normalized.append(
            {
                "price": _safe_float(level.get("price")) or 0.0,
                "size": _safe_float(level.get("size")) or 0.0,
            }
        )
    return normalized


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return records


def _iter_stream_records(paths: list[Path] | Path | None) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in _normalize_stream_paths(paths):
        records.extend(_read_jsonl(path))
    return records


def _normalize_stream_paths(paths: list[Path] | Path | None) -> list[Path]:
    if paths is None:
        return []
    if isinstance(paths, Path):
        return [paths] if paths.exists() else []
    return [path for path in paths if path.exists()]


def _downsample(points: list[dict[str, Any]], max_points: int) -> list[dict[str, Any]]:
    if len(points) <= max_points:
        return points
    if max_points <= 2:
        return [points[0], points[-1]]

    step = (len(points) - 1) / (max_points - 1)
    result: list[dict[str, Any]] = []
    seen_indexes: set[int] = set()
    for i in range(max_points):
        idx = round(i * step)
        if idx >= len(points):
            idx = len(points) - 1
        if idx in seen_indexes:
            continue
        seen_indexes.add(idx)
        result.append(points[idx])
    if result[-1] is not points[-1]:
        result[-1] = points[-1]
    return result


def _maybe_downsample(points: list[dict[str, Any]], max_points: int | None) -> list[dict[str, Any]]:
    if max_points is None:
        return points
    return _downsample(points, max_points)


def _topic_from_filename(filename: str) -> str:
    stem = filename[:-6] if filename.endswith(".jsonl") else filename
    mapping = {
        "feeds_binance_tick": "feeds.binance.tick",
        "feeds_binance_depth": "feeds.binance.depth",
        "feeds_chainlink_tick": "feeds.chainlink.tick",
        "feeds_polymarket_market_new_market": "feeds.polymarket.market.new_market",
        "feeds_polymarket_market_book_top": "feeds.polymarket.market.book_top",
        "feeds_polymarket_market_depth": "feeds.polymarket.market.depth",
        "pricing_theo": "pricing.theo",
        "pricing_quote_plan": "pricing.quote_plan",
        "market_metadata": "market.metadata",
        "market_lifecycle_transition": "market.lifecycle.transition",
    }
    return mapping.get(stem, stem.replace("_", "."))


def _record_market_matches(record: dict[str, Any], market_id: str) -> bool:
    raw_market_id = str(record.get("market_id") or "")
    if raw_market_id:
        return raw_market_id == market_id
    payload_market_id = str(record.get("payload", {}).get("market_id") or "")
    if payload_market_id:
        return payload_market_id == market_id
    return True


def _market_has_dashboard_data(market: dict[str, Any]) -> bool:
    window = market.get("window", {})
    if not isinstance(window, dict) or not window.get("has_valid_bounds"):
        return False
    counts = market.get("summary", {}).get("event_counts", {})
    if not isinstance(counts, dict):
        return False
    return any(
        _safe_int(counts.get(key)) > 0
        for key in ("quote", "pair_book", "depth")
    )


def _best_top_candidate(items: list[dict[str, Any]]) -> dict[str, Any] | None:
    candidates = [
        item
        for item in items
        if item.get("bid") is not None and item.get("ask") is not None
    ]
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda item: (
            round((item["ask"] or 0.0) - (item["bid"] or 0.0), 6),
            -((item.get("bid_size") or 0.0) + (item.get("ask_size") or 0.0)),
        ),
    )


def _is_placeholder_quote(item: dict[str, Any]) -> bool:
    bid = item.get("bid")
    ask = item.get("ask")
    if bid is None or ask is None:
        return True
    spread = ask - bid
    return bid <= 0.011 and ask >= 0.989 and spread >= 0.95


def _compress_series(
    series: list[dict[str, Any]],
    *,
    identity_keys: list[str],
) -> list[dict[str, Any]]:
    if len(series) <= 2:
        return series

    compressed: list[dict[str, Any]] = [series[0]]
    for item in series[1:-1]:
        if _series_signature(item, identity_keys) == _series_signature(compressed[-1], identity_keys):
            continue
        compressed.append(item)
    compressed.append(series[-1])
    return compressed


def _series_signature(item: dict[str, Any], keys: list[str]) -> tuple[Any, ...]:
    return tuple(item.get(key) for key in keys)


def _safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return round(float(value), 6)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _sum_nullable(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return round(left + right, 6)


def _subtract_nullable(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return round(left - right, 6)


def _mid(bid: float | None, ask: float | None) -> float | None:
    if bid is None or ask is None:
        return None
    return round((bid + ask) / 2, 6)


if __name__ == "__main__":
    main()
