from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from bisect import bisect_left, bisect_right
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from time import perf_counter
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RECORDS_ROOT = PROJECT_ROOT / "runtime_data" / "records"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "runtime_data" / "research" / "events"
BUILD_SCRIPT_PATH = PROJECT_ROOT / "scripts" / "build_dashboard_data.py"

EVENT_FAST_CLOSE_ENTER = "market_fast_close_enter"
EVENT_ADJACENT_HANDOFF = "adjacent_market_handoff"
EVENT_BINANCE_IMPULSE_WITHOUT_REFRESH = "binance_impulse_without_poly_refresh"
EVENT_ONE_SIDED_DEPTH_DEPLETION = "one_sided_depth_depletion"
EVENT_DEPTH_REFILL_AFTER_GAP = "depth_refill_after_gap"

EVENT_TYPES = {
    EVENT_FAST_CLOSE_ENTER,
    EVENT_ADJACENT_HANDOFF,
    EVENT_BINANCE_IMPULSE_WITHOUT_REFRESH,
    EVENT_ONE_SIDED_DEPTH_DEPLETION,
    EVENT_DEPTH_REFILL_AFTER_GAP,
}

DEFAULT_HORIZONS_MS = [1_000, 3_000, 10_000, 30_000]
DEFAULT_ACTIVE_BINANCE_IMPULSE_BPS = 5.0
DEFAULT_POLY_REFRESH_LAG_MS = 3_000
DEFAULT_EVENT_COOLDOWN_MS = 3_000
DEFAULT_DEPTH_LOOKBACK_MS = 3_000
DEFAULT_DEPTH_DROP_RATIO = 0.35
DEFAULT_DEPTH_MIN_DROP_SIZE = 50.0
DEFAULT_DEPTH_OPPOSITE_STABLE_RATIO = 0.15
DEFAULT_DEPTH_REFILL_WINDOW_MS = 10_000
DEFAULT_DEPTH_REFILL_RATIO = 0.6
FAST_CLOSE_WINDOW_MS = 30_000
DEFAULT_BATCH_SIZE = 50
LOAD_CONTEXT_PROGRESS_EVERY = 50


def _load_builder_module():
    spec = importlib.util.spec_from_file_location("build_dashboard_data", BUILD_SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load dashboard builder from {BUILD_SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@dataclass(slots=True)
class SeriesIndex:
    points: list[dict[str, Any]]
    timestamps: list[int]

    @classmethod
    def from_points(cls, points: list[dict[str, Any]]) -> "SeriesIndex":
        filtered = [
            point
            for point in points
            if isinstance(point.get("ts"), int) and point["ts"] > 0
        ]
        filtered.sort(key=lambda item: item["ts"])
        return cls(points=filtered, timestamps=[point["ts"] for point in filtered])

    def at_or_before(self, ts: int | None) -> dict[str, Any] | None:
        if ts is None or not self.points:
            return self.points[-1] if self.points else None
        idx = bisect_right(self.timestamps, ts) - 1
        if idx < 0:
            return None
        return self.points[idx]

    def first_after(self, ts: int) -> dict[str, Any] | None:
        if not self.points:
            return None
        idx = bisect_right(self.timestamps, ts)
        if idx >= len(self.points):
            return None
        return self.points[idx]

    def between(self, start_ts: int, end_ts: int) -> list[dict[str, Any]]:
        if not self.points or end_ts < start_ts:
            return []
        left = bisect_left(self.timestamps, start_ts)
        right = bisect_right(self.timestamps, end_ts)
        return self.points[left:right]


@dataclass(slots=True)
class MarketContext:
    market_id: str
    files: dict[str, list[Path]]
    metadata: dict[str, Any]
    lifecycle: list[dict[str, Any]]
    pair: SeriesIndex
    theo: SeriesIndex
    quote: SeriesIndex
    depth_up: SeriesIndex
    depth_down: SeriesIndex

    @property
    def start_ts(self) -> int:
        return int(self.metadata.get("start_ts_ms") or 0)

    @property
    def end_ts(self) -> int:
        return int(self.metadata.get("end_ts_ms") or 0)

    @property
    def latest_ts(self) -> int:
        candidates = [
            self.pair.points[-1]["ts"] if self.pair.points else 0,
            self.theo.points[-1]["ts"] if self.theo.points else 0,
            self.quote.points[-1]["ts"] if self.quote.points else 0,
        ]
        return max(candidates)


@dataclass(slots=True)
class MarketDescriptor:
    market_id: str
    files: dict[str, list[Path]]
    metadata: dict[str, Any]

    @property
    def start_ts(self) -> int:
        return int(self.metadata.get("start_ts_ms") or 0)

    @property
    def end_ts(self) -> int:
        return int(self.metadata.get("end_ts_ms") or 0)


@dataclass(slots=True)
class TimeWindow:
    start_ts: int
    end_ts: int


@dataclass(slots=True)
class FileTimeRange:
    path: Path
    start_ts: int
    end_ts: int


@dataclass(slots=True)
class SummaryAccumulator:
    event_type: str
    count: int = 0
    unique_markets: set[str] = field(default_factory=set)
    phase_counts: Counter[str] = field(default_factory=Counter)
    metric_sums: dict[str, float] = field(default_factory=dict)
    metric_counts: dict[str, int] = field(default_factory=dict)
    directional_counts: Counter[str] = field(default_factory=Counter)
    directional_metric_sums: dict[tuple[str, str], float] = field(default_factory=dict)
    directional_metric_counts: dict[tuple[str, str], int] = field(default_factory=dict)

    def update(self, rows: list[dict[str, Any]], horizons_ms: list[int]) -> None:
        for row in rows:
            self.count += 1
            market_id = row.get("market_id")
            if market_id:
                self.unique_markets.add(str(market_id))
            self.phase_counts[row.get("phase") or "UNKNOWN"] += 1
            for key in _summary_metric_keys(horizons_ms):
                value = row.get(key)
                if not isinstance(value, (int, float)):
                    continue
                self.metric_sums[key] = self.metric_sums.get(key, 0.0) + float(value)
                self.metric_counts[key] = self.metric_counts.get(key, 0) + 1
            self._update_event_specific(row, horizons_ms)

    def build(self, horizons_ms: list[int]) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "event_type": self.event_type,
            "count": self.count,
            "unique_markets": len(self.unique_markets),
            "phase_counts": dict(self.phase_counts),
            "horizons": {},
        }
        for horizon_ms in horizons_ms:
            suffix = _horizon_label(horizon_ms)
            metrics: dict[str, float | None] = {}
            for key in _summary_metric_keys_for_suffix(suffix):
                count = self.metric_counts.get(key, 0)
                metrics[key] = _round(self.metric_sums[key] / count) if count > 0 else None
            summary["horizons"][suffix] = {
                "avg_up_mid_move": metrics[f"up_mid_move_{suffix}"],
                "avg_down_mid_move": metrics[f"down_mid_move_{suffix}"],
                "avg_sum_best_ask_move": metrics[f"sum_best_ask_move_{suffix}"],
                "avg_sum_best_bid_move": metrics[f"sum_best_bid_move_{suffix}"],
                "avg_binance_mid_move": metrics[f"binance_mid_move_{suffix}"],
                "avg_mfe_up_mid": metrics[f"mfe_up_mid_{suffix}"],
                "avg_mae_up_mid": metrics[f"mae_up_mid_{suffix}"],
                "avg_tradable_size": metrics[f"tradable_size_{suffix}"],
            }
        event_specific = self._build_event_specific_summary(horizons_ms)
        if event_specific:
            summary.update(event_specific)
        return summary

    def _update_event_specific(self, row: dict[str, Any], horizons_ms: list[int]) -> None:
        if self.event_type != EVENT_BINANCE_IMPULSE_WITHOUT_REFRESH:
            return
        direction = str(row.get("impulse_direction") or "")
        if not direction:
            return
        self.directional_counts[direction] += 1
        for key in _impulse_directional_metric_keys(horizons_ms):
            value = row.get(key)
            if not isinstance(value, (int, float)):
                continue
            metric_key = (direction, key)
            self.directional_metric_sums[metric_key] = self.directional_metric_sums.get(metric_key, 0.0) + float(value)
            self.directional_metric_counts[metric_key] = self.directional_metric_counts.get(metric_key, 0) + 1

    def _build_event_specific_summary(self, horizons_ms: list[int]) -> dict[str, Any]:
        if self.event_type != EVENT_BINANCE_IMPULSE_WITHOUT_REFRESH:
            return {}
        directional: dict[str, Any] = {}
        for direction in sorted(self.directional_counts):
            stats: dict[str, Any] = {
                "count": self.directional_counts[direction],
                "with_first_refresh_count": self.directional_metric_counts.get((direction, "poly_refresh_delay_ms"), 0),
            }
            for key in _impulse_directional_metric_keys(horizons_ms):
                metric_key = (direction, key)
                count = self.directional_metric_counts.get(metric_key, 0)
                stats[f"avg_{key}"] = (
                    _round(self.directional_metric_sums[metric_key] / count)
                    if count > 0
                    else None
                )
            directional[direction] = stats
        return {"directional": directional}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan event-driven trading opportunities from runtime_data/records.",
    )
    parser.add_argument(
        "--records-root",
        default=str(RECORDS_ROOT),
        help="Path to runtime_data/records",
    )
    parser.add_argument(
        "--event-type",
        default="all",
        choices=["all", *sorted(EVENT_TYPES)],
        help="Event type to scan. Use 'all' to emit one file per event type.",
    )
    parser.add_argument(
        "--market-id",
        action="append",
        dest="market_ids",
        default=[],
        help="Specific market id(s) to scan. Defaults to all markets.",
    )
    parser.add_argument(
        "--limit-markets",
        type=int,
        default=0,
        help="Optional cap on number of markets after selection and sorting.",
    )
    parser.add_argument(
        "--horizons",
        default="1000,3000,10000,30000",
        help="Comma-separated forward horizons in milliseconds.",
    )
    parser.add_argument(
        "--impulse-threshold-bps",
        type=float,
        default=DEFAULT_ACTIVE_BINANCE_IMPULSE_BPS,
        help="Binance 5s move threshold in bps for impulse event detection.",
    )
    parser.add_argument(
        "--poly-refresh-lag-ms",
        type=int,
        default=DEFAULT_POLY_REFRESH_LAG_MS,
        help="Required Polymarket refresh lag after a Binance impulse.",
    )
    parser.add_argument(
        "--cooldown-ms",
        type=int,
        default=DEFAULT_EVENT_COOLDOWN_MS,
        help="Cooldown for repeated event emission inside the same market.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Optional jsonl output path for a single event type.",
    )
    parser.add_argument(
        "--output-summary",
        default="",
        help="Optional summary JSON path for a single event type.",
    )
    parser.add_argument(
        "--depth-lookback-ms",
        type=int,
        default=DEFAULT_DEPTH_LOOKBACK_MS,
        help="Lookback window for depth depletion detection.",
    )
    parser.add_argument(
        "--depth-drop-ratio",
        type=float,
        default=DEFAULT_DEPTH_DROP_RATIO,
        help="Minimum relative depth drop to flag a depletion event.",
    )
    parser.add_argument(
        "--depth-min-drop-size",
        type=float,
        default=DEFAULT_DEPTH_MIN_DROP_SIZE,
        help="Minimum absolute depth drop to flag a depletion event.",
    )
    parser.add_argument(
        "--depth-opposite-stable-ratio",
        type=float,
        default=DEFAULT_DEPTH_OPPOSITE_STABLE_RATIO,
        help="Maximum absolute change ratio allowed on the opposite depth side.",
    )
    parser.add_argument(
        "--depth-refill-window-ms",
        type=int,
        default=DEFAULT_DEPTH_REFILL_WINDOW_MS,
        help="Maximum window after depletion to look for depth refill events.",
    )
    parser.add_argument(
        "--depth-refill-ratio",
        type=float,
        default=DEFAULT_DEPTH_REFILL_RATIO,
        help="Required fraction of lost depth that must refill to emit a refill event.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Number of markets to load and process per batch.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Disable progress logs and only emit the final JSON payload.",
    )
    return parser.parse_args()


def main() -> None:
    started_at = perf_counter()
    args = _parse_args()
    builder = _load_builder_module()
    records_root = Path(args.records_root).resolve()
    output_dir = DEFAULT_OUTPUT_DIR.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    horizons_ms = _parse_horizons(args.horizons)
    global_files, market_files = builder._discover_record_files(records_root)

    selected_market_ids = args.market_ids or sorted(market_files.keys(), key=builder._safe_int)
    if args.limit_markets > 0:
        selected_market_ids = selected_market_ids[: args.limit_markets]
    batch_size = max(1, int(args.batch_size))
    _log(
        f"discovered {len(market_files)} markets, selected {len(selected_market_ids)}; "
        f"event_type={args.event_type}; batch_size={batch_size}",
        quiet=args.quiet,
    )

    descriptors = _load_market_descriptors(
        builder,
        market_files,
        selected_market_ids,
        quiet=args.quiet,
    )
    ordered_descriptors = sorted(descriptors, key=lambda item: (item.start_ts, builder._safe_int(item.market_id)))
    _log(f"prepared {len(ordered_descriptors)} market descriptors", quiet=args.quiet)

    preload_ms = max(15_000, args.depth_lookback_ms)
    future_ms = max(horizons_ms + [args.depth_refill_window_ms, args.poly_refresh_lag_ms])
    requested_event_types = sorted(EVENT_TYPES) if args.event_type == "all" else [args.event_type]
    reports: dict[str, dict[str, Any]] = {}
    accumulators: dict[str, SummaryAccumulator] = {}
    for event_type in requested_event_types:
        output_path, summary_path = _resolve_output_paths(
            output_dir=output_dir,
            event_type=event_type,
            requested_output=args.output,
            requested_summary=args.output_summary,
            multi_event=len(requested_event_types) > 1,
        )
        output_path.write_text("", encoding="utf-8")
        reports[event_type] = {
            "count": 0,
            "output": str(output_path),
            "summary": str(summary_path),
        }
        accumulators[event_type] = SummaryAccumulator(event_type=event_type)

    total_batches = (len(ordered_descriptors) + batch_size - 1) // batch_size if ordered_descriptors else 0
    for batch_idx, descriptor_batch in enumerate(_chunked(ordered_descriptors, batch_size), start=1):
        batch_started_at = perf_counter()
        batch_descriptors = list(descriptor_batch)
        if not batch_descriptors:
            continue
        batch_start_index = (batch_idx - 1) * batch_size
        include_prev_overlap = EVENT_ADJACENT_HANDOFF in requested_event_types and batch_start_index > 0
        scan_descriptors = batch_descriptors
        if include_prev_overlap:
            scan_descriptors = [ordered_descriptors[batch_start_index - 1], *batch_descriptors]
        _log(
            f"processing batch {batch_idx}/{total_batches}: markets={len(batch_descriptors)} "
            f"with_overlap={1 if include_prev_overlap else 0}",
            quiet=args.quiet,
        )
        batch_contexts = _load_market_context_batch(builder, scan_descriptors, quiet=args.quiet)
        if not batch_contexts:
            _log(f"batch {batch_idx}/{total_batches} has no usable contexts", quiet=args.quiet)
            continue
        active_market_ids = {item.market_id for item in batch_descriptors}
        active_contexts = [context for context in batch_contexts if context.market_id in active_market_ids]
        external_windows = _build_external_windows(batch_contexts, preload_ms=preload_ms, future_ms=future_ms)
        _log(
            f"batch {batch_idx}/{total_batches}: built {len(external_windows)} external windows",
            quiet=args.quiet,
        )
        binance_index = _load_global_windowed_index(
            paths=global_files.get("feeds.binance.tick"),
            windows=external_windows,
            record_ts_extractor=_extract_binance_ts,
            point_extractor=_extract_binance_point,
            label=f"binance batch {batch_idx}",
            quiet=args.quiet,
        )
        chainlink_index = _load_global_windowed_index(
            paths=global_files.get("feeds.chainlink.tick"),
            windows=external_windows,
            record_ts_extractor=_extract_chainlink_ts,
            point_extractor=_extract_chainlink_point,
            label=f"chainlink batch {batch_idx}",
            quiet=args.quiet,
        )
        batch_contexts_by_id = {context.market_id: context for context in batch_contexts}
        active_contexts_by_id = {context.market_id: context for context in active_contexts}

        for event_type in requested_event_types:
            event_started_at = perf_counter()
            event_contexts = batch_contexts if event_type == EVENT_ADJACENT_HANDOFF else active_contexts
            event_contexts_by_id = batch_contexts_by_id if event_type == EVENT_ADJACENT_HANDOFF else active_contexts_by_id
            if not event_contexts:
                continue
            _log(
                f"batch {batch_idx}/{total_batches}: scanning {event_type} over {len(event_contexts)} contexts",
                quiet=args.quiet,
            )
            rows = _scan_events(
                builder=builder,
                event_type=event_type,
                ordered_contexts=event_contexts,
                contexts_by_id=event_contexts_by_id,
                binance_index=binance_index,
                chainlink_index=chainlink_index,
                horizons_ms=horizons_ms,
                impulse_threshold_bps=args.impulse_threshold_bps,
                poly_refresh_lag_ms=args.poly_refresh_lag_ms,
                cooldown_ms=args.cooldown_ms,
                depth_lookback_ms=args.depth_lookback_ms,
                depth_drop_ratio=args.depth_drop_ratio,
                depth_min_drop_size=args.depth_min_drop_size,
                depth_opposite_stable_ratio=args.depth_opposite_stable_ratio,
                depth_refill_window_ms=args.depth_refill_window_ms,
                depth_refill_ratio=args.depth_refill_ratio,
            )
            rows.sort(key=lambda row: (row.get("trigger_ts_ms") or 0, row.get("market_id") or ""))
            output_path = Path(reports[event_type]["output"])
            _append_jsonl(output_path, rows)
            accumulators[event_type].update(rows, horizons_ms)
            reports[event_type]["count"] += len(rows)
            _log(
                f"batch {batch_idx}/{total_batches}: finished {event_type} count={len(rows)} "
                f"elapsed={perf_counter() - event_started_at:.2f}s",
                quiet=args.quiet,
            )
        _log(
            f"finished batch {batch_idx}/{total_batches} elapsed={perf_counter() - batch_started_at:.2f}s",
            quiet=args.quiet,
        )

    for event_type in requested_event_types:
        summary = accumulators[event_type].build(horizons_ms)
        Path(reports[event_type]["summary"]).write_text(
            json.dumps(summary, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

    payload = {
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "records_root": str(records_root),
        "market_count": len(ordered_descriptors),
        "event_reports": reports,
    }
    _log(f"scan complete in {perf_counter() - started_at:.2f}s", quiet=args.quiet)
    print(json.dumps(payload, indent=2, ensure_ascii=False))


def _parse_horizons(raw: str) -> list[int]:
    values: list[int] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            value = int(chunk)
        except ValueError as exc:
            raise SystemExit(f"invalid horizon: {chunk}") from exc
        if value <= 0:
            raise SystemExit(f"horizon must be positive: {chunk}")
        values.append(value)
    if not values:
        return list(DEFAULT_HORIZONS_MS)
    return sorted(set(values))


def _load_market_descriptors(
    builder,
    market_files: dict[str, dict[str, list[Path]]],
    market_ids: list[str],
    *,
    quiet: bool = False,
) -> list[MarketDescriptor]:
    descriptors: list[MarketDescriptor] = []
    total = len(market_ids)
    _log(f"loading market descriptors for {total} markets", quiet=quiet)
    for idx, market_id in enumerate(market_ids, start=1):
        files = market_files.get(market_id)
        if files is None:
            if idx % LOAD_CONTEXT_PROGRESS_EVERY == 0 or idx == total:
                _log(f"market descriptor progress: {idx}/{total}, usable={len(descriptors)}", quiet=quiet)
            continue
        metadata = builder._load_market_metadata(files.get("market.metadata"), market_id)
        if metadata is None:
            if idx % LOAD_CONTEXT_PROGRESS_EVERY == 0 or idx == total:
                _log(f"market descriptor progress: {idx}/{total}, usable={len(descriptors)}", quiet=quiet)
            continue
        if metadata.get("start_ts_ms") is None or metadata.get("end_ts_ms") is None:
            if idx % LOAD_CONTEXT_PROGRESS_EVERY == 0 or idx == total:
                _log(f"market descriptor progress: {idx}/{total}, usable={len(descriptors)}", quiet=quiet)
            continue
        descriptors.append(
            MarketDescriptor(
                market_id=market_id,
                files=files,
                metadata=metadata,
            )
        )
        if idx % LOAD_CONTEXT_PROGRESS_EVERY == 0 or idx == total:
            _log(f"market descriptor progress: {idx}/{total}, usable={len(descriptors)}", quiet=quiet)
    return descriptors


def _load_market_context_batch(
    builder,
    descriptors: list[MarketDescriptor],
    *,
    quiet: bool = False,
) -> list[MarketContext]:
    contexts: list[MarketContext] = []
    total = len(descriptors)
    _log(f"loading market contexts for batch of {total}", quiet=quiet)
    for idx, descriptor in enumerate(descriptors, start=1):
        metadata = descriptor.metadata
        lifecycle = builder._load_lifecycle(descriptor.files.get("market.lifecycle.transition"), descriptor.market_id)["series"]
        pair = SeriesIndex.from_points(
            builder._load_pair_book_series(
                descriptor.files.get("feeds.polymarket.market.book_top"),
                metadata,
                descriptor.market_id,
            )["series"]
        )
        theo = SeriesIndex.from_points(
            builder._load_theo_series(descriptor.files.get("pricing.theo"), descriptor.market_id)["series"]
        )
        quote = SeriesIndex.from_points(
            builder._load_quote_series(descriptor.files.get("pricing.quote_plan"), descriptor.market_id)["series"]
        )
        depth_up, depth_down = _load_depth_series(
            builder,
            descriptor.files.get("feeds.polymarket.market.depth"),
            metadata,
            descriptor.market_id,
        )
        contexts.append(
            MarketContext(
                market_id=descriptor.market_id,
                files=descriptor.files,
                metadata=metadata,
                lifecycle=lifecycle,
                pair=pair,
                theo=theo,
                quote=quote,
                depth_up=depth_up,
                depth_down=depth_down,
            )
        )
        if idx % LOAD_CONTEXT_PROGRESS_EVERY == 0 or idx == total:
            _log(f"market context batch progress: {idx}/{total}", quiet=quiet)
    return contexts


def _load_depth_series(builder, paths: list[Path] | Path | None, metadata: dict[str, Any], market_id: str) -> tuple[SeriesIndex, SeriesIndex]:
    up_token_id = str(metadata.get("up_token_id") or "")
    down_token_id = str(metadata.get("down_token_id") or "")
    up_points: list[dict[str, Any]] = []
    down_points: list[dict[str, Any]] = []

    for record in builder._iter_stream_records(paths):
        if not builder._record_market_matches(record, market_id):
            continue
        snapshot = record.get("payload", {}).get("snapshot")
        if not isinstance(snapshot, dict):
            continue
        token_id = str(snapshot.get("token_id") or "")
        bids = builder._normalize_levels(snapshot.get("bids"))
        asks = builder._normalize_levels(snapshot.get("asks"))
        point = {
            "ts": builder._safe_int(snapshot.get("last_update_ts_ms")),
            "bid_depth_5": _depth_total(bids),
            "ask_depth_5": _depth_total(asks),
        }
        point["depth_imbalance"] = _depth_imbalance(point["bid_depth_5"], point["ask_depth_5"])
        if token_id == up_token_id:
            up_points.append(point)
        elif token_id == down_token_id:
            down_points.append(point)

    return SeriesIndex.from_points(up_points), SeriesIndex.from_points(down_points)


def _build_external_windows(
    contexts: list[MarketContext],
    *,
    preload_ms: int,
    future_ms: int,
) -> list[TimeWindow]:
    windows: list[TimeWindow] = []
    for context in contexts:
        if context.start_ts <= 0 and context.end_ts <= 0 and context.latest_ts <= 0:
            continue
        start_ts = max(0, min(value for value in [context.start_ts, context.latest_ts] if value > 0) - preload_ms)
        end_base = max(context.end_ts, context.latest_ts)
        end_ts = end_base + future_ms if end_base > 0 else future_ms
        windows.append(TimeWindow(start_ts=start_ts, end_ts=end_ts))
    return _merge_time_windows(windows)


def _merge_time_windows(windows: list[TimeWindow]) -> list[TimeWindow]:
    if not windows:
        return []
    ordered = sorted(windows, key=lambda item: (item.start_ts, item.end_ts))
    merged: list[TimeWindow] = [ordered[0]]
    for window in ordered[1:]:
        latest = merged[-1]
        if window.start_ts <= latest.end_ts:
            latest.end_ts = max(latest.end_ts, window.end_ts)
            continue
        merged.append(window)
    return merged


def _load_global_windowed_index(
    *,
    paths: list[Path] | Path | None,
    windows: list[TimeWindow],
    record_ts_extractor,
    point_extractor,
    label: str,
    quiet: bool = False,
) -> SeriesIndex:
    if not windows:
        _log(f"skip loading {label}: no external windows", quiet=quiet)
        return SeriesIndex.from_points([])
    normalized_paths = _normalize_paths(paths)
    selected_paths = _select_paths_for_windows(normalized_paths, windows, record_ts_extractor)
    _log(
        f"loading {label}: selected {len(selected_paths)}/{len(normalized_paths)} files "
        f"for {len(windows)} windows",
        quiet=quiet,
    )
    points: list[dict[str, Any]] = []
    for record in _iter_jsonl_records(selected_paths):
        point = point_extractor(record)
        if point is None:
            continue
        ts = _safe_int(point.get("ts"))
        if ts is None or not _ts_in_windows(ts, windows):
            continue
        points.append(point)
    _log(f"loaded {label}: {len(points)} points", quiet=quiet)
    return SeriesIndex.from_points(points)


def _select_paths_for_windows(
    paths: list[Path] | Path | None,
    windows: list[TimeWindow],
    record_ts_extractor,
) -> list[Path]:
    normalized_paths = tuple(_normalize_paths(paths))
    entries = _build_file_time_ranges(normalized_paths, record_ts_extractor)
    if not entries:
        return []
    starts = [entry.start_ts for entry in entries]
    ends = [entry.end_ts for entry in entries]
    selected_indexes: set[int] = set()
    for window in windows:
        left = bisect_left(ends, window.start_ts)
        right = bisect_right(starts, window.end_ts)
        for idx in range(left, right):
            entry = entries[idx]
            if entry.start_ts <= window.end_ts and entry.end_ts >= window.start_ts:
                selected_indexes.add(idx)
    return [entries[idx].path for idx in sorted(selected_indexes)]


@lru_cache(maxsize=8)
def _build_file_time_ranges(
    paths: tuple[Path, ...],
    record_ts_extractor,
) -> list[FileTimeRange]:
    entries: list[FileTimeRange] = []
    for path in paths:
        start_ts = _read_first_valid_ts(path, record_ts_extractor)
        end_ts = _read_last_valid_ts(path, record_ts_extractor)
        if start_ts is None or end_ts is None:
            continue
        entries.append(FileTimeRange(path=path, start_ts=start_ts, end_ts=end_ts))
    entries.sort(key=lambda item: (item.start_ts, item.end_ts, str(item.path)))
    return entries


def _read_first_valid_ts(path: Path, record_ts_extractor) -> int | None:
    with path.open(encoding="utf-8") as handle:
        for raw_line in handle:
            record = _parse_jsonl_line(raw_line)
            if record is None:
                continue
            ts = record_ts_extractor(record)
            if ts is not None and ts > 0:
                return ts
    return None


def _read_last_valid_ts(path: Path, record_ts_extractor) -> int | None:
    for raw_line in _iter_tail_lines(path):
        record = _parse_jsonl_line(raw_line)
        if record is None:
            continue
        ts = record_ts_extractor(record)
        if ts is not None and ts > 0:
            return ts
    return None


def _iter_tail_lines(path: Path, *, max_lines: int = 64, chunk_size: int = 65_536) -> list[str]:
    with path.open("rb") as handle:
        handle.seek(0, 2)
        file_size = handle.tell()
        position = file_size
        buffer = b""
        lines: list[bytes] = []
        while position > 0 and len(lines) <= max_lines:
            read_size = min(chunk_size, position)
            position -= read_size
            handle.seek(position)
            buffer = handle.read(read_size) + buffer
            lines = buffer.splitlines()
        return [line.decode("utf-8", errors="ignore") for line in reversed(lines[-max_lines:])]


def _iter_jsonl_records(paths: list[Path] | Path | None):
    for path in _normalize_paths(paths):
        with path.open(encoding="utf-8") as handle:
            for raw_line in handle:
                record = _parse_jsonl_line(raw_line)
                if record is not None:
                    yield record


def _normalize_paths(paths: list[Path] | tuple[Path, ...] | Path | None) -> list[Path]:
    if paths is None:
        return []
    if isinstance(paths, Path):
        return [paths] if paths.exists() else []
    return [path for path in paths if path.exists()]


def _parse_jsonl_line(raw_line: str) -> dict[str, Any] | None:
    line = raw_line.strip()
    if not line:
        return None
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None


def _ts_in_windows(ts: int, windows: list[TimeWindow]) -> bool:
    for window in windows:
        if ts < window.start_ts:
            return False
        if window.start_ts <= ts <= window.end_ts:
            return True
    return False


def _extract_binance_ts(record: dict[str, Any]) -> int | None:
    tick = record.get("payload", {}).get("tick")
    if not isinstance(tick, dict):
        return None
    return _safe_int(tick.get("recv_ts_ms") or tick.get("event_ts_ms"))


def _extract_chainlink_ts(record: dict[str, Any]) -> int | None:
    tick = record.get("payload", {}).get("tick")
    if not isinstance(tick, dict):
        return None
    return _safe_int(tick.get("oracle_ts_ms") or tick.get("recv_ts_ms"))


def _extract_binance_point(record: dict[str, Any]) -> dict[str, Any] | None:
    tick = record.get("payload", {}).get("tick")
    if not isinstance(tick, dict):
        return None
    bid = _safe_float(tick.get("best_bid"))
    ask = _safe_float(tick.get("best_ask"))
    ts = _safe_int(tick.get("recv_ts_ms") or tick.get("event_ts_ms"))
    if ts is None:
        return None
    return {
        "ts": ts,
        "last": _safe_float(tick.get("last_price")),
        "bid": bid,
        "ask": ask,
        "mid": _mid(bid, ask),
    }


def _extract_chainlink_point(record: dict[str, Any]) -> dict[str, Any] | None:
    tick = record.get("payload", {}).get("tick")
    if not isinstance(tick, dict):
        return None
    ts = _safe_int(tick.get("oracle_ts_ms") or tick.get("recv_ts_ms"))
    if ts is None:
        return None
    return {
        "ts": ts,
        "price": _safe_float(tick.get("price")),
        "bid": _safe_float(tick.get("bid")),
        "ask": _safe_float(tick.get("ask")),
    }


def _mid(bid: float | None, ask: float | None) -> float | None:
    if bid is None or ask is None:
        return None
    return _round((bid + ask) / 2.0)


def _scan_events(
    *,
    builder,
    event_type: str,
    ordered_contexts: list[MarketContext],
    contexts_by_id: dict[str, MarketContext],
    binance_index: SeriesIndex,
    chainlink_index: SeriesIndex,
    horizons_ms: list[int],
    impulse_threshold_bps: float,
    poly_refresh_lag_ms: int,
    cooldown_ms: int,
    depth_lookback_ms: int,
    depth_drop_ratio: float,
    depth_min_drop_size: float,
    depth_opposite_stable_ratio: float,
    depth_refill_window_ms: int,
    depth_refill_ratio: float,
) -> list[dict[str, Any]]:
    if event_type == EVENT_FAST_CLOSE_ENTER:
        return _detect_fast_close_enter_events(
            builder=builder,
            contexts=ordered_contexts,
            binance_index=binance_index,
            chainlink_index=chainlink_index,
            horizons_ms=horizons_ms,
        )
    if event_type == EVENT_ADJACENT_HANDOFF:
        return _detect_adjacent_handoff_events(
            builder=builder,
            contexts=ordered_contexts,
            contexts_by_id=contexts_by_id,
            binance_index=binance_index,
            chainlink_index=chainlink_index,
            horizons_ms=horizons_ms,
        )
    if event_type == EVENT_BINANCE_IMPULSE_WITHOUT_REFRESH:
        return _detect_binance_impulse_without_refresh_events(
            builder=builder,
            contexts=ordered_contexts,
            binance_index=binance_index,
            chainlink_index=chainlink_index,
            horizons_ms=horizons_ms,
            impulse_threshold_bps=impulse_threshold_bps,
            poly_refresh_lag_ms=poly_refresh_lag_ms,
            cooldown_ms=cooldown_ms,
        )
    if event_type == EVENT_ONE_SIDED_DEPTH_DEPLETION:
        rows, _ = _detect_depth_depletion_candidates(
            builder=builder,
            contexts=ordered_contexts,
            binance_index=binance_index,
            chainlink_index=chainlink_index,
            horizons_ms=horizons_ms,
            cooldown_ms=cooldown_ms,
            depth_lookback_ms=depth_lookback_ms,
            depth_drop_ratio=depth_drop_ratio,
            depth_min_drop_size=depth_min_drop_size,
            depth_opposite_stable_ratio=depth_opposite_stable_ratio,
        )
        return rows
    if event_type == EVENT_DEPTH_REFILL_AFTER_GAP:
        _, candidates = _detect_depth_depletion_candidates(
            builder=builder,
            contexts=ordered_contexts,
            binance_index=binance_index,
            chainlink_index=chainlink_index,
            horizons_ms=horizons_ms,
            cooldown_ms=cooldown_ms,
            depth_lookback_ms=depth_lookback_ms,
            depth_drop_ratio=depth_drop_ratio,
            depth_min_drop_size=depth_min_drop_size,
            depth_opposite_stable_ratio=depth_opposite_stable_ratio,
        )
        return _detect_depth_refill_events(
            builder=builder,
            candidates=candidates,
            binance_index=binance_index,
            chainlink_index=chainlink_index,
            horizons_ms=horizons_ms,
            cooldown_ms=cooldown_ms,
            depth_refill_window_ms=depth_refill_window_ms,
            depth_refill_ratio=depth_refill_ratio,
        )
    raise ValueError(f"unsupported event_type: {event_type}")


def _detect_fast_close_enter_events(
    *,
    builder,
    contexts: list[MarketContext],
    binance_index: SeriesIndex,
    chainlink_index: SeriesIndex,
    horizons_ms: list[int],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for context in contexts:
        for transition in context.lifecycle:
            if str(transition.get("phase") or "") != "FAST_CLOSE":
                continue
            trigger_ts = builder._safe_int(transition.get("ts"))
            row = _build_event_row(
                builder=builder,
                event_type=EVENT_FAST_CLOSE_ENTER,
                context=context,
                trigger_ts_ms=trigger_ts,
                horizons_ms=horizons_ms,
                binance_index=binance_index,
                chainlink_index=chainlink_index,
                extra_fields={
                    "transition_previous_phase": str(transition.get("previous_phase") or ""),
                    "transition_new_phase": str(transition.get("phase") or ""),
                },
            )
            if row is not None:
                rows.append(row)
            break
    return rows


def _detect_adjacent_handoff_events(
    *,
    builder,
    contexts: list[MarketContext],
    contexts_by_id: dict[str, MarketContext],
    binance_index: SeriesIndex,
    chainlink_index: SeriesIndex,
    horizons_ms: list[int],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for idx in range(1, len(contexts)):
        prev_ctx = contexts[idx - 1]
        next_ctx = contexts[idx]
        if prev_ctx.end_ts <= 0 or next_ctx.start_ts <= 0:
            continue
        gap_ms = next_ctx.start_ts - prev_ctx.end_ts
        if abs(gap_ms) > 60_000:
            continue
        trigger_ts = next_ctx.start_ts
        prev_pair = prev_ctx.pair.at_or_before(trigger_ts)
        prev_theo = prev_ctx.theo.at_or_before(trigger_ts)
        extra_fields = {
            "handoff_gap_ms": gap_ms,
            "prev_market_id": prev_ctx.market_id,
            "prev_market_phase": _phase_at_ts(prev_ctx, trigger_ts),
            "prev_up_mid": _value(prev_pair, "up_mid"),
            "prev_down_mid": _value(prev_pair, "down_mid"),
            "prev_sum_best_ask": _value(prev_pair, "sum_best_ask"),
            "prev_sum_best_bid": _value(prev_pair, "sum_best_bid"),
            "prev_theo_up": _value(prev_theo, "theo_up"),
            "prev_theo_down": _value(prev_theo, "theo_down"),
        }
        row = _build_event_row(
            builder=builder,
            event_type=EVENT_ADJACENT_HANDOFF,
            context=next_ctx,
            trigger_ts_ms=trigger_ts,
            horizons_ms=horizons_ms,
            binance_index=binance_index,
            chainlink_index=chainlink_index,
            extra_fields=extra_fields,
        )
        if row is not None:
            rows.append(row)
    return rows


def _detect_binance_impulse_without_refresh_events(
    *,
    builder,
    contexts: list[MarketContext],
    binance_index: SeriesIndex,
    chainlink_index: SeriesIndex,
    horizons_ms: list[int],
    impulse_threshold_bps: float,
    poly_refresh_lag_ms: int,
    cooldown_ms: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    threshold = impulse_threshold_bps / 10_000.0
    for context in contexts:
        if not context.pair.points:
            continue
        last_event_ts = -10**18
        binance_points = binance_index.between(context.start_ts, context.end_ts)
        for point in binance_points:
            trigger_ts = point["ts"]
            if trigger_ts - last_event_ts < cooldown_ms:
                continue
            if _phase_at_ts(context, trigger_ts) != "ACTIVE":
                continue
            ret_5s = _return_over_window(binance_index, trigger_ts, 5_000)
            if ret_5s is None or abs(ret_5s) < threshold:
                continue
            current_pair = context.pair.at_or_before(trigger_ts)
            if current_pair is None:
                continue
            next_pair = context.pair.first_after(trigger_ts)
            current_theo = context.theo.at_or_before(trigger_ts)
            refresh_delay_ms = None
            if next_pair is not None:
                refresh_delay_ms = next_pair["ts"] - trigger_ts
            if refresh_delay_ms is not None and refresh_delay_ms <= poly_refresh_lag_ms:
                continue
            impulse_direction = "up" if ret_5s > 0 else "down"
            impulse_sign = 1.0 if ret_5s > 0 else -1.0
            refresh_ts = _value(next_pair, "ts")
            refresh_theo = context.theo.at_or_before(refresh_ts) if refresh_ts is not None else None
            refresh_binance = binance_index.at_or_before(refresh_ts) if refresh_ts is not None else None
            current_up_mid = _value(current_pair, "up_mid")
            refresh_up_mid = _value(next_pair, "up_mid")
            current_sum_best_ask = _value(current_pair, "sum_best_ask")
            refresh_sum_best_ask = _value(next_pair, "sum_best_ask")
            current_sum_best_bid = _value(current_pair, "sum_best_bid")
            refresh_sum_best_bid = _value(next_pair, "sum_best_bid")
            current_theo_up = _value(current_theo, "theo_up")
            refresh_theo_up = _value(refresh_theo, "theo_up")
            current_binance_mid = _value(point, "mid")
            refresh_binance_mid = _value(refresh_binance, "mid")
            first_refresh_up_mid_move = _subtract(refresh_up_mid, current_up_mid)
            first_refresh_theo_up_move = _subtract(refresh_theo_up, current_theo_up)
            pre_refresh_theo_gap_signed = _signed_gap(current_theo_up, current_up_mid, impulse_sign)
            post_refresh_theo_gap_signed = _signed_gap(refresh_theo_up, refresh_up_mid, impulse_sign)
            first_refresh_theo_gap_close_signed = _subtract(pre_refresh_theo_gap_signed, post_refresh_theo_gap_signed)
            first_refresh_theo_gap_close_ratio = _safe_ratio(
                first_refresh_theo_gap_close_signed,
                abs(pre_refresh_theo_gap_signed) if pre_refresh_theo_gap_signed is not None else None,
            )
            row = _build_event_row(
                builder=builder,
                event_type=EVENT_BINANCE_IMPULSE_WITHOUT_REFRESH,
                context=context,
                trigger_ts_ms=trigger_ts,
                horizons_ms=horizons_ms,
                binance_index=binance_index,
                chainlink_index=chainlink_index,
                extra_fields={
                    "impulse_direction": impulse_direction,
                    "impulse_abs_bps": _round(abs(ret_5s) * 10_000.0),
                    "binance_ret_5s_trigger": _round(ret_5s),
                    "poly_refresh_delay_ms": refresh_delay_ms,
                    "first_poly_refresh_ts_ms": refresh_ts,
                    "pre_refresh_up_mid": _round(current_up_mid),
                    "post_refresh_up_mid": _round(refresh_up_mid),
                    "first_refresh_up_mid_move": first_refresh_up_mid_move,
                    "first_refresh_up_mid_move_signed": _signed_value(first_refresh_up_mid_move, impulse_sign),
                    "pre_refresh_sum_best_ask": _round(current_sum_best_ask),
                    "post_refresh_sum_best_ask": _round(refresh_sum_best_ask),
                    "first_refresh_sum_best_ask_move": _subtract(refresh_sum_best_ask, current_sum_best_ask),
                    "pre_refresh_sum_best_bid": _round(current_sum_best_bid),
                    "post_refresh_sum_best_bid": _round(refresh_sum_best_bid),
                    "first_refresh_sum_best_bid_move": _subtract(refresh_sum_best_bid, current_sum_best_bid),
                    "first_refresh_theo_up": _round(refresh_theo_up),
                    "first_refresh_theo_up_move": first_refresh_theo_up_move,
                    "first_refresh_theo_up_move_signed": _signed_value(first_refresh_theo_up_move, impulse_sign),
                    "pre_refresh_theo_gap_signed": pre_refresh_theo_gap_signed,
                    "post_refresh_theo_gap_signed": post_refresh_theo_gap_signed,
                    "first_refresh_theo_gap_close_signed": first_refresh_theo_gap_close_signed,
                    "first_refresh_theo_gap_close_ratio": first_refresh_theo_gap_close_ratio,
                    "first_refresh_binance_mid": _round(refresh_binance_mid),
                    "first_refresh_binance_mid_move": _subtract(refresh_binance_mid, current_binance_mid),
                },
            )
            if row is not None:
                _attach_impulse_post_refresh_labels(
                    row=row,
                    context=context,
                    refresh_ts_ms=refresh_ts,
                    refresh_pair=next_pair,
                    refresh_theo=refresh_theo,
                    refresh_binance=refresh_binance,
                    horizons_ms=horizons_ms,
                    binance_index=binance_index,
                    impulse_sign=impulse_sign,
                    refresh_theo_gap_signed=post_refresh_theo_gap_signed,
                )
                rows.append(row)
                last_event_ts = trigger_ts
    return rows


def _detect_depth_depletion_candidates(
    *,
    builder,
    contexts: list[MarketContext],
    binance_index: SeriesIndex,
    chainlink_index: SeriesIndex,
    horizons_ms: list[int],
    cooldown_ms: int,
    depth_lookback_ms: int,
    depth_drop_ratio: float,
    depth_min_drop_size: float,
    depth_opposite_stable_ratio: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    for context in contexts:
        last_event_ts_by_group: dict[str, int] = {}
        side_configs = [
            ("up_bid", context.depth_up, "bid_depth_5", "ask_depth_5"),
            ("up_ask", context.depth_up, "ask_depth_5", "bid_depth_5"),
            ("down_bid", context.depth_down, "bid_depth_5", "ask_depth_5"),
            ("down_ask", context.depth_down, "ask_depth_5", "bid_depth_5"),
        ]
        for side_name, index, target_key, opposite_key in side_configs:
            signal_group = _depth_signal_group(side_name)
            canonical_side = _depth_signal_canonical_side(side_name)
            mirror_side = _depth_signal_mirror_side(side_name)
            for point in index.points:
                trigger_ts = point["ts"]
                last_event_ts = last_event_ts_by_group.get(signal_group, -10**18)
                if trigger_ts - last_event_ts < cooldown_ms:
                    continue
                phase = _phase_at_ts(context, trigger_ts)
                if phase == "PREWARM":
                    continue
                previous = index.at_or_before(trigger_ts - depth_lookback_ms)
                if previous is None or previous["ts"] >= trigger_ts:
                    continue
                prev_depth = _value(previous, target_key)
                curr_depth = _value(point, target_key)
                prev_opp_depth = _value(previous, opposite_key)
                curr_opp_depth = _value(point, opposite_key)
                if prev_depth is None or curr_depth is None or prev_depth <= 0:
                    continue
                drop_size = prev_depth - curr_depth
                drop_ratio = drop_size / prev_depth
                opposite_change_ratio = _change_ratio(prev_opp_depth, curr_opp_depth)
                if drop_size < depth_min_drop_size or drop_ratio < depth_drop_ratio:
                    continue
                if opposite_change_ratio is not None and abs(opposite_change_ratio) > depth_opposite_stable_ratio:
                    continue
                extra_fields = {
                    "depth_signal_side": canonical_side,
                    "depth_signal_raw_side": side_name,
                    "depth_signal_mirror_side": mirror_side,
                    "depth_signal_group": signal_group,
                    "depth_signal_kind": "depletion",
                    "depth_prev_value": _round(prev_depth),
                    "depth_curr_value": _round(curr_depth),
                    "depth_drop_size": _round(drop_size),
                    "depth_drop_ratio": _round(drop_ratio),
                    "depth_opposite_prev_value": _round(prev_opp_depth),
                    "depth_opposite_curr_value": _round(curr_opp_depth),
                    "depth_opposite_change_ratio": _round(opposite_change_ratio),
                    "depth_lookback_ms": depth_lookback_ms,
                }
                row = _build_event_row(
                    builder=builder,
                    event_type=EVENT_ONE_SIDED_DEPTH_DEPLETION,
                    context=context,
                    trigger_ts_ms=trigger_ts,
                    horizons_ms=horizons_ms,
                    binance_index=binance_index,
                    chainlink_index=chainlink_index,
                    event_key_suffix=signal_group,
                    extra_fields=extra_fields,
                )
                if row is not None:
                    rows.append(row)
                    candidates.append(
                        {
                            "context": context,
                            "trigger_ts_ms": trigger_ts,
                            "side_name": canonical_side,
                            "raw_side_name": side_name,
                            "mirror_side_name": mirror_side,
                            "signal_group": signal_group,
                            "index": index,
                            "target_key": target_key,
                            "previous_value": prev_depth,
                            "current_value": curr_depth,
                            "drop_size": drop_size,
                            "drop_ratio": drop_ratio,
                            "row": row,
                        }
                    )
                    last_event_ts_by_group[signal_group] = trigger_ts
    return rows, candidates


def _detect_depth_refill_events(
    *,
    builder,
    candidates: list[dict[str, Any]],
    binance_index: SeriesIndex,
    chainlink_index: SeriesIndex,
    horizons_ms: list[int],
    cooldown_ms: int,
    depth_refill_window_ms: int,
    depth_refill_ratio: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    last_event_ts_by_key: dict[tuple[str, str], int] = {}
    for candidate in candidates:
        context: MarketContext = candidate["context"]
        side_name = str(candidate["side_name"])
        raw_side_name = str(candidate["raw_side_name"])
        mirror_side_name = str(candidate["mirror_side_name"])
        signal_group = str(candidate["signal_group"])
        trigger_ts = int(candidate["trigger_ts_ms"])
        target_key = str(candidate["target_key"])
        current_value = float(candidate["current_value"])
        drop_size = float(candidate["drop_size"])
        target_refill_value = current_value + depth_refill_ratio * drop_size
        index: SeriesIndex = candidate["index"]
        future_points = index.between(trigger_ts + 1, trigger_ts + depth_refill_window_ms)
        refill_point = None
        for point in future_points:
            value = _value(point, target_key)
            if value is None:
                continue
            if value >= target_refill_value:
                refill_point = point
                break
        if refill_point is None:
            continue
        dedupe_key = (context.market_id, signal_group)
        refill_ts = int(refill_point["ts"])
        last_event_ts = last_event_ts_by_key.get(dedupe_key, -10**18)
        if refill_ts - last_event_ts < cooldown_ms:
            continue
        recovered_value = _value(refill_point, target_key)
        recovered_ratio = None
        if drop_size > 0 and recovered_value is not None:
            recovered_ratio = (recovered_value - current_value) / drop_size
        extra_fields = {
            "depth_signal_side": side_name,
            "depth_signal_raw_side": raw_side_name,
            "depth_signal_mirror_side": mirror_side_name,
            "depth_signal_group": signal_group,
            "depth_signal_kind": "refill",
            "source_depletion_ts_ms": trigger_ts,
            "source_depletion_event_id": candidate["row"]["event_id"],
            "depth_refill_window_ms": depth_refill_window_ms,
            "depth_refill_target_ratio": _round(depth_refill_ratio),
            "depth_refill_achieved_ratio": _round(recovered_ratio),
            "depth_refill_prev_value": _round(current_value),
            "depth_refill_curr_value": _round(recovered_value),
            "depth_refill_needed_value": _round(target_refill_value),
        }
        row = _build_event_row(
            builder=builder,
            event_type=EVENT_DEPTH_REFILL_AFTER_GAP,
            context=context,
            trigger_ts_ms=refill_ts,
            horizons_ms=horizons_ms,
            binance_index=binance_index,
            chainlink_index=chainlink_index,
            event_key_suffix=signal_group,
            extra_fields=extra_fields,
        )
        if row is not None:
            rows.append(row)
            last_event_ts_by_key[dedupe_key] = refill_ts
    return rows


def _build_event_row(
    *,
    builder,
    event_type: str,
    context: MarketContext,
    trigger_ts_ms: int,
    horizons_ms: list[int],
    binance_index: SeriesIndex,
    chainlink_index: SeriesIndex,
    event_key_suffix: str | None = None,
    extra_fields: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    pair = context.pair.at_or_before(trigger_ts_ms)
    theo = context.theo.at_or_before(trigger_ts_ms)
    quote = context.quote.at_or_before(trigger_ts_ms)
    depth_up = context.depth_up.at_or_before(trigger_ts_ms)
    depth_down = context.depth_down.at_or_before(trigger_ts_ms)
    binance = binance_index.at_or_before(trigger_ts_ms)
    chainlink = chainlink_index.at_or_before(trigger_ts_ms)

    phase = _phase_at_ts(context, trigger_ts_ms)
    event_id = f"{event_type}:{context.market_id}:{trigger_ts_ms}"
    if event_key_suffix:
        event_id = f"{event_id}:{event_key_suffix}"
    row: dict[str, Any] = {
        "event_id": event_id,
        "event_type": event_type,
        "market_id": context.market_id,
        "trigger_ts_ms": trigger_ts_ms,
        "phase": phase,
        "start_ts_ms": context.start_ts,
        "end_ts_ms": context.end_ts,
        "ms_to_start": context.start_ts - trigger_ts_ms if context.start_ts else None,
        "ms_to_end": context.end_ts - trigger_ts_ms if context.end_ts else None,
        "up_bid": _value(pair, "up_bid"),
        "up_ask": _value(pair, "up_ask"),
        "down_bid": _value(pair, "down_bid"),
        "down_ask": _value(pair, "down_ask"),
        "up_bid_sz": _value(pair, "up_bid_size"),
        "up_ask_sz": _value(pair, "up_ask_size"),
        "down_bid_sz": _value(pair, "down_bid_size"),
        "down_ask_sz": _value(pair, "down_ask_size"),
        "up_mid": _value(pair, "up_mid"),
        "down_mid": _value(pair, "down_mid"),
        "sum_best_bid": _value(pair, "sum_best_bid"),
        "sum_best_ask": _value(pair, "sum_best_ask"),
        "spread_up": _spread(_value(pair, "up_bid"), _value(pair, "up_ask")),
        "spread_down": _spread(_value(pair, "down_bid"), _value(pair, "down_ask")),
        "depth_up_bid_5": _value(depth_up, "bid_depth_5"),
        "depth_up_ask_5": _value(depth_up, "ask_depth_5"),
        "depth_down_bid_5": _value(depth_down, "bid_depth_5"),
        "depth_down_ask_5": _value(depth_down, "ask_depth_5"),
        "depth_imbalance_up": _value(depth_up, "depth_imbalance"),
        "depth_imbalance_down": _value(depth_down, "depth_imbalance"),
        "theo_up": _value(theo, "theo_up"),
        "theo_down": _value(theo, "theo_down"),
        "sigma_short": _value(theo, "sigma_short"),
        "directional_bias": _value(theo, "directional_bias"),
        "target_full_set_cost": _value(theo, "target_full_set_cost"),
        "quote_up_bid": _value(quote, "up_bid"),
        "quote_up_ask": _value(quote, "up_ask"),
        "quote_down_bid": _value(quote, "down_bid"),
        "quote_down_ask": _value(quote, "down_ask"),
        "quote_bid_sum": _value(quote, "quote_bid_sum"),
        "quote_ask_sum": _value(quote, "quote_ask_sum"),
        "quote_reason": _value(quote, "reason"),
        "binance_mid": _value(binance, "mid"),
        "binance_bid": _value(binance, "bid"),
        "binance_ask": _value(binance, "ask"),
        "binance_ret_1s": _round(_return_over_window(binance_index, trigger_ts_ms, 1_000)),
        "binance_ret_5s": _round(_return_over_window(binance_index, trigger_ts_ms, 5_000)),
        "binance_ret_15s": _round(_return_over_window(binance_index, trigger_ts_ms, 15_000)),
        "chainlink_price": _value(chainlink, "price"),
        "chainlink_bid": _value(chainlink, "bid"),
        "chainlink_ask": _value(chainlink, "ask"),
        "basis_binance_chainlink": _basis(binance, chainlink),
        "chainlink_staleness_ms": trigger_ts_ms - chainlink["ts"] if chainlink is not None else None,
        "edge_yes": _subtract(_value(theo, "theo_up"), _value(pair, "up_ask")),
        "edge_no": _subtract(_value(theo, "theo_down"), _value(pair, "down_ask")),
        "current_tradable_size": _current_tradable_size(pair),
    }

    row["quote_vs_market_up_bid"] = _subtract(_value(quote, "up_bid"), _value(pair, "up_bid"))
    row["quote_vs_market_up_ask"] = _subtract(_value(quote, "up_ask"), _value(pair, "up_ask"))
    row["quote_vs_market_down_bid"] = _subtract(_value(quote, "down_bid"), _value(pair, "down_bid"))
    row["quote_vs_market_down_ask"] = _subtract(_value(quote, "down_ask"), _value(pair, "down_ask"))
    row["quote_vs_theo_up_ask"] = _subtract(_value(quote, "up_ask"), _value(theo, "theo_up"))
    row["quote_vs_theo_down_ask"] = _subtract(_value(quote, "down_ask"), _value(theo, "theo_down"))

    _attach_forward_labels(
        row=row,
        context=context,
        trigger_ts_ms=trigger_ts_ms,
        horizons_ms=horizons_ms,
        binance_index=binance_index,
    )
    if extra_fields:
        row.update(extra_fields)
    return row


def _attach_forward_labels(
    *,
    row: dict[str, Any],
    context: MarketContext,
    trigger_ts_ms: int,
    horizons_ms: list[int],
    binance_index: SeriesIndex,
) -> None:
    current_up_mid = row.get("up_mid")
    current_down_mid = row.get("down_mid")
    current_sum_best_ask = row.get("sum_best_ask")
    current_sum_best_bid = row.get("sum_best_bid")
    current_binance_mid = row.get("binance_mid")

    for horizon_ms in horizons_ms:
        suffix = _horizon_label(horizon_ms)
        target_ts = trigger_ts_ms + horizon_ms
        future_pair = context.pair.at_or_before(target_ts)
        if future_pair is not None and future_pair["ts"] <= trigger_ts_ms:
            future_pair = None
        future_theo = context.theo.at_or_before(target_ts)
        if future_theo is not None and future_theo["ts"] <= trigger_ts_ms:
            future_theo = None
        future_binance = binance_index.at_or_before(target_ts)
        if future_binance is not None and future_binance["ts"] <= trigger_ts_ms:
            future_binance = None
        window_pairs = context.pair.between(trigger_ts_ms, target_ts)
        if window_pairs and window_pairs[0]["ts"] == trigger_ts_ms:
            window_pairs = window_pairs[1:]

        row[f"fwd_ts_{suffix}"] = _value(future_pair, "ts")
        row[f"fwd_up_bid_{suffix}"] = _value(future_pair, "up_bid")
        row[f"fwd_up_ask_{suffix}"] = _value(future_pair, "up_ask")
        row[f"fwd_down_bid_{suffix}"] = _value(future_pair, "down_bid")
        row[f"fwd_down_ask_{suffix}"] = _value(future_pair, "down_ask")
        row[f"fwd_theo_up_{suffix}"] = _value(future_theo, "theo_up")
        row[f"fwd_theo_down_{suffix}"] = _value(future_theo, "theo_down")
        row[f"fwd_binance_mid_{suffix}"] = _value(future_binance, "mid")
        row[f"up_mid_move_{suffix}"] = _subtract(_value(future_pair, "up_mid"), current_up_mid)
        row[f"down_mid_move_{suffix}"] = _subtract(_value(future_pair, "down_mid"), current_down_mid)
        row[f"sum_best_ask_move_{suffix}"] = _subtract(_value(future_pair, "sum_best_ask"), current_sum_best_ask)
        row[f"sum_best_bid_move_{suffix}"] = _subtract(_value(future_pair, "sum_best_bid"), current_sum_best_bid)
        row[f"binance_mid_move_{suffix}"] = _subtract(_value(future_binance, "mid"), current_binance_mid)
        row[f"mfe_up_mid_{suffix}"] = _mfe(window_pairs, current_up_mid, "up_mid")
        row[f"mae_up_mid_{suffix}"] = _mae(window_pairs, current_up_mid, "up_mid")
        row[f"tradable_size_{suffix}"] = _window_tradable_size(window_pairs)


def _attach_impulse_post_refresh_labels(
    *,
    row: dict[str, Any],
    context: MarketContext,
    refresh_ts_ms: int | None,
    refresh_pair: dict[str, Any] | None,
    refresh_theo: dict[str, Any] | None,
    refresh_binance: dict[str, Any] | None,
    horizons_ms: list[int],
    binance_index: SeriesIndex,
    impulse_sign: float,
    refresh_theo_gap_signed: float | None,
) -> None:
    if refresh_ts_ms is None or refresh_pair is None:
        return
    refresh_up_mid = _value(refresh_pair, "up_mid")
    refresh_sum_best_ask = _value(refresh_pair, "sum_best_ask")
    refresh_sum_best_bid = _value(refresh_pair, "sum_best_bid")
    refresh_theo_up = _value(refresh_theo, "theo_up")
    refresh_binance_mid = _value(refresh_binance, "mid")

    for horizon_ms in horizons_ms:
        suffix = _horizon_label(horizon_ms)
        target_ts = refresh_ts_ms + horizon_ms
        future_pair = context.pair.at_or_before(target_ts)
        if future_pair is not None and future_pair["ts"] <= refresh_ts_ms:
            future_pair = None
        future_theo = context.theo.at_or_before(target_ts)
        if future_theo is not None and future_theo["ts"] <= refresh_ts_ms:
            future_theo = None
        future_binance = binance_index.at_or_before(target_ts)
        if future_binance is not None and future_binance["ts"] <= refresh_ts_ms:
            future_binance = None
        window_pairs = context.pair.between(refresh_ts_ms, target_ts)
        if window_pairs and window_pairs[0]["ts"] == refresh_ts_ms:
            window_pairs = window_pairs[1:]

        post_refresh_up_mid_move = _subtract(_value(future_pair, "up_mid"), refresh_up_mid)
        post_refresh_theo_up_move = _subtract(_value(future_theo, "theo_up"), refresh_theo_up)
        future_theo_gap_signed = _signed_gap(_value(future_theo, "theo_up"), _value(future_pair, "up_mid"), impulse_sign)
        post_refresh_theo_gap_close_signed = _subtract(refresh_theo_gap_signed, future_theo_gap_signed)
        row[f"post_refresh_fwd_ts_{suffix}"] = _value(future_pair, "ts")
        row[f"post_refresh_up_mid_move_{suffix}"] = post_refresh_up_mid_move
        row[f"post_refresh_up_mid_move_signed_{suffix}"] = _signed_value(post_refresh_up_mid_move, impulse_sign)
        row[f"post_refresh_sum_best_ask_move_{suffix}"] = _subtract(_value(future_pair, "sum_best_ask"), refresh_sum_best_ask)
        row[f"post_refresh_sum_best_bid_move_{suffix}"] = _subtract(_value(future_pair, "sum_best_bid"), refresh_sum_best_bid)
        row[f"post_refresh_theo_up_move_{suffix}"] = post_refresh_theo_up_move
        row[f"post_refresh_theo_up_move_signed_{suffix}"] = _signed_value(post_refresh_theo_up_move, impulse_sign)
        row[f"post_refresh_theo_gap_signed_{suffix}"] = future_theo_gap_signed
        row[f"post_refresh_theo_gap_close_signed_{suffix}"] = post_refresh_theo_gap_close_signed
        row[f"post_refresh_theo_gap_close_ratio_{suffix}"] = _safe_ratio(
            post_refresh_theo_gap_close_signed,
            abs(refresh_theo_gap_signed) if refresh_theo_gap_signed is not None else None,
        )
        row[f"post_refresh_binance_mid_move_{suffix}"] = _subtract(_value(future_binance, "mid"), refresh_binance_mid)
        row[f"post_refresh_mfe_up_mid_{suffix}"] = _mfe(window_pairs, refresh_up_mid, "up_mid")
        row[f"post_refresh_mae_up_mid_{suffix}"] = _mae(window_pairs, refresh_up_mid, "up_mid")
        row[f"post_refresh_tradable_size_{suffix}"] = _window_tradable_size(window_pairs)


def _build_summary(event_type: str, rows: list[dict[str, Any]], horizons_ms: list[int]) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "event_type": event_type,
        "count": len(rows),
        "unique_markets": len({row.get("market_id") for row in rows if row.get("market_id")}),
        "phase_counts": dict(Counter(row.get("phase") or "UNKNOWN" for row in rows)),
        "horizons": {},
    }
    for horizon_ms in horizons_ms:
        suffix = _horizon_label(horizon_ms)
        metrics = {
            "avg_up_mid_move": _avg_numeric(rows, f"up_mid_move_{suffix}"),
            "avg_down_mid_move": _avg_numeric(rows, f"down_mid_move_{suffix}"),
            "avg_sum_best_ask_move": _avg_numeric(rows, f"sum_best_ask_move_{suffix}"),
            "avg_sum_best_bid_move": _avg_numeric(rows, f"sum_best_bid_move_{suffix}"),
            "avg_binance_mid_move": _avg_numeric(rows, f"binance_mid_move_{suffix}"),
            "avg_mfe_up_mid": _avg_numeric(rows, f"mfe_up_mid_{suffix}"),
            "avg_mae_up_mid": _avg_numeric(rows, f"mae_up_mid_{suffix}"),
            "avg_tradable_size": _avg_numeric(rows, f"tradable_size_{suffix}"),
        }
        summary["horizons"][suffix] = metrics
    return summary


def _summary_metric_keys(horizons_ms: list[int]) -> list[str]:
    keys: list[str] = []
    for horizon_ms in horizons_ms:
        keys.extend(_summary_metric_keys_for_suffix(_horizon_label(horizon_ms)))
    return keys


def _summary_metric_keys_for_suffix(suffix: str) -> list[str]:
    return [
        f"up_mid_move_{suffix}",
        f"down_mid_move_{suffix}",
        f"sum_best_ask_move_{suffix}",
        f"sum_best_bid_move_{suffix}",
        f"binance_mid_move_{suffix}",
        f"mfe_up_mid_{suffix}",
        f"mae_up_mid_{suffix}",
        f"tradable_size_{suffix}",
    ]


def _impulse_directional_metric_keys(horizons_ms: list[int]) -> list[str]:
    keys = [
        "impulse_abs_bps",
        "poly_refresh_delay_ms",
        "first_refresh_up_mid_move",
        "first_refresh_up_mid_move_signed",
        "first_refresh_sum_best_ask_move",
        "first_refresh_sum_best_bid_move",
        "first_refresh_theo_up_move",
        "first_refresh_theo_up_move_signed",
        "first_refresh_theo_gap_close_signed",
        "first_refresh_theo_gap_close_ratio",
        "first_refresh_binance_mid_move",
    ]
    for horizon_ms in horizons_ms:
        suffix = _horizon_label(horizon_ms)
        keys.extend(
            [
                f"post_refresh_up_mid_move_{suffix}",
                f"post_refresh_up_mid_move_signed_{suffix}",
                f"post_refresh_theo_up_move_{suffix}",
                f"post_refresh_theo_up_move_signed_{suffix}",
                f"post_refresh_theo_gap_close_signed_{suffix}",
                f"post_refresh_theo_gap_close_ratio_{suffix}",
                f"post_refresh_binance_mid_move_{suffix}",
                f"post_refresh_tradable_size_{suffix}",
            ]
        )
    return keys


def _resolve_output_paths(
    *,
    output_dir: Path,
    event_type: str,
    requested_output: str,
    requested_summary: str,
    multi_event: bool,
) -> tuple[Path, Path]:
    if requested_output and not multi_event:
        output_path = Path(requested_output).resolve()
    else:
        output_path = output_dir / f"{event_type}.jsonl"
    if requested_summary and not multi_event:
        summary_path = Path(requested_summary).resolve()
    else:
        summary_path = output_dir / f"{event_type}.summary.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    return output_path, summary_path


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _append_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _log(message: str, *, quiet: bool = False) -> None:
    if quiet:
        return
    stamp = datetime.now().strftime("%H:%M:%S")
    sys.stderr.write(f"[{stamp}] {message}\n")
    sys.stderr.flush()


def _chunked(items: list[Any], size: int):
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def _phase_at_ts(context: MarketContext, ts: int) -> str:
    latest = None
    for item in context.lifecycle:
        item_ts = int(item.get("ts") or 0)
        if item_ts <= 0 or item_ts > ts:
            break
        latest = item
    if latest is not None:
        phase = str(latest.get("phase") or "")
        if phase:
            return phase
    if ts < context.start_ts:
        return "PREWARM"
    if ts >= max(context.end_ts - FAST_CLOSE_WINDOW_MS, context.start_ts):
        return "FAST_CLOSE"
    return "ACTIVE"


def _depth_signal_group(side_name: str) -> str:
    if side_name in {"up_ask", "down_bid"}:
        return "up_ask_down_bid"
    if side_name in {"up_bid", "down_ask"}:
        return "up_bid_down_ask"
    return side_name


def _depth_signal_canonical_side(side_name: str) -> str:
    if side_name in {"up_ask", "down_bid"}:
        return "up_ask"
    if side_name in {"up_bid", "down_ask"}:
        return "up_bid"
    return side_name


def _depth_signal_mirror_side(side_name: str) -> str:
    mapping = {
        "up_ask": "down_bid",
        "down_bid": "up_ask",
        "up_bid": "down_ask",
        "down_ask": "up_bid",
    }
    return mapping.get(side_name, side_name)


def _return_over_window(index: SeriesIndex, ts: int, window_ms: int) -> float | None:
    current = index.at_or_before(ts)
    previous = index.at_or_before(ts - window_ms)
    current_mid = _value(current, "mid")
    previous_mid = _value(previous, "mid")
    if current_mid in (None, 0) or previous_mid in (None, 0):
        return None
    return (current_mid - previous_mid) / previous_mid


def _depth_total(levels: list[dict[str, float]]) -> float | None:
    if not levels:
        return None
    return _round(sum((level.get("size") or 0.0) for level in levels))


def _depth_imbalance(bid_depth: float | None, ask_depth: float | None) -> float | None:
    if bid_depth is None or ask_depth is None:
        return None
    total = bid_depth + ask_depth
    if total <= 0:
        return None
    return _round((bid_depth - ask_depth) / total)


def _change_ratio(previous: float | None, current: float | None) -> float | None:
    if previous is None or current is None or previous <= 0:
        return None
    return (current - previous) / previous


def _basis(binance: dict[str, Any] | None, chainlink: dict[str, Any] | None) -> float | None:
    binance_mid = _value(binance, "mid")
    chainlink_price = _value(chainlink, "price")
    if binance_mid is None or chainlink_price is None:
        return None
    return _round(binance_mid - chainlink_price)


def _current_tradable_size(pair: dict[str, Any] | None) -> float | None:
    if pair is None:
        return None
    sizes = [
        _value(pair, "up_bid_size"),
        _value(pair, "up_ask_size"),
        _value(pair, "down_bid_size"),
        _value(pair, "down_ask_size"),
    ]
    numeric = [value for value in sizes if isinstance(value, (int, float)) and value > 0]
    if not numeric:
        return None
    return _round(min(numeric))


def _window_tradable_size(points: list[dict[str, Any]]) -> float | None:
    values = [_current_tradable_size(point) for point in points]
    numeric = [value for value in values if isinstance(value, (int, float))]
    if not numeric:
        return None
    return _round(min(numeric))


def _mfe(points: list[dict[str, Any]], base_value: float | None, key: str) -> float | None:
    if base_value is None or not points:
        return None
    values = [
        point.get(key)
        for point in points
        if isinstance(point.get(key), (int, float))
    ]
    if not values:
        return None
    return _round(max(values) - base_value)


def _mae(points: list[dict[str, Any]], base_value: float | None, key: str) -> float | None:
    if base_value is None or not points:
        return None
    values = [
        point.get(key)
        for point in points
        if isinstance(point.get(key), (int, float))
    ]
    if not values:
        return None
    return _round(min(values) - base_value)


def _value(item: dict[str, Any] | None, key: str) -> Any:
    if item is None:
        return None
    return item.get(key)


def _spread(bid: float | None, ask: float | None) -> float | None:
    if bid is None or ask is None:
        return None
    return _round(ask - bid)


def _subtract(left: float | None, right: float | None) -> float | None:
    if left is None or right is None:
        return None
    return _round(left - right)


def _signed_value(value: float | None, sign: float) -> float | None:
    if value is None:
        return None
    return _round(value * sign)


def _signed_gap(left: float | None, right: float | None, sign: float) -> float | None:
    gap = _subtract(left, right)
    if gap is None:
        return None
    return _round(gap * sign)


def _safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator in (None, 0):
        return None
    return _round(numerator / denominator)


def _avg_numeric(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [
        row.get(key)
        for row in rows
        if isinstance(row.get(key), (int, float))
    ]
    if not values:
        return None
    return _round(sum(values) / len(values))


def _round(value: float | None) -> float | None:
    if value is None:
        return None
    return round(float(value), 6)


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _safe_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _horizon_label(horizon_ms: int) -> str:
    if horizon_ms % 1_000 == 0:
        return f"{horizon_ms // 1_000}s"
    return f"{horizon_ms}ms"


if __name__ == "__main__":
    main()
