from __future__ import annotations

import importlib.util
import json
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[2] / "scripts" / "build_dashboard_data.py"
SPEC = importlib.util.spec_from_file_location("build_dashboard_data", MODULE_PATH)
assert SPEC is not None
assert SPEC.loader is not None
build_dashboard_data = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(build_dashboard_data)


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )


def test_build_dashboard_payload_keeps_valid_15m_window_and_skips_empty_markets(
    tmp_path: Path,
) -> None:
    records_root = tmp_path / "records"
    good_market_dir = records_root / "markets" / "1001"
    empty_market_dir = records_root / "markets" / "1002"

    end_ts = 1_772_978_400_000
    start_ts = end_ts - 15 * 60_000

    _write_jsonl(
        good_market_dir / "market_metadata.jsonl",
        [
            {
                "topic": "market.metadata",
                "payload": {
                    "market": {
                        "market_id": "1001",
                        "condition_id": "cond_1001",
                        "up_token_id": "up_1001",
                        "down_token_id": "down_1001",
                        "start_ts_ms": start_ts,
                        "end_ts_ms": end_ts,
                        "tick_size": "0.01",
                        "fee_rate_bps": "0",
                        "min_order_size": "1",
                        "status": "active",
                        "reference_price": None,
                    }
                },
            }
        ],
    )
    _write_jsonl(
        good_market_dir / "pricing_theo.jsonl",
        [
            {
                "market_id": "1001",
                "payload": {
                    "snapshot": {
                        "ts_ms": start_ts - 1_000,
                        "theo_up": "0.4",
                        "theo_down": "0.6",
                        "sigma_short": "0.01",
                        "directional_bias": "0",
                        "target_full_set_cost": "0.996",
                    }
                },
            },
            {
                "market_id": "1001",
                "payload": {
                    "snapshot": {
                        "ts_ms": start_ts + 120_000,
                        "theo_up": "0.55",
                        "theo_down": "0.45",
                        "sigma_short": "0.012",
                        "directional_bias": "0",
                        "target_full_set_cost": "0.996",
                    }
                },
            },
        ],
    )
    _write_jsonl(
        good_market_dir / "pricing_quote_plan.jsonl",
        [
            {
                "market_id": "1001",
                "payload": {
                    "plan": {
                        "ts_ms": start_ts + 120_000,
                        "up_bid_px": "0.52",
                        "up_ask_px": "0.56",
                        "down_bid_px": "0.43",
                        "down_ask_px": "0.47",
                        "reason": "unit_test",
                    }
                },
            }
        ],
    )

    _write_jsonl(
        empty_market_dir / "market_metadata.jsonl",
        [
            {
                "topic": "market.metadata",
                "payload": {
                    "market": {
                        "market_id": "1002",
                        "condition_id": "cond_1002",
                        "up_token_id": "up_1002",
                        "down_token_id": "down_1002",
                        "start_ts_ms": start_ts,
                        "end_ts_ms": end_ts,
                        "tick_size": "0.01",
                        "fee_rate_bps": "0",
                        "min_order_size": "1",
                        "status": "active",
                        "reference_price": None,
                    }
                },
            }
        ],
    )

    payload = build_dashboard_data.build_dashboard_payload(records_root)

    assert payload["market_order"] == ["1001"]
    market = payload["markets"]["1001"]
    assert market["window"]["start_ts"] == start_ts
    assert market["window"]["end_ts"] == end_ts
    assert market["window"]["duration_minutes"] == 15.0
    assert market["window"]["has_valid_bounds"] is True
    assert market["summary"]["event_counts"]["theo"] == 1
    assert market["summary"]["event_counts"]["quote"] == 1
    assert len(market["series"]["theo"]) == 1
    assert market["series"]["theo"][0]["theo_up"] == 0.55


def test_build_dashboard_payload_skips_market_with_non_15m_metadata(
    tmp_path: Path,
) -> None:
    records_root = tmp_path / "records"
    market_dir = records_root / "markets" / "1004"

    end_ts = 1_772_978_400_000
    _write_jsonl(
        market_dir / "market_metadata.jsonl",
        [
            {
                "topic": "market.metadata",
                "payload": {
                    "market": {
                        "market_id": "1004",
                        "condition_id": "cond_1004",
                        "up_token_id": "up_1004",
                        "down_token_id": "down_1004",
                        "start_ts_ms": end_ts - 2_000_000,
                        "end_ts_ms": end_ts,
                        "tick_size": "0.01",
                        "fee_rate_bps": "0",
                        "min_order_size": "1",
                        "status": "active",
                        "reference_price": None,
                    }
                },
            }
        ],
    )
    _write_jsonl(
        market_dir / "pricing_quote_plan.jsonl",
        [
            {
                "market_id": "1004",
                "payload": {
                    "plan": {
                        "ts_ms": end_ts - 60_000,
                        "up_bid_px": "0.52",
                        "up_ask_px": "0.56",
                        "down_bid_px": "0.43",
                        "down_ask_px": "0.47",
                        "reason": "unit_test",
                    }
                },
            }
        ],
    )

    payload = build_dashboard_data.build_dashboard_payload(records_root)

    assert payload["market_order"] == []
    assert payload["markets"] == {}


def test_build_market_payload_can_return_full_resolution_series(tmp_path: Path) -> None:
    records_root = tmp_path / "records"
    market_dir = records_root / "markets" / "1003"
    start_ts = 1_772_978_400_000
    end_ts = start_ts + 15 * 60_000

    _write_jsonl(
        market_dir / "market_metadata.jsonl",
        [
            {
                "topic": "market.metadata",
                "payload": {
                    "market": {
                        "market_id": "1003",
                        "condition_id": "cond_1003",
                        "up_token_id": "up_1003",
                        "down_token_id": "down_1003",
                        "start_ts_ms": start_ts,
                        "end_ts_ms": end_ts,
                        "tick_size": "0.01",
                        "fee_rate_bps": "0",
                        "min_order_size": "1",
                        "status": "active",
                        "reference_price": None,
                    }
                },
            }
        ],
    )
    _write_jsonl(
        market_dir / "pricing_theo.jsonl",
        [
            {
                "market_id": "1003",
                "payload": {
                        "snapshot": {
                            "ts_ms": start_ts + offset,
                            "theo_up": str(0.4 + (offset / 1_000_000)),
                            "theo_down": str(0.6 - (offset / 1_000_000)),
                            "sigma_short": "0.01",
                            "directional_bias": "0",
                            "target_full_set_cost": "0.996",
                        }
                },
            }
            for offset in range(0, 721_000, 1_000)
        ],
    )

    _, market_files = build_dashboard_data._discover_record_files(records_root)
    metadata = build_dashboard_data._load_market_metadata(
        market_files["1003"].get("market.metadata"),
        "1003",
    )

    market = build_dashboard_data._build_market_payload(
        market_id="1003",
        files=market_files["1003"],
        metadata=metadata,
        binance_raw=[],
        chainlink_raw=[],
        market_max_points=None,
    )

    assert len(market["series"]["theo"]) == 721


def test_build_dashboard_payload_merges_sealed_and_live_streams(tmp_path: Path) -> None:
    records_root = tmp_path / "records"
    market_id = "1005"
    start_ts = 1_772_978_400_000
    end_ts = start_ts + 15 * 60_000

    _write_jsonl(
        records_root / "sealed" / "global" / "feeds_binance_tick" / "00000000000000000001.jsonl",
        [
            {
                "payload": {
                    "tick": {
                        "recv_ts_ms": start_ts + 1_000,
                        "best_bid": "50000",
                        "best_ask": "50010",
                        "last_price": "50005",
                    }
                }
            }
        ],
    )
    _write_jsonl(
        records_root / "global" / "feeds_binance_tick.jsonl",
        [
            {
                "payload": {
                    "tick": {
                        "recv_ts_ms": start_ts + 2_000,
                        "best_bid": "50020",
                        "best_ask": "50030",
                        "last_price": "50025",
                    }
                }
            }
        ],
    )
    _write_jsonl(
        records_root / "sealed" / "markets" / market_id / "market_metadata" / "00000000000000000001.jsonl",
        [
            {
                "topic": "market.metadata",
                "payload": {
                    "market": {
                        "market_id": market_id,
                        "condition_id": f"cond_{market_id}",
                        "up_token_id": f"up_{market_id}",
                        "down_token_id": f"down_{market_id}",
                        "start_ts_ms": start_ts,
                        "end_ts_ms": end_ts,
                        "tick_size": "0.01",
                        "fee_rate_bps": "0",
                        "min_order_size": "1",
                        "status": "active",
                        "reference_price": None,
                    }
                },
            }
        ],
    )
    _write_jsonl(
        records_root / "sealed" / "markets" / market_id / "pricing_quote_plan" / "00000000000000000001.jsonl",
        [
            {
                "market_id": market_id,
                "payload": {
                    "plan": {
                        "ts_ms": start_ts + 10_000,
                        "up_bid_px": "0.51",
                        "up_ask_px": "0.55",
                        "down_bid_px": "0.44",
                        "down_ask_px": "0.48",
                        "reason": "sealed",
                    }
                },
            }
        ],
    )
    _write_jsonl(
        records_root / "markets" / market_id / "pricing_quote_plan.jsonl",
        [
            {
                "market_id": market_id,
                "payload": {
                    "plan": {
                        "ts_ms": start_ts + 20_000,
                        "up_bid_px": "0.52",
                        "up_ask_px": "0.56",
                        "down_bid_px": "0.43",
                        "down_ask_px": "0.47",
                        "reason": "live",
                    }
                },
            }
        ],
    )

    payload = build_dashboard_data.build_dashboard_payload(records_root)

    assert payload["global"]["binance"]["count"] is None
    assert payload["global"]["binance"]["latest"]["ts"] == start_ts + 2_000
    assert payload["market_order"] == [market_id]
    assert len(payload["markets"][market_id]["series"]["quote"]) == 2
    assert payload["markets"][market_id]["series"]["quote"][-1]["reason"] == "live"
