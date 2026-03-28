from __future__ import annotations

import importlib.util
import io
import json
import tarfile
import threading
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[2] / "dashboard" / "server.py"
SPEC = importlib.util.spec_from_file_location("dashboard_server", MODULE_PATH)
assert SPEC is not None
assert SPEC.loader is not None
dashboard_server = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(dashboard_server)


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )


def test_load_jsonl_window_seeks_to_requested_time_range(tmp_path: Path) -> None:
    path = tmp_path / "global.jsonl"
    rows = [
        {"payload": {"tick": {"recv_ts_ms": ts, "value": ts / 1000}}}
        for ts in (1_000, 2_000, 3_000, 4_000, 5_000)
    ]
    _write_jsonl(path, rows)

    points = dashboard_server.DashboardHTTPServer._load_jsonl_window(
        paths=path,
        start_ts=2_500,
        end_ts=4_500,
        ts_getter=lambda record: record.get("payload", {}).get("tick", {}).get("recv_ts_ms"),
        point_builder=lambda record: {
            "ts": record["payload"]["tick"]["recv_ts_ms"],
            "value": record["payload"]["tick"]["value"],
        },
    )

    assert [point["ts"] for point in points] == [3_000, 4_000]


def test_find_record_offset_for_ts_returns_start_for_early_target(tmp_path: Path) -> None:
    path = tmp_path / "global.jsonl"
    rows = [
        {"payload": {"tick": {"recv_ts_ms": ts}}}
        for ts in (10_000, 20_000, 30_000)
    ]
    _write_jsonl(path, rows)

    with path.open("rb") as handle:
        offset = dashboard_server.DashboardHTTPServer._find_record_offset_for_ts(
            handle,
            5_000,
            lambda record: record.get("payload", {}).get("tick", {}).get("recv_ts_ms"),
        )

    assert offset == 0


def test_select_stream_paths_for_window_skips_non_overlapping_segments(tmp_path: Path) -> None:
    stream_a = tmp_path / "0001.jsonl"
    stream_b = tmp_path / "0002.jsonl"
    stream_c = tmp_path / "0003.jsonl"
    _write_jsonl(stream_a, [{"payload": {"tick": {"recv_ts_ms": 1_000}}}])
    _write_jsonl(stream_b, [{"payload": {"tick": {"recv_ts_ms": 2_000}}}])
    _write_jsonl(stream_c, [{"payload": {"tick": {"recv_ts_ms": 3_000}}}])

    server = object.__new__(dashboard_server.DashboardHTTPServer)
    server._cache_lock = threading.Lock()
    server._stream_file_ranges = {}

    selected = server._select_stream_paths_for_window(
        cache_key="feeds.binance.tick",
        paths=[stream_a, stream_b, stream_c],
        start_ts=1_500,
        end_ts=2_500,
        ts_getter=lambda record: record.get("payload", {}).get("tick", {}).get("recv_ts_ms"),
    )

    assert selected == [stream_b]


def test_global_window_body_returns_empty_series_for_invalid_window() -> None:
    server = object.__new__(dashboard_server.DashboardHTTPServer)
    server._cache_lock = threading.Lock()
    server._global_window_bodies = {}
    server._records_signature = lambda: (0, 0)
    server._ensure_live_caches_current = lambda current_signature: None
    server._get_live_market_payload = lambda market_id: {
        "window": {
            "start_ts": None,
            "end_ts": None,
            "has_valid_bounds": False,
        }
    }
    server._load_builder_and_files = lambda current_signature=None: (_ for _ in ()).throw(
        AssertionError("invalid windows should not load global streams")
    )

    body = dashboard_server.DashboardHTTPServer.global_window_body(server, "1604309")
    payload = json.loads(body.decode("utf-8"))

    assert payload["market_id"] == "1604309"
    assert payload["global"]["binance"] == {"count": 0, "latest": None, "series": []}
    assert payload["global"]["chainlink"] == {"count": 0, "latest": None, "series": []}
    assert payload["global"]["basis"] == {"count": 0, "latest": None, "series": []}


def test_load_or_create_sync_token_persists_generated_token(tmp_path: Path) -> None:
    token_path = tmp_path / "sync_token.txt"

    token = dashboard_server._load_or_create_sync_token("", token_path)

    assert token
    assert token_path.read_text(encoding="utf-8").strip() == token
    assert dashboard_server._load_or_create_sync_token("", token_path) == token


def test_sync_manifest_archive_and_delete_work_on_relative_paths(tmp_path: Path) -> None:
    records_root = tmp_path / "records"
    live_global_path = records_root / "global" / "feeds_binance_tick.jsonl"
    sealed_global_path = records_root / "sealed" / "global" / "feeds_binance_tick" / "00000000000000000001.jsonl"
    sealed_market_path = (
        records_root
        / "sealed"
        / "markets"
        / "1604309"
        / "market_metadata"
        / "00000000000000000001.jsonl"
    )
    _write_jsonl(live_global_path, [{"payload": {"tick": {"recv_ts_ms": 2_000}}}])
    _write_jsonl(sealed_global_path, [{"payload": {"tick": {"recv_ts_ms": 1_000}}}])
    _write_jsonl(
        sealed_market_path,
        [{"payload": {"market": {"market_id": "1604309", "start_ts_ms": 1_000, "end_ts_ms": 901_000}}}],
    )

    original_snapshot_path = dashboard_server.SNAPSHOT_PATH
    dashboard_server.SNAPSHOT_PATH = tmp_path / "market-dashboard.json"
    try:
        server = object.__new__(dashboard_server.DashboardHTTPServer)
        server.records_root = records_root
        server.sync_token_path = tmp_path / "sync_token.txt"
        server._refresh_after_records_mutation = lambda: None

        manifest = server.sync_manifest()
        assert manifest["file_count"] == 2
        entry_by_path = {entry["path"]: entry for entry in manifest["entries"]}
        assert set(entry_by_path) == {
            "global/feeds_binance_tick/00000000000000000001.jsonl",
            "markets/1604309/market_metadata/00000000000000000001.jsonl",
        }
        assert entry_by_path["global/feeds_binance_tick/00000000000000000001.jsonl"]["stream_key"] == "global/feeds_binance_tick"
        assert entry_by_path["global/feeds_binance_tick/00000000000000000001.jsonl"]["segment_seq"] == 1
        assert entry_by_path["global/feeds_binance_tick/00000000000000000001.jsonl"]["sha256"]

        archive_body, filename = server.sync_archive(["markets/1604309"])
        assert filename.endswith(".tar.gz")
        with tarfile.open(fileobj=io.BytesIO(archive_body), mode="r:gz") as archive:
            assert archive.getnames() == ["markets/1604309/market_metadata/00000000000000000001.jsonl"]

        archive_path, streamed_filename, archive_size = server.create_sync_archive(["markets/1604309"])
        assert streamed_filename.endswith(".tar.gz")
        assert archive_size == archive_path.stat().st_size
        try:
            with tarfile.open(archive_path, mode="r:gz") as archive:
                assert archive.getnames() == ["markets/1604309/market_metadata/00000000000000000001.jsonl"]
        finally:
            archive_path.unlink(missing_ok=True)

        result = server.delete_sync_paths(["markets/1604309"])
        assert result["deleted_files"] == ["markets/1604309/market_metadata/00000000000000000001.jsonl"]
        assert "markets/1604309" in result["deleted_dirs"]
        assert not sealed_market_path.exists()
        assert live_global_path.exists()
        assert sealed_global_path.exists()
    finally:
        dashboard_server.SNAPSHOT_PATH = original_snapshot_path


def test_delete_sync_paths_rejects_stale_checksum(tmp_path: Path) -> None:
    records_root = tmp_path / "records"
    sealed_market_path = (
        records_root
        / "sealed"
        / "markets"
        / "1604309"
        / "market_metadata"
        / "00000000000000000001.jsonl"
    )
    _write_jsonl(
        sealed_market_path,
        [{"payload": {"market": {"market_id": "1604309"}}}],
    )

    server = object.__new__(dashboard_server.DashboardHTTPServer)
    server.records_root = records_root
    server.sync_token_path = tmp_path / "sync_token.txt"
    server._refresh_after_records_mutation = lambda: None

    try:
        server.delete_sync_paths(
            ["markets/1604309"],
            [{"path": "markets/1604309/market_metadata/00000000000000000001.jsonl", "sha256": "deadbeef"}],
        )
    except ValueError as exc:
        assert "checksum changed" in str(exc)
    else:
        raise AssertionError("delete_sync_paths should reject stale checksums")


def test_ack_sync_entries_persists_gc_state_and_collects_due_segments(tmp_path: Path) -> None:
    records_root = tmp_path / "records"
    sealed_market_path = (
        records_root
        / "sealed"
        / "markets"
        / "1604309"
        / "market_metadata"
        / "00000000000000000001.jsonl"
    )
    _write_jsonl(
        sealed_market_path,
        [{"payload": {"market": {"market_id": "1604309"}}}],
    )

    server = object.__new__(dashboard_server.DashboardHTTPServer)
    server.records_root = records_root
    server.sync_token_path = tmp_path / "sync_token.txt"
    server.sync_state_dir = tmp_path / "sync_state"
    server.sync_delete_grace_seconds = 0
    server._refresh_after_records_mutation = lambda: None

    entry = server.sync_manifest()["entries"][0]
    ack_result = server.ack_sync_entries(
        [
            {
                "path": entry["path"],
                "sha256": entry["sha256"],
                "size_bytes": entry["size_bytes"],
            }
        ],
        source="sync --delete-remote",
        client_receipt_path="/tmp/receipt.json",
    )
    assert ack_result["acked_file_count"] == 1

    gc_result = server.collect_pending_sync_garbage()
    assert gc_result["deleted_file_count"] == 1
    assert gc_result["pending_file_count"] == 0
    assert not sealed_market_path.exists()

    state_path = tmp_path / "sync_state" / "gc_index.json"
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    entry_payload = payload["entries"][entry["path"]]
    assert entry_payload["status"] == "deleted"
    assert entry_payload["deleted_at"]


def test_collect_pending_sync_garbage_marks_stale_segments_without_deleting(tmp_path: Path) -> None:
    records_root = tmp_path / "records"
    sealed_market_path = (
        records_root
        / "sealed"
        / "markets"
        / "1604309"
        / "market_metadata"
        / "00000000000000000001.jsonl"
    )
    _write_jsonl(
        sealed_market_path,
        [{"payload": {"market": {"market_id": "1604309"}}}],
    )

    server = object.__new__(dashboard_server.DashboardHTTPServer)
    server.records_root = records_root
    server.sync_token_path = tmp_path / "sync_token.txt"
    server.sync_state_dir = tmp_path / "sync_state"
    server.sync_delete_grace_seconds = 0
    server._refresh_after_records_mutation = lambda: None

    entry = server.sync_manifest()["entries"][0]
    server.ack_sync_entries(
        [
            {
                "path": entry["path"],
                "sha256": entry["sha256"],
                "size_bytes": entry["size_bytes"],
            }
        ],
        source="delete",
    )
    _write_jsonl(
        sealed_market_path,
        [{"payload": {"market": {"market_id": "1604309", "status": "changed"}}}],
    )

    gc_result = server.collect_pending_sync_garbage(force=True)
    assert gc_result["deleted_file_count"] == 0
    assert gc_result["stale_file_count"] == 1
    assert sealed_market_path.exists()

    state_path = tmp_path / "sync_state" / "gc_index.json"
    payload = json.loads(state_path.read_text(encoding="utf-8"))
    entry_payload = payload["entries"][entry["path"]]
    assert entry_payload["status"] == "stale"
    assert "checksum changed" in entry_payload["last_error"]
