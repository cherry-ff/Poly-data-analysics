from __future__ import annotations

import importlib.util
import io
import tarfile
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[2] / "local_sync_client" / "poly15_sync_client.py"
SPEC = importlib.util.spec_from_file_location("poly15_sync_client", MODULE_PATH)
assert SPEC is not None
assert SPEC.loader is not None
poly15_sync_client = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(poly15_sync_client)


def test_expand_requested_paths_resolves_directory_to_exact_manifest_entries() -> None:
    manifest = {
        "entries": [
            {"path": "global/feeds_binance_tick/00000000000000000001.jsonl"},
            {"path": "markets/1604309/market_metadata/00000000000000000001.jsonl"},
            {"path": "markets/1604309/pricing_quote_plan/00000000000000000001.jsonl"},
        ]
    }

    resolved = poly15_sync_client._expand_requested_paths(manifest, ["markets/1604309"])

    assert resolved == [
        "markets/1604309/market_metadata/00000000000000000001.jsonl",
        "markets/1604309/pricing_quote_plan/00000000000000000001.jsonl",
    ]


def test_sync_index_skips_already_confirmed_segments(tmp_path: Path) -> None:
    output_dir = tmp_path / "synced"
    state_dir = tmp_path / "state"
    relative_path = "global/feeds_binance_tick/00000000000000000001.jsonl"
    local_path = output_dir / relative_path
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text('{"payload":{"tick":{"recv_ts_ms":1000}}}\n', encoding="utf-8")

    sha256 = poly15_sync_client._file_sha256(local_path)
    verified_entries = [
        {
            "path": relative_path,
            "segment_id": relative_path,
            "size_bytes": local_path.stat().st_size,
            "sha256": sha256,
        }
    ]
    receipt_path = state_dir / "receipts" / "receipt.json"
    sync_index: dict[str, dict[str, object]] = {}
    poly15_sync_client._update_sync_index(state_dir, sync_index, verified_entries, receipt_path)
    loaded_index = poly15_sync_client._load_sync_index(state_dir)

    manifest_entries = {
        relative_path: {
            "path": relative_path,
            "size_bytes": local_path.stat().st_size,
            "sha256": sha256,
        }
    }
    filtered = poly15_sync_client._filter_confirmed_paths(
        [relative_path],
        manifest_entries,
        loaded_index,
        output_dir,
    )

    assert filtered == []


def test_plan_delete_entries_skips_unconfirmed_segments(tmp_path: Path) -> None:
    output_dir = tmp_path / "synced"
    state_dir = tmp_path / "state"
    confirmed_path = "global/feeds_binance_tick/00000000000000000001.jsonl"
    pending_path = "global/feeds_binance_tick/00000000000000000002.jsonl"

    local_path = output_dir / confirmed_path
    local_path.parent.mkdir(parents=True, exist_ok=True)
    local_path.write_text('{"payload":{"tick":{"recv_ts_ms":1000}}}\n', encoding="utf-8")

    sha256 = poly15_sync_client._file_sha256(local_path)
    poly15_sync_client._update_sync_index(
        state_dir,
        {},
        [
            {
                "path": confirmed_path,
                "segment_id": confirmed_path,
                "size_bytes": local_path.stat().st_size,
                "sha256": sha256,
            }
        ],
        state_dir / "receipts" / "receipt.json",
    )
    sync_index = poly15_sync_client._load_sync_index(state_dir)

    manifest = {
        "entries": [
            {
                "path": confirmed_path,
                "size_bytes": local_path.stat().st_size,
                "sha256": sha256,
            },
            {
                "path": pending_path,
                "size_bytes": 123,
                "sha256": "pending",
            },
        ]
    }

    planned_paths, planned_entries, skipped_paths = poly15_sync_client._plan_delete_entries(
        manifest,
        [],
        sync_index,
        output_dir,
        require_local_confirmation=True,
    )

    assert planned_paths == [confirmed_path]
    assert [entry["path"] for entry in planned_entries] == [confirmed_path]
    assert skipped_paths == [pending_path]


def test_extract_archive_rejects_symlink_members(tmp_path: Path) -> None:
    archive_buffer = io.BytesIO()
    with tarfile.open(fileobj=archive_buffer, mode="w:gz") as archive:
        member = tarfile.TarInfo("global/feeds_binance_tick/link.jsonl")
        member.type = tarfile.SYMTYPE
        member.linkname = "/tmp/evil.jsonl"
        archive.addfile(member)

    archive_path = tmp_path / "archive.tar.gz"
    archive_path.write_bytes(archive_buffer.getvalue())

    try:
        poly15_sync_client._extract_archive(archive_path, tmp_path / "synced")
    except poly15_sync_client.SyncApiError as exc:
        assert "unsupported link type" in str(exc)
    else:
        raise AssertionError("symlink members should be rejected during archive extraction")


def test_build_sync_batches_respects_byte_and_file_limits() -> None:
    manifest_entries = {
        "global/a.jsonl": {"path": "global/a.jsonl", "size_bytes": 50},
        "global/b.jsonl": {"path": "global/b.jsonl", "size_bytes": 40},
        "global/c.jsonl": {"path": "global/c.jsonl", "size_bytes": 60},
        "global/d.jsonl": {"path": "global/d.jsonl", "size_bytes": 10},
    }

    batches = poly15_sync_client._build_sync_batches(
        ["global/a.jsonl", "global/b.jsonl", "global/c.jsonl", "global/d.jsonl"],
        manifest_entries,
        max_batch_bytes=100,
        max_batch_files=2,
    )

    assert batches == [
        ["global/a.jsonl", "global/b.jsonl"],
        ["global/c.jsonl", "global/d.jsonl"],
    ]
