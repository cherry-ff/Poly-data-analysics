from __future__ import annotations

from pathlib import Path

from storage.records_layout import migrate_records_layout


def test_migrate_records_layout_archives_legacy_files_and_invalid_market_dirs(tmp_path: Path) -> None:
    (tmp_path / "feeds_binance_tick.jsonl").write_text("x\n", encoding="utf-8")
    valid_market_dir = tmp_path / "markets" / "1523668"
    valid_market_dir.mkdir(parents=True)
    (valid_market_dir / "pricing_theo.jsonl").write_text("y\n", encoding="utf-8")
    invalid_market_dir = tmp_path / "markets" / "0xabc123"
    invalid_market_dir.mkdir(parents=True)
    (invalid_market_dir / "feeds_polymarket_market_new_market.jsonl").write_text("z\n", encoding="utf-8")

    report = migrate_records_layout(tmp_path)

    assert report.archived_flat_files == 1
    assert report.archived_invalid_market_dirs == 1
    assert (tmp_path / "_legacy_flat" / "feeds_binance_tick.jsonl").exists()
    assert (tmp_path / "_ignored_market_refs" / "0xabc123").exists()
    assert (tmp_path / "markets" / "1523668" / "pricing_theo.jsonl").exists()
