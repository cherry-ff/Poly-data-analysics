from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil


@dataclass(slots=True)
class RecordsLayoutMigrationReport:
    root: str
    archived_flat_files: int
    archived_invalid_market_dirs: int

    def to_dict(self) -> dict[str, object]:
        return {
            "root": self.root,
            "archived_flat_files": self.archived_flat_files,
            "archived_invalid_market_dirs": self.archived_invalid_market_dirs,
        }


def migrate_records_layout(root: str | Path) -> RecordsLayoutMigrationReport:
    root_path = Path(root)
    root_path.mkdir(parents=True, exist_ok=True)

    legacy_dir = root_path / "_legacy_flat"
    ignored_dir = root_path / "_ignored_market_refs"
    archived_flat_files = 0
    archived_invalid_market_dirs = 0

    for entry in list(root_path.iterdir()):
        if entry.is_file() and entry.suffix == ".jsonl":
            legacy_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(entry), str(legacy_dir / entry.name))
            archived_flat_files += 1

    markets_dir = root_path / "markets"
    if markets_dir.exists():
        for entry in list(markets_dir.iterdir()):
            if not entry.is_dir():
                continue
            if entry.name.isdigit():
                continue
            ignored_dir.mkdir(parents=True, exist_ok=True)
            shutil.move(str(entry), str(ignored_dir / entry.name))
            archived_invalid_market_dirs += 1

    return RecordsLayoutMigrationReport(
        root=str(root_path),
        archived_flat_files=archived_flat_files,
        archived_invalid_market_dirs=archived_invalid_market_dirs,
    )
