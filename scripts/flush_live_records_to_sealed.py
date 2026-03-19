#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from storage.recorder import flush_live_records_to_sealed


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Move every on-disk live record JSONL into the sealed segment layout.",
    )
    parser.add_argument(
        "--records-root",
        default=str(PROJECT_ROOT / "runtime_data" / "records"),
        help="Path to the records root that contains global/ and markets/",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    result = flush_live_records_to_sealed(Path(args.records_root).resolve())
    print(json.dumps(result, indent=2, ensure_ascii=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
