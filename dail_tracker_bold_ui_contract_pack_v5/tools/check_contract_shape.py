from __future__ import annotations

import sys
from pathlib import Path

REQUIRED_STRINGS = [
    "duckdb_in_process_registered_analytical_views",
    "approved_registered_views",
    "ui_creativity_budget",
    "TODO_PIPELINE_VIEW_REQUIRED",
]


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python tools/check_contract_shape.py <contract.yaml>")
        return 2
    path = Path(sys.argv[1])
    text = path.read_text(encoding="utf-8")
    missing = [s for s in REQUIRED_STRINGS if s not in text]
    if missing:
        print(f"FAIL {path}: missing {missing}")
        return 1
    print(f"PASS {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
