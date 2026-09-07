"""Inventory every row-materialising call in the repo, for the >100k-row anti-pattern rule.

Row iteration in polars is an anti-pattern above the row threshold set in the runbook: the data is
columnar, so every `iter_rows`/`to_dicts`/`rows()` builds one Python object per row and forfeits
the parallelism polars would otherwise apply
[polars docs: "Row iteration is not optimal as the underlying data is stored in columnar form"].

AST, NOT REGEX, for one specific reason: `openpyxl` worksheets expose a same-named `iter_rows`
that has nothing to do with polars, and a regex sweep reports those as violations. This walks the
tree and records the receiver expression so a reader can tell them apart, and flags files that
never import polars as `not-polars`.

It classifies, it does not judge — the runbook (tools/row_iteration_runbook.yaml) carries the
verified tier and the row scale, because neither is knowable from syntax alone.

    python tools/scan_row_iteration.py                 # table
    python tools/scan_row_iteration.py --json          # machine-readable, for the runbook
"""

from __future__ import annotations

import argparse
import ast
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Calls that materialise Python objects per row.
ROW_CALLS = {"iter_rows", "to_dicts", "rows", "iterrows", "itertuples"}
# Per-row Python callbacks inside a dataframe engine — the same cost, different spelling.
CALLBACK_CALLS = {"map_elements", "apply"}

SKIP_PARTS = {".venv", ".cache", "node_modules", "__pycache__", ".tmp", ".git", "site-packages"}


def _iter_python_files(roots: list[Path]):
    for root in roots:
        if not root.exists():
            continue
        for path in root.rglob("*.py"):
            if any(part in SKIP_PARTS for part in path.parts):
                continue
            yield path


def _receiver(node: ast.Call) -> str:
    """Source text of what the method was called on, so openpyxl vs polars is legible."""
    func = node.func
    if not isinstance(func, ast.Attribute):
        return ""
    try:
        return ast.unparse(func.value)
    except Exception:  # noqa: BLE001 - unparse is best-effort labelling, never fatal
        return ""


def scan(paths: list[Path]) -> list[dict]:
    findings: list[dict] = []
    for path in _iter_python_files(paths):
        try:
            source = path.read_text(encoding="utf-8", errors="ignore")
            tree = ast.parse(source)
        except SyntaxError:
            continue
        imports_polars = "import polars" in source
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                continue
            name = node.func.attr
            if name not in ROW_CALLS and name not in CALLBACK_CALLS:
                continue
            receiver = _receiver(node)
            # A group_by(...).len() chain yields one row per group — bounded by cardinality, not
            # by frame height. Recorded so the runbook can exempt it on row count.
            grouped = "group_by" in receiver or "value_counts" in receiver
            bounded = any(token in receiver for token in ("head(", "limit(", "unique(", "first()"))
            findings.append(
                {
                    "file": str(path.relative_to(ROOT)).replace("\\", "/"),
                    "line": node.lineno,
                    "call": name,
                    "receiver": receiver[:80],
                    "kind": "callback" if name in CALLBACK_CALLS else "row-materialise",
                    "polars_in_file": imports_polars,
                    "grouped_or_counted": grouped,
                    "bounded_slice": bounded,
                }
            )
    return sorted(findings, key=lambda f: (f["file"], f["line"]))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit JSON for the runbook")
    parser.add_argument(
        "--roots",
        nargs="*",
        default=[
            "services",
            "extractors",
            "planning",
            "dail_tracker_core",
            "reference",
            "tools",
            "legislation",
            "members",
        ],
    )
    args = parser.parse_args()

    findings = scan([ROOT / r for r in args.roots])
    if args.json:
        json.dump(findings, sys.stdout, indent=2)
        return

    unbounded = [f for f in findings if f["polars_in_file"] and not f["grouped_or_counted"] and not f["bounded_slice"]]
    print(f"total row-materialising / callback sites : {len(findings)}")
    print(f"  in files that import polars            : {sum(1 for f in findings if f['polars_in_file'])}")
    print(f"  group_by/value_counts (bounded by card.): {sum(1 for f in findings if f['grouped_or_counted'])}")
    print(f"  head/limit/unique (bounded slice)       : {sum(1 for f in findings if f['bounded_slice'])}")
    print(f"  NOT polars (openpyxl etc, ignore)       : {sum(1 for f in findings if not f['polars_in_file'])}")
    print(f"\nUNBOUNDED candidates needing a runbook entry: {len(unbounded)}\n")
    for f in unbounded:
        print(f"  {f['file']}:{f['line']:<5} {f['call']:<14} on {f['receiver']}")


if __name__ == "__main__":
    main()
