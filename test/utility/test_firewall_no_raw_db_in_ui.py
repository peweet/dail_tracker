"""Logic-firewall guard: the Streamlit presentation layer must not touch the
database directly.

The rule (see dail_tracker_core/queries/__init__.py, results.py): pages_code/ and
ui/ FILTER and DISPLAY; all retrieval SQL lives in dail_tracker_core.queries and is
reached through utility/data_access wrappers. A page or UI component that runs raw
``conn.execute``, opens a DuckDB connection, or reads parquet itself is a leak — it
puts business logic above the firewall where it escapes the contract tests.

This test AST-scans utility/pages_code and utility/ui and fails on any such access.
It is a RATCHET: the allowlist below is empty today (the last leak,
ui/vote_explorer.py, was migrated to dail_tracker_core.queries.votes). Do not add to
the allowlist to make a new leak pass — move the SQL into the read layer instead.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCAN_DIRS = [ROOT / "utility" / "pages_code", ROOT / "utility" / "ui"]

# Raw cursor methods and connection/parquet entry points that belong below the firewall.
_FORBIDDEN_METHODS = {"execute", "executemany"}
_FORBIDDEN_CALLS = {"read_parquet", "scan_parquet", "connect_with_views", "get_warehouse_connection"}

# (relative_posix_path, lineno) pairs that are knowingly accepted. KEEP EMPTY.
_ALLOWLIST: set[tuple[str, int]] = set()


def _scan(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    rel = path.relative_to(ROOT).as_posix()
    hits: list[str] = []

    def record(lineno: int, why: str) -> None:
        if (rel, lineno) not in _ALLOWLIST:
            hits.append(f"{rel}:{lineno} — {why}")

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                if a.name.split(".")[0] == "duckdb":
                    record(node.lineno, "imports duckdb")
        elif isinstance(node, ast.ImportFrom):
            if (node.module or "").split(".")[0] == "duckdb":
                record(node.lineno, "imports from duckdb")
        elif isinstance(node, ast.Call):
            fn = node.func
            if isinstance(fn, ast.Attribute):
                if fn.attr in _FORBIDDEN_METHODS:
                    record(node.lineno, f"raw .{fn.attr}() — move SQL to dail_tracker_core.queries")
                elif fn.attr in _FORBIDDEN_CALLS:
                    record(node.lineno, f".{fn.attr}() — connection/parquet access belongs in data_access/core")
            elif isinstance(fn, ast.Name) and fn.id in _FORBIDDEN_CALLS:
                record(node.lineno, f"{fn.id}() — connection/parquet access belongs in data_access/core")
    return hits


def test_no_raw_db_access_in_presentation_layer():
    violations: list[str] = []
    for d in SCAN_DIRS:
        for py in sorted(d.rglob("*.py")):
            if py.name.endswith(".bak") or "__pycache__" in py.parts:
                continue
            violations.extend(_scan(py))
    assert not violations, (
        "Logic-firewall leak: the UI layer must not run SQL / open DuckDB / read parquet directly.\n"
        "Move retrieval into dail_tracker_core.queries and call it via utility/data_access.\n  "
        + "\n  ".join(violations)
    )
