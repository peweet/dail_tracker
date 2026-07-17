"""SQL AST scanning for sql_views/ — the real dependency graph, from DuckDB's own parser.

The registered-view layer encodes its dependency order in FILENAME ALPHABETICS
(sorted-glob registration; `zz_` prefixes; sort-first naming tricks; hand-ordered
lists in dail_tracker_core/connections.py). Nothing verified that the encoded order
matches the views' ACTUAL dependencies — until this module: it parses every
sql_views/**/*.sql with ``json_serialize_sql`` (DuckDB's own parser, so dialect
fidelity is exact and there are no new dependencies) and extracts, per view:

- which other ``v_*`` views it selects from (the dependency edges);
- which parquet files it reads via ``read_parquet``;
- the parse mode — ``ast`` when DuckDB serialized the body, ``regex`` for the few
  bodies its JSON serializer refuses (measured 2026-07-17: 232/245 files parse
  clean; the rest are multi-statement files or non-SELECT bodies).

On top of the graph it computes ORDER RISKS: a same-directory edge whose dependency
filename sorts AFTER its consumer would fail sorted-glob registration — the exact
trap the `zz_` convention exists to avoid, now checkable instead of remembered.

Pure + stdlib/duckdb only; importable without the optional ``mcp`` extra.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import duckdb

_HEADER = re.compile(r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(\w+)\s+AS\b", re.I)
_REGEX_TABLES = re.compile(r"\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*)", re.I)
_REGEX_PARQUET = re.compile(r"read_parquet\(\s*'([^']+)'", re.I)

_GRAPH: dict | None = None  # built once per process, like server._PROJECT_INDEX


def _statements(text: str) -> list[tuple[str, str, int]]:
    """(view_name, body, line_no) per CREATE VIEW in a file — several files hold >1 view."""
    out = []
    matches = list(_HEADER.finditer(text))
    for i, m in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[m.end():end].strip().rstrip(";").strip()
        out.append((m.group(1), body, text[: m.start()].count("\n") + 1))
    return out


def _walk(node, tables: set[str], parquet: list[str]) -> None:
    if isinstance(node, dict):
        if node.get("type") == "BASE_TABLE" and node.get("table_name"):
            tables.add(node["table_name"])
        if node.get("type") == "TABLE_FUNCTION":
            fn = node.get("function", {})
            if fn.get("function_name", "").lower() == "read_parquet":
                for ch in fn.get("children", []):
                    v = ch.get("value", {})
                    if isinstance(v, dict) and v.get("value"):
                        parquet.append(str(v["value"]))
                        break
        for v in node.values():
            _walk(v, tables, parquet)
    elif isinstance(node, list):
        for v in node:
            _walk(v, tables, parquet)


def _parse_body(conn: duckdb.DuckDBPyConnection, body: str) -> tuple[set[str], list[str], str]:
    """(referenced tables, read_parquet paths, mode) — DuckDB AST first, regex fallback."""
    try:
        (js,) = conn.execute("SELECT json_serialize_sql(?::varchar)", [body]).fetchone()
        tree = json.loads(js)
        if not tree.get("error"):
            tables: set[str] = set()
            parquet: list[str] = []
            _walk(tree, tables, parquet)
            return tables, parquet, "ast"
    except Exception:  # noqa: BLE001 — fall through to regex
        pass
    tables = {t for t in _REGEX_TABLES.findall(body) if t.lower() != "read_parquet"}
    return tables, _REGEX_PARQUET.findall(body), "regex"


def build_graph(repo: Path) -> dict:
    """{view_name: {file, line, reads(view deps), reads_parquet, mode}} over sql_views/."""
    conn = duckdb.connect()
    raw: list[tuple[str, str, int, Path]] = []
    for f in sorted((repo / "sql_views").rglob("*.sql")):
        try:
            for name, body, line in _statements(f.read_text(encoding="utf-8")):
                raw.append((name, body, line, f))
        except OSError:
            continue

    known = {name for name, *_ in raw}
    views: dict[str, dict] = {}
    for name, body, line, f in raw:
        tables, parquet, mode = _parse_body(conn, body)
        views[name] = {
            "file": f.relative_to(repo).as_posix(),
            "line": line,
            "reads": sorted((tables & known) - {name}),
            "reads_parquet": sorted(set(parquet)),
            "mode": mode,
        }
    conn.close()
    return views


def graph(repo: Path) -> dict:
    global _GRAPH
    if _GRAPH is None:
        _GRAPH = build_graph(repo)
    return _GRAPH


def order_risks(views: dict) -> list[dict]:
    """Same-directory edges where the dependency's filename sorts AFTER its consumer —
    sorted-glob registration would try the consumer first and fail (or silently skip
    it under swallow_errors). Cross-directory edges are a separate, caller-ordered
    concern and are reported by the summary, not here."""
    risks = []
    for name, v in views.items():
        for dep in v["reads"]:
            d = views.get(dep)
            if d is None:
                continue
            vf, df = Path(v["file"]), Path(d["file"])
            if vf.parent == df.parent and df.name > vf.name:
                risks.append({"view": name, "file": v["file"], "needs": dep, "needs_file": d["file"]})
    return sorted(risks, key=lambda r: r["file"])


def summary(repo: Path, limit: int = 60) -> dict:
    views = graph(repo)
    edges = [(n, dep) for n, v in views.items() for dep in v["reads"]]
    cross = [
        {"view": n, "needs": dep, "file": views[n]["file"], "needs_file": views[dep]["file"]}
        for n, dep in edges
        if Path(views[n]["file"]).parent != Path(views[dep]["file"]).parent
    ]
    modes: dict[str, int] = {}
    for v in views.values():
        modes[v["mode"]] = modes.get(v["mode"], 0) + 1
    return {
        "views": len(views),
        "edges": len(edges),
        "parse_modes": modes,
        "order_risks": order_risks(views),
        "cross_directory_edges": {"count": len(cross), "sample": cross[:limit]},
        "hint": "call with view='v_name' for one view's deps/dependents",
    }


def detail(repo: Path, view: str) -> dict:
    views = graph(repo)
    v = views.get(view)
    if v is None:
        near = [n for n in views if view.lower().lstrip("v_") in n.lower()][:8]
        return {"error": f"unknown view: {view}", "did_you_mean": near}
    dependents = sorted(n for n, o in views.items() if view in o["reads"])
    return {"view": view, **v, "dependents": dependents}
