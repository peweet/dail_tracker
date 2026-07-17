"""Tests for mcp_server/sql_index.py — the SQL-AST dependency graph over sql_views/.

Pure-module tests (duckdb only, no ``mcp`` extra needed). Assertions pin structural
invariants plus two dependency edges that are documented as load-bearing elsewhere
(dail_tracker_core/connections.py ordering comments; the lobbying base-view memory),
so a regression here means real registration breakage, not test brittleness.
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from mcp_server import sql_index  # noqa: E402


def _views() -> dict:
    return sql_index.graph(REPO)  # cached across tests in this process


def test_graph_covers_corpus_in_ast_mode():
    views = _views()
    assert len(views) >= 260
    modes = {}
    for v in views.values():
        modes[v["mode"]] = modes.get(v["mode"], 0) + 1
    # DuckDB's own parser handles the whole corpus today; allow a small regex tail
    # so one future exotic view doesn't fail the suite, but a broad fallback means
    # the header-splitting regressed.
    assert modes.get("ast", 0) >= 0.9 * len(views)
    # multi-view files are split into their component views
    assert len({v["file"] for v in views.values()}) < len(views)


def test_known_load_bearing_edges():
    views = _views()
    # connections.py DOMAIN_FILES comment: year_rank reads ONLY member_year_summary
    assert views["v_attendance_year_rank"]["reads"] == ["v_attendance_member_year_summary"]
    # lobbying base-view consolidation (Phase 1): revolving_door joins the shared resolver
    assert "v_lobbying_base_member_codes" in views["v_lobbying_revolving_door"]["reads"]


def test_detail_and_dependents():
    d = sql_index.detail(REPO, "v_payments_base")
    assert "v_payments_member_detail" in d["dependents"]
    missing = sql_index.detail(REPO, "v_no_such_view")
    assert "error" in missing and "did_you_mean" in missing


def test_order_risks_are_internally_consistent():
    views = _views()
    for r in sql_index.order_risks(views):
        vf, df = Path(r["file"]), Path(r["needs_file"])
        assert vf.parent == df.parent, "order risks are same-directory by definition"
        assert df.name > vf.name, "flagged dependency must sort after its consumer"
        assert r["needs"] in views[r["view"]]["reads"]


def test_summary_shape():
    s = sql_index.summary(REPO)
    assert s["views"] >= 260
    assert s["edges"] >= 100
    assert isinstance(s["order_risks"], list)
    assert s["cross_directory_edges"]["count"] >= 0
