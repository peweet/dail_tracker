"""Tests for mcp_server/fts_index.py — the FTS5/BM25 content index behind search_project.

Pure-module tests over a synthetic mini-repo in tmp_path (no MCP extra, no real repo
walk — the real corpus is exercised by test_mcp_server_smoke). Assertions pin the
contract the tool's callers rely on: cAST-style scope headers, line spans that a
bounded Read can consume directly, mtime-incremental refresh, and the AND→OR query
fallback. A regression here means search_project v2 returns wrong spans — worse than
returning nothing.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from mcp_server import fts_index  # noqa: E402

PY_SRC = '''"""Module docstring for widget maths."""
import os


def compute_widget_ratio(a, b):
    """Divide widgets safely."""
    return a / max(b, 1)


class WidgetStore:
    """Holds widgets."""

    def save_widget(self, w):
        return w
'''

MD_SRC = """# Widget guide

intro line about widgets

## Ratio semantics

the ratio is never summed across grains

## Storage

widgets persist to parquet
"""


def _mini_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    (repo / "sql_views").mkdir(parents=True)
    (repo / "pkg").mkdir()
    (repo / "pkg" / "widgets.py").write_text(PY_SRC, encoding="utf-8")
    (repo / "doc.md").write_text(MD_SRC, encoding="utf-8")
    (repo / "sql_views" / "v_widgets.sql").write_text(
        "-- widget rollup view\nCREATE VIEW v_widgets AS SELECT 1;\n", encoding="utf-8"
    )
    return repo


def test_build_chunks_and_scope_headers(tmp_path):
    repo = _mini_repo(tmp_path)
    r = fts_index.refresh(repo)
    assert r["total_files"] == 3 and r["indexed"] == 3

    hits = fts_index.search(repo, "compute widget ratio")
    assert hits, "AND query over indexed terms must hit"
    top = hits[0]
    # cAST-style scope header: path::name(), plus a machine-usable line span
    assert "pkg/widgets.py::compute_widget_ratio()" in top["name"]
    a, b = map(int, top["span"].split("-"))
    assert 1 <= a < b, "span must be a 1-indexed start-end pair for bounded Read"
    assert top["kind"] == "code-chunk"


def test_md_sections_and_kind_filter(tmp_path):
    repo = _mini_repo(tmp_path)
    fts_index.refresh(repo)
    hits = fts_index.search(repo, "ratio never summed", kind="doc-section")
    assert hits and all(h["kind"] == "doc-section" for h in hits)
    assert "§ Ratio semantics" in hits[0]["name"]


def test_or_fallback_when_and_misses(tmp_path):
    repo = _mini_repo(tmp_path)
    fts_index.refresh(repo)
    # 'widgets' hits, 'zzzmissing' can't — AND finds nothing, OR must still recall
    hits = fts_index.search(repo, "widgets zzzmissing")
    assert hits, "OR fallback must fire when the AND query has no rows"


def test_import_edges_both_directions(tmp_path):
    repo = _mini_repo(tmp_path)
    (repo / "app.py").write_text(
        "from pkg.widgets import compute_widget_ratio\nimport os\n", encoding="utf-8"
    )
    (repo / "pkg" / "__init__.py").write_text("", encoding="utf-8")
    fts_index.refresh(repo)

    d = fts_index.deps(repo, "pkg/widgets.py")
    assert "app.py" in d["imported_by"], "reverse edge (blast radius) must exist"
    assert fts_index.deps(repo, "app.py")["imports"] == ["pkg/widgets.py"], (
        "stdlib import (os) must be dropped; only repo-internal edges stored"
    )


def test_import_edges_update_on_change(tmp_path):
    repo = _mini_repo(tmp_path)
    (repo / "app.py").write_text("from pkg import widgets\n", encoding="utf-8")
    (repo / "pkg" / "__init__.py").write_text("", encoding="utf-8")
    fts_index.refresh(repo)
    assert "app.py" in fts_index.deps(repo, "pkg/widgets.py")["imported_by"]

    time.sleep(0.01)
    (repo / "app.py").write_text("import os\n", encoding="utf-8")  # edge removed
    fts_index.refresh(repo)
    assert "app.py" not in fts_index.deps(repo, "pkg/widgets.py")["imported_by"]


def test_incremental_refresh_and_delete(tmp_path):
    repo = _mini_repo(tmp_path)
    fts_index.refresh(repo)
    # unchanged → no reindex work
    assert fts_index.refresh(repo)["indexed"] == 0

    # touch one file with new content → exactly one file reindexed, new term findable
    p = repo / "pkg" / "widgets.py"
    time.sleep(0.01)  # ensure mtime moves on coarse filesystems
    p.write_text(PY_SRC.replace("Divide widgets", "Divide sprockets"), encoding="utf-8")
    assert fts_index.refresh(repo)["indexed"] == 1
    assert fts_index.search(repo, "sprockets")

    # delete a file → its chunks are purged
    (repo / "doc.md").unlink()
    assert fts_index.refresh(repo)["removed"] == 1
    assert not fts_index.search(repo, "parquet persist", kind="doc-section")
