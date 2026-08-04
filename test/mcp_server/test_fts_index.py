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

import pytest

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
    (repo / "app.py").write_text("from pkg.widgets import compute_widget_ratio\nimport os\n", encoding="utf-8")
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


def test_python_chunks_cover_decorators_top_level_gaps_and_long_bodies():
    body_lines = [f"    value_{index} = '{'x' * 60}'" for index in range(120)]
    source = "\n".join(
        [
            "@register('first')",
            "def first():",
            "    return 1",
            "",
            "ROUTES = {'home': first}",
            "",
            "@register('second')",
            "def second():",
            *body_lines,
            "    return value_119",
            "",
            "AFTER_DEFINITIONS = 'indexed too'",
        ]
    )

    chunks = fts_index._py_chunks("app.py", source)
    lines = source.splitlines()
    assert any("@register('second')" in header for header, *_ in chunks)
    assert any("ROUTES" in chunk for _, chunk, _, _ in chunks)
    assert any("AFTER_DEFINITIONS" in chunk for _, chunk, _, _ in chunks)
    assert sum("second()." in header for header, *_ in chunks) > 1
    for _, body, span, _ in chunks:
        start, end = map(int, span.split("-"))
        assert body == "\n".join(lines[start - 1 : end])


def test_large_class_keeps_summary_bases_attributes_and_methods():
    source = "\n".join(
        [
            "@registry.bind",
            "class LargeStore(BaseStore, metaclass=StoreMeta):",
            '    """Stores large widgets."""',
            "    table_name = 'widgets'",
            "",
            "    def save(self, item):",
            "        return item",
            *[f"    field_{index} = {index}" for index in range(160)],
            "    final_marker = 'still indexed'",
        ]
    )
    chunks = fts_index._py_chunks("store.py", source)
    headers = [header for header, *_ in chunks]
    bodies = [body for _, body, _, _ in chunks]
    assert any("class LargeStore(BaseStore, metaclass=StoreMeta) summary" in header for header in headers)
    assert any("@registry.bind" in header for header in headers)
    assert any("LargeStore.save()." in header for header in headers)
    assert any("final_marker" in body for body in bodies)


def test_scan_policy_and_external_memory_are_explicit(tmp_path):
    repo = _mini_repo(tmp_path)
    for directory in (".agents", "doc/private", "pipeline_sandbox"):
        (repo / directory).mkdir(parents=True)
        (repo / directory / "hidden.md").write_text("confidential_private_marker\n", encoding="utf-8")
    (repo / "memory").mkdir()
    (repo / "memory" / "note.md").write_text("repository_memory_marker\n", encoding="utf-8")
    external = tmp_path / "external-memory"
    external.mkdir()
    (external / "note.md").write_text("external_memory_marker\n", encoding="utf-8")

    fts_index.refresh(repo, external)
    assert not fts_index.search(repo, "confidential_private_marker")
    assert not fts_index.search(repo, "external_memory_marker")
    assert fts_index.search(repo, "repository_memory_marker")[0]["path"] == "memory/note.md"

    fts_index.refresh(repo, external, include_external_memory=True)
    external_hits = fts_index.search(repo, "external_memory_marker", kind="memory")
    assert external_hits[0]["path"] == "memory://external/note.md"
    with pytest.raises(ValueError, match="invalid chunk kind"):
        fts_index.search(repo, "widgets", kind="private")


def test_parse_errors_are_reported_and_stale_chunks_removed(tmp_path):
    repo = _mini_repo(tmp_path)
    path = repo / "pkg" / "widgets.py"
    path.write_text("def valid():\n    return 'temporary_search_marker'\n", encoding="utf-8")
    fts_index.refresh(repo)
    assert fts_index.search(repo, "temporary_search_marker")

    path.write_text("def invalid(:\n    temporary_search_marker = True\n", encoding="utf-8")
    report = fts_index.refresh(repo)
    assert report["error_count"] == 1
    assert report["errors"][0]["path"] == "pkg/widgets.py"
    assert "SyntaxError" in report["errors"][0]["error"]
    assert not fts_index.search(repo, "temporary_search_marker")


def test_import_edges_rebuild_when_target_is_added_or_deleted(tmp_path):
    repo = _mini_repo(tmp_path)
    (repo / "app.py").write_text("from newmod import value\n", encoding="utf-8")
    fts_index.refresh(repo)
    assert fts_index.deps(repo, "app.py")["imports"] == []

    target = repo / "newmod.py"
    target.write_text("value = 1\n", encoding="utf-8")
    fts_index.refresh(repo)
    assert fts_index.deps(repo, "newmod.py")["imported_by"] == ["app.py"]

    target.unlink()
    fts_index.refresh(repo)
    assert fts_index.deps(repo, "app.py")["imports"] == []
    assert fts_index.deps(repo, "newmod.py")["imported_by"] == []


def test_python_source_uses_declared_encoding(tmp_path):
    repo = _mini_repo(tmp_path)
    encoded = b'# -*- coding: latin-1 -*-\n"""caf\xe9 encoded marker"""\n'
    (repo / "encoded.py").write_bytes(encoded)
    report = fts_index.refresh(repo)
    assert report["error_count"] == 0
    assert fts_index.search(repo, "encoded marker")
