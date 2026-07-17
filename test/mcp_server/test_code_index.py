"""Tests for mcp_server/code_index.py — the cheap code-scanning layer.

The pure module (stdlib ast) is tested without the optional ``mcp`` extra; the
server-integration tests (tool registered, search_project 'code' surface) skip
when ``mcp`` is absent, mirroring test_mcp_server_smoke.py.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from mcp_server import code_index  # noqa: E402


def test_outline_file_shape():
    out = code_index.outline(REPO, "mcp_server/code_index.py")
    assert "error" not in out
    assert out["path"] == "mcp_server/code_index.py"
    assert out["lines"] > 50
    assert out["doc"].startswith("Cheap programmatic code scanning")
    names = {d["name"] for d in out["defs"]}
    assert {"outline", "build_code_index"} <= names
    # spans are "start-end" and parse as ints
    for d in out["defs"]:
        a, b = d["span"].split("-")
        assert int(a) <= int(b)


def test_outline_captures_signature_and_decorators():
    out = code_index.outline(REPO, "dail_tracker_core/db.py")
    assert "error" not in out
    by_name = {d["name"]: d for d in out["defs"]}
    assert "register_views" in by_name
    assert "conn" in by_name["register_views"]["sig"]
    assert "swallow_errors" in by_name["register_views"]["sig"]
    assert "duckdb" in out["imports"]


def test_outline_directory_mode():
    out = code_index.outline(REPO, "mcp_server")
    assert "error" not in out
    mods = {m["name"]: m for m in out["modules"]}
    assert "code_index.py" in mods
    assert "server.py" in mods
    assert mods["server.py"]["lines"] > 1000
    # directory mode returns def NAMES only — no bodies, no signatures
    assert isinstance(mods["code_index.py"]["defs"][0], str)


def test_outline_rejects_escape_and_missing():
    assert "error" in code_index.outline(REPO, "../outside.py")
    assert "error" in code_index.outline(REPO, "no/such/file.py")
    assert "error" in code_index.outline(REPO, "README.md")


def test_build_code_index_covers_repo_and_skips_env():
    idx = code_index.build_code_index(REPO)
    assert len(idx) > 300  # whole-project coverage, not a single package
    assert all(e["kind"] == "code" for e in idx)
    paths = [e["path"] for e in idx]
    assert not any(p.split("/")[0].startswith(".") or "__pycache__" in p for p in paths)
    by_name = {e["name"]: e for e in idx}
    db = by_name["dail_tracker_core.db"]
    assert "register_views" in db["haystack"]  # def names are searchable


# ── server integration (needs the optional mcp extra) ────────────────────────

mcp_mod = pytest.importorskip("mcp")

from mcp_server import server  # noqa: E402


def test_code_outline_tool_registered():
    import asyncio

    names = {t.name for t in asyncio.run(server.mcp.list_tools())}
    assert "code_outline" in names


def test_search_project_finds_code():
    hits = server.search_project("register_views connection", kind="code")
    assert hits["count"] >= 1
    assert any(r["path"] == "dail_tracker_core/db.py" for r in hits["results"])
