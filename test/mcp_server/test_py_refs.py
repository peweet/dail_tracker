"""Tests for mcp_server/py_refs.py — symbol-grain reference lookup.

The pure module needs jedi (mcp extra) so everything here importorskips it; the
server-integration test additionally skips when ``mcp`` is absent, mirroring
test_code_index.py. CI's full lane installs neither, so this file skips there —
it guards the local/MCP environment.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from mcp_server import py_refs  # noqa: E402


@pytest.fixture(autouse=True)
def _require_jedi_for_reference_tests(request):
    if request.node.name.startswith("test_refs"):
        pytest.importorskip("jedi")


def test_refs_finds_cross_file_call_sites():
    out = py_refs.refs(REPO, "mcp_server/code_index.py", "outline")
    assert "error" not in out
    assert out["symbol"] == "mcp_server/code_index.py:outline"
    paths = {r["path"] for r in out["references"]}
    assert "test/mcp_server/test_code_index.py" in paths
    assert out["counts"]["references"] >= 3
    # every row carries a real source line
    assert all(r["code"] and r["line"] > 0 for r in out["references"])


def test_refs_accepts_dotted_module_and_counts_import_bindings():
    out = py_refs.refs(REPO, "mcp_server.code_index", "build_code_index")
    assert "error" not in out
    # `from mcp_server.code_index import build_code_index`-style bindings land in
    # definitions (jedi semantics) — rename sites either way, so both lists matter
    everywhere = {r["path"] for r in out["references"]} | {d["path"] for d in out["definitions"]}
    assert "test/mcp_server/test_code_index.py" in everywhere


def test_refs_class_method_lookup():
    # _LazyModule._load is called from __getattr__ in the same file — a genuine
    # Class.method resolution through `self`, analysed statically (no import of server)
    out = py_refs.refs(REPO, "mcp_server/server.py", "_LazyModule._load")
    assert "error" not in out
    assert any(r["path"] == "mcp_server/server.py" and "self._load()" in r["code"] for r in out["references"])


def test_refs_wide_symbol_not_silently_capped():
    # jedi's _PARSED_FILE_LIMIT (30, an IDE latency guard) silently truncated wide
    # symbols: save_parquet spans ~140 files but a capped search reported 29. refs()
    # lifts the cap for the call — a repo-wide helper must report repo-wide.
    out = py_refs.refs(REPO, "services.parquet_io", "save_parquet", limit=200)
    assert "error" not in out
    assert out["counts"]["files"] > 100


def test_refs_limit_truncates_both_lists():
    # definitions (139 import bindings for save_parquet) must respect limit too —
    # an uncapped list is the context flood these tools exist to avoid
    out = py_refs.refs(REPO, "services.parquet_io", "save_parquet", limit=5)
    assert len(out["references"]) <= 5
    assert len(out["definitions"]) <= 5
    assert out["counts"]["definitions"] > 100  # counts still report the true totals


def test_refs_never_leaks_environment_paths():
    out = py_refs.refs(REPO, "mcp_server/code_index.py", "outline")
    rows = out["references"] + out["definitions"]
    assert not any(r["path"].startswith((".venv", ".git")) for r in rows)


def test_refs_error_shapes():
    assert "error" in py_refs.refs(REPO, "no/such/module.py", "x")
    assert "error" in py_refs.refs(REPO, "../outside.py", "x")
    miss = py_refs.refs(REPO, "mcp_server/code_index.py", "no_such_def")
    assert "error" in miss
    assert "outline" in miss["available"]
    assert "error" in py_refs.refs(REPO, "mcp_server/code_index.py", "a.b.c")
    not_a_class = py_refs.refs(REPO, "mcp_server/code_index.py", "outline.method")
    assert "not a class" in not_a_class["error"]


def test_locate_uses_tokens_and_unicode_character_columns():
    source = "é = 0; target = 1\n\ndef target_target(target='target'):\n    return target\n"
    assert py_refs._locate(source, "target") == (1, 7)
    assert py_refs._locate(source, "target_target") == (3, 4)


def test_resolver_rejects_excluded_and_absolute_paths(tmp_path):
    repo = tmp_path / "repo"
    (repo / "doc" / "private").mkdir(parents=True)
    private = repo / "doc" / "private" / "secret.py"
    private.write_text("SECRET = 1\n", encoding="utf-8")
    assert py_refs._resolve_module(repo, "doc/private/secret.py") is None
    assert py_refs._resolve_module(repo, str(private.resolve())) is None


def test_allowed_python_paths_honours_git_ignores_and_policy(tmp_path):
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    (repo / ".gitignore").write_text("ignored/\n", encoding="utf-8")
    for directory in ("public", "ignored", "pipeline_sandbox"):
        (repo / directory).mkdir()
        (repo / directory / "module.py").write_text("VALUE = 1\n", encoding="utf-8")

    assert py_refs._allowed_python_paths(repo.resolve()) == {"public/module.py"}


# ── server integration (needs the optional mcp extra) ────────────────────────


def test_py_refs_tool_registered():
    import asyncio

    pytest.importorskip("mcp")
    from mcp_server import server

    names = {t.name for t in asyncio.run(server.mcp.list_tools())}
    assert "py_refs" in names


def test_py_refs_tool_end_to_end():
    pytest.importorskip("jedi")
    pytest.importorskip("mcp")
    from mcp_server import server

    out = server.py_refs("mcp_server/code_index.py", "outline", limit=5)
    assert "error" not in out
    assert len(out["references"]) <= 5
    assert out["counts"]["references"] >= 3
