"""Tests for mcp_server/code_index.py — the cheap code-scanning layer.

The pure module (stdlib ast) is tested without the optional ``mcp`` extra; the
server-integration tests (tool registered, search_project 'code' surface) skip
when ``mcp`` is absent, mirroring test_mcp_server_smoke.py.
"""

from __future__ import annotations

import subprocess
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


def test_outline_concise_mode_is_names_and_spans_only():
    out = code_index.outline(REPO, "mcp_server/code_index.py", response_format="concise")
    assert "error" not in out
    assert all(isinstance(d, str) for d in out["defs"])
    assert any(d.startswith("def outline ") for d in out["defs"])
    assert "imports" not in out
    import json

    detailed = code_index.outline(REPO, "mcp_server/code_index.py")
    assert len(json.dumps(out)) < len(json.dumps(detailed)) / 2


def test_concise_defs_flattens_methods_with_class_prefix():
    import ast

    tree = ast.parse("class A:\n    def m(self):\n        pass\n\ndef top():\n    pass\n")
    lines = code_index._concise_defs(code_index._outline_tree(tree))
    assert lines[0].startswith("class A ")
    assert any(ln.startswith("def A.m ") for ln in lines)
    assert any(ln.startswith("def top ") for ln in lines)


def test_outline_rejects_bad_response_format():
    assert "error" in code_index.outline(REPO, "mcp_server/code_index.py", response_format="terse")


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


def test_outline_reports_complete_class_contract(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "models.py").write_text(
        """@registered('widget')
class Widget[T](Base, Protocol, metaclass=WidgetMeta, frozen=True):
    \"\"\"A generic widget.\"\"\"

    class State(Enum):
        READY = 'ready'

    @classmethod
    def make(cls, value: T) -> T:
        def validate(item: T) -> T:
            return item
        return validate(value)
""",
        encoding="utf-8",
    )

    out = code_index.outline(repo, "models.py")
    widget = out["defs"][0]
    assert widget["span"].startswith("1-")
    assert widget["decorators"] == ["registered('widget')"]
    assert widget["bases"] == ["Base", "Protocol"]
    assert widget["metaclass"] == "WidgetMeta"
    assert widget["keywords"] == ["frozen=True"]
    assert widget["type_params"] == ["T"]
    assert widget["classes"][0]["name"] == "State"
    assert widget["methods"][0]["nested"][0]["name"] == "validate"
    assert out["def_count"] == 4


def test_parse_errors_and_declared_source_encoding_are_reported(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    encoded = b'# -*- coding: latin-1 -*-\n"""caf\xe9 module"""\ndef ok():\n    pass\n'
    (repo / "encoded.py").write_bytes(encoded)
    (repo / "broken.py").write_text("def broken(:\n", encoding="utf-8")

    assert code_index.outline(repo, "encoded.py")["doc"] == "caf\u00e9 module"
    error = code_index.outline(repo, "broken.py")["error"]
    assert "SyntaxError" in error and "line 1" in error
    indexed = {entry["path"]: entry for entry in code_index.build_code_index(repo)}
    assert "parse_error" in indexed["broken.py"]


def test_scan_policy_excludes_dot_private_and_sandbox_trees(tmp_path):
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    (repo / ".gitignore").write_text("ignored/\n", encoding="utf-8")
    for directory in ("public", ".agents", "doc/private", "pipeline_sandbox", "ignored"):
        (repo / directory).mkdir(parents=True, exist_ok=True)
        (repo / directory / "module.py").write_text("VALUE = 1\n", encoding="utf-8")

    paths = {entry["path"] for entry in code_index.build_code_index(repo)}
    assert paths == {"public/module.py"}


# ── server integration (needs the optional mcp extra) ────────────────────────


def test_code_outline_tool_registered():
    import asyncio

    pytest.importorskip("mcp")
    from mcp_server import server

    names = {t.name for t in asyncio.run(server.mcp.list_tools())}
    assert "code_outline" in names


def test_search_project_finds_code():
    pytest.importorskip("mcp")
    from mcp_server import server

    hits = server.search_project("register_views connection", kind="code")
    assert hits["count"] >= 1
    assert any(r["path"] == "dail_tracker_core/db.py" for r in hits["results"])


def test_search_project_rejects_unknown_kind():
    pytest.importorskip("mcp")
    from mcp_server import server

    out = server.search_project("widget", kind="private")
    assert "error" in out
    assert "memory" in out["allowed"]


def test_json_peek_rejects_prefix_sibling_escape(tmp_path, monkeypatch):
    pytest.importorskip("mcp")
    from mcp_server import server

    repo = tmp_path / "repo"
    sibling = tmp_path / "repo_private"
    repo.mkdir()
    sibling.mkdir()
    (sibling / "secret.json").write_text('{"secret": true}', encoding="utf-8")
    monkeypatch.setattr(server, "REPO", repo.resolve())

    out = server.json_peek("../repo_private/secret.json")
    assert "error" in out
