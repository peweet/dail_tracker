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


def test_index_names_handle_async_and_qualified_nested_defs():
    import ast

    tree = ast.parse("class Client:\n    async def fetch(self):\n        pass\n")
    names = code_index._definition_names(code_index._outline_tree(tree))
    assert names == ["Client", "fetch", "Client.fetch"]


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
    subprocess.run(["git", "-C", str(repo), "add", "public/module.py"], check=True)

    paths = {entry["path"] for entry in code_index.build_code_index(repo)}
    assert paths == {"public/module.py"}


def test_git_navigation_excludes_untracked_files_even_when_not_ignored(tmp_path):
    repo = tmp_path / "repo"
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    (repo / "tracked.py").write_text("VALUE = 'reviewed'\n", encoding="utf-8")
    (repo / "untracked.py").write_text("VALUE = 'scratch'\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(repo), "add", "tracked.py"], check=True)

    assert [path.name for path in code_index.iter_repository_files(repo, {".py"})] == ["tracked.py"]
    assert code_index.outline(repo, "tracked.py")["path"] == "tracked.py"
    assert "not tracked" in code_index.outline(repo, "untracked.py")["error"]


def test_outline_enforces_scan_policy_for_explicit_files_and_subpackages(tmp_path):
    repo = tmp_path / "repo"
    public = repo / "public"
    public.mkdir(parents=True)
    (public / "visible.py").write_text("def visible():\n    pass\n", encoding="utf-8")
    (public / "visible_pkg").mkdir()
    (public / "visible_pkg" / "__init__.py").write_text("", encoding="utf-8")

    excluded = (".agents", "private_models", "pipeline_sandbox", "generated_clients")
    for name in excluded:
        package = public / name
        package.mkdir()
        (package / "__init__.py").write_text("", encoding="utf-8")
        (package / "module.py").write_text("def hidden():\n    pass\n", encoding="utf-8")
    excluded_files = (".hidden.py", "private_module.py", "sandbox_helpers.py", "generated_client.py")
    for name in excluded_files:
        (public / name).write_text("def hidden():\n    pass\n", encoding="utf-8")

    directory = code_index.outline(repo, "public")
    assert directory["subpackages"] == ["visible_pkg"]
    for name in excluded:
        assert "excluded by repository scan policy" in code_index.outline(repo, f"public/{name}")["error"]
        assert "excluded by repository scan policy" in code_index.outline(repo, f"public/{name}/module.py")["error"]
    for name in excluded_files:
        assert "excluded by repository scan policy" in code_index.outline(repo, f"public/{name}")["error"]


def test_outline_limit_budgets_nested_definitions_and_keeps_parent_shell(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    methods = "\n".join(f"    def method_{index}(self):\n        pass" for index in range(8))
    (repo / "large.py").write_text(
        f"class Large:\n{methods}\n\ndef after():\n    pass\n",
        encoding="utf-8",
    )

    detailed = code_index.outline(repo, "large.py", limit=3)
    assert detailed["def_count"] == 10
    assert [entry["name"] for entry in detailed["defs"]] == ["Large"]
    assert [entry["name"] for entry in detailed["defs"][0]["methods"]] == ["method_0", "method_1"]
    assert detailed["truncated"] == "7 more definitions (nested included)"
    assert code_index._definition_count(detailed["defs"]) == 3

    concise = code_index.outline(repo, "large.py", limit=3, response_format="concise")
    assert len(concise["defs"]) == 3
    assert concise["defs"][0].startswith("class Large ")


def test_outline_clamps_client_limit_to_server_side_maximum(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    methods = "\n".join(f"    def method_{index}(self):\n        pass" for index in range(205))
    (repo / "huge.py").write_text(f"class Huge:\n{methods}\n", encoding="utf-8")

    out = code_index.outline(repo, "huge.py", limit=1_000_000, response_format="concise")
    assert out["def_count"] == 206
    assert len(out["defs"]) == code_index.OUTLINE_DEFINITION_CAP
    assert out["truncated"] == "6 more definitions (nested included)"


def test_git_scan_failure_fails_closed(tmp_path, monkeypatch):
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)
    (repo / "visible.py").write_text("VALUE = 1\n", encoding="utf-8")
    monkeypatch.setattr(code_index, "_git_visible_paths", lambda _repo: None)
    assert list(code_index.iter_repository_files(repo, {".py"})) == []


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
    assert "external_memory" in out["allowed"]


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


def test_json_peek_rejects_json_beyond_bounded_read_size(tmp_path, monkeypatch):
    pytest.importorskip("mcp")
    from mcp_server import server

    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "large.json").write_text('{"value":"' + "x" * 64 + '"}', encoding="utf-8")
    monkeypatch.setattr(server, "REPO", repo.resolve())
    monkeypatch.setattr(server, "_JSON_PEEK_MAX_BYTES", 32)

    out = server.json_peek("large.json")
    assert "bounded read size" in out["error"]
    assert out["max_bytes"] == 32


def test_json_peek_streams_only_bounded_jsonl_sample(tmp_path, monkeypatch):
    pytest.importorskip("mcp")
    from mcp_server import server

    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "sample.jsonl").write_text('{"ok": 1}\n' + "x" * 80 + "\n", encoding="utf-8")
    monkeypatch.setattr(server, "REPO", repo.resolve())
    monkeypatch.setattr(server, "_JSONL_PEEK_MAX_LINE_BYTES", 32)

    sampled = server.json_peek("sample.jsonl", limit=1)
    assert "error" not in sampled
    assert sampled["value"]["len"] == 1

    oversized = server.json_peek("sample.jsonl", limit=2)
    assert "bounded line size" in oversized["error"]
    assert oversized["max_line_bytes"] == 32
