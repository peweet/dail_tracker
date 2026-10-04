from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load():
    spec = importlib.util.spec_from_file_location(
        "session_context_mcp_test", ROOT / "tools" / "hooks" / "session_context.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_mcp_note_resolves_bare_executable_from_path(tmp_path, monkeypatch):
    module = _load()
    server_dir = tmp_path / "mcp_server"
    server_dir.mkdir()
    (server_dir / "server.py").write_text("value = 1\n", encoding="utf-8")
    (tmp_path / ".mcp.json").write_text(
        json.dumps(
            {
                "mcpServers": {
                    "dail-tracker": {
                        "command": "uv",
                        "args": ["run", "python", "mcp_server/server.py"],
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    uv = tmp_path / "uv.exe"
    uv.write_text("", encoding="utf-8")

    monkeypatch.setattr(module, "REPO", tmp_path)
    monkeypatch.setattr(module.shutil, "which", lambda command: str(uv) if command == "uv" else None)
    monkeypatch.setenv("DAIL_SKIP_MCP_PROBE", "1")

    assert "config+code OK" in module._mcp_note()


def test_mcp_note_rejects_missing_bare_executable(tmp_path, monkeypatch):
    module = _load()
    (tmp_path / ".mcp.json").write_text(
        json.dumps({"mcpServers": {"dail-tracker": {"command": "missing-command", "args": []}}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "REPO", tmp_path)
    monkeypatch.setattr(module.shutil, "which", lambda command: None)

    assert "NOT FOUND" in module._mcp_note()


def test_session_start_context_has_a_hard_budget_and_reports_omissions():
    module = _load()
    parts = ["branch: test", *(f"note-{index}: " + "x" * 300 for index in range(20))]

    context = module._bounded_context(parts)

    assert len(context) <= module.SESSION_CONTEXT_MAX_CHARS
    assert "branch: test" in context
    assert "lower-priority note(s) omitted" in context
    assert "don't scan parquet" in context


def _probe_repo(tmp_path, monkeypatch):
    module = _load()
    (tmp_path / "mcp_server").mkdir()
    (tmp_path / "mcp_server/server.py").write_text("value = 1\n", encoding="utf-8")
    executable = tmp_path / "uv.exe"
    executable.write_text("", encoding="utf-8")
    (tmp_path / ".mcp.json").write_text(
        json.dumps(
            {
                "mcpServers": {
                    "dail-tracker": {"command": "uv", "args": ["run", "python", "mcp_server/server.py"]},
                    "other": {"command": "uv", "args": ["unrelated"]},
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "REPO", tmp_path)
    monkeypatch.setattr(module.shutil, "which", lambda _: str(executable))
    monkeypatch.delenv("DAIL_SKIP_MCP_PROBE", raising=False)
    monkeypatch.setattr(module.time, "time", lambda: 1000)
    return module


def test_probe_targets_dail_tracker_and_reuses_labeled_success(tmp_path, monkeypatch):
    module = _probe_repo(tmp_path, monkeypatch)
    calls = []

    def probe(command, args):
        calls.append(args)
        return "MCP: handshake OK (dail-tracker tools reachable)"

    monkeypatch.setattr(module, "_mcp_connect_probe", probe)
    assert "handshake OK" in module._mcp_note()
    assert "cached" in module._mcp_note()
    assert calls == [["run", "python", "mcp_server/server.py"]]
    monkeypatch.setattr(module.time, "time", lambda: 1061)
    module._mcp_note()
    assert len(calls) == 2


def test_broken_optional_server_does_not_mask_dail_tracker(tmp_path, monkeypatch):
    module = _probe_repo(tmp_path, monkeypatch)
    config = json.loads((tmp_path / ".mcp.json").read_text(encoding="utf-8"))
    config["mcpServers"]["optional-missing"] = {"args": ["missing.py"]}
    config["mcpServers"]["optional-placeholder"] = {"command": "${OPTIONAL_CMD}", "args": []}
    (tmp_path / ".mcp.json").write_text(json.dumps(config), encoding="utf-8")
    calls = []
    monkeypatch.setattr(module, "_mcp_connect_probe", lambda command, args: calls.append(args) or "MCP: handshake OK")

    assert "handshake OK" in module._mcp_note()
    assert calls == [["run", "python", "mcp_server/server.py"]]


def test_probe_failure_cache_expires_early(tmp_path, monkeypatch):
    module = _probe_repo(tmp_path, monkeypatch)
    calls = []
    monkeypatch.setattr(module, "_mcp_connect_probe", lambda *_: calls.append(1) or "MCP: WARN timeout")
    module._mcp_note()
    assert "cached" in module._mcp_note()
    monkeypatch.setattr(module.time, "time", lambda: 1016)
    module._mcp_note()
    assert len(calls) == 2


@pytest.mark.parametrize("changed", ["source", "config", "corrupt", "future"])
def test_probe_cache_invalidates(tmp_path, monkeypatch, changed):
    module = _probe_repo(tmp_path, monkeypatch)
    calls = []
    monkeypatch.setattr(module, "_mcp_connect_probe", lambda *_: calls.append(1) or "MCP: handshake OK")
    module._mcp_note()
    cache = tmp_path / "logs/mcp_probe_cache.json"
    if changed == "source":
        (tmp_path / "mcp_server/server.py").write_text("value = 2\n", encoding="utf-8")
    elif changed == "config":
        path = tmp_path / ".mcp.json"
        value = json.loads(path.read_text(encoding="utf-8"))
        value["mcpServers"]["dail-tracker"]["args"].insert(0, "--frozen")
        path.write_text(json.dumps(value), encoding="utf-8")
    elif changed == "corrupt":
        cache.write_text("broken", encoding="utf-8")
    else:
        value = json.loads(cache.read_text(encoding="utf-8"))
        value["captured_at"] = 2000
        cache.write_text(json.dumps(value), encoding="utf-8")
    module._mcp_note()
    assert len(calls) == 2


def test_skip_probe_never_reads_or_writes_cache(tmp_path, monkeypatch):
    module = _probe_repo(tmp_path, monkeypatch)
    monkeypatch.setenv("DAIL_SKIP_MCP_PROBE", "1")
    monkeypatch.setattr(module, "_mcp_connect_probe", lambda *_: pytest.fail("probe called"))
    assert "probe skipped" in module._mcp_note()
    assert not (tmp_path / "logs").exists()
