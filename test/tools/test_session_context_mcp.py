from __future__ import annotations

import importlib.util
import json
from pathlib import Path

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
