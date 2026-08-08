from __future__ import annotations

import io
import json

from tools.hooks import guard_mcp_memory as guard


def _run(monkeypatch, capsys, payload: dict) -> tuple[int, str]:
    monkeypatch.setattr(guard.sys, "stdin", io.StringIO(json.dumps(payload)))
    result = guard.main()
    return result, capsys.readouterr().err


def test_blocks_mcp_tool_call_below_floor(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr(guard, "sample_memory", lambda: {"free_mb": 266, "total_mb": 16459})
    monkeypatch.setattr(guard, "record", lambda entry: None)
    payload = {"tool_name": "mcp__dail-tracker__code_outline", "tool_input": {"path": "foo.py"}}
    result, error = _run(monkeypatch, capsys, payload)
    assert result == 2
    assert "266 MB of physical RAM free" in error
    assert "mcp__dail-tracker__code_outline" in error


def test_allows_mcp_tool_call_above_floor(monkeypatch, capsys):
    monkeypatch.setattr(guard, "sample_memory", lambda: {"free_mb": 4000, "total_mb": 16459})
    monkeypatch.setattr(guard, "record", lambda entry: None)
    payload = {"tool_name": "mcp__dail-tracker__search_project", "tool_input": {"query": "x"}}
    assert _run(monkeypatch, capsys, payload) == (0, "")


def test_ignores_non_mcp_tools_even_below_floor(monkeypatch, capsys):
    monkeypatch.setattr(guard, "sample_memory", lambda: {"free_mb": 100, "total_mb": 16459})
    monkeypatch.setattr(guard, "record", lambda entry: None)
    payload = {"tool_name": "Read", "tool_input": {"file_path": "foo.py"}}
    assert _run(monkeypatch, capsys, payload) == (0, "")


def test_override_env_var_allows_call_below_floor(monkeypatch, capsys):
    monkeypatch.setenv("DAIL_SKIP_MCP_MEM_GUARD", "1")
    monkeypatch.setattr(guard, "sample_memory", lambda: {"free_mb": 200, "total_mb": 16459})
    monkeypatch.setattr(guard, "record", lambda entry: None)
    payload = {"tool_name": "mcp__dail-tracker__code_outline", "tool_input": {"path": "foo.py"}}
    assert _run(monkeypatch, capsys, payload) == (0, "")
