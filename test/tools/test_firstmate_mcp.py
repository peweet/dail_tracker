"""Protocol and safety tests for the narrow Pi Firstmate MCP bridge."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import subprocess

import pytest


REPO = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def firstmate_mcp():
    spec = importlib.util.spec_from_file_location("firstmate_mcp", REPO / "tools" / "firstmate_mcp.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_initialize_and_tool_catalogue_are_stdio_mcp_compatible(firstmate_mcp):
    initialized = firstmate_mcp.dispatch(
        {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-03-26"}}
    )
    assert initialized["result"]["capabilities"] == {"tools": {}}
    assert initialized["result"]["serverInfo"]["name"] == "pi-firstmate"

    listed = firstmate_mcp.dispatch({"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
    tools = {tool["name"]: tool for tool in listed["result"]["tools"]}
    assert set(tools) == {"firstmate_doctor", "firstmate_preflight"}
    assert all(tool["annotations"]["readOnlyHint"] for tool in tools.values())
    assert tools["firstmate_preflight"]["inputSchema"]["required"] == ["task"]
    assert "workspace" not in tools["firstmate_preflight"]["inputSchema"]["properties"]


def test_notifications_do_not_emit_responses(firstmate_mcp):
    assert firstmate_mcp.dispatch({"jsonrpc": "2.0", "method": "notifications/initialized"}) is None


def test_preflight_rejects_unbounded_or_missing_arguments(firstmate_mcp):
    missing = firstmate_mcp.dispatch(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "firstmate_preflight"}}
    )
    assert missing["result"]["isError"] is True
    assert "requires a non-empty task" in missing["result"]["content"][0]["text"]

    extra = firstmate_mcp.dispatch(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "firstmate_preflight", "arguments": {"task": "review", "workspace": "C:/"}},
        }
    )
    assert extra["result"]["isError"] is True
    assert "exactly one argument" in extra["result"]["content"][0]["text"]


def test_preflight_uses_fixed_read_only_pi_command(firstmate_mcp, monkeypatch):
    seen: dict[str, object] = {}

    def fake_run(command, **kwargs):
        seen["command"] = command
        seen["kwargs"] = kwargs
        return subprocess.CompletedProcess(command, 0, stdout="advisory", stderr="")

    monkeypatch.setattr(firstmate_mcp, "_run", fake_run)
    response = firstmate_mcp.dispatch(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "firstmate_preflight", "arguments": {"task": "Check the repair."}},
        }
    )

    command = seen["command"]
    assert command[:7] == ["wsl.exe", "-d", "Ubuntu", "--cd", "/home/pglyn/firstmate", "--", "env"]
    assert "--no-session" in command and "--no-extensions" in command
    assert command[command.index("--tools") + 1] == "read,grep,find,ls"
    assert "bash" not in command and "edit" not in command and "write" not in command
    assert seen["kwargs"]["timeout"] == firstmate_mcp.PREFLIGHT_TIMEOUT_SECONDS
    assert response["result"]["content"][0]["text"] == "advisory"
