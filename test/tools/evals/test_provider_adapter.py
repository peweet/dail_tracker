from __future__ import annotations

import json
import subprocess
from pathlib import Path
from types import SimpleNamespace

import anyio
import pytest

from tools.evals import provider_adapter as adapter

REPO = Path(__file__).resolve().parents[3]
RUNNERS = (
    "sdk_smoke.py",
    "routing_probe.py",
    "harness_bench.py",
    "package_bench.py",
    "build_bench.py",
    "cost_of_change_bench.py",
    "claude_md_prune_bench.py",
)


def _which(found: bool):
    return lambda name: str(Path("C:/bin") / ("codex.exe" if name == "codex" else name)) if found else None


def test_auto_prefers_codex_then_falls_back_to_claude():
    assert (
        adapter.select_provider(
            environ={},
            which=_which(True),
            claude_available=lambda: True,
        )
        == "codex"
    )


def test_all_paid_runners_use_adapter_and_are_import_safe():
    for name in RUNNERS:
        source = (REPO / "tools" / "evals" / name).read_text(encoding="utf-8")
        assert "run_eval(" in source, name
        assert "claude_agent_sdk" not in source, name
        assert 'if __name__ == "__main__":' in source, name
    assert (
        adapter.select_provider(
            environ={},
            which=_which(False),
            claude_available=lambda: True,
        )
        == "claude"
    )


def test_provider_and_model_environment_overrides_are_validated(tmp_path):
    request = adapter.EvalRequest(prompt="x", cwd=tmp_path, claude_model="claude-old")
    assert adapter.resolve_model("codex", request, environ={}) == "gpt-5.6-sol"
    assert adapter.resolve_model("claude", request, environ={}) == "claude-old"
    assert adapter.resolve_model("codex", request, environ={"DAIL_EVAL_MODEL": "gpt-test"}) == "gpt-test"
    assert adapter.resolve_reasoning_effort(environ={}) == "medium"
    assert (
        adapter.resolve_reasoning_effort(environ={"DAIL_EVAL_REASONING_EFFORT": "HIGH"})
        == "high"
    )
    with pytest.raises(adapter.EvalProviderError, match="DAIL_EVAL_REASONING_EFFORT"):
        adapter.resolve_reasoning_effort(environ={"DAIL_EVAL_REASONING_EFFORT": "turbo"})
    with pytest.raises(adapter.EvalProviderError, match="DAIL_EVAL_PROVIDER"):
        adapter.select_provider("other", environ={}, which=_which(True))


def test_codex_command_is_noninteractive_scoped_and_reproducible(tmp_path):
    request = adapter.EvalRequest(
        prompt="inspect",
        cwd=tmp_path,
        project_settings=False,
        sandbox="workspace-write",
        allowed_tools=["Read"],
        disallowed_tools=["mcp__dail-tracker__search_speeches"],
        mcp_servers={
            "dail-tracker": {
                "command": "python",
                "args": ["mcp_server/server.py"],
                "env": {"PYTHONUTF8": "1"},
            }
        },
    )
    command = adapter.build_codex_command(
        request,
        executable="codex",
        model="gpt-5.6-sol",
        reasoning_effort="high",
    )

    assert command[:3] == ["codex", "exec", "--json"]
    assert "--ephemeral" in command
    assert command[command.index("--sandbox") + 1] == "workspace-write"
    assert command[command.index("--cd") + 1] == str(tmp_path.resolve())
    assert command[command.index("--model") + 1] == "gpt-5.6-sol"
    assert "--ignore-user-config" in command
    assert "--ignore-rules" in command
    assert "--skip-git-repo-check" in command
    assert command[-1] == "-"
    rendered = "\n".join(command)
    assert 'approval_policy="never"' in rendered
    assert 'web_search="disabled"' in rendered
    assert 'model_reasoning_effort="high"' in rendered
    assert 'mcp_servers.dail-tracker.disabled_tools=["search_speeches"]' in rendered


def test_codex_jsonl_parser_normalizes_text_tools_and_usage():
    lines = [
        {"type": "thread.started", "thread_id": "t1"},
        {"type": "turn.started"},
        {
            "type": "item.started",
            "item": {
                "id": "cmd-1",
                "type": "command_execution",
                "command": "powershell -Command rg -n needle .",
            },
        },
        {
            "type": "item.started",
            "item": {
                "id": "mcp-1",
                "type": "mcp_tool_call",
                "server": "dail-tracker",
                "tool": "describe_dataset",
            },
        },
        {
            "type": "item.completed",
            "item": {
                "id": "mcp-1",
                "type": "mcp_tool_call",
                "server": "dail-tracker",
                "tool": "describe_dataset",
            },
        },
        {
            "type": "item.completed",
            "item": {"id": "msg-1", "type": "agent_message", "text": "final answer"},
        },
        {
            "type": "turn.completed",
            "usage": {
                "input_tokens": 500,
                "cached_input_tokens": 185,
                "output_tokens": 12,
                "reasoning_output_tokens": 4,
            },
        },
    ]
    stream = "\n".join(json.dumps(line) for line in lines)
    result = adapter.parse_codex_jsonl(
        stream,
        model="gpt-5.6-sol",
        reasoning_effort="medium",
    )

    assert result.final_text == "final answer"
    assert result.tool_names == ["Grep", "mcp__dail-tracker__describe_dataset"]
    assert result.num_turns == 1
    assert result.usage["input_tokens"] == 315
    assert result.usage["cache_read_input_tokens"] == 185
    assert result.usage["raw_input_tokens"] == 500
    assert result.reasoning_effort == "medium"
    assert not result.is_error


def test_codex_runner_uses_stdin_without_shell_and_records_cli_limitations(tmp_path):
    captured = {}
    stdout = "\n".join(
        [
            json.dumps({"type": "turn.started"}),
            json.dumps(
                {
                    "type": "item.completed",
                    "item": {"id": "m", "type": "agent_message", "text": "ok"},
                }
            ),
            json.dumps({"type": "turn.completed", "usage": {"input_tokens": 2, "output_tokens": 1}}),
        ]
    )

    def fake_run(command, **kwargs):
        captured["command"] = command
        captured.update(kwargs)
        return subprocess.CompletedProcess(command, 0, stdout=stdout, stderr="progress")

    request = adapter.EvalRequest(
        prompt="task",
        system_prompt="system",
        cwd=tmp_path,
        allowed_tools=["Read"],
        disallowed_tools=["Bash"],
    )
    result = adapter.run_codex(
        request,
        executable="codex",
        model="gpt-5.6-sol",
        reasoning_effort="medium",
        run=fake_run,
        environ={"PATH": "C:/bin", "USERPROFILE": "C:/Users/tester"},
    )

    assert captured["shell"] is False
    assert captured["input"] == "system\n\ntask"
    assert captured["cwd"] == str(tmp_path.resolve())
    assert captured["command"][-1] == "-"
    if adapter.os.name == "nt":
        assert captured["env"]["HOME"] == "C:/Users/tester"
    assert result.final_text == "ok"
    assert any("max-turns" in note for note in result.diagnostics)
    assert any("allowed-tools" in note for note in result.diagnostics)
    assert any("Bash" in note for note in result.diagnostics)


def test_codex_runner_preserves_partial_jsonl_on_timeout(tmp_path):
    partial = "\n".join(
        [
            json.dumps({"type": "turn.started"}),
            json.dumps(
                {
                    "type": "item.started",
                    "item": {"id": "cmd", "type": "command_execution", "command": "rg needle"},
                }
            ),
        ]
    )

    def timeout_run(command, **kwargs):
        raise subprocess.TimeoutExpired(command, kwargs["timeout"], output=partial)

    result = adapter.run_codex(
        adapter.EvalRequest(prompt="task", cwd=tmp_path, timeout_seconds=3),
        executable="codex",
        model="gpt-5.6-sol",
        run=timeout_run,
        environ={},
    )
    assert result.is_error
    assert "timed out after 3 seconds" in (result.error or "")
    assert result.tool_names == ["Grep"]


def test_codex_parser_reports_malformed_and_failed_streams():
    stream = "not json\n" + json.dumps({"type": "error", "message": "provider failed"})
    result = adapter.parse_codex_jsonl(stream, returncode=7, stderr="bad exit")
    assert result.is_error
    assert "provider failed" in (result.error or "")
    assert "status 7" in (result.error or "")
    assert result.diagnostics and "invalid JSONL line" in result.diagnostics[0]


def test_legacy_claude_backend_remains_lazy_and_contract_compatible(tmp_path):
    class TextBlock:
        def __init__(self, text):
            self.text = text

    class ToolUseBlock:
        def __init__(self, name, input_):
            self.name = name
            self.input = input_

    class AssistantMessage:
        def __init__(self):
            self.content = [ToolUseBlock("Read", {"file_path": "README.MD"}), TextBlock("draft")]

    class ResultMessage:
        result = "final"
        total_cost_usd = 0.25
        num_turns = 2
        usage = {"input_tokens": 10, "output_tokens": 3}
        is_error = False

    captured = {}

    class ClaudeAgentOptions:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    async def query(**kwargs):
        captured.update(kwargs)
        yield AssistantMessage()
        yield ResultMessage()

    sdk = SimpleNamespace(
        AssistantMessage=AssistantMessage,
        ClaudeAgentOptions=ClaudeAgentOptions,
        ResultMessage=ResultMessage,
        TextBlock=TextBlock,
        ToolUseBlock=ToolUseBlock,
        query=query,
    )

    async def exercise():
        return await adapter.run_claude(
            adapter.EvalRequest(
                prompt="task",
                cwd=tmp_path,
                claude_model="claude-test",
                allowed_tools=["Read"],
                mcp_servers={
                    "dail-tracker": {
                        "command": "python",
                        "args": ["server.py"],
                        "env": {"PYTHONUTF8": "1"},
                        "tool_timeout_sec": 360,
                    }
                },
            ),
            model="claude-test",
            sdk_importer=lambda _name: sdk,
            environ={},
        )

    result = anyio.run(exercise)
    assert result.provider == "claude"
    assert result.model == "claude-test"
    assert result.final_text == "final"
    assert result.tool_names == ["Read"]
    assert result.cost_usd == 0.25
    assert captured["allowed_tools"] == ["Read"]
    assert captured["mcp_servers"]["dail-tracker"] == {
        "command": "python",
        "args": ["server.py"],
        "env": {"PYTHONUTF8": "1"},
    }
