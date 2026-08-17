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
    assert adapter.resolve_reasoning_effort(environ={"DAIL_EVAL_REASONING_EFFORT": "HIGH"}) == "high"
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


def test_codex_read_only_mcp_tools_are_explicitly_enabled(tmp_path):
    request = adapter.EvalRequest(
        prompt="inspect",
        cwd=tmp_path,
        mcp_servers={"dail-tracker": {"command": "python", "args": ["server.py"]}},
        allowed_tools=["mcp__dail-tracker__describe_dataset"],
    )

    command = adapter.build_codex_command(request, executable="codex", model=None)

    rendered = "\n".join(command)
    assert 'mcp_servers.dail-tracker.enabled_tools=["describe_dataset"]' in rendered
    assert 'mcp_servers.dail-tracker.enabled_tools=["*"]' not in rendered


@pytest.mark.parametrize(
    "bad_tool",
    ["*", "mcp__dail-tracker__*", "mcp__dail-tracker__", "mcp____describe_dataset"],
)
def test_read_only_mcp_allowlist_rejects_wildcards_and_empty_names(tmp_path, bad_tool):
    request = adapter.EvalRequest(
        prompt="inspect",
        cwd=tmp_path,
        allowed_tools=[bad_tool],
        mcp_servers={"dail-tracker": {"command": "python", "args": ["server.py"]}},
    )

    with pytest.raises(adapter.EvalProviderError, match="wildcard or empty MCP"):
        adapter.build_codex_command(request, executable="codex", model=None)


def test_codex_read_only_clears_inherited_project_mcp_without_dropping_guidance(tmp_path):
    command = adapter.build_codex_command(
        adapter.EvalRequest(prompt="inspect", cwd=tmp_path, project_settings=True),
        executable="codex",
        model=None,
    )

    rendered = "\n".join(command)
    assert "mcp_servers={}" in rendered
    assert "--ignore-rules" not in command


def test_codex_on_arm_explicitly_trusts_vetted_cleanroom_hooks(tmp_path):
    request = adapter.EvalRequest(prompt="x", cwd=tmp_path, trusted_project_hooks=True)

    command = adapter.build_codex_command(
        request,
        executable="codex",
        model="gpt-5.6-sol",
        reasoning_effort="high",
    )

    rendered = "\n".join(command)
    assert "--dangerously-bypass-hook-trust" in command
    assert 'trust_level="trusted"' in rendered
    assert str(tmp_path.resolve()).replace("\\", "\\\\") in rendered


def test_codex_hook_trust_cannot_be_enabled_when_project_settings_are_off(tmp_path):
    request = adapter.EvalRequest(
        prompt="x",
        cwd=tmp_path,
        project_settings=False,
        trusted_project_hooks=True,
    )

    with pytest.raises(ValueError, match="requires project_settings"):
        adapter.build_codex_command(request, executable="codex", model=None)


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

    class HookMatcher:
        def __init__(self, *, matcher, hooks):
            self.matcher = matcher
            self.hooks = hooks

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
        HookMatcher=HookMatcher,
        query=query,
    )

    async def exercise():
        return await adapter.run_claude(
            adapter.EvalRequest(
                prompt="task",
                cwd=tmp_path,
                claude_model="claude-test",
                allowed_tools=["Read", "mcp__dail-tracker__describe_dataset"],
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
    assert captured["permission_mode"] == "dontAsk"
    assert captured["setting_sources"] == []
    assert captured["skills"] == []
    assert captured["tools"] == ["Read", "Glob", "Grep"]
    assert captured["allowed_tools"] == [
        "Read",
        "Glob",
        "Grep",
        "mcp__dail-tracker__describe_dataset",
    ]
    assert captured["strict_mcp_config"] is True
    assert captured["mcp_servers"]["dail-tracker"] == {
        "command": "python",
        "args": ["server.py"],
        "env": {"PYTHONUTF8": "1"},
    }
    matcher = captured["hooks"]["PreToolUse"][0]
    assert matcher.matcher == "mcp__.*"
    hook = matcher.hooks[0]
    assert anyio.run(hook, {"tool_name": "mcp__dail-tracker__describe_dataset"}, None, {}) == {}
    for tool in (
        "mcp__dail-tracker__search_project",
        "mcp__dail-tracker__siting_decision_documents",
        "mcp__dail-tracker__search_planning_precedents",
        "mcp__dail-tracker__siting_check",
    ):
        denied = anyio.run(hook, {"tool_name": tool}, None, {})
        assert denied["hookSpecificOutput"]["permissionDecision"] == "deny"


def test_claude_sandbox_maps_to_safe_or_write_capable_permissions(tmp_path):
    captured = {}

    class ClaudeAgentOptions:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    async def query(**kwargs):
        captured.update(kwargs)
        if False:
            yield None

    sdk = SimpleNamespace(ClaudeAgentOptions=ClaudeAgentOptions, query=query)

    async def exercise(request):
        await adapter.run_claude(
            request,
            model="claude-test",
            sdk_importer=lambda _name: sdk,
            environ={},
        )

    anyio.run(
        exercise,
        adapter.EvalRequest(
            prompt="inspect",
            cwd=tmp_path,
            disallowed_tools=["mcp__dail-tracker__search_speeches"],
        ),
    )
    assert captured["permission_mode"] == "dontAsk"
    assert captured["strict_mcp_config"] is True
    assert captured["mcp_servers"] == {}
    assert captured["setting_sources"] == []
    assert captured["skills"] == []
    assert captured["tools"] == ["Read", "Glob", "Grep"]
    assert captured["allowed_tools"] == ["Read", "Glob", "Grep"]
    assert "bypassPermissions" not in captured.values()
    assert set(captured["disallowed_tools"]) >= {
        "Bash",
        "Edit",
        "Write",
        "NotebookEdit",
        "mcp__dail-tracker__search_speeches",
    }

    captured.clear()
    anyio.run(
        exercise,
        adapter.EvalRequest(
            prompt="change",
            cwd=tmp_path,
            sandbox="workspace-write",
            disallowed_tools=["mcp__dail-tracker__search_speeches"],
        ),
    )
    assert captured["permission_mode"] == "bypassPermissions"
    assert "strict_mcp_config" not in captured
    assert "hooks" not in captured
    assert "tools" not in captured
    assert "skills" not in captured
    assert captured["disallowed_tools"] == ["mcp__dail-tracker__search_speeches"]


def test_strict_read_only_uses_bounded_agents_guidance_only(tmp_path):
    # newline="" keeps the byte on disk LF on every platform. The adapter reads raw
    # bytes and does not normalise, so a default write_text would store CRLF on
    # Windows and make the exact-prompt assertion below platform-dependent.
    (tmp_path / "AGENTS.md").write_text("ON-SENTINEL\n", encoding="utf-8", newline="")
    (tmp_path / ".claude").mkdir()
    (tmp_path / ".claude" / "CLAUDE.md").write_text("MUST-NOT-LOAD", encoding="utf-8")
    captured = {}

    class ClaudeAgentOptions:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    async def query(**kwargs):
        captured.update(kwargs)
        if False:
            yield None

    sdk = SimpleNamespace(ClaudeAgentOptions=ClaudeAgentOptions, query=query)

    async def exercise(request):
        await adapter.run_claude(
            request,
            model="claude-test",
            sdk_importer=lambda _name: sdk,
            environ={},
        )

    anyio.run(
        exercise,
        adapter.EvalRequest(
            prompt="task",
            cwd=tmp_path,
            project_settings=True,
            system_prompt="CALLER-SYSTEM",
        ),
    )
    assert captured["permission_mode"] == "dontAsk"
    assert captured["setting_sources"] == []
    assert captured["skills"] == []
    assert captured["system_prompt"] == (
        "<project-guidance>\nON-SENTINEL\n\n</project-guidance>\n\n"
        "<caller-system-prompt>\nCALLER-SYSTEM\n</caller-system-prompt>"
    )
    assert "MUST-NOT-LOAD" not in captured["system_prompt"]

    captured.clear()
    anyio.run(
        exercise,
        adapter.EvalRequest(
            prompt="task",
            cwd=tmp_path,
            project_settings=False,
        ),
    )
    assert "system_prompt" not in captured
    assert captured["setting_sources"] == []
    assert captured["skills"] == []

    captured.clear()
    anyio.run(
        exercise,
        adapter.EvalRequest(
            prompt="task",
            cwd=tmp_path,
            sandbox="workspace-write",
            project_settings=True,
            system_prompt="CALLER-SYSTEM",
        ),
    )
    assert captured["setting_sources"] == ["project"]
    assert captured["system_prompt"] == "<caller-system-prompt>\nCALLER-SYSTEM\n</caller-system-prompt>"
    assert "ON-SENTINEL" not in captured["system_prompt"]


def test_strict_guidance_rejects_oversize_and_directory(tmp_path):
    async def exercise():
        with pytest.raises(adapter.EvalProviderError, match="64 KiB"):
            await adapter.run_claude(
                adapter.EvalRequest(prompt="task", cwd=tmp_path),
                model="claude-test",
                sdk_importer=lambda _name: (_ for _ in ()).throw(AssertionError("SDK must stay lazy")),
                environ={},
            )

    (tmp_path / "AGENTS.md").write_bytes(b"x" * (64 * 1024 + 1))
    anyio.run(exercise)
    (tmp_path / "AGENTS.md").unlink()
    (tmp_path / "AGENTS.md").mkdir()

    async def exercise_directory():
        with pytest.raises(adapter.EvalProviderError, match="regular file"):
            await adapter.run_claude(
                adapter.EvalRequest(prompt="task", cwd=tmp_path),
                model="claude-test",
                sdk_importer=lambda _name: (_ for _ in ()).throw(AssertionError("SDK must stay lazy")),
                environ={},
            )

    anyio.run(exercise_directory)


def test_read_only_mcp_requires_an_explicit_allowlist(tmp_path):
    async def exercise():
        with pytest.raises(adapter.EvalProviderError, match="explicit MCP tool allowlist"):
            await adapter.run_claude(
                adapter.EvalRequest(
                    prompt="inspect",
                    cwd=tmp_path,
                    mcp_servers={"dail-tracker": {"command": "python", "args": ["server.py"]}},
                ),
                model="claude-test",
                sdk_importer=lambda _name: (_ for _ in ()).throw(AssertionError("SDK must stay lazy")),
                environ={},
            )

    anyio.run(exercise)


def test_read_only_mcp_allowlist_cannot_name_an_unconfigured_server(tmp_path):
    async def exercise():
        with pytest.raises(adapter.EvalProviderError, match="absent from request.mcp_servers"):
            await adapter.run_claude(
                adapter.EvalRequest(
                    prompt="inspect",
                    cwd=tmp_path,
                    allowed_tools=["Read", "mcp__inherited__search_project"],
                ),
                model="claude-test",
                sdk_importer=lambda _name: (_ for _ in ()).throw(AssertionError("SDK must stay lazy")),
                environ={},
            )

    anyio.run(exercise)


@pytest.mark.parametrize(
    "forbidden",
    sorted(adapter._PUBLIC_DAIL_TRACKER_MCP_TOOLS),
)
def test_read_only_dail_tracker_forbids_project_sensitive_tools(tmp_path, forbidden):
    async def exercise():
        with pytest.raises(adapter.EvalProviderError, match="public read-only dail-tracker policy"):
            await adapter.run_claude(
                adapter.EvalRequest(
                    prompt="inspect",
                    cwd=tmp_path,
                    allowed_tools=[forbidden],
                    mcp_servers={"dail-tracker": {"command": "python", "args": ["server.py"]}},
                ),
                model="claude-test",
                sdk_importer=lambda _name: (_ for _ in ()).throw(AssertionError("SDK must stay lazy")),
                environ={},
            )

    anyio.run(exercise)


def test_eval_callers_use_explicit_mcp_policies(monkeypatch):
    from tools.evals import package_bench, routing_probe

    assert set(package_bench.BASELINE_TOOLS).isdisjoint(package_bench.NEW_TOOLS)
    assert not {"siting_decision_documents", "search_planning_precedents", "siting_check"} & set(
        package_bench.BASELINE_TOOLS
    )

    captured = {}

    async def fake_run_eval(request):
        captured["request"] = request
        return adapter.EvalResult(provider="claude", model="test", final_text="[]")

    monkeypatch.setattr(routing_probe, "run_eval", fake_run_eval)
    anyio.run(routing_probe.run_probe, "data-shape", routing_probe.PROBES[0][1])
    assert captured["request"].allowed_tools == list(routing_probe.NAV_ALLOWED_TOOLS)
    assert set(captured["request"].allowed_tools) == {
        "mcp__dail-tracker__describe_dataset",
        "mcp__dail-tracker__search_project",
        "mcp__dail-tracker__list_datasets",
        "mcp__dail-tracker__code_outline",
        "mcp__dail-tracker__view_deps",
    }

    monkeypatch.setattr(package_bench, "run_eval", fake_run_eval)
    anyio.run(package_bench.run_task, "column-lineage", "newtools")
    assert set(captured["request"].allowed_tools) == set(package_bench.NEW_TOOLS)
    assert captured["request"].mcp_servers

    anyio.run(package_bench.run_task, "column-lineage", "baseline")
    assert captured["request"].allowed_tools == [f"mcp__dail-tracker__{tool}" for tool in package_bench.BASELINE_TOOLS]
    assert captured["request"].mcp_servers
