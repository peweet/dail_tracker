from __future__ import annotations

import json
import tomllib
from pathlib import Path

import tools.build_doc_index as doc_index
import tools.check_agent_context as agent_context
import tools.dev as dev

ROOT = Path(__file__).resolve().parents[2]


def test_portable_root_guidance_is_present_and_not_ignored():
    guides = {
        "AGENTS.md",
        "CLAUDE.md",
        "dail_tracker_core/AGENTS.md",
        "extractors/AGENTS.md",
        "mcp_server/AGENTS.md",
        "planning/civic/extractors/AGENTS.md",
        "sql_views/AGENTS.md",
        "utility/pages_code/AGENTS.md",
    }
    assert not [path for path in sorted(guides) if not (ROOT / path).is_file()]
    active_ignores = {
        line.strip()
        for line in (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    assert "CLAUDE.md" not in active_ignores


def test_mcp_launchers_are_cross_platform_and_install_the_extra():
    config = json.loads((ROOT / ".mcp.json").read_text(encoding="utf-8"))
    server = config["mcpServers"]["dail-tracker"]
    assert server["command"] == "uv"
    assert {"--frozen", "--extra", "mcp"} <= set(server["args"])

    vscode = (ROOT / ".vscode" / "mcp.json").read_text(encoding="utf-8")
    assert ".venv/Scripts" not in vscode
    assert "\\\\.venv\\\\Scripts" not in vscode
    assert '"command": "uv"' in vscode


def test_ui_prompt_entry_points_reference_existing_roots():
    build = (ROOT / ".github" / "prompts" / "build-page.prompt.md").read_text(encoding="utf-8")
    redesign = (ROOT / ".github" / "prompts" / "bold-redesign-page.prompt.md").read_text(encoding="utf-8")
    pack = ROOT / "dail_tracker_bold_ui_contract_pack_v5"

    assert (pack / "page_runbooks").is_dir()
    assert (pack / "utility" / "page_contracts").is_dir()
    assert "dail_tracker_bold_ui_contract_pack_v5/page_runbooks/" in build
    assert "dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/" in build
    assert "dail_tracker_bold_ui_contract_pack_v5/page_runbooks/" in redesign


def test_canonical_tasks_are_documented_and_accepted():
    contributing = (ROOT / "CONTRIBUTING.md").read_text(encoding="utf-8")
    assert "tools/dev.py verify" in contributing
    assert "tools/dev.py check" in contributing
    assert {"verify", "check", "mcp-catalog", "agent-context", "ui-contracts", "doc-index"} <= set(dev.task_names())


def test_reusable_agent_prompts_keep_the_context_contract():
    assert agent_context.check_repository() == []


def test_codex_agent_files_must_be_parseable_and_complete(tmp_path, monkeypatch):
    invalid = tmp_path / "worker.toml"
    monkeypatch.setattr(agent_context, "CODEX_ROLE_ROOT", tmp_path)

    invalid.write_text("name = [", encoding="utf-8")
    assert any("invalid Codex agent TOML" in error for error in agent_context.check_prompt(invalid))

    invalid.write_text('name = "other"\ndescription = ""\n', encoding="utf-8")
    errors = agent_context.check_prompt(invalid)

    assert any("missing required fields: description, developer_instructions" in error for error in errors)


def test_codex_roles_and_bounded_session_hook_are_portable():
    config = tomllib.loads((ROOT / ".codex" / "config.toml").read_text(encoding="utf-8"))
    assert config["features"]["hooks"] is True
    session_hook = config["hooks"]["SessionStart"][0]
    assert session_hook["matcher"] == "^(startup|resume|clear|compact)$"
    command = session_hook["hooks"][0]
    assert command["additionalContextLimit"] == 1600
    assert "tools/hooks/session_context.py" in command["command"]
    assert "tools/hooks/session_context.py" in command["command_windows"]
    assert {"reviewer.toml", "scout.toml", "worker.toml"} <= {
        path.name for path in (ROOT / ".codex" / "agents").glob("*.toml")
    }
    pre_tool = config["hooks"]["PreToolUse"][0]
    assert pre_tool["matcher"] == "^(Agent|spawn_agent)$"
    guard = pre_tool["hooks"][0]
    assert "tools/hooks/guard_subagent_spawn.py" in guard["command"]
    assert "tools/hooks/guard_subagent_spawn.py" in guard["command_windows"]


def test_doc_index_is_current(monkeypatch):
    monkeypatch.setattr(doc_index.sys, "argv", ["build_doc_index.py", "--check"])
    assert doc_index.main() == 0
