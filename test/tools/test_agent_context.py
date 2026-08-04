from __future__ import annotations

import json
from pathlib import Path

import tools.build_doc_index as doc_index
import tools.dev as dev

ROOT = Path(__file__).resolve().parents[2]


def test_portable_root_guidance_is_present_and_not_ignored():
    assert (ROOT / "AGENTS.md").is_file()
    assert (ROOT / "CLAUDE.md").is_file()
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
    assert {"verify", "check", "mcp-catalog", "doc-index"} <= set(dev.task_names())


def test_doc_index_is_current(monkeypatch):
    monkeypatch.setattr(doc_index.sys, "argv", ["build_doc_index.py", "--check"])
    assert doc_index.main() == 0
