"""Coverage for tools/wiki.py — the two-tier wiki engine.

The global tier is redirected to tmp_path through the module constants, so no test touches
the real ~/.claude. Project-tier checks read the real rule pages (that is the point: the
ratchet runs against this checkout) but only assert on shapes the checkout must satisfy.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def wiki(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("wiki_tool", ROOT / "tools" / "wiki.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # dataclasses resolve annotations through sys.modules[cls.__module__]; register first.
    monkeypatch.setitem(sys.modules, "wiki_tool", module)
    spec.loader.exec_module(module)
    home = tmp_path / "home"
    monkeypatch.setattr(module, "HOME", home)
    monkeypatch.setattr(module, "GLOBAL_DIR", home / ".claude")
    monkeypatch.setattr(module, "GLOBAL_RULES", home / ".claude" / "rules")
    monkeypatch.setattr(module, "GLOBAL_INDEX", home / ".claude" / "CLAUDE.md")
    monkeypatch.setattr(module, "AUTO_MEMORY", home / "nomemory")
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    return module


def test_templates_exist_and_are_small(wiki):
    pages = wiki.global_templates()
    assert {p.name for p in pages} >= {"windows-shell-traps.md", "git-safety.md", "working-style.md"}
    for page in pages:
        assert wiki._lines(page) <= wiki.MAX_PAGE_LINES, page.name
        assert wiki._front_matter(page).get("description"), page.name
    assert (wiki.TEMPLATES / "global_CLAUDE.md").exists()


def test_setup_global_installs_once_and_never_overwrites(wiki, capsys):
    assert wiki.setup_global() == 0
    installed = sorted(p.name for p in wiki.GLOBAL_RULES.glob("*.md"))
    assert installed == sorted(p.name for p in wiki.global_templates())
    assert wiki.GLOBAL_INDEX.exists()
    custom = wiki.GLOBAL_RULES / "git-safety.md"
    custom.write_text("---\ndescription: mine\n---\n# mine\n", encoding="utf-8")
    assert wiki.setup_global() == 0
    assert custom.read_text(encoding="utf-8").startswith("---\ndescription: mine")
    out = capsys.readouterr().out
    assert "kept (already present): " in out and "git-safety.md" in out.split("kept")[-1]
    assert wiki.setup_global(force=True) == 0
    assert "mine" not in custom.read_text(encoding="utf-8")


def test_check_global_fails_when_tier_missing_then_passes(wiki):
    report = wiki.Report()
    wiki.check_global(report)
    assert any(f.level == "FAIL" and f.tier == "global" for f in report.findings)
    wiki.setup_global()
    report = wiki.Report()
    wiki.check_global(report)
    assert not any(f.level == "FAIL" for f in report.findings)
    big = wiki.GLOBAL_RULES / "huge.md"
    big.write_text("---\ndescription: big\n---\n" + "line\n" * (wiki.MAX_PAGE_LINES + 1), encoding="utf-8")
    report = wiki.Report()
    wiki.check_global(report)
    assert any("huge.md" in f.text and f.level == "FAIL" for f in report.findings)


def test_ci_skips_local_tiers_and_exits_zero(wiki, monkeypatch, capsys):
    monkeypatch.setenv("CI", "true")
    assert wiki.main(["status", "--strict"]) in (0, 1)  # project ratchet may legitimately fail; tiers must skip
    out = capsys.readouterr().out
    assert "SKIP [global]" in out and "SKIP [memory]" in out


def test_project_checks_report_claude_md_and_rules(wiki):
    report = wiki.Report()
    wiki.check_project(report)
    texts = [f.text for f in report.findings]
    assert any(t.startswith("CLAUDE.md: ") for t in texts)
    always = next(t for t in texts if "load in every session" in t)
    assert "work-queue.md" in always
    assert not any("tests.md" in t for t in texts if "load in every session" in t)


@pytest.fixture
def project(wiki, tmp_path, monkeypatch):
    monkeypatch.setattr(wiki, "PROJECT_RULES", tmp_path / "proj" / ".claude" / "rules")
    monkeypatch.setattr(wiki, "PROJECT_SKILLS", tmp_path / "proj" / ".claude" / "skills")
    monkeypatch.setattr(wiki, "PROJECT_SETTINGS", tmp_path / "proj" / ".claude" / "settings.json")
    monkeypatch.setattr(wiki, "ROOT", tmp_path / "proj")
    (tmp_path / "proj").mkdir()
    return wiki


def test_project_templates_are_tracked_small_pages(wiki):
    pairs = wiki.project_templates()
    names = {src.relative_to(wiki.PROJECT_TEMPLATES).as_posix() for src, _ in pairs}
    assert names >= {"rules/work-queue.md", "rules/tests.md", "skills/wiki/SKILL.md"}
    for src, _ in pairs:
        assert wiki._lines(src) <= wiki.MAX_PAGE_LINES, src.name
        assert wiki._front_matter(src).get("description"), src.name
    fragment = wiki._hook_fragment()
    assert set(fragment) == {"SessionStart", "PreCompact", "PostToolUse"}
    assert len(wiki._commands([g for groups in fragment.values() for g in groups])) == 4


def test_setup_project_installs_keeps_and_forces(project, capsys):
    wiki = project
    assert wiki.setup_project() == 0
    installed = {dst.relative_to(wiki.ROOT).as_posix() for _, dst in wiki.project_templates() if dst.exists()}
    assert installed == {".claude/rules/work-queue.md", ".claude/rules/tests.md", ".claude/skills/wiki/SKILL.md"}
    page = wiki.PROJECT_RULES / "tests.md"
    page.write_text("---\ndescription: local edit\n---\n", encoding="utf-8")
    assert wiki.setup_project() == 0
    assert "local edit" in page.read_text(encoding="utf-8")
    report = wiki.Report()
    wiki.check_project_templates(report)
    drift = [f for f in report.findings if "differs from" in f.text]
    assert len(drift) == 1 and drift[0].text.startswith(".claude/rules/tests.md")
    assert wiki.setup_project(force=True) == 0
    assert "local edit" not in page.read_text(encoding="utf-8")
    assert "not wired; re-run with --hooks" in capsys.readouterr().out


def test_merge_hooks_is_idempotent_and_joins_existing_matcher(project):
    wiki = project
    settings = {
        "hooks": {
            "PostToolUse": [
                {"matcher": "Edit|Write|MultiEdit", "hooks": [{"type": "command", "command": "existing"}]},
                {"matcher": "Grep", "hooks": [{"type": "command", "command": "grep-hook"}]},
            ]
        },
        "model": "sonnet",
    }
    fragment = wiki._hook_fragment()
    assert wiki.merge_hooks(settings, fragment) == 4
    edit_group = next(g for g in settings["hooks"]["PostToolUse"] if g["matcher"] == "Edit|Write|MultiEdit")
    assert [h["command"] for h in edit_group["hooks"]][0] == "existing"
    assert len(edit_group["hooks"]) == 3
    assert len(settings["hooks"]["PostToolUse"]) == 2  # joined the matcher group, did not add a third
    assert "PreCompact" in settings["hooks"] and "SessionStart" in settings["hooks"]
    assert settings["model"] == "sonnet"
    assert wiki.merge_hooks(settings, fragment) == 0
    wiki.PROJECT_SETTINGS.parent.mkdir(parents=True)
    wiki.PROJECT_SETTINGS.write_text(json.dumps(settings), encoding="utf-8")
    assert wiki.missing_hooks() == []
    assert wiki.setup_project(hooks=True) == 0
    assert wiki.merge_hooks(json.loads(wiki.PROJECT_SETTINGS.read_text(encoding="utf-8")), fragment) == 0


def test_setup_project_hooks_creates_settings_when_absent(project, capsys):
    wiki = project
    assert len(wiki.missing_hooks()) == 4
    assert wiki.setup_project(hooks=True) == 0
    assert "4 added" in capsys.readouterr().out
    assert wiki.missing_hooks() == []
    report = wiki.Report()
    wiki.check_project_templates(report)
    assert not any("not wired" in f.text for f in report.findings)


def test_new_page_writes_skeletons(wiki, tmp_path, monkeypatch):
    monkeypatch.setattr(wiki, "PROJECT_RULES", tmp_path / "rules")
    monkeypatch.setattr(wiki, "PROJECT_CARDS", tmp_path / "cards")
    assert wiki.new_page("global", "My Topic!", "My topic", []) == 0
    assert (wiki.GLOBAL_RULES / "my-topic.md").read_text(encoding="utf-8").startswith("---\ndescription: My topic")
    assert wiki.new_page("rule", "views", "View rules", ["sql_views/**"]) == 0
    rule = (tmp_path / "rules" / "views.md").read_text(encoding="utf-8")
    assert 'paths:\n  - "sql_views/**"\n' in rule
    assert wiki.new_page("card", "lesson_x", "Lesson X", []) == 0
    assert "## Evidence" in (tmp_path / "cards" / "lesson_x.md").read_text(encoding="utf-8")
    assert wiki.new_page("card", "lesson_x", "Lesson X", []) == 1  # never overwrites
    assert wiki.new_page("card", "!!!", "bad", []) == 1
