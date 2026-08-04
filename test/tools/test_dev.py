from __future__ import annotations

import tools.dev as dev


def test_canonical_task_surface_contains_required_checks():
    assert {
        "verify",
        "check",
        "lint",
        "format-check",
        "type",
        "deps",
        "firewall",
        "conventions",
        "mcp-catalog",
        "doc-index",
        "test-fast",
    } <= set(dev.task_names())


def test_check_expands_to_each_fast_gate():
    commands = dev.commands_for("check")
    rendered = [" ".join(command) for command in commands]
    assert any("ruff check" in command for command in rendered)
    assert any("basedpyright" in command for command in rendered)
    assert any("check_mcp_catalog.py" in command for command in rendered)
    assert any("pytest" in command for command in rendered)


def test_dry_run_prints_without_spawning(monkeypatch, capsys):
    monkeypatch.setattr(dev.subprocess, "run", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError()))
    assert dev.main(["lint", "--dry-run"]) == 0
    assert "ruff" in capsys.readouterr().out
