from __future__ import annotations

import io
import subprocess

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
        "ui-contracts",
        "doc-index",
        "test-fast",
        "mutation-data-contracts",
    } <= set(dev.task_names())


def test_mutation_pilot_is_opt_in_not_a_check_gate():
    assert "mutation-data-contracts" in dev.task_names()
    assert "mutation-data-contracts" not in dev.CHECK_TASKS
    command = dev.commands_for("mutation-data-contracts")
    assert command == ((dev.PYTHON, "tools/run_mutation_pilot.py"),)


def test_check_expands_to_each_fast_gate():
    commands = dev.commands_for("check")
    rendered = [" ".join(command) for command in commands]
    assert any("ruff check" in command for command in rendered)
    assert any("basedpyright" in command for command in rendered)
    assert any("check_mcp_catalog.py" in command for command in rendered)
    assert any("extract_url_contract.py --check" in command for command in rendered)
    assert any("extract_class_contract.py --check" in command for command in rendered)
    assert any("scan_framework_coupling.py --check-markup" in command for command in rendered)
    assert any("pytest" in command for command in rendered)


def test_dry_run_prints_without_spawning(monkeypatch, capsys):
    monkeypatch.setattr(dev.subprocess, "run", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError()))
    assert dev.main(["lint", "--dry-run"]) == 0
    assert "ruff" in capsys.readouterr().out


def test_dry_run_uses_ascii_status_on_a_legacy_console(monkeypatch):
    output = io.BytesIO()
    stream = io.TextIOWrapper(output, encoding="cp1252", errors="strict")
    monkeypatch.setattr(dev.sys, "stdout", stream)
    monkeypatch.setattr(dev.subprocess, "run", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError()))

    assert dev.main(["lint", "--dry-run"]) == 0

    stream.flush()
    assert output.getvalue().decode("cp1252").startswith("-> ")


def test_real_task_reexecutes_once_in_the_full_dev_profile(monkeypatch):
    captured: dict[str, object] = {}

    def run(command, **kwargs):
        captured["command"] = command
        captured.update(kwargs)
        return subprocess.CompletedProcess(command, 0)

    monkeypatch.delenv(dev.DEV_PROFILE_ENV, raising=False)
    monkeypatch.setattr(dev, "_dev_profile_is_available", lambda: False)
    monkeypatch.setattr(dev.subprocess, "run", run)

    assert dev.main(["test-fast"]) == 0
    assert captured["command"] == (
        dev.UV,
        "run",
        *dev.DEV_PROFILE_ARGS,
        "python",
        str(dev.Path(dev.__file__).resolve()),
        "test-fast",
    )
    assert captured["cwd"] == dev.ROOT
    assert captured["env"][dev.DEV_PROFILE_ENV] == "1"


def test_available_dev_profile_does_not_reexec(monkeypatch):
    monkeypatch.setattr(dev, "_dev_profile_is_available", lambda: True)

    assert dev._reexec_in_dev_profile(["test-fast"]) is None
