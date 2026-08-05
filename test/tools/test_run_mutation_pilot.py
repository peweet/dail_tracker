from __future__ import annotations

import sqlite3
import subprocess
import tomllib
from collections import Counter

import pytest

import tools.run_mutation_pilot as pilot


def test_new_session_initializes_baselines_executes_and_reports():
    assert pilot._commands(session_exists=False, baseline_exists=False, prepare_only=False) == (
        ("cosmic-ray", "init", str(pilot.CONFIG), str(pilot.SESSION)),
        ("cr-filter-operators", str(pilot.SESSION), str(pilot.CONFIG)),
        ("cosmic-ray", "baseline", "--session-file", str(pilot.BASELINE), str(pilot.CONFIG)),
        ("cosmic-ray", "exec", str(pilot.CONFIG), str(pilot.SESSION)),
        ("cr-report", str(pilot.SESSION), "--surviving-only"),
    )


def test_existing_session_resumes_pending_mutations_without_reinitializing():
    assert pilot._commands(session_exists=True, baseline_exists=True, prepare_only=False) == (
        ("cr-filter-operators", str(pilot.SESSION), str(pilot.CONFIG)),
        ("cosmic-ray", "exec", str(pilot.CONFIG), str(pilot.SESSION)),
        ("cr-report", str(pilot.SESSION), "--surviving-only"),
    )


def test_prepare_only_stops_after_baseline():
    commands = pilot._commands(session_exists=False, baseline_exists=False, prepare_only=True)
    assert [command[0] for command in commands] == ["cosmic-ray", "cr-filter-operators", "cosmic-ray"]
    assert commands[0][1] == "init"
    assert commands[-1][1] == "baseline"


def test_runtime_config_pins_the_active_interpreter_and_remains_valid_toml():
    python = r"C:\Program Files\Python312\python.exe"
    rendered = pilot._runtime_config_text(python)
    config = tomllib.loads(rendered)["cosmic-ray"]

    assert config["module-path"] == "services/data_contracts.py"
    assert config["test-command"].startswith('"C:/Program Files/Python312/python.exe" -m pytest')
    assert "__PYTHON__" not in rendered
    assert config["distributor"]["name"] == "local"
    assert config["filters"]["operators-filter"]["exclude-operators"]


def test_plan_does_not_resolve_tools_or_touch_session(monkeypatch, capsys, tmp_path):
    monkeypatch.setattr(pilot, "SESSION", tmp_path / "data-contracts.sqlite")
    monkeypatch.setattr(pilot, "BASELINE", tmp_path / "data-contracts.baseline.sqlite")
    monkeypatch.setattr(
        pilot,
        "_resolve_tool",
        lambda _name: (_ for _ in ()).throw(AssertionError("plan must not inspect installed tools")),
    )
    monkeypatch.setattr(
        pilot,
        "_remove_fresh_state",
        lambda: (_ for _ in ()).throw(AssertionError("plan must not remove session state")),
    )

    assert pilot.main(["--fresh", "--plan"]) == 0
    output = capsys.readouterr().out
    assert "cosmic-ray" in output
    assert "cr-report" in output


def test_dirty_target_is_rejected_before_cosmic_ray_runs(monkeypatch, capsys):
    monkeypatch.setattr(pilot, "_target_status", lambda: " M services/data_contracts.py")
    monkeypatch.setattr(
        pilot,
        "_resolve_tool",
        lambda _name: (_ for _ in ()).throw(AssertionError("dirty target must stop before tool lookup")),
    )

    assert pilot.main([]) == 1
    assert "Refusing to mutate" in capsys.readouterr().err


def test_nonzero_tool_exit_is_reported(monkeypatch):
    def fail(command, **_kwargs):
        return subprocess.CompletedProcess(command, 7)

    monkeypatch.setattr(pilot.subprocess, "run", fail)
    try:
        pilot._run((("cosmic-ray", "exec"),))
    except pilot.PilotError as exc:
        assert "exit code 7" in str(exc)
    else:
        raise AssertionError("expected PilotError")


def test_incompetent_mutations_are_rejected_even_when_cosmic_ray_exits_zero(monkeypatch):
    monkeypatch.setattr(
        pilot,
        "_session_outcomes",
        lambda _session: (Counter({("NORMAL", "INCOMPETENT"): 2}), 0),
    )

    with pytest.raises(pilot.PilotError, match="INCOMPETENT"):
        pilot._validate_mutations()


def test_valid_mutation_outcomes_report_each_disposition(monkeypatch, capsys):
    monkeypatch.setattr(
        pilot,
        "_session_outcomes",
        lambda _session: (
            Counter({("NORMAL", "KILLED"): 7, ("NORMAL", "SURVIVED"): 2, ("SKIPPED", "NONE"): 3}),
            0,
        ),
    )

    pilot._validate_mutations()
    assert "killed=7 survived=2 filtered=3" in capsys.readouterr().out


def test_retry_invalid_mutations_preserves_valid_and_filtered_results(monkeypatch, tmp_path):
    session = tmp_path / "session.sqlite"
    with sqlite3.connect(session) as connection:
        connection.execute(
            "CREATE TABLE work_results (worker_outcome TEXT, test_outcome TEXT, job_id TEXT PRIMARY KEY)"
        )
        connection.executemany(
            "INSERT INTO work_results VALUES (?, ?, ?)",
            [
                ("NORMAL", "KILLED", "killed"),
                ("NORMAL", "SURVIVED", "survived"),
                ("SKIPPED", None, "filtered"),
                ("NORMAL", "INCOMPETENT", "bad-test"),
                ("EXCEPTION", "INCOMPETENT", "bad-worker"),
            ],
        )
    monkeypatch.setattr(pilot, "SESSION", session)

    assert pilot._retry_invalid_mutations() == 2
    with sqlite3.connect(session) as connection:
        remaining = {row[0] for row in connection.execute("SELECT job_id FROM work_results")}
    assert remaining == {"killed", "survived", "filtered"}
