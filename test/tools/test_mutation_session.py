from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

import tools.mutation_session as session


def _write_session_db(path: Path, rows: list[tuple[int, str | None]]) -> None:
    """Build the three columns of Cosmic Ray's schema these gates actually read."""

    con = sqlite3.connect(path)
    con.execute("create table mutation_specs (job_id text, start_pos_row int)")
    con.execute("create table work_results (job_id text, test_outcome text)")
    con.execute("create table work_items (job_id text)")
    for index, (row, outcome) in enumerate(rows):
        job = f"job{index}"
        con.execute("insert into mutation_specs values (?,?)", (job, row))
        con.execute("insert into work_results values (?,?)", (job, outcome))
        con.execute("insert into work_items values (?)", (job,))
    con.commit()
    con.close()


def test_bare_python_test_command_is_pinned_to_this_interpreter(tmp_path):
    config = tmp_path / "cosmic.toml"
    config.write_text('test-command = "python .cosmic-ray/x/harness.py"\n', encoding="utf-8")

    assert session.pin_interpreter(config) is True
    assert Path(sys.executable).as_posix() in config.read_text(encoding="utf-8")


def test_a_wrong_absolute_interpreter_is_repointed_not_left_alone(tmp_path):
    """A dead interpreter path rots exactly like a bare `python` -- both must be repaired."""

    config = tmp_path / "cosmic.toml"
    config.write_text('test-command = "C:/gone/.tmpXYZ/Scripts/python.exe harness.py"\n', encoding="utf-8")

    assert session.pin_interpreter(config) is True
    assert "gone" not in config.read_text(encoding="utf-8")


def test_failing_unmutated_harness_raises_instead_of_scoring_a_false_hundred(tmp_path):
    """The gate that would have caught all five false sessions."""

    harness = tmp_path / "harness.py"
    harness.write_text("import no_such_module_at_all\n", encoding="utf-8")

    with pytest.raises(session.SessionError, match="Baseline FAILED"):
        session.assert_baseline(harness)


def test_passing_unmutated_harness_is_accepted(tmp_path):
    harness = tmp_path / "harness.py"
    harness.write_text("assert 1 + 1 == 2\n", encoding="utf-8")

    session.assert_baseline(harness)


def test_zero_survivors_across_many_mutants_is_reported_as_suspect():
    outcome = session.Outcome(killed=200, survived=0, filtered=0, incompetent=0, pending=0, unearned=0)

    assert any("zero survivors" in problem for problem in session.validate(outcome))


def test_zero_survivors_on_a_tiny_session_is_not_flagged():
    outcome = session.Outcome(killed=3, survived=0, filtered=0, incompetent=0, pending=0, unearned=0)

    assert session.validate(outcome) == []


def test_incompetent_and_pending_mutants_both_invalidate_a_session():
    outcome = session.Outcome(killed=10, survived=5, filtered=0, incompetent=7, pending=2, unearned=0)
    problems = " ".join(session.validate(outcome))

    assert "INCOMPETENT" in problems
    assert "never ran" in problems


def test_signature_marker_kills_are_counted_unearned_and_leave_the_kill_rate(tmp_path):
    """A `*` marker mutant is a SyntaxError: it kills without any assertion noticing."""

    target = tmp_path / "x_target.py"
    target.write_text("def f(\n    *,\n    a: int = 1,\n): return a\n", encoding="utf-8")
    db = tmp_path / "session.sqlite"
    _write_session_db(db, [(2, "KILLED"), (2, "KILLED"), (4, "KILLED"), (4, "SURVIVED")])

    outcome = session.read_outcome(db, target)

    assert outcome.unearned == 2
    assert outcome.killed == 3
    # Only the line-4 kill was earned, against one survivor.
    assert outcome.earned_active == 2
    assert outcome.kill_rate == pytest.approx(50.0)


def test_a_marker_sharing_its_line_with_parameters_still_counts_unearned(tmp_path):
    """`*, a: int` is the common real shape -- anchoring the match to end-of-line missed it."""

    target = tmp_path / "x_target.py"
    target.write_text("def f(\n    *, state: str, threshold: int = 3,\n): return state\n", encoding="utf-8")
    db = tmp_path / "session.sqlite"
    _write_session_db(db, [(2, "KILLED")])

    assert session.read_outcome(db, target).unearned == 1


def test_multiplication_in_ordinary_code_is_not_mistaken_for_a_marker(tmp_path):
    target = tmp_path / "x_target.py"
    target.write_text("def f(r):\n    return r[0] * r[1] - r[1] * r[0], 0\n", encoding="utf-8")
    db = tmp_path / "session.sqlite"
    _write_session_db(db, [(2, "KILLED")])

    assert session.read_outcome(db, target).unearned == 0


def test_the_name_guard_also_kills_without_earning_it(tmp_path):
    target = tmp_path / "x_target.py"
    target.write_text("a = 1\nif __name__ == '__main__':\n    pass\n", encoding="utf-8")
    db = tmp_path / "session.sqlite"
    _write_session_db(db, [(2, "KILLED")])

    assert session.read_outcome(db, target).unearned == 1
