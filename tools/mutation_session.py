#!/usr/bin/env python
"""Run a copy-based Cosmic Ray session under gates that make a broken harness FAIL.

Companion to ``tools/run_mutation_pilot.py``, which owns the pytest-driven
``services/data_contracts.py`` audit and its git-worktree isolation. This module owns the
``.cosmic-ray/<name>/`` sessions, which isolate differently: they mutate a *copy* of the target
module, so an interrupted run can never leave the real file mutated. Both are the same process --
"run a mutation session" -- so keep the gates below in step with that tool rather than growing a
third way to do this.

Every one of those sessions previously reported a 100% kill rate while testing nothing. Three
defects made that possible, and each has a gate here:

1. ``test-command = "python ..."`` did not resolve to the project interpreter, so every mutant
   died on ``ModuleNotFoundError`` before reaching an assertion. Cosmic Ray records a non-zero
   exit as KILLED (``cosmic_ray/testing.py:50-51``), so total harness failure looked like perfect
   coverage. Gate: the interpreter is rewritten to ``sys.executable`` and the unmutated harness
   must exit 0 before any mutant runs.
2. ``cosmic-ray init`` does NOT apply the config's ``[filters]`` block -- ``cr-filter-operators``
   is a separate command whose arguments are ``(session, config)``, reversed from init/exec.
   Gate: it is always run, in that order.
3. Cosmic Ray mutates the ``*`` and ``/`` markers in a signature as arithmetic operators, and the
   ``__name__`` guard as a string. Those mutants are SyntaxErrors or import-time changes that kill
   without any assertion noticing. Gate: they are counted and reported as UNEARNED, and excluded
   from the kill rate.

A relative interpreter path is not enough: Windows CreateProcess rejects it, Cosmic Ray catches
the FileNotFoundError (``testing.py:56-57``) and marks every mutant INCOMPETENT -- which is why
the pinned path below is absolute.
"""

from __future__ import annotations

import argparse
import re
import shutil
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SESSION_ROOT = ROOT / ".cosmic-ray"

# A kill earned by no assertion: the mutant broke import instead of behaviour. The first branch is
# a keyword-only (`*,`) or positional-only (`/,`) signature marker -- which may carry parameters on
# the same line, so it cannot be anchored to end-of-line -- and the second is the __name__ guard.
_UNEARNED_LINE = re.compile(r"(?:^|[(,])\s*[*/]\s*,|^\s*if\s+__name__\s*==")

# Below this many active mutants, a zero-survivor result is plausible rather than suspicious.
_SUSPICION_FLOOR = 20


class SessionError(RuntimeError):
    """A user-actionable mutation-session failure."""


@dataclass(frozen=True)
class Outcome:
    killed: int
    survived: int
    filtered: int
    incompetent: int
    pending: int
    unearned: int

    @property
    def earned_active(self) -> int:
        return self.killed - self.unearned + self.survived

    @property
    def kill_rate(self) -> float:
        return 100 * (self.killed - self.unearned) / self.earned_active if self.earned_active else 0.0


def _tool(name: str) -> str:
    resolved = shutil.which(name)
    if resolved is None:
        raise SessionError(f"{name!r} is not on PATH; run through the canonical uv dev profile")
    return resolved


def available() -> list[str]:
    return sorted(p.parent.name for p in SESSION_ROOT.glob("*/cosmic.toml"))


def pin_interpreter(config: Path) -> bool:
    """Rewrite a bare ``python`` test-command to this interpreter's absolute POSIX path."""

    text = config.read_text(encoding="utf-8")
    # as_posix() because Cosmic Ray splits the command with POSIX shlex even on Windows, which
    # would otherwise eat the backslashes as escapes before CreateProcess ever sees the path.
    pinned = re.sub(
        r'(test-command = ")(?:python|[^"]*?[/\\]python\.exe)(\s)',
        lambda m: f"{m.group(1)}{Path(sys.executable).as_posix()}{m.group(2)}",
        text,
    )
    if pinned == text:
        return False
    config.write_text(pinned, encoding="utf-8")
    return True


def assert_baseline(harness: Path) -> None:
    """Refuse to measure anything until the UNMUTATED harness passes.

    This is the gate that would have caught all five false sessions: a harness that cannot import
    its target fails here instead of reporting every mutant as killed.
    """

    completed = subprocess.run([sys.executable, str(harness)], cwd=ROOT, capture_output=True, text=True, timeout=600)
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout).strip().splitlines()
        raise SessionError(
            f"Baseline FAILED (exit {completed.returncode}) before any mutation: "
            f"{detail[-1][:160] if detail else '<no output>'}\n"
            "  Every mutant would be recorded KILLED, so the session would report a false 100%."
        )


def _unearned_kills(db: Path, target: Path) -> int:
    source = target.read_text(encoding="utf-8").splitlines()
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        rows = con.execute(
            "select s.start_pos_row from mutation_specs s join work_results r on s.job_id = r.job_id"
            " where r.test_outcome = 'KILLED'"
        ).fetchall()
    finally:
        con.close()
    return sum(1 for (row,) in rows if 0 < row <= len(source) and _UNEARNED_LINE.match(source[row - 1]))


def read_outcome(db: Path, target: Path) -> Outcome:
    con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        tally = dict(
            con.execute("select coalesce(test_outcome,'FILTERED'), count(*) from work_results group by 1").fetchall()
        )
        pending = con.execute(
            "select count(*) from work_items i left join work_results r on r.job_id = i.job_id where r.job_id is null"
        ).fetchone()[0]
    finally:
        con.close()
    return Outcome(
        killed=tally.get("KILLED", 0),
        survived=tally.get("SURVIVED", 0),
        filtered=tally.get("FILTERED", 0),
        incompetent=tally.get("INCOMPETENT", 0),
        pending=pending,
        unearned=_unearned_kills(db, target),
    )


def validate(outcome: Outcome) -> list[str]:
    """Return the reasons this session's numbers must not be trusted."""

    problems: list[str] = []
    if outcome.incompetent:
        problems.append(
            f"{outcome.incompetent} mutant(s) INCOMPETENT - the test command could not be launched "
            "(a relative interpreter path does this on Windows)"
        )
    if outcome.pending:
        problems.append(f"{outcome.pending} mutant(s) never ran - the session is incomplete")
    if outcome.survived == 0 and outcome.earned_active >= _SUSPICION_FLOOR:
        problems.append(
            f"zero survivors across {outcome.earned_active} active mutants - a 100% kill rate is "
            "far more often a broken harness than perfect tests; verify before trusting it"
        )
    return problems


def run_session(name: str, *, keep_evidence: bool = True) -> Outcome:
    directory = SESSION_ROOT / name
    config, db = directory / "cosmic.toml", directory / "session.sqlite"
    harness = directory / "harness.py"
    targets = sorted(directory.glob("*_target.py"))
    if not config.is_file() or not harness.is_file() or not targets:
        raise SessionError(f"{name}: expected cosmic.toml, harness.py and a *_target.py in {directory}")

    print(f"  interpreter pinned : {pin_interpreter(config)}")
    assert_baseline(harness)
    print("  baseline           : PASS (unmutated harness exits 0)")

    if db.exists():
        if keep_evidence:
            shutil.copy2(db, db.with_name("session.previous.sqlite"))
        db.unlink()

    for label, command in (
        ("init", (_tool("cosmic-ray"), "init", str(config), str(db))),
        ("filter", (_tool("cr-filter-operators"), str(db), str(config))),
        ("exec", (_tool("cosmic-ray"), "exec", str(config), str(db))),
    ):
        completed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
        if completed.returncode != 0:
            tail = (completed.stderr or completed.stdout).strip().splitlines()
            raise SessionError(f"{label} failed: {tail[-1][:160] if tail else '<no output>'}")

    outcome = read_outcome(db, targets[0])
    print(
        f"  killed={outcome.killed} (unearned={outcome.unearned}) survived={outcome.survived} "
        f"filtered={outcome.filtered} -> earned kill rate {outcome.kill_rate:.0f}% "
        f"of {outcome.earned_active} active"
    )
    return outcome


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("session", nargs="*", help="session name(s) under .cosmic-ray/; default all")
    parser.add_argument("--list", action="store_true", help="list available sessions and exit")
    args = parser.parse_args(argv)

    if args.list:
        for name in available():
            print(name)
        return 0

    names = args.session or available()
    if not names:
        print("mutation session: no sessions found under .cosmic-ray/", file=sys.stderr)
        return 1

    failures: list[str] = []
    for name in names:
        print(f"\n=== {name}")
        try:
            outcome = run_session(name)
        except (SessionError, subprocess.TimeoutExpired) as exc:
            print(f"  FAILED: {exc}", file=sys.stderr)
            failures.append(name)
            continue
        for problem in validate(outcome):
            print(f"  SUSPECT: {problem}", file=sys.stderr)
            failures.append(name)

    if failures:
        print(f"\nmutation session: {len(set(failures))} session(s) need attention", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
