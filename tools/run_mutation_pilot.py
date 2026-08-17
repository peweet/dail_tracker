#!/usr/bin/env python
"""Run the scoped Cosmic Ray data-contract mutation pilot safely.

Cosmic Ray changes the module under test on disk while it executes. This wrapper
therefore runs it in a detached temporary Git worktree, keeps resumable session
state under an ignored directory, and verifies that the temporary target bytes
are restored. It deliberately remains outside ``dev.py check``: mutation testing
is a periodic test-quality audit, not a per-change fast gate.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import tomllib
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CONFIG_TEMPLATE = ROOT / "cosmic-ray-data-contracts.toml"
TARGET = ROOT / "services" / "data_contracts.py"
SESSION_DIR = ROOT / ".cosmic-ray"
CONFIG = SESSION_DIR / "data-contracts.toml"
SESSION = SESSION_DIR / "data-contracts.sqlite"
BASELINE = SESSION.with_name(f"{SESSION.stem}.baseline.sqlite")
SESSION_HEAD = SESSION.with_suffix(".git-head")


class PilotError(RuntimeError):
    """A user-actionable mutation-pilot failure."""


def _target_status() -> str:
    completed = subprocess.run(
        ["git", "status", "--porcelain=v1", "--untracked-files=no", "--", str(TARGET.relative_to(ROOT))],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode:
        detail = completed.stderr.strip() or "git status failed"
        raise PilotError(f"Could not verify the mutation target is clean: {detail}")
    return completed.stdout.strip()


def _git_head() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    head = completed.stdout.strip()
    if completed.returncode or not head:
        detail = completed.stderr.strip() or "git rev-parse HEAD failed"
        raise PilotError(f"Could not identify the commit to mutate: {detail}")
    return head


def _check_session_head(head: str, *, session_exists: bool) -> None:
    if not session_exists:
        return
    if not SESSION_HEAD.is_file():
        raise PilotError("The existing mutation session predates commit binding; rerun with --fresh")
    session_head = SESSION_HEAD.read_text(encoding="ascii").strip()
    if session_head != head:
        raise PilotError(
            f"The mutation session belongs to commit {session_head[:12]}, not {head[:12]}; rerun with --fresh"
        )


@contextmanager
def _detached_worktree(head: str) -> Iterator[Path]:
    """Yield an isolated checkout so a live mutant never touches the main tree."""

    temporary_root = Path(tempfile.mkdtemp(prefix="dail-cosmic-ray-"))
    worktree = temporary_root / "worktree"
    added = False
    body_failed = False
    try:
        completed = subprocess.run(
            ["git", "worktree", "add", "--detach", str(worktree), head],
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        if completed.returncode:
            detail = completed.stderr.strip() or "git worktree add failed"
            raise PilotError(f"Could not create the isolated mutation worktree: {detail}")
        added = True
        yield worktree
    except BaseException:
        body_failed = True
        raise
    finally:
        cleanup_error = ""
        if added:
            completed = subprocess.run(
                ["git", "worktree", "remove", "--force", str(worktree)],
                cwd=ROOT,
                check=False,
                capture_output=True,
                text=True,
            )
            if completed.returncode:
                cleanup_error = completed.stderr.strip() or "git worktree remove failed"
        if not cleanup_error:
            shutil.rmtree(temporary_root, ignore_errors=True)
        elif body_failed:
            print(
                f"mutation pilot: could not remove temporary worktree {worktree}: {cleanup_error}",
                file=sys.stderr,
            )
        else:
            raise PilotError(f"Could not remove temporary mutation worktree {worktree}: {cleanup_error}")


def _resolve_tool(name: str) -> str:
    resolved = shutil.which(name)
    if resolved is None:
        raise PilotError(
            f"{name!r} is not installed in this environment; run the pilot through the canonical "
            "uv dev profile documented in CONTRIBUTING.md"
        )
    return resolved


def _runtime_config_text(python: str) -> str:
    data = tomllib.loads(CONFIG_TEMPLATE.read_text(encoding="utf-8"))["cosmic-ray"]
    # Cosmic Ray uses POSIX shlex even on Windows. Forward slashes prevent it
    # from consuming path backslashes as escapes before CreateProcess sees them.
    # The explicit replace is what makes this deterministic: PurePosixPath treats a
    # backslash as an ordinary character, so as_posix() alone converts a Windows
    # interpreter path only when the generator itself runs on Windows.
    python_command = subprocess.list2cmdline((Path(python).as_posix().replace("\\", "/"),))
    test_command = data["test-command"].replace("__PYTHON__", python_command)
    if "__PYTHON__" in test_command:
        raise PilotError("The mutation config contains more than one unresolved Python placeholder")
    distributor = data["distributor"]["name"]
    excluded_operators = data.get("filters", {}).get("operators-filter", {}).get("exclude-operators", [])
    return "\n".join(
        (
            "# Generated by tools/run_mutation_pilot.py; do not edit.",
            "[cosmic-ray]",
            f"module-path = {json.dumps(data['module-path'])}",
            f"timeout = {float(data['timeout'])}",
            f"excluded-modules = {json.dumps(data.get('excluded-modules', []))}",
            f"test-command = {json.dumps(test_command)}",
            "",
            "[cosmic-ray.distributor]",
            f"name = {json.dumps(distributor)}",
            "",
            "[cosmic-ray.filters.operators-filter]",
            f"exclude-operators = {json.dumps(excluded_operators)}",
            "",
        )
    )


def _write_runtime_config() -> None:
    CONFIG.write_text(_runtime_config_text(sys.executable), encoding="utf-8")


def _commands(
    *,
    cosmic_ray: str = "cosmic-ray",
    operator_filter: str = "cr-filter-operators",
    report: str = "cr-report",
    session_exists: bool,
    baseline_exists: bool,
    prepare_only: bool,
    show_survivors: bool = False,
) -> tuple[tuple[str, ...], ...]:
    commands: list[tuple[str, ...]] = []
    if not session_exists:
        commands.append((cosmic_ray, "init", str(CONFIG), str(SESSION)))
    commands.append((operator_filter, str(SESSION), str(CONFIG)))
    if not baseline_exists:
        commands.append((cosmic_ray, "baseline", "--session-file", str(BASELINE), str(CONFIG)))
    if prepare_only:
        return tuple(commands)
    commands.append((cosmic_ray, "exec", str(CONFIG), str(SESSION)))
    if show_survivors:
        commands.append((report, str(SESSION), "--surviving-only"))
    return tuple(commands)


def _remove_fresh_state() -> None:
    for path in (SESSION, BASELINE, SESSION_HEAD):
        if path.exists():
            path.unlink()


def _run(commands: tuple[tuple[str, ...], ...], *, cwd: Path = ROOT) -> None:
    for command in commands:
        print(f"-> {subprocess.list2cmdline(command)}", flush=True)
        completed = subprocess.run(command, cwd=cwd, check=False)
        if completed.returncode:
            raise PilotError(f"Command failed with exit code {completed.returncode}: {command[0]}")


def _session_outcomes(session: Path) -> tuple[Counter[tuple[str, str]], int]:
    # Cosmic Ray 8.4.6's `dump` command crashes on filtered rows because their
    # test_outcome is NULL. Query the locked version's three-table session schema
    # narrowly instead of trusting that broken reporting path.
    try:
        with sqlite3.connect(session) as connection:
            grouped = connection.execute(
                """
                SELECT worker_outcome, COALESCE(test_outcome, 'NONE'), COUNT(*)
                FROM work_results
                GROUP BY worker_outcome, COALESCE(test_outcome, 'NONE')
                """
            ).fetchall()
            pending = connection.execute(
                """
                SELECT COUNT(*)
                FROM work_items AS item
                LEFT JOIN work_results AS result ON result.job_id = item.job_id
                WHERE result.job_id IS NULL
                """
            ).fetchone()[0]
    except sqlite3.Error as exc:
        raise PilotError(f"Could not validate Cosmic Ray session {session.name}: {exc}") from exc
    outcomes = Counter({(str(worker).upper(), str(test).upper()): count for worker, test, count in grouped})
    return outcomes, pending


def _retry_invalid_mutations() -> int:
    """Return invalid generated results to Cosmic Ray's pending queue."""

    if not SESSION.exists():
        return 0
    try:
        with sqlite3.connect(SESSION) as connection:
            invalid = connection.execute(
                """
                SELECT COUNT(*) FROM work_results
                WHERE NOT (
                    (worker_outcome = 'NORMAL' AND test_outcome IN ('KILLED', 'SURVIVED'))
                    OR (worker_outcome = 'SKIPPED' AND test_outcome IS NULL)
                )
                """
            ).fetchone()[0]
            if invalid:
                connection.execute(
                    """
                    DELETE FROM work_results
                    WHERE NOT (
                        (worker_outcome = 'NORMAL' AND test_outcome IN ('KILLED', 'SURVIVED'))
                        OR (worker_outcome = 'SKIPPED' AND test_outcome IS NULL)
                    )
                    """
                )
    except sqlite3.Error as exc:
        raise PilotError(f"Could not reset invalid Cosmic Ray outcomes: {exc}") from exc
    return invalid


def _validate_baseline() -> None:
    outcomes, pending = _session_outcomes(BASELINE)
    if pending or outcomes != Counter({("NORMAL", "SURVIVED"): 1}):
        raise PilotError(
            f"Cosmic Ray produced an invalid baseline outcome: outcomes={dict(outcomes)}, pending={pending}"
        )


def _validate_mutations() -> None:
    outcomes, pending = _session_outcomes(SESSION)
    invalid = {
        outcome: count
        for outcome, count in outcomes.items()
        if outcome not in {("NORMAL", "KILLED"), ("NORMAL", "SURVIVED"), ("SKIPPED", "NONE")}
    }
    if pending or invalid:
        raise PilotError(f"Cosmic Ray produced invalid mutation outcomes: invalid={invalid}, pending={pending}")
    active = outcomes[("NORMAL", "KILLED")] + outcomes[("NORMAL", "SURVIVED")]
    kill_rate = 100 * outcomes[("NORMAL", "KILLED")] / active if active else 0.0
    print(
        "mutation outcomes: "
        f"killed={outcomes[('NORMAL', 'KILLED')]} "
        f"survived={outcomes[('NORMAL', 'SURVIVED')]} "
        f"filtered={outcomes[('SKIPPED', 'NONE')]} "
        f"active-kill-rate={kill_rate:.1f}%"
    )


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="discard only the generated data-contract session and start a new audit",
    )
    parser.add_argument(
        "--prepare-only",
        action="store_true",
        help="initialize and baseline the session without executing mutations",
    )
    parser.add_argument(
        "--plan",
        action="store_true",
        help="print the commands without changing session or source files",
    )
    parser.add_argument(
        "--show-survivors",
        action="store_true",
        help="print Cosmic Ray's detailed surviving-mutant list after the validated summary",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        if not CONFIG_TEMPLATE.is_file() or not TARGET.is_file():
            raise PilotError("The pilot configuration or mutation target is missing")

        session_exists = SESSION.exists() and not args.fresh
        baseline_exists = BASELINE.exists() and not args.fresh
        planned = _commands(
            session_exists=session_exists,
            baseline_exists=baseline_exists,
            prepare_only=args.prepare_only,
            show_survivors=args.show_survivors,
        )
        if args.plan:
            for command in planned:
                print(f"-> {subprocess.list2cmdline(command)}")
            return 0

        dirty = _target_status()
        if dirty:
            raise PilotError(f"Refusing to mutate services/data_contracts.py because it has tracked changes: {dirty}")

        head = _git_head()
        _check_session_head(head, session_exists=session_exists)
        if BASELINE.exists() and not session_exists and not args.fresh:
            raise PilotError("A baseline exists without its mutation session; rerun with --fresh")
        if args.fresh:
            _remove_fresh_state()
        SESSION_DIR.mkdir(parents=True, exist_ok=True)
        if not SESSION.exists():
            SESSION_HEAD.write_text(f"{head}\n", encoding="ascii")
        _write_runtime_config()
        retried = _retry_invalid_mutations()
        if retried:
            print(f"retrying {retried} invalid mutation outcome(s)")

        # Cosmic Ray decodes captured output as UTF-8 unconditionally. Pin the
        # child pytest encoding so Windows punctuation cannot turn a killed
        # mutant into an INCOMPETENT UnicodeDecodeError.
        os.environ["PYTHONUTF8"] = "1"
        os.environ["PYTHONIOENCODING"] = "utf-8"

        commands = _commands(
            cosmic_ray=_resolve_tool("cosmic-ray"),
            operator_filter=_resolve_tool("cr-filter-operators"),
            report=_resolve_tool("cr-report"),
            session_exists=SESSION.exists(),
            baseline_exists=BASELINE.exists(),
            prepare_only=args.prepare_only,
            show_survivors=args.show_survivors,
        )
        with _detached_worktree(head) as worktree:
            worktree_target = worktree / TARGET.relative_to(ROOT)
            original = worktree_target.read_bytes()
            run_error: Exception | None = None
            try:
                for command in commands:
                    _run((command,), cwd=worktree)
                    executable = Path(command[0]).stem.lower()
                    if executable == "cosmic-ray" and len(command) > 1 and command[1] == "baseline":
                        _validate_baseline()
                    elif executable == "cosmic-ray" and len(command) > 1 and command[1] == "exec":
                        _validate_mutations()
            except Exception as exc:  # preserve the source-integrity check on tool failure/interrupt
                run_error = exc
            if worktree_target.read_bytes() != original:
                raise PilotError(
                    "Cosmic Ray exited without restoring the isolated services/data_contracts.py target; "
                    f"the temporary worktree is {worktree}."
                ) from run_error
            if run_error is not None:
                raise run_error
        return 0
    except PilotError as exc:
        print(f"mutation pilot: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
