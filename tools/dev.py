#!/usr/bin/env python
"""One cross-platform command surface for local and agent development.

Run `uv run python tools/dev.py list` to see the stable task names. The task
surface deliberately wraps existing project checks rather than inventing a
second set of policies.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable
UV = shutil.which("uv") or "uv"
FAST_MARKERS = "not integration and not sql and not sources and not bronze and not layers"


@dataclass(frozen=True)
class Task:
    description: str
    commands: tuple[tuple[str, ...], ...]


TASKS: dict[str, Task] = {
    "lint": Task("Ruff lint for the repository", ((PYTHON, "-m", "ruff", "check", "."),)),
    "format-check": Task(
        "Check formatting without changing files",
        ((PYTHON, "-m", "ruff", "format", "--check", "."),),
    ),
    "type": Task("Run the scoped basedpyright contract", ((PYTHON, "-m", "basedpyright"),)),
    "firewall": Task(
        "Enforce the Streamlit data/logic boundary",
        ((PYTHON, "tools/check_streamlit_logic_firewall.py"),),
    ),
    "conventions": Task(
        "Run repository convention ratchets",
        ((PYTHON, "tools/check_conventions.py"),),
    ),
    "mcp-catalog": Task(
        "Check the MCP read-only and always-loaded context budget",
        ((PYTHON, "tools/check_mcp_catalog.py"),),
    ),
    "doc-index": Task(
        "Check that doc/INDEX.md matches the documentation tree",
        ((PYTHON, "tools/build_doc_index.py", "--check"),),
    ),
    "deps": Task(
        "Check lock/export parity and the undeclared-import contract",
        (
            (PYTHON, "tools/check_dependency_state.py"),
            (PYTHON, "tools/check_dependency_declarations.py"),
        ),
    ),
    "test": Task("Run pytest with caller-supplied arguments", ((PYTHON, "-m", "pytest"),)),
    "test-fast": Task(
        "Run the deterministic, low-memory pytest lane",
        ((PYTHON, "-m", "pytest", "-q", "-m", FAST_MARKERS),),
    ),
    "sql-contracts": Task(
        "Run SQL contracts against committed gold data",
        ((PYTHON, "-m", "pytest", "-q", "-m", "sql"),),
    ),
}

CHECK_TASKS = (
    "lint",
    "format-check",
    "type",
    "deps",
    "firewall",
    "conventions",
    "mcp-catalog",
    "doc-index",
    "test-fast",
)


def task_names() -> tuple[str, ...]:
    return ("verify", "check", *TASKS)


def commands_for(name: str, extra: tuple[str, ...] = ()) -> tuple[tuple[str, ...], ...]:
    if name == "check":
        return tuple(command for task in CHECK_TASKS for command in TASKS[task].commands)
    if name not in TASKS:
        raise KeyError(name)
    commands = TASKS[name].commands
    if extra:
        commands = (*commands[:-1], (*commands[-1], *extra))
    return commands


def _display(command: tuple[str, ...]) -> str:
    return subprocess.list2cmdline(command)


def run_task(name: str, extra: tuple[str, ...] = (), *, dry_run: bool = False) -> int:
    commands = commands_for(name, extra)
    for command in commands:
        print(f"→ {_display(command)}")
        if dry_run:
            continue
        env = None
        if name == "sql-contracts":
            import os

            env = dict(os.environ, DAIL_INTEGRATION_TESTS="1")
        completed = subprocess.run(command, cwd=ROOT, env=env)
        if completed.returncode:
            return completed.returncode
    return 0


def print_tasks() -> None:
    width = max(len(name) for name in task_names())
    print(f"{'verify':<{width}}  Focused checks selected from the Git diff; successful states are cached")
    print(f"{'check':<{width}}  Local approximation of the deterministic CI merge gates")
    for name, task in TASKS.items():
        print(f"{name:<{width}}  {task.description}")


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] in {"list", "--list", "-l"}:
        print_tasks()
        return 0

    name = args.pop(0)
    dry_run = "--dry-run" in args
    if dry_run:
        args.remove("--dry-run")

    if name == "verify":
        from verify_changed import main as verify_main

        return verify_main(args)
    if name not in task_names():
        print(f"Unknown task: {name}. Run `uv run python tools/dev.py list`.", file=sys.stderr)
        return 2
    return run_task(name, tuple(args), dry_run=dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
