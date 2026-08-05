#!/usr/bin/env python
"""One cross-platform command surface for local and agent development.

Use the CI-equivalent uv command documented in AGENTS.md with `tools/dev.py
list` to see the stable task names. The task surface deliberately wraps existing
project checks rather than inventing a second set of policies.
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import sys
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHON = sys.executable
UV = shutil.which("uv") or "uv"
FAST_MARKERS = "not integration and not sql and not sources and not bronze and not layers"
DEV_PROFILE_ENV = "DAIL_DEV_PROFILE_ACTIVE"
DEV_PROFILE_ARGS = (
    "--locked",
    "--group",
    "dev",
    "--extra",
    "pipeline",
    "--extra",
    "api",
    "--extra",
    "mcp",
)
DEV_PROFILE_MODULES = (
    "cosmic_ray",
    "fastapi",
    "hypothesis",
    "jedi",
    "mcp",
    "openpyxl",
    "pandera",
    "polars",
    "sqlglot",
)
STDLIB_TASKS = frozenset({"agent-context"})


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
    "agent-context": Task(
        "Check reusable prompt contracts, budgets, and review rubrics",
        ((PYTHON, "tools/check_agent_context.py"),),
    ),
    "ui-contracts": Task(
        "Check live URL, CSS-class, and anonymous-markup contracts",
        (
            (PYTHON, "tools/migration/extract_url_contract.py", "--check"),
            (PYTHON, "tools/migration/extract_class_contract.py", "--check"),
            (PYTHON, "tools/migration/scan_framework_coupling.py", "--check-markup"),
        ),
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
    "mutation-data-contracts": Task(
        "Run the opt-in Cosmic Ray audit for data-contract business rules",
        ((PYTHON, "tools/run_mutation_pilot.py"),),
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
    "agent-context",
    "ui-contracts",
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


def _configure_console_output() -> None:
    """Keep legacy console encodings from aborting a development command."""

    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        with suppress(OSError, TypeError, ValueError):
            reconfigure(errors="backslashreplace")


def _requires_dev_profile(args: list[str]) -> bool:
    """Whether an invocation runs a check rather than only inspecting it."""

    if not args or args[0] not in task_names() or "--dry-run" in args:
        return False
    if args[0] in STDLIB_TASKS:
        return False
    return not (args[0] == "verify" and "--plan" in args)


def _dev_profile_is_available() -> bool:
    """Whether this interpreter already has the capability profile checks require."""

    return all(importlib.util.find_spec(module) is not None for module in DEV_PROFILE_MODULES)


def _reexec_in_dev_profile(args: list[str]) -> int | None:
    """Run executable tasks once in the same dependency profile CI uses."""

    if not _requires_dev_profile(args) or os.environ.get(DEV_PROFILE_ENV) == "1" or _dev_profile_is_available():
        return None

    command = (
        UV,
        "run",
        *DEV_PROFILE_ARGS,
        "python",
        str(Path(__file__).resolve()),
        *args,
    )
    env = dict(os.environ)
    env[DEV_PROFILE_ENV] = "1"
    return subprocess.run(command, cwd=ROOT, env=env).returncode


def run_task(name: str, extra: tuple[str, ...] = (), *, dry_run: bool = False) -> int:
    commands = commands_for(name, extra)
    for command in commands:
        print(f"-> {_display(command)}")
        if dry_run:
            continue
        env = None
        if name == "sql-contracts":
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
    _configure_console_output()
    if not args or args[0] in {"list", "--list", "-l"}:
        print_tasks()
        return 0

    name = args[0]
    if name not in task_names():
        print(
            f"Unknown task: {name}. Run `uv run --locked --group dev --extra pipeline --extra api "
            "--extra mcp python tools/dev.py list`.",
            file=sys.stderr,
        )
        return 2

    reexec_result = _reexec_in_dev_profile(args)
    if reexec_result is not None:
        return reexec_result

    args.pop(0)
    dry_run = "--dry-run" in args
    if dry_run:
        args.remove("--dry-run")

    if name == "verify":
        from verify_changed import main as verify_main

        return verify_main(args)
    return run_task(name, tuple(args), dry_run=dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
