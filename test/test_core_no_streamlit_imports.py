"""Firewall guard: dail_tracker_core must stay Streamlit-free.

The whole point of the core package is that the same query/business logic can
back the Streamlit UI today and a FastAPI/React interface later. If anything
under ``dail_tracker_core/`` imports streamlit — directly or transitively via a
``utility.*`` helper — that contract is broken and the package can no longer be
served headless.

Two complementary checks:

1. **Static** — AST-walk every ``.py`` file in the package and fail on any
   ``import streamlit`` / ``from streamlit import ...`` (catches the direct case
   without executing anything; auto-covers new modules).
2. **Dynamic** — import every core module in a *fresh subprocess* and assert
   ``streamlit`` never landed in ``sys.modules`` (catches the transitive case:
   a core module importing a ``utility`` helper that drags streamlit in). Runs
   isolated because the pytest session itself has streamlit loaded by other
   tests, so an in-process ``sys.modules`` check would give a false positive.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_CORE_DIR = _ROOT / "dail_tracker_core"


def _core_py_files() -> list[Path]:
    return sorted(_CORE_DIR.rglob("*.py"))


def test_core_dir_exists_and_has_files():
    # Guards against the rglob silently matching nothing (e.g. a rename) and the
    # parametrized test below collecting zero cases, which would pass vacuously.
    files = _core_py_files()
    assert files, f"no .py files found under {_CORE_DIR} — has the package moved?"


@pytest.mark.parametrize("py_file", _core_py_files(), ids=lambda p: str(p.relative_to(_ROOT)))
def test_no_direct_streamlit_import(py_file: Path):
    tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
    offenders: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "streamlit" or alias.name.startswith("streamlit."):
                    offenders.append(f"line {node.lineno}: import {alias.name}")
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if mod == "streamlit" or mod.startswith("streamlit."):
                offenders.append(f"line {node.lineno}: from {mod} import ...")
    assert not offenders, (
        f"{py_file.relative_to(_ROOT)} imports streamlit (core must be UI-free):\n  "
        + "\n  ".join(offenders)
    )


def test_no_transitive_streamlit_on_import():
    """Import the whole core package in isolation; streamlit must not appear."""
    code = (
        "import importlib, pkgutil, sys\n"
        "import dail_tracker_core as core\n"
        "for m in pkgutil.walk_packages(core.__path__, core.__name__ + '.'):\n"
        "    importlib.import_module(m.name)\n"
        "leaked = sorted(n for n in sys.modules if n == 'streamlit' or n.startswith('streamlit.'))\n"
        "print('LEAKED:' + ','.join(leaked))\n"
        "sys.exit(1 if leaked else 0)\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        cwd=_ROOT,
        capture_output=True,
        text=True,
    )
    assert proc.returncode == 0, (
        "importing dail_tracker_core pulled streamlit into sys.modules "
        f"(transitive leak via a utility import?):\n{proc.stdout}\n{proc.stderr}"
    )
