"""Guards on utility/app.py's planning-page routing (the siting-engine import fallback).

utility/app.py wires the "Planning" nav section to real page functions imported from
planning.product.ui.pages.{siting_assistant,siting_check}, wrapped in a
``try/except ImportError`` that falls back to a stub page ("unavailable here: ...")
when the optional siting extra isn't installed (e.g. Streamlit Cloud). Two ways that
routing can go wrong silently:

  1. The fallback triggers even though the real pages ARE importable (a false trigger) —
     e.g. an import-path typo after a file move, degrading a working feature to a stub
     with no test noticing.
  2. The except clause is broadened past ``ImportError`` (bare ``except:`` or
     ``except Exception``) and starts swallowing real bugs inside the page modules,
     rendering them as "unavailable" instead of failing loudly.

utility/app.py itself can't be imported directly in a test: it calls st.navigation(...)
and pg.run() unconditionally at module scope, which actually renders a page outside any
Streamlit runtime. These tests instead exercise the exact source of the try/except block
(extracted via ast from the live file, so they can't silently drift from the real code)
and the concrete import path it uses.
"""

from __future__ import annotations

import ast
import subprocess
import sys
import types
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
APP_PY = PROJECT_ROOT / "utility" / "app.py"


def _planning_try_node() -> ast.Try:
    """The module-level ``try/except ImportError`` that wires the Planning nav pages."""
    tree = ast.parse(APP_PY.read_text(encoding="utf-8"), filename=str(APP_PY))
    for node in tree.body:
        if isinstance(node, ast.Try) and any(
            isinstance(stmt, ast.ImportFrom) and (stmt.module or "").startswith("planning.product.ui.pages")
            for stmt in node.body
        ):
            return node
    raise AssertionError("app.py no longer has a planning.product.ui.pages try/except block — routing moved?")


def _exec_try_node(node: ast.Try, *, st_mock: types.SimpleNamespace) -> dict:
    """Run the extracted try/except in a fresh namespace with a fake ``st``.

    Returns the resulting namespace so callers can inspect which branch bound
    ``siting_check_page`` / ``siting_assistant_page``.
    """
    module = ast.Module(body=[node], type_ignores=[])
    ast.fix_missing_locations(module)
    ns = {"st": st_mock}
    exec(compile(module, filename=str(APP_PY), mode="exec"), ns)
    return ns


# ── does the except clause stay scoped to ImportError? ─────────────────────────
def test_fallback_only_catches_importerror() -> None:
    """A bare or ``Exception``-wide except would falsely swallow real bugs as 'unavailable'."""
    node = _planning_try_node()
    assert len(node.handlers) == 1, f"expected exactly one except clause, found {len(node.handlers)}"
    handler = node.handlers[0]
    assert handler.type is not None, "bare `except:` would swallow every exception, not just missing imports"
    assert isinstance(handler.type, ast.Name) and handler.type.id == "ImportError", (
        f"except clause catches {ast.dump(handler.type)!r}, not ImportError — "
        "a routing bug in the real page would now render as 'unavailable' instead of failing loudly"
    )


def test_fallback_defines_both_page_names() -> None:
    """Whatever the except branch does, it must bind both names the nav dict wires up."""
    node = _planning_try_node()
    handler = node.handlers[0]
    defined = {stmt.name for stmt in handler.body if isinstance(stmt, ast.FunctionDef)}
    assert defined == {"siting_check_page", "siting_assistant_page"}


# ── real environment: the try branch must win, not fall through to the stub ────
def test_planning_pages_import_cleanly_in_this_env() -> None:
    """Guards against a false trigger of the fallback: in this dev env the siting extra
    IS installed, so app.py's routing must bind the real page functions, never the stub.
    Run in a subprocess (matching test_page_imports.py's pattern) for a clean import table.
    """
    code = (
        "from planning.product.ui.pages.siting_assistant import siting_assistant_page\n"
        "from planning.product.ui.pages.siting_check import siting_check_page\n"
        "assert siting_assistant_page.__module__ == 'planning.product.ui.pages.siting_assistant'\n"
        "assert siting_check_page.__module__ == 'planning.product.ui.pages.siting_check'\n"
        "print('ROUTED_TO_REAL_PAGES')\n"
    )
    env = {"PYTHONPATH": str(PROJECT_ROOT)}
    import os

    env = {**os.environ, **env}
    r = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, env=env, timeout=120, check=False
    )
    assert r.returncode == 0, f"planning nav import failed (would silently fall back to stub):\n{r.stderr[-3000:]}"
    assert "ROUTED_TO_REAL_PAGES" in r.stdout


def test_try_branch_wins_over_fallback_when_import_succeeds() -> None:
    """Execute app.py's actual try/except source; with real imports available the except
    branch must never run, so neither stub function should end up bound.
    """
    node = _planning_try_node()
    st_mock = types.SimpleNamespace(title=lambda *a, **k: None, info=lambda *a, **k: None, caption=lambda *a, **k: None)
    ns = _exec_try_node(node, st_mock=st_mock)
    assert callable(ns["siting_assistant_page"])
    assert callable(ns["siting_check_page"])
    assert ns["siting_assistant_page"].__module__ == "planning.product.ui.pages.siting_assistant"
    assert ns["siting_check_page"].__module__ == "planning.product.ui.pages.siting_check"
    assert "_siting_import_error" not in ns, "fallback branch ran even though the real import succeeded"


# ── forced failure: the fallback must trigger correctly when it genuinely should ──
def test_fallback_triggers_on_genuine_importerror(monkeypatch: pytest.MonkeyPatch) -> None:
    """Force the exact import app.py performs to fail, then run the real source lines and
    check the stub captures the error and both stub pages render the caption.
    """
    import builtins

    real_import = builtins.__import__

    def _blocking_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "planning.product.ui.pages.siting_assistant" or (
            name.startswith("planning.product.ui.pages") and fromlist and "siting_assistant_page" in fromlist
        ):
            raise ImportError("simulated: siting extra not installed")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _blocking_import)

    calls: list[tuple[str, tuple]] = []
    st_mock = types.SimpleNamespace(
        title=lambda *a, **k: calls.append(("title", a)),
        info=lambda *a, **k: calls.append(("info", a)),
        caption=lambda *a, **k: calls.append(("caption", a)),
    )

    node = _planning_try_node()
    ns = _exec_try_node(node, st_mock=st_mock)

    assert ns["_siting_import_error"] == "simulated: siting extra not installed"
    assert callable(ns["siting_assistant_page"])
    assert callable(ns["siting_check_page"])

    calls.clear()
    ns["siting_check_page"]()
    captions = [a[0] for kind, a in calls if kind == "caption"]
    assert any("simulated: siting extra not installed" in c for c in captions), (
        f"stub page didn't surface the real import error in its caption: {captions}"
    )

    calls.clear()
    ns["siting_assistant_page"]()
    captions = [a[0] for kind, a in calls if kind == "caption"]
    assert any("simulated: siting extra not installed" in c for c in captions)
