"""Guard: page tests must not bare-``import`` a page module whose name is also a root package.

Six Streamlit page modules in ``utility/pages_code/`` share a name with a root-level ETL
package: attendance, committees, corporate, legislation, payments, votes. Page tests put
``utility/pages_code`` on sys.path, so a bare ``import corporate`` is ambiguous — it returns
whichever module reached ``sys.modules`` first. Alone, the test file wins and passes; in a
full-suite run ``test/corporate/`` is collected first (alphabetically, and this is exactly
what CI does), the ETL package wins, and every assertion in the page test dies with
``AttributeError: module 'corporate' has no attribute ...``.

That is not hypothetical: test_corporate_page_smoke.py shipped with a bare ``import corporate``
and its 8 tests failed in CI while passing for anyone who ran the file on its own.

The fix is always the same — import the page by dotted path, which is unambiguous:

    page = importlib.import_module("utility.pages_code.corporate")

This guard fails on any NEW bare import of a colliding name, so the trap can't be re-set.
"""

from __future__ import annotations

import ast
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_PAGES_DIR = _ROOT / "utility" / "pages_code"
_TEST_DIR = _ROOT / "test"


def _page_module_names() -> set[str]:
    return {p.stem for p in _PAGES_DIR.glob("*.py") if p.stem != "__init__"}


def _root_package_names() -> set[str]:
    return {p.name for p in _ROOT.iterdir() if p.is_dir() and (p / "__init__.py").exists()}


def test_collisions_are_known() -> None:
    """Documents the shadowed names. If this changes, the list in the docstring is stale."""
    collisions = _page_module_names() & _root_package_names()
    assert collisions, "expected page/package name collisions to exist — has the layout changed?"
    assert "corporate" in collisions  # the one that actually bit us


def test_no_test_bare_imports_a_shadowed_page_module() -> None:
    """No test file may ``import <name>`` where <name> is both a page and a root package."""
    collisions = _page_module_names() & _root_package_names()
    offenders: list[str] = []

    for path in _TEST_DIR.rglob("test_*.py"):
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover — a broken test file is its own failure
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Import):
                continue
            for alias in node.names:
                # `import corporate` / `import corporate as c` — ambiguous.
                # `import corporate.cro_poller` is fine: it names the package explicitly.
                if alias.name in collisions:
                    rel = path.relative_to(_ROOT).as_posix()
                    offenders.append(f"{rel}:{node.lineno}: import {alias.name}")

    assert not offenders, (
        "These tests bare-import a page module that a root ETL package shadows. They pass alone "
        "and fail in a full-suite/CI run. Import by dotted path instead — "
        'importlib.import_module("utility.pages_code.<name>"):\n  ' + "\n  ".join(offenders)
    )
