"""Dual-config parity tripwire.

The repo has TWO modules that answer to the import name ``config``:

* ``config.py`` (root)      — pipeline paths + constants (~69 importers)
* ``utility/config.py``     — app paths + UI constants  (~10 importers)

Which one ``from config import X`` resolves to depends on sys.path order at
import time. Code under ``utility/`` and ``dail_tracker_core/`` runs in BOTH
resolution contexts — the live Streamlit app binds utility/config.py first,
while pytest and pipeline entrypoints bind the root one — so every symbol that
code imports from ``config`` must exist in BOTH modules, with equal values.

Past incidents this guards against (see memory/feedback_dual_config_files):
* new path consts added to one file only → view registration silently no-ops
  in the live app while all headless tests pass;
* ``COMMITTEE_TYPES`` added to utility/config.py only (2026-06-11) → the page
  import smoke test failed for member_overview.

Root config.py carries a ``_bridge_ui_constants()`` block that re-exports
utility/config.py's public UPPERCASE symbols, so UI constants keep a single
source. These tests are the tripwire for everything the bridge can't prove:
symbols defined in both files drifting apart, and ambiguous-context importers
referencing a name missing from either resolution.

Delete this module (and the bridge) when the reorg moves config into a proper
package — a package-qualified import name removes the ambiguity entirely.
"""

from __future__ import annotations

import ast
import importlib.util
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Trees whose code runs under BOTH config resolutions.
AMBIGUOUS_TREES = (
    PROJECT_ROOT / "utility",
    PROJECT_ROOT / "dail_tracker_core",
)


def _load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None, f"cannot load {path}"
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def configs():
    """(root config module, utility config module) loaded by file path —
    sidesteps the sys.modules['config'] name race this file is about."""
    if str(PROJECT_ROOT) not in sys.path:
        sys.path.insert(0, str(PROJECT_ROOT))  # root config does `from paths import ...`
    root = _load(PROJECT_ROOT / "config.py", "_parity_root_config")
    util = _load(PROJECT_ROOT / "utility" / "config.py", "_parity_util_config")
    return root, util


def _public_constants(mod) -> dict[str, object]:
    return {k: v for k, v in vars(mod).items() if k.isupper() and not k.startswith("_")}


def _config_imports(trees: tuple[Path, ...]) -> dict[str, Path]:
    """Every name imported via ``from config import X`` anywhere under trees
    (module level or deferred inside functions), mapped to one importing file."""
    found: dict[str, Path] = {}
    for tree in trees:
        for py in sorted(tree.rglob("*.py")):
            try:
                module_ast = ast.parse(py.read_text(encoding="utf-8"))
            except (SyntaxError, UnicodeDecodeError):
                continue  # unparseable file is some other test's problem
            for node in ast.walk(module_ast):
                if isinstance(node, ast.ImportFrom) and node.module == "config" and node.level == 0:
                    for alias in node.names:
                        if alias.name != "*":
                            found.setdefault(alias.name, py)
    return found


def _values_equal(a: object, b: object) -> bool:
    if isinstance(a, Path) and isinstance(b, Path):
        return a.resolve() == b.resolve()
    return bool(a == b)


def test_ambiguous_importers_resolve_in_both_configs(configs) -> None:
    """Any `from config import X` under utility/ or dail_tracker_core/ must
    find X in BOTH config modules — those trees run under both resolutions."""
    root, util = configs
    imported = _config_imports(AMBIGUOUS_TREES)
    assert imported, "scanner found no config imports — scan is broken, not the code"

    problems = []
    for name, importer in sorted(imported.items()):
        rel = importer.relative_to(PROJECT_ROOT)
        if not hasattr(root, name):
            problems.append(f"{name} (imported by {rel}) missing from root config.py")
        if not hasattr(util, name):
            problems.append(f"{name} (imported by {rel}) missing from utility/config.py")
    assert not problems, (
        "Two-config trap: these symbols exist in only one of the two `config` "
        "modules, so they resolve in one runtime context and ImportError in the "
        "other. Add them to the missing side (UI constants belong in "
        "utility/config.py — root re-exports them via _bridge_ui_constants):\n  " + "\n  ".join(problems)
    )


def test_shared_constants_hold_equal_values(configs) -> None:
    """Every public constant defined in BOTH files must agree — a drifted
    duplicate means app and pipeline silently disagree about the same name."""
    root, util = configs
    r, u = _public_constants(root), _public_constants(util)
    drifted = []
    for name in sorted(set(r) & set(u)):
        if not _values_equal(r[name], u[name]):
            drifted.append(f"{name}:\n    root:    {r[name]!r}\n    utility: {u[name]!r}")
    assert not drifted, (
        "Two-config trap: the same constant holds different values in the two "
        "`config` modules. Pick one source of truth (usually utility/config.py "
        "for UI constants, root for pipeline paths) and remove the stale copy:\n  " + "\n  ".join(drifted)
    )
