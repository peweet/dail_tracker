"""
Streamlit page import smoke tests.

Imports every module under utility/pages_code/*.py without running Streamlit.
Catches import-time regressions for free: missing symbols, broken `from ... import`,
typos, NameError at module load. Does NOT catch runtime/Streamlit-context bugs —
those need a full `streamlit run`.

Why this matters: page-level Streamlit code is 3,200+ lines, all 0% covered by the
schema/unit tests. A typo in a top-level import or a renamed-but-not-updated symbol
ships to Streamlit Cloud and only fails when a user clicks the page.

Streamlit emits "No runtime found" warnings during these imports — that's expected
and not a failure (Streamlit's cache_data wrapper falls back to MemoryCacheStorageManager
when there's no script run context).
"""

from __future__ import annotations

import importlib
import sys
import warnings
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PAGES_DIR = PROJECT_ROOT / "utility" / "pages_code"

# Ensure both repo root and utility/ are on sys.path before importing pages.
# Pages do `from shared_css import ...` (resolved via utility/) and
# `from config import ...` (resolved via repo root).
for path in (str(PROJECT_ROOT), str(PROJECT_ROOT / "utility")):
    if path not in sys.path:
        sys.path.insert(0, path)


def _discover_pages() -> list[str]:
    """Return the stem of every page module, skipping __init__ and private files."""
    return sorted(p.stem for p in PAGES_DIR.glob("*.py") if not p.name.startswith("_"))


PAGE_MODULES = _discover_pages()


@pytest.fixture
def isolated_config_resolution():
    """Reset sys.modules entries that shadow utility/config.py.

    The repo has two `config.py` files — root and utility/. Once a non-page
    test imports the root one (e.g. test_payments_golden imports
    payments_full_psa_etl which loads root config), sys.modules['config']
    is bound to root, and the subsequent page import sees the wrong module.
    Restoring sys.modules around each page import keeps the resolution
    clean regardless of test order.

    See note in module docstring on the two-config-modules collision.
    """
    saved_modules = {k: sys.modules[k] for k in list(sys.modules) if k == "config" or k.startswith("utility.")}
    # Force re-resolution: pages do `from config import …` and must find
    # utility/config.py via the test-prepared sys.path priority.
    sys.modules.pop("config", None)
    yield
    # Restore so we don't leak into other tests in the file.
    sys.modules.pop("config", None)
    for k, v in saved_modules.items():
        sys.modules[k] = v


@pytest.mark.parametrize("page", PAGE_MODULES)
def test_page_imports_cleanly(page: str, isolated_config_resolution) -> None:
    """Importing the page module must not raise.

    Streamlit's cache_data warns "No runtime found" outside `streamlit run`;
    we suppress that here so test output stays clean. Any other warning still
    surfaces.
    """
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="No runtime found")
        importlib.import_module(f"utility.pages_code.{page}")


def test_page_discovery_finds_expected_count() -> None:
    """Guard against accidentally hiding pages from the smoke test.

    If a new page is added under utility/pages_code/ the count goes up — fine.
    If the count drops, either a page was deleted (update this assertion) or
    the discovery glob broke (real bug).
    """
    assert len(PAGE_MODULES) >= 9, f"Expected at least 9 pages, found {len(PAGE_MODULES)}: {PAGE_MODULES}"
