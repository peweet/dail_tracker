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

import os
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
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


@pytest.mark.parametrize("page", PAGE_MODULES)
def test_page_imports_cleanly(page: str) -> None:
    """Importing the page module must not raise.

    Each page is imported in a FRESH SUBPROCESS. Same-process importing was
    flaky (28/16/7 pass non-deterministically across runs): the 27 pages share
    one interpreter's ``sys.modules``, and pages do function-level
    ``from data_access.X import Y`` where ``data_access.X`` does module-level
    ``from ui.components import …`` — so a shift in import ORDER can observe
    ``ui.components`` mid-initialisation and raise ``NameError: dt_page`` (or any
    other symbol not yet bound). A subprocess gives every page a clean module
    table, which is what an import-smoke test actually wants to prove: "this page
    imports from a cold start", exactly as ``streamlit run`` does. It also fixes
    the two-``config.py`` (root vs utility/) shadowing the old fixture juggled.

    Streamlit's cache_data warns "No runtime found" outside ``streamlit run``;
    ``-W ignore`` keeps that out of stderr. Any real error surfaces in the
    captured output on failure.
    """
    code = (
        "import warnings; warnings.filterwarnings('ignore', message='No runtime found'); "
        f"import utility.pages_code.{page}"
    )
    env = {**os.environ, "PYTHONPATH": os.pathsep.join((str(PROJECT_ROOT), str(PROJECT_ROOT / "utility")))}
    r = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, env=env, timeout=120, check=False
    )
    assert r.returncode == 0, f"page {page!r} failed to import:\n{r.stderr[-3000:]}"


def test_page_discovery_finds_expected_count() -> None:
    """Guard against accidentally hiding pages from the smoke test.

    If a new page is added under utility/pages_code/ the count goes up — fine.
    If the count drops, either a page was deleted (update this assertion) or
    the discovery glob broke (real bug).
    """
    assert len(PAGE_MODULES) >= 9, f"Expected at least 9 pages, found {len(PAGE_MODULES)}: {PAGE_MODULES}"
