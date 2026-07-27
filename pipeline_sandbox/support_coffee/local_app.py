"""Run the REAL Dáil Tracker app locally with the Support page + footer added.
SANDBOX ONLY — a local preview harness, never a deployment entry point.

    .venv/Scripts/python -m streamlit run pipeline_sandbox/support_coffee/local_app.py --server.port 8599

Why this exists rather than an edit to utility/app.py: Streamlit Cloud deploys
from the repo, so ANY wiring committed into utility/app.py ships to the live
site. This file leaves app.py byte-identical and does the wiring at runtime, in
a process that only ever runs on this machine. Nothing here is importable by
the deployed app — it is a separate entry point under pipeline_sandbox/.

How it works:
  1. ``st.navigation`` is patched BEFORE utility/app.py is executed, so the
     Support page is appended to the "Glossary" nav group (the plan's placement
     — a tip jar does not earn its own top-level nav section).
  2. The object app.py gets back is a thin proxy whose ``run()`` calls the real
     one and then renders the footer strip. That reproduces the exact position
     the footer would occupy in production: app level, AFTER pg.run(), outside
     the per-page subtree so it stays mounted across navigation.
  3. utility/app.py is then executed unmodified via runpy.

If app.py's shape ever changes, this harness breaks loudly here — it cannot
fail silently in production, because it is not part of production.
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

# services.runtime_env caps BLAS threads and MUST be the first project import
# in any entry point (CLAUDE.md / project_oom_root_cause_blas_threads).
_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
import services.runtime_env  # noqa: F401,E402  (side-effect import, keep first)

_UTILITY = _ROOT / "utility"
sys.path.insert(0, str(_UTILITY))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import streamlit as st  # noqa: E402

from coffee_ui import SUPPORT_CSS, site_footer_html  # noqa: E402
from page_support import support_page as _support_body  # noqa: E402

_SANDBOX_NOTE = (
    "not wired into utility/app.py · all three figures are queried live from the "
    "repo's own metadata and registers · the Buy Me a Coffee slug is not registered yet"
)


def support(page_fn=None):  # noqa: D401 - st.Page needs a plain named callable
    """Support — the page as it would appear in the real nav."""
    from ui.components import hide_sidebar

    hide_sidebar()
    _support_body(sandbox_note=_SANDBOX_NOTE)


class _NavProxy:
    """Proxies the StreamlitPage app.py gets back, appending the footer.

    app.py reads ``pg.url_path`` (for log_page_view) and calls ``pg.run()``;
    everything else falls through to the real object untouched.
    """

    def __init__(self, page, footer: str) -> None:
        self._page = page
        self._footer = footer

    def __getattr__(self, name):
        return getattr(self._page, name)

    def run(self, *args, **kwargs):
        try:
            return self._page.run(*args, **kwargs)
        finally:
            # `finally`, so the footer still renders if a page body raises and
            # the error boundary swallows it — production would behave the same.
            st.markdown(SUPPORT_CSS, unsafe_allow_html=True)
            st.markdown(site_footer_html(last_refresh="26 July 2026"), unsafe_allow_html=True)


# Streamlit re-executes the MAIN script on every rerun, but the `st` module
# object persists in sys.modules. A naive `_real = st.navigation` would
# therefore capture the PREVIOUS run's patched function on rerun 2, nesting a
# proxy inside a proxy and appending the Support page twice. Stash the genuine
# callable on the module the first time and read it back thereafter.
if not getattr(st, "_dt_sandbox_real_navigation", None):
    st._dt_sandbox_real_navigation = st.navigation
_real_navigation = st._dt_sandbox_real_navigation


def _patched_navigation(pages, **kwargs):
    """Append the Support page to the Glossary group, then delegate."""
    if isinstance(pages, dict):
        pages = {k: list(v) for k, v in pages.items()}
        entry = st.Page(
            support,
            title="Support",
            icon=":material/local_cafe:",
            url_path="support",
        )
        pages.setdefault("Glossary", []).append(entry)
    else:  # app.py passes a dict today; fail loudly here if that ever changes
        raise TypeError(f"local_app harness expects a dict of nav groups, got {type(pages)!r}")
    return _NavProxy(_real_navigation(pages, **kwargs), "")


st.navigation = _patched_navigation

# Execute the real app, unmodified.
runpy.run_path(str(_UTILITY / "app.py"), run_name="__main__")
