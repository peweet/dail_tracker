"""Streamlit adapter for the shared D?il Tracker design system.

The order-dependent stylesheet lives in utility/static/dailtracker.css so it can
be consumed by another frontend without importing Streamlit. Page-local CSS
still exists in the renderers listed by utility/pages_code/MANIFEST.md.
"""

from functools import lru_cache
from pathlib import Path

import streamlit as st

_CSS_PATH = Path(__file__).with_name("static") / "dailtracker.css"


@lru_cache(maxsize=1)
def _load_css() -> str:
    """Load the order-preserving stylesheet once per process."""
    return _CSS_PATH.read_text(encoding="utf-8")


def inject_css() -> None:
    """Inject the design system once per script run.

    Rendered once per script run at app level (utility/app.py, before
    pg.run()) so the stylesheet + banner stay mounted across page
    navigations. Previously each page called this inside its own function,
    so the <style> and .site-banner lived under the page's element subtree
    and were torn down on every navigation — a frame with no design system
    (white/unstyled, collapsed content) that read as a flash/flicker,
    worst on the heavier pages. The per-run guard below makes the legacy
    per-page inject_css() calls harmless no-ops; the guard is reset at the
    top of each run in app.py."""
    if st.session_state.get("_dt_css_injected"):
        return
    st.session_state["_dt_css_injected"] = True
    st.markdown("<style>\n" + _load_css() + "</style>", unsafe_allow_html=True)
    st.html(
        """
        <div class="site-banner">
          <div class="site-banner-inner">
            <a class="site-banner-title" href="./" aria-label="Dáil Tracker — back to home">Dáil Tracker</a>
            <span class="site-banner-sep"></span>
            <span class="site-banner-sub">Irish public data, made searchable</span>
          </div>
        </div>
        """
    )
