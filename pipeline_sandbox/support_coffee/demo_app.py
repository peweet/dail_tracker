"""Standalone preview app for the support / coffee surface. SANDBOX ONLY.

    .venv/Scripts/python -m streamlit run pipeline_sandbox/support_coffee/demo_app.py --server.port 8599

This is its OWN Streamlit app. It does not import utility/app.py, registers no
pages with it, and changes nothing the live site renders. It only READS the
production design system (utility/shared_css.inject_css) so the preview sits on
the real tokens and fonts rather than a mock palette — an import, not a mutation.

Two things to look at:
  1. the /support page body;
  2. the app-level footer strip, rendered last, standing in for the position it
     would occupy in utility/app.py after pg.run().
"""

from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

# services.runtime_env caps BLAS threads and MUST be the first project import
# in any entry point (see CLAUDE.md / project_oom_root_cause_blas_threads).
_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
import services.runtime_env  # noqa: F401,E402  (import for side effect, keep first)

sys.path.insert(0, str(_ROOT / "utility"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from coffee_ui import SUPPORT_CSS, site_footer_html  # noqa: E402
from page_support import support_page  # noqa: E402

st.set_page_config(
    page_title="Support — Dáil Tracker (sandbox)",
    page_icon=":material/local_cafe:",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Read-only use of the production design system so the preview is honest about
# how this lands against the real fonts, palette and spacing.
try:
    st.session_state["_dt_css_injected"] = False
    from shared_css import inject_css

    inject_css()
    from ui.components import hide_sidebar

    hide_sidebar()
    _design_system = "real (utility/shared_css.py)"
except Exception as err:  # pragma: no cover - preview convenience
    st.warning(f"Production CSS not loaded, preview falls back to Streamlit defaults: {err}")
    _design_system = f"FALLBACK — {err}"

support_page(
    sandbox_note=(
        "not wired into the app · all three figures queried live · coffee slug not "
        f"registered yet · design system: {_design_system}"
    )
)

# Stand-in for the app-level footer. In utility/app.py this would be the single
# line after pg.run() — outside the page subtree so it survives navigation.
st.markdown(SUPPORT_CSS, unsafe_allow_html=True)
st.markdown(
    f'<div class="sc-wrap">{site_footer_html(last_refresh="26 July 2026")}</div>',
    unsafe_allow_html=True,
)
