"""/support page body — SANDBOX ONLY, deliberately unregistered.

NOT in utility/app.py's st.navigation() and must not be added there without a
separate decision. Parked here the same way pipeline_sandbox/news_mentions/
page_news.py is parked while a feature is being tested.

Firewall note: this page has NO data access and NO business logic — it is
static copy plus the HTML builders in coffee_ui. If the cost strip ever shows
real figures, they come from a registered contract via utility/data_access/,
never from a query written here.
"""

from __future__ import annotations

import streamlit as st

from coffee_ui import SUPPORT_CSS, support_page_html


def support_page(*, sandbox_note: str | None = None) -> None:
    """Render the support page body.

    Deliberately NOT decorated with @dt_page: that decorator is the production
    page bootstrap (inject_css + hide_sidebar + error boundary) and wiring it
    here would make this look like a registered page. The demo app does the
    boot itself.

    One markdown call for the whole body — see support_page_html on why the
    max-width column cannot survive being split across several calls.
    """
    st.markdown(SUPPORT_CSS, unsafe_allow_html=True)
    st.markdown(support_page_html(sandbox_note=sandbox_note), unsafe_allow_html=True)
