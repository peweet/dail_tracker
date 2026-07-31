from __future__ import annotations

import streamlit as st

from ui.components import dt_page, hide_sidebar
from data_access.member_overview_data import get_member_overview_conn

from ._shared import _STAGE_KEY
from .browse import _render_browse
from .profile import _render_stage2

# ── Main entry point ───────────────────────────────────────────────────────────


@dt_page
def member_overview_page() -> None:
    conn = get_member_overview_conn()

    url_jk = st.query_params.get("member")
    if url_jk:
        st.session_state[_STAGE_KEY] = url_jk

    join_key = st.session_state.get(_STAGE_KEY)

    # Sidebar→filter-bar migration: identity is carried by the top-nav tab +
    # each view's own hero. The only sidebar control was a vote-date filter,
    # now relocated into the Votes section it filters (see _render_stage2).
    hide_sidebar()

    if join_key:
        _render_stage2(conn, join_key)
    else:
        _render_browse(conn)
