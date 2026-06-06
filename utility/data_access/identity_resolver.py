"""Canonical TD name → unique_member_code lookup.

Round-3 audit fix for the broken cross-page contract: every Phases 3-8
dimension page was building card hrefs via name_join_key(name), which
returns a sorted-letters string (e.g. "aacddllmmnooruy") that does NOT
match the registered unique_member_code format used by v_member_registry
(e.g. "Mary-Lou-McDonald.D.2011-03-09"). Every cross-page click landed on
a "TD is not in the dataset" error.

This module provides the actual bridge: a single lookup against
v_member_registry, cached. Use whenever a page has a name string but
needs the canonical code for member_profile_url().

Once the pipeline ships unique_member_code on every dimension view
(the TODO_PIPELINE_VIEW_REQUIRED notes scattered through interests /
payments / attendance / committees), pages can read the code directly
from their own queries and skip this resolver.
"""

from __future__ import annotations

import streamlit as st
from data_access.member_overview_data import get_member_overview_conn
from dail_tracker_core.queries import member_overview as moq


@st.cache_data(ttl=600, show_spinner=False)
def resolve_member_code(name: str) -> str | None:
    """Look up the canonical ``unique_member_code`` for a TD name.

    Returns the registered code (e.g. ``"Mary-Lou-McDonald.D.2011-03-09"``)
    or ``None`` if the name isn't in the registry. Cached per-name for 10
    minutes so the same lookup across multiple cards on one page hits the
    DB at most once.

    Exact-match only. Trailing/leading whitespace is stripped; case is
    preserved (the registry stores canonical casing). The retrieval is the
    same WHERE member_name=? LIMIT 1 lookup, now via the shared core query
    ``moq.join_key_by_name`` (which also maps a None conn / DuckDB error to an
    empty result → None here).
    """
    if not name:
        return None
    df = moq.join_key_by_name(get_member_overview_conn(), name.strip()).data
    return str(df.iloc[0]["unique_member_code"]) if not df.empty else None
