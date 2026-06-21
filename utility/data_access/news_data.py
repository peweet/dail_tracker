"""News-feed data access — thin Streamlit wrapper over dail_tracker_core.

The cross-member "In the News" feed reads ``v_news_mentions_recent`` (news mentions
JOINed to the member registry for display names), which is registered on the standard
member-overview connection — so this module reuses ``get_member_overview_conn`` rather
than building a second connection. Retrieval SQL lives in
``dail_tracker_core.queries.member_overview.news_feed``; this file owns only Streamlit
caching and unwraps ``.data`` (empty frame on a source failure — same contract as the
other data-access modules).

Forbidden here (unchanged across the layer): JOIN / multi-col GROUP BY / HAVING / WINDOW
in ad-hoc SQL, CREATE VIEW, read_parquet, pandas merge/pivot, business-metric
definitions. The page does outlet-tier / headline-only faceting off this frame.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st
from data_access.member_overview_data import get_member_overview_conn

from dail_tracker_core.queries import member_overview as _q


@st.cache_data(ttl=300)
def fetch_news_feed(limit: int = 600) -> pd.DataFrame:
    """Recent cross-member news mentions, most-recent first. Empty when the view is
    missing (fresh clone with no news_mentions parquet)."""
    return _q.news_feed(get_member_overview_conn(), limit=limit).data
