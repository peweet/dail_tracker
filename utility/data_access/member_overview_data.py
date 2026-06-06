"""
Member Overview data-access layer — unified DuckDB connection (CORE-BACKED).

Thin Streamlit wrapper. The bespoke 4-phase view registration (ordered domain
glob + registry/external-links/vote phases with their parquet-path substitutions)
now lives in the Streamlit-free ``dail_tracker_core.connections`` so the read-only
API can build the IDENTICAL connection without importing Streamlit. This module
just wraps it in ``@st.cache_resource`` (one connection per Streamlit session).

The four file lists are re-exported under their original ``_``-prefixed names
because ``test_member_overview_connection_builds`` imports them by name.

Forbidden here (unchanged): JOIN/GROUP_BY_MULTI_DIM/HAVING/WINDOW in ad-hoc
retrieval SQL, business-metric definitions. (The retrieval SQL itself lives in
dail_tracker_core.queries.member_overview.)
"""

from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_UTIL = _HERE.parent
if str(_UTIL) not in sys.path:
    sys.path.insert(0, str(_UTIL))

import duckdb  # noqa: E402
import streamlit as st  # noqa: E402

from dail_tracker_core.connections import (  # noqa: E402
    DOMAIN_FILES,
    EXTERNAL_LINKS_FILES,
    REGISTRY_FILES,
    VOTE_FILES,
    member_overview_conn,
)

# Back-compat aliases — test_member_overview_connection_builds imports these.
_DOMAIN_FILES = DOMAIN_FILES
_REGISTRY_FILES = REGISTRY_FILES
_EXTERNAL_LINKS_FILES = EXTERNAL_LINKS_FILES
_VOTE_FILES = VOTE_FILES


@st.cache_resource
def get_member_overview_conn() -> duckdb.DuckDBPyConnection:
    return member_overview_conn()
