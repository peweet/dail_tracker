"""Public Appointments data access — thin framework-neutral cached wrapper over dail_tracker_core.

Retrieval SQL lives in ``dail_tracker_core.queries.appointments``; this file owns
only framework-neutral caching and unwraps ``.data`` (empty on a source failure — same
contract as the old ``_safe``).

Forbidden here (unchanged): JOIN/multi-col GROUP BY/HAVING/WINDOW in SQL,
CREATE VIEW, read_parquet, pandas merge/pivot, business-metric definitions.
"""

from __future__ import annotations

import duckdb
import pandas as pd
from data_access._cache import cache_data, cache_resource

from dail_tracker_core.connections import domain_conn
from dail_tracker_core.queries import appointments as _q


@cache_resource
def get_appointments_conn() -> duckdb.DuckDBPyConnection:
    return domain_conn("appointments")


@cache_data(ttl=300)
def fetch_public_appointments() -> pd.DataFrame:
    """Every public-appointment notice as a row — the full v_public_appointments
    view. One registered analytical surface; the page does its filtering,
    faceting, and grouping in pandas off this frame."""
    return _q.public_appointments(get_appointments_conn()).data


@cache_data(ttl=300)
def fetch_stateboards_roster() -> pd.DataFrame:
    """Every current state-board seat as a row — v_stateboards_roster. The live
    DPER membership register; the page facets/groups in pandas off this frame."""
    return _q.stateboards_roster(get_appointments_conn()).data


@cache_data(ttl=300)
def fetch_stateboards_boards() -> pd.DataFrame:
    """The state-board universe (one row per board) with legal basis and
    gender-balance metadata — v_stateboards_boards."""
    return _q.stateboards_boards(get_appointments_conn()).data
