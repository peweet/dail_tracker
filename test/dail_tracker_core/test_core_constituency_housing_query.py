"""Contract for the registered supply-and-demand constituency context view."""

from __future__ import annotations

import duckdb

from dail_tracker_core.queries import constituency as q


def test_housing_context_with_ssha_missing_view_is_unavailable() -> None:
    conn = duckdb.connect()
    try:
        assert q.constituency_housing_context_with_ssha(conn, "Dublin Central").ok is False
    finally:
        conn.close()
