"""BAM / National Children's Hospital PQ-disclosure retrieval — Streamlit-free.

Retrieval-only SELECT against ``v_nphdb_bam_disclosures`` (a static, hand-verified
Dáil written-answer disclosure — not a scraped corpus). Build a connection with
``connect_with_views(["nphdb_bam_disclosures.sql"])``.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import make_runner
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)

_run = make_runner("nphdb_bam", _log)


def disclosures(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Every row of the BAM schedule-slippage ledger + 2017/2018 cost table, in
    source table order. Two disjoint grains (a schedule date vs a cost estimate,
    2017 vs 2018) that are never summed with each other or with any payments/awards
    figure elsewhere in the app — this is a disclosure, not a total."""
    return _run(
        conn,
        "SELECT disclosure_type, sort_order, row_label, forecast_completion,"
        " delay_from_original, cost_2017_eur_m, cost_2018_eur_m, source_date,"
        " source_pq_ref, source_url, notes"
        " FROM v_nphdb_bam_disclosures ORDER BY disclosure_type, sort_order",
    )
