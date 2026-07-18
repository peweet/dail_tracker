"""Organisation entity-crosswalk retrieval — Streamlit-free.

Retrieval-only SQL over ``v_supplier_entity_xref`` (the organisation-360 spine, built by
``extractors/entity_xref_build.py``): one row per procurement supplier (keyed on
``supplier_norm``) carrying its cross-register presence + counts, fused on the canonical
name key. All aggregation / the canonical re-norm lives in the extractor + view; this layer
only SELECTs, returning a ``QueryResult`` so a caller can tell "source unavailable" from
"no such supplier".

Build a connection with the procurement glob (``connect_with_views(["procurement_*.sql"])``);
the view registers with it.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import make_runner
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)


_run = make_runner("entity", _log)


def xref_summary(conn: duckdb.DuckDBPyConnection, supplier_norm: str) -> QueryResult:
    """The cross-register presence row for one supplier (by canonical ``supplier_norm``)."""
    return _run(conn, "SELECT * FROM v_supplier_entity_xref WHERE supplier_norm = ?", [supplier_norm])


def top_cross_register(conn: duckdb.DuckDBPyConnection, min_registers: int = 2, limit: int = 50) -> QueryResult:
    """Entities present on the most public registers (the 'most cross-referenced orgs' lens).
    ``min_registers`` counts registers BEYOND procurement (lobbying / corporate / charity / EPA)."""
    return _run(
        conn,
        "SELECT * FROM v_supplier_entity_xref WHERE cross_register_count >= ?"
        " ORDER BY cross_register_count DESC, awarded_value_safe_eur DESC NULLS LAST LIMIT ?",
        [min_registers, limit],
    )
