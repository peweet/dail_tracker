"""SQL contracts for the consolidated council evidence lanes."""

from __future__ import annotations

from pathlib import Path

import pytest

from dail_tracker_core.connections import constituency_conn

ROOT = Path(__file__).resolve().parents[2]
REQUIRED = (
    ROOT / "data/silver/parquet/la_afs_capital_divisions.parquet",
    ROOT / "data/gold/parquet/council_minutes_corpus.parquet",
    ROOT / "data/gold/parquet/council_ce_reports_corpus.parquet",
    ROOT / "data/gold/parquet/council_ce_report_leads.parquet",
)

pytestmark = [
    pytest.mark.sql,
    pytest.mark.skipif(not all(path.exists() for path in REQUIRED), reason="council evidence data absent"),
]


@pytest.fixture(scope="module")
def conn():
    connection = constituency_conn()
    yield connection
    connection.close()


def test_capital_year_view_keeps_reconciliation_and_provenance(conn):
    councils, unreconciled, unsourced = conn.execute(
        "SELECT count(DISTINCT council), count(*) FILTER (WHERE NOT reconciled), "
        "count(*) FILTER (WHERE source_url IS NULL OR source_url = '') "
        "FROM v_procurement_afs_capital_by_year"
    ).fetchone()
    assert councils == 30
    assert unreconciled == 0
    assert unsourced == 0


def test_minutes_document_view_has_one_row_per_document(conn):
    rows, documents = conn.execute(
        "SELECT count(*), count(DISTINCT document_id) FROM v_la_council_minutes_documents"
    ).fetchone()
    assert rows == documents
    assert rows >= 828


def test_ce_report_coverage_is_document_grain(conn):
    documents, covered = conn.execute(
        "SELECT (SELECT count(*) FROM v_la_ce_report_documents), (SELECT sum(documents) FROM v_la_ce_report_coverage)"
    ).fetchone()
    assert documents == covered
    assert documents >= 232


def test_ce_signal_view_contains_only_completed_review_fields(conn):
    incomplete = conn.execute(
        "SELECT count(*) FROM v_la_ce_report_signals "
        "WHERE reviewed_project_name IS NULL OR reviewed_project_name = '' "
        "OR reviewed_stage IS NULL OR reviewed_stage = ''"
    ).fetchone()[0]
    assert incomplete == 0
