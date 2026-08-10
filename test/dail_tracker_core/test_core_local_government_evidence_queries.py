"""Integration contracts for the consolidated council evidence record."""

from __future__ import annotations

from pathlib import Path

import pytest

from dail_tracker_core.connections import constituency_conn
from dail_tracker_core.queries import local_government as q

ROOT = Path(__file__).resolve().parents[2]
REQUIRED = (
    ROOT / "data/silver/parquet/la_afs_capital_divisions.parquet",
    ROOT / "data/gold/parquet/council_minutes_corpus.parquet",
    ROOT / "data/gold/parquet/council_ce_reports_corpus.parquet",
    ROOT / "data/gold/parquet/council_ce_report_leads.parquet",
)

pytestmark = pytest.mark.skipif(not all(path.exists() for path in REQUIRED), reason="council evidence data absent")


@pytest.fixture(scope="module")
def conn():
    connection = constituency_conn()
    yield connection
    connection.close()


def test_recovered_kildare_capital_history_is_reconciled(conn):
    result = q.capital_history(conn, "Kildare")
    assert result.ok and not result.data.empty
    latest = result.data.iloc[0]
    assert int(latest["year"]) == 2025
    assert bool(latest["reconciled"])
    assert float(latest["capital_expenditure_eur"]) == pytest.approx(277_417_057)
    assert latest["source_url"]


def test_capital_divisions_preserve_document_provenance(conn):
    result = q.capital_divisions(conn, "Kerry", 2024)
    assert result.ok and len(result.data) == 8
    assert result.data["source_url"].str.startswith("http").all()
    assert result.data["reconciled"].all()


def test_minutes_queries_are_document_grain(conn):
    coverage = q.minutes_coverage(conn, "Clare")
    documents = q.minutes_documents(conn, "Clare")
    assert coverage.ok and len(coverage.data) == 1
    assert documents.ok and 0 < len(documents.data) <= 12
    assert documents.data["document_id"].is_unique
    assert int(coverage.data.iloc[0]["documents"]) >= len(documents.data)


def test_ce_report_dlr_name_is_canonicalised_for_council_join(conn):
    coverage = q.ce_report_coverage(conn, "Dun Laoghaire-Rathdown")
    documents = q.ce_report_documents(conn, "Dun Laoghaire-Rathdown")
    assert coverage.ok and len(coverage.data) == 1
    assert documents.ok and len(documents.data) == 4


def test_ce_report_public_signal_view_never_returns_unreviewed_rows(conn):
    signals = q.ce_report_signals(conn, "Fingal")
    assert signals.ok
    if not signals.data.empty:
        assert signals.data["reviewed_project_name"].str.len().gt(0).all()
        assert signals.data["reviewed_stage"].str.len().gt(0).all()
