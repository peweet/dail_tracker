from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

from dail_tracker_core.queries.procurement.opportunities import (
    public_signal_ce_leads_contract,
    public_signal_work_package_contract,
)
from dail_tracker_core.queries.procurement.pre_tender import (
    pre_tender_lead_by_id,
    pre_tender_leads,
    pre_tender_work_packages,
)


def _connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute(
        """
        CREATE TABLE v_procurement_pre_tender_leads (
            lead_id VARCHAR, source_corpus VARCHAR, source_record_id VARCHAR,
            source_date DATE, date_precision VARCHAR, buyer_or_sponsor VARCHAR,
            reporting_area VARCHAR, area_basis VARCHAR, project_name VARCHAR,
            sector VARCHAR, likely_work_package VARCHAR, published_stage VARCHAR,
            normalized_stage VARCHAR, stage_display_order BIGINT, amount_text VARCHAR,
            amount_is_not_aggregable BOOLEAN, source_url VARCHAR,
            source_review_required BOOLEAN, current_status_verified BOOLEAN,
            tender_notice_status VARCHAR, report_as_of VARCHAR, classification_schema VARCHAR,
            school_roll_number VARCHAR, snapshot_freshness VARCHAR
        )
        """
    )
    rows = [
        (
            "lead-1",
            "semi_state_minutes",
            "record-1",
            "2026-03-31",
            "day",
            "Uisce Eireann",
            "Navan",
            "explicit",
            "Navan wastewater upgrade",
            "Water",
            "Civil works",
            "approved",
            "approved_to_procure",
            2,
            None,
            True,
            "https://example.ie/1",
            True,
            False,
            "not_checked_against_live_tenders",
            "2026-08-07",
            "pre-tender-lead/1",
            None,
            None,
        ),
        (
            "lead-2",
            "pq_attachment_project_table",
            "record-2",
            "2025-06-17",
            "day",
            "Department",
            "Cork",
            "explicit",
            "School extension",
            "Education",
            "Building works",
            "design",
            "detailed_design",
            5,
            "EUR 2m",
            True,
            "https://example.ie/2",
            True,
            False,
            "not_checked_against_live_tenders",
            "2026-08-07",
            "pre-tender-lead/1",
            "12345A",
            "recent_180_days",
        ),
    ]
    conn.executemany(
        "INSERT INTO v_procurement_pre_tender_leads VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.execute(
        """
        CREATE TABLE v_procurement_pre_tender_work_packages (
            package_row_id VARCHAR, lead_id VARCHAR, source_corpus VARCHAR,
            source_record_id VARCHAR, source_date DATE, reporting_area VARCHAR,
            project_name VARCHAR, package_code VARCHAR, package_label VARCHAR,
            package_group VARCHAR, evidence_phrase VARCHAR, matched_field VARCHAR,
            classification_basis VARCHAR, source_url VARCHAR,
            source_review_required BOOLEAN, current_status_verified BOOLEAN,
            amount_is_not_aggregable BOOLEAN, classification_schema VARCHAR
        )
        """
    )
    conn.execute(
        "INSERT INTO v_procurement_pre_tender_work_packages VALUES "
        "('lead-2:electrical_lighting_bems', 'lead-2', 'pq_school_project_table', 'record-2',"
        " '2025-06-17', 'Cork', 'School extension', 'electrical_lighting_bems',"
        " 'Electrical, lighting and BEMS', 'Building services', 'electrical works',"
        " 'evidence_text', 'source_literal_rule', 'https://example.ie/2', TRUE, FALSE, TRUE,"
        " 'pre-tender-work-package/1')"
    )
    return conn


def test_pre_tender_query_filters_without_changing_grain():
    conn = _connection()
    try:
        result = pre_tender_leads(conn, area="nav", sector="Water", stage="approved_to_procure")
        assert result.ok is True
        assert result.data["lead_id"].tolist() == ["lead-1"]
        assert result.data.iloc[0]["amount_is_not_aggregable"]
        assert not result.data.iloc[0]["current_status_verified"]
    finally:
        conn.close()


def test_pre_tender_detail_and_unavailable_state():
    conn = _connection()
    try:
        result = pre_tender_lead_by_id(conn, "lead-2")
        assert result.ok is True
        assert result.data.iloc[0]["source_url"] == "https://example.ie/2"
        assert result.data.iloc[0]["school_roll_number"] == "12345A"
        assert result.data.iloc[0]["snapshot_freshness"] == "recent_180_days"
    finally:
        conn.close()

    empty_conn = duckdb.connect()
    try:
        assert pre_tender_leads(empty_conn).ok is False
    finally:
        empty_conn.close()


def test_work_package_query_and_public_contract_preserve_package_grain():
    conn = _connection()
    try:
        result = pre_tender_work_packages(conn, package_code="electrical_lighting_bems")
        assert result.ok is True
        assert result.data["lead_id"].tolist() == ["lead-2"]
        assert result.data.iloc[0]["evidence_phrase"] == "electrical works"
        contract = public_signal_work_package_contract(conn)
    finally:
        conn.close()
    assert contract["status"] == "reviewed"
    assert contract["rows"][0]["package_group"] == "Building services"
    assert any("must not be counted as projects" in caveat for caveat in contract["caveats"])


def test_ce_contract_reports_review_queue_without_exposing_unreviewed_rows(tmp_path: Path):
    leads_path = tmp_path / "ce_leads.parquet"
    common = {
        "council": "Test County Council",
        "report_title": "Chief Executive Report",
        "report_month": "2026-06",
        "source_landing_url": "https://example.ie/reports",
        "source_url": "https://example.ie/report.pdf",
        "source_page": 3,
        "source_locator": "page 3",
        "lead_types": ["tender"],
        "amount_mentions": [],
        "evidence_band": "extracted",
        "reviewer_state": "NOT_REVIEWED",
        "relevance_status": "NOT_REVIEWED",
        "site_relationship": "NOT_REVIEWED",
        "reviewed_project_name": None,
        "reviewed_stage": None,
    }
    pl.DataFrame(
        [
            {
                **common,
                "lead_id": "queued",
                "quote": "Tender documents are being prepared.",
                "promotion_permitted": False,
            },
            {
                **common,
                "lead_id": "reviewed",
                "quote": "The works tender will issue in quarter four.",
                "promotion_permitted": True,
                "reviewer_state": "REVIEWED",
                "relevance_status": "REVIEWED_RELEVANT",
                "site_relationship": "RESOLVED",
                "reviewed_project_name": "Named works project",
                "reviewed_stage": "tender_preparation",
            },
        ]
    ).write_parquet(leads_path)

    conn = duckdb.connect()
    try:
        contract = public_signal_ce_leads_contract(conn, leads_path=leads_path)
    finally:
        conn.close()

    assert contract["candidate_count"] == 2
    assert contract["review_queue_count"] == 1
    assert contract["promoted_count"] == 1
    assert [row["lead_id"] for row in contract["rows"]] == ["reviewed"]
    assert contract["rows"][0]["project_name"] == "Named works project"
