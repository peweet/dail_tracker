from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl

from paths import PROJECT_ROOT


def test_pre_tender_view_preserves_observation_grain_and_provenance(tmp_path: Path):
    parquet_path = tmp_path / "pre_tender_leads.parquet"
    pl.DataFrame(
        [
            {
                "lead_id": "lead-1",
                "source_corpus": "semi_state_minutes",
                "source_record_id": "record-1",
                "source_date": "2026-03-31",
                "date_precision": "day",
                "buyer_or_sponsor": "Uisce Eireann",
                "reporting_area": "Navan",
                "area_basis": "explicit",
                "project_name": "Navan wastewater upgrade",
                "sector": "Water",
                "likely_work_package": "Civil works",
                "published_stage": "approved",
                "normalized_stage": "approved_to_procure",
                "stage_display_order": 2,
                "amount_text": None,
                "amount_is_not_aggregable": True,
                "source_url": "https://example.ie/source",
                "source_review_required": True,
                "current_status_verified": False,
                "tender_notice_status": "not_checked_against_live_tenders",
                "report_as_of": "2026-08-07",
                "classification_schema": "pre-tender-lead/1",
                "school_roll_number": None,
                "snapshot_freshness": None,
                "stage_1_start_date": None,
                "stage_2a_start_date": None,
                "stage_2b_start_date": None,
                "stage_3_start_date": None,
                "stage_4_start_date": None,
                "start_on_site_date": None,
                "start_on_site_note": None,
            },
            {
                "lead_id": "school-1",
                "source_corpus": "pq_school_project_table",
                "source_record_id": "record-2",
                "source_date": "2026-04-30",
                "date_precision": "day",
                "buyer_or_sponsor": "Department of Education and Youth programme",
                "reporting_area": "Offaly",
                "area_basis": "attachment_scope",
                "project_name": "Colaiste Choilm",
                "sector": "Schools and education",
                "likely_work_package": "General classrooms",
                "published_stage": "Stage 4",
                "normalized_stage": "procurement_or_delivery_check",
                "stage_display_order": 20,
                "amount_text": None,
                "amount_is_not_aggregable": True,
                "source_url": "https://example.ie/school.xlsx",
                "source_review_required": True,
                "current_status_verified": False,
                "tender_notice_status": "not_checked_against_live_tenders",
                "report_as_of": "2026-08-07",
                "classification_schema": "school-pre-tender-lead/1",
                "school_roll_number": "65610S",
                "snapshot_freshness": "recent_180_days",
                "stage_1_start_date": "2020-04-15",
                "stage_2a_start_date": "2022-04-11",
                "stage_2b_start_date": "2022-11-28",
                "stage_3_start_date": "2024-08-15",
                "stage_4_start_date": "2025-05-29",
                "start_on_site_date": "2025-05-29",
                "start_on_site_note": None,
            },
        ]
    ).write_parquet(parquet_path)
    sql = (PROJECT_ROOT / "sql_views" / "procurement" / "procurement_pre_tender_leads.sql").read_text(encoding="utf-8")
    sql = sql.replace(
        "'data/gold/parquet/pre_tender_leads.parquet'",
        f"'{parquet_path.as_posix()}'",
    )
    conn = duckdb.connect()
    try:
        conn.execute(sql)
        frame = conn.execute("SELECT * FROM v_procurement_pre_tender_leads ORDER BY lead_id").fetchdf()
    finally:
        conn.close()

    row = frame[frame["lead_id"] == "lead-1"].iloc[0]
    assert row["source_url"] == "https://example.ie/source"
    assert row["source_date"].date() == date(2026, 3, 31)
    assert bool(row["amount_is_not_aggregable"])
    assert not bool(row["current_status_verified"])
    assert pd.isna(row["school_roll_number"])
    # A corpus that publishes no approval dates must stay null, not blank.
    assert pd.isna(row["start_on_site_date"])

    # The published approval dates have to survive the view, or nothing downstream
    # can tell a live building site from a lead.
    school = frame[frame["lead_id"] == "school-1"].iloc[0]
    assert school["stage_1_start_date"] == "2020-04-15"
    assert school["stage_4_start_date"] == "2025-05-29"
    assert school["start_on_site_date"] == "2025-05-29"
