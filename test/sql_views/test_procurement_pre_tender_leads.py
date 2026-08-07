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
            }
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
        row = conn.execute("SELECT * FROM v_procurement_pre_tender_leads").fetchdf().iloc[0]
    finally:
        conn.close()

    assert row["lead_id"] == "lead-1"
    assert row["source_url"] == "https://example.ie/source"
    assert row["source_date"].date() == date(2026, 3, 31)
    assert bool(row["amount_is_not_aggregable"])
    assert not bool(row["current_status_verified"])
    assert pd.isna(row["school_roll_number"])
