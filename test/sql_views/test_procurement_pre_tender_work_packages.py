from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import polars as pl

from paths import PROJECT_ROOT


def test_work_package_view_preserves_lead_package_grain_and_evidence(tmp_path: Path):
    parquet_path = tmp_path / "pre_tender_work_packages.parquet"
    pl.DataFrame(
        [
            {
                "package_row_id": "lead-1:electrical_lighting_bems",
                "lead_id": "lead-1",
                "source_corpus": "pq_school_project_table",
                "source_record_id": "record-1",
                "source_date": "2026-05-28",
                "reporting_area": "Kildare",
                "project_name": "Named School",
                "package_code": "electrical_lighting_bems",
                "package_label": "Electrical, lighting and BEMS",
                "package_group": "Building services",
                "evidence_phrase": "electrical works",
                "matched_field": "evidence_text",
                "classification_basis": "source_literal_rule",
                "source_url": "https://example.ie/source",
                "source_review_required": True,
                "current_status_verified": False,
                "amount_is_not_aggregable": True,
                "classification_schema": "pre-tender-work-package/1",
            }
        ]
    ).write_parquet(parquet_path)
    sql = (PROJECT_ROOT / "sql_views" / "procurement" / "procurement_pre_tender_work_packages.sql").read_text(
        encoding="utf-8"
    )
    sql = sql.replace(
        "'data/gold/parquet/pre_tender_work_packages.parquet'",
        f"'{parquet_path.as_posix()}'",
    )
    conn = duckdb.connect()
    try:
        conn.execute(sql)
        row = conn.execute("SELECT * FROM v_procurement_pre_tender_work_packages").fetchone()
        columns = [item[0] for item in conn.description]
    finally:
        conn.close()
    record = dict(zip(columns, row, strict=True))
    assert record["package_row_id"] == "lead-1:electrical_lighting_bems"
    assert record["source_date"] == date(2026, 5, 28)
    assert record["evidence_phrase"] == "electrical works"
    assert record["amount_is_not_aggregable"] is True
    assert record["current_status_verified"] is False
