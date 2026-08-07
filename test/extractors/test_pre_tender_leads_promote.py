from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from extractors.pre_tender_leads_promote import SOURCE_FILES, promote


def _row(lead_id: str, corpus: str) -> dict:
    return {
        "lead_id": lead_id,
        "source_corpus": corpus,
        "source_record_id": f"record-{lead_id}",
        "source_date": "2026-03-31",
        "date_precision": "day",
        "buyer_or_sponsor": "Public body",
        "reporting_area": "Navan",
        "area_basis": "explicit in source sentence",
        "project_name": f"Project {lead_id}",
        "sector": "Water and wastewater",
        "likely_work_package": "Civil, mechanical and process works",
        "published_stage": "approved to procure",
        "normalized_stage": "approved_to_procure",
        "stage_display_order": 2,
        "amount_text": None,
        "amount_is_not_aggregable": True,
        "evidence_text": "Full source quotation retained only in silver.",
        "source_url": "https:/data.oireachtas.ie/source.pdf",
        "source_review_required": True,
        "current_status_verified": False,
        "tender_notice_status": "not assessed in this pre-tender layer",
        "report_as_of": "2026-08-07",
        "classification_schema": "pre-tender-lead/1",
    }


def _write_inputs(root: Path) -> None:
    corpora = ("semi_state_minutes", "pq_attachment_project_table", "council_part8_decision")
    for index, (name, corpus) in enumerate(zip(SOURCE_FILES, corpora, strict=True), start=1):
        pl.DataFrame([_row(f"lead-{index}", corpus)]).write_parquet(root / name)


def test_promote_preserves_evidence_in_silver_and_condenses_gold(tmp_path: Path):
    _write_inputs(tmp_path)
    silver, gold, coverage = promote(tmp_path)

    assert silver.height == gold.height == 3
    assert "evidence_text" in silver.columns
    assert "evidence_text" not in gold.columns
    assert set(gold["tender_notice_status"]) == {"not_checked_against_live_tenders"}
    assert set(gold["source_url"]) == {"https://data.oireachtas.ie/source.pdf"}
    assert gold["current_status_verified"].to_list() == [False, False, False]
    assert coverage["area_specific_rows"] == 3


def test_promote_rejects_any_sum_safe_amount_claim(tmp_path: Path):
    _write_inputs(tmp_path)
    broken = pl.read_parquet(tmp_path / SOURCE_FILES[0]).with_columns(pl.lit(False).alias("amount_is_not_aggregable"))
    broken.write_parquet(tmp_path / SOURCE_FILES[0])

    with pytest.raises(ValueError, match="non-aggregable"):
        promote(tmp_path)


def test_promote_maps_school_observations_and_packages_onto_shared_lead_grain(
    tmp_path: Path,
) -> None:
    _write_inputs(tmp_path)
    opportunities = tmp_path / "school_sme_opportunities.parquet"
    packages = tmp_path / "school_sme_opportunity_packages.parquet"
    pl.DataFrame(
        [
            {
                "opportunity_id": "school-1",
                "reporting_area": "Offaly",
                "reporting_area_basis": "attachment_scope",
                "school_name": "SN Mhuire",
                "roll_number": "18115Q",
                "snapshot_date": "2026-04-30",
                "reported_stage": "Stage 3",
                "engagement_window": "pre_tender_watch",
                "published_scope": "Two SEN classrooms and refurbishment works",
                "source_attachment_url": "https://data.oireachtas.ie/school.xlsx",
                "source_review_required": True,
                "snapshot_freshness": "recent_180_days",
            }
        ]
    ).write_parquet(opportunities)
    pl.DataFrame(
        [
            {"opportunity_id": "school-1", "work_package": "sen_set_and_specialist_rooms"},
            {"opportunity_id": "school-1", "work_package": "refurbishment_and_reconfiguration"},
        ]
    ).write_parquet(packages)

    silver, gold, coverage = promote(
        tmp_path,
        school_opportunities_path=opportunities,
        school_packages_path=packages,
        report_as_of="2026-08-07",
    )

    school = gold.filter(pl.col("source_corpus") == "pq_school_project_table").row(0, named=True)
    assert silver.height == gold.height == 4
    assert school["lead_id"] == "school:school-1"
    assert school["project_name"] == "SN Mhuire"
    assert school["school_roll_number"] == "18115Q"
    assert school["normalized_stage"] == "tender_preparation"
    assert school["stage_display_order"] == 10
    assert school["snapshot_freshness"] == "recent_180_days"
    assert school["likely_work_package"] == ("Refurbishment and reconfiguration, SEN, SET and specialist rooms")
    assert school["amount_is_not_aggregable"]
    assert not school["current_status_verified"]
    assert coverage["school_rows"] == 1
    assert coverage["school_recent_rows"] == 1
    assert coverage["school_stale_rows"] == 0


def test_school_source_requires_all_three_promotion_arguments(tmp_path: Path) -> None:
    _write_inputs(tmp_path)
    with pytest.raises(ValueError, match="must be supplied together"):
        promote(tmp_path, school_opportunities_path=tmp_path / "school.parquet")
