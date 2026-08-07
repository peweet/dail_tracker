from __future__ import annotations

from pathlib import Path

import polars as pl

from extractors.pre_tender_work_packages import classify_work_packages


def _pre_tender(path: Path) -> None:
    pl.DataFrame(
        [
            {
                "lead_id": "school-1",
                "source_corpus": "pq_school_project_table",
                "source_record_id": "record-1",
                "source_date": "2026-05-28",
                "reporting_area": "Kildare",
                "project_name": "Named School",
                "evidence_text": "Approved scheme: electrical works, toilet facilities and roof works.",
                "likely_work_package": "Electrical and lighting, Toilets and sanitary works, Roofing",
                "source_url": "https://example.ie/school",
                "source_review_required": True,
            },
            {
                "lead_id": "water-1",
                "source_corpus": "semi_state_minutes",
                "source_record_id": "record-2",
                "source_date": "2026-03-31",
                "reporting_area": "Navan",
                "project_name": "Wastewater Network Upgrade",
                "evidence_text": "Capital approval for wastewater network and civil works.",
                "likely_work_package": "Civil, mechanical and process works",
                "source_url": "https://example.ie/water",
                "source_review_required": True,
            },
            {
                "lead_id": "digital-1",
                "source_corpus": "semi_state_minutes",
                "source_record_id": "record-3",
                "source_date": "2026-03-31",
                "reporting_area": "Area not explicit",
                "project_name": "Digital configuration programme",
                "evidence_text": "Approval for digital configuration and software services.",
                "likely_work_package": "ICT and systems integration",
                "source_url": "https://example.ie/digital",
                "source_review_required": True,
            },
        ]
    ).write_parquet(path)


def _ce_leads(path: Path) -> None:
    pl.DataFrame(
        [
            {
                "lead_id": "queued",
                "council": "Test Council",
                "report_title": "CE report",
                "reviewed_project_name": "Queued public realm works",
                "report_month": "2026-06",
                "quote": "Tender documents for public realm landscaping are being prepared.",
                "lead_types": ["tender"],
                "source_url": "https://example.ie/queued.pdf",
                "source_landing_url": "https://example.ie/reports",
                "promotion_permitted": False,
            },
            {
                "lead_id": "reviewed",
                "council": "Test Council",
                "report_title": "CE report",
                "reviewed_project_name": "Reviewed CCTV works",
                "report_month": "2026-06",
                "quote": "The reviewed CCTV and access control package will proceed to tender.",
                "lead_types": ["tender"],
                "source_url": "https://example.ie/reviewed.pdf",
                "source_landing_url": "https://example.ie/reports",
                "promotion_permitted": True,
            },
        ]
    ).write_parquet(path)


def test_classifier_emits_many_to_many_rows_and_blocks_network_as_it(tmp_path: Path):
    pre_path = tmp_path / "pre.parquet"
    ce_path = tmp_path / "ce.parquet"
    _pre_tender(pre_path)
    _ce_leads(ce_path)

    silver, gold, coverage = classify_work_packages(pre_path, ce_leads_path=ce_path)

    school_codes = set(gold.filter(pl.col("lead_id") == "school-1")["package_code"])
    assert school_codes == {"electrical_lighting_bems", "plumbing_sanitary", "roof_windows_doors"}
    water_codes = set(gold.filter(pl.col("lead_id") == "water-1")["package_code"])
    assert "water_wastewater_civil" in water_codes
    assert "it_digital_systems" not in water_codes
    assert "it_digital_systems" in set(gold.filter(pl.col("lead_id") == "digital-1")["package_code"])
    assert set(silver.filter(pl.col("lead_id") == "ce:queued")["package_code"]) == {
        "landscape_external_site",
        "roads_public_realm",
    }
    assert gold.filter(pl.col("lead_id") == "ce:queued").is_empty()
    assert "security_cctv_access" in set(gold.filter(pl.col("lead_id") == "ce:reviewed")["package_code"])
    assert coverage["ce_candidate_rows"] == 3
    assert coverage["ce_promoted_rows"] == 1
    assert coverage["ce_candidate_leads"] == 2
    assert coverage["ce_promoted_leads"] == 1


def test_classifier_retains_exact_phrase_and_non_money_boundary(tmp_path: Path):
    pre_path = tmp_path / "pre.parquet"
    _pre_tender(pre_path)

    _silver, gold, _coverage = classify_work_packages(pre_path, ce_leads_path=None)
    row = gold.filter((pl.col("lead_id") == "school-1") & (pl.col("package_code") == "electrical_lighting_bems")).row(
        0, named=True
    )

    assert row["evidence_phrase"].lower() == "electrical works"
    assert row["matched_field"] == "evidence_text"
    assert row["classification_basis"] == "source_literal_rule"
    assert row["amount_is_not_aggregable"] is True
    assert row["current_status_verified"] is False
    assert "evidence_text" not in gold.columns
