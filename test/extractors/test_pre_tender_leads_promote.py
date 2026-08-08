from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from extractors.pre_tender_leads_promote import (
    FRESHNESS_AGEING,
    FRESHNESS_BANDS,
    FRESHNESS_RECENT,
    FRESHNESS_STALE,
    SOURCE_FILES,
    promote,
)


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


# ── age banding ──────────────────────────────────────────────────────────────────────────────
# Only the school lane arrived pre-banded, so 62 of 210 gold rows once carried no age signal at
# all: they rendered no freshness pill, and the page's "reported over a year ago" count read 55
# when 84 rows were genuinely that old. The banding has to cover every dated row, whatever corpus
# it came from — these tests are what stop that reopening silently.
def _dated(root: Path, source_date: str) -> None:
    _write_inputs(root)
    frame = pl.read_parquet(root / SOURCE_FILES[0]).with_columns(pl.lit(source_date).alias("source_date"))
    frame.write_parquet(root / SOURCE_FILES[0])


@pytest.mark.parametrize(
    ("source_date", "expected"),
    [
        ("2026-08-01", FRESHNESS_RECENT),  # 6 days before report_as_of
        ("2026-02-08", FRESHNESS_RECENT),  # exactly 180 days -> the last day of the recent band
        ("2026-02-07", FRESHNESS_AGEING),  # 181 days -> the first day of the ageing band
        ("2025-08-07", FRESHNESS_AGEING),  # exactly 365 days -> still ageing, not yet stale
        ("2025-08-06", FRESHNESS_STALE),  # 366 days -> over the year
        ("2024-07-23", FRESHNESS_STALE),  # the real corpus's oldest observation
    ],
)
def test_promote_bands_every_dated_observation_whatever_the_corpus(
    tmp_path: Path, source_date: str, expected: str
) -> None:
    _dated(tmp_path, source_date)
    _, gold, _ = promote(tmp_path)

    banded = gold.filter(pl.col("lead_id") == "lead-1")["snapshot_freshness"].to_list()
    assert banded == [expected], f"{source_date} against report_as_of 2026-08-07 should band {expected}"
    assert gold.filter(pl.col("snapshot_freshness").is_null()).height == 0


def _school_band(tmp_path: Path, band: str) -> dict:
    """School inputs whose upstream band is `band` while the snapshot date implies `recent`.

    The school lane is the only one that carries its own band — the base corpora do not have the
    column and are filled from their date. Deliberately disagreeing with the date is what makes
    an overwrite visible: if the fill overwrote, agreement would hide it.
    """
    opportunities, packages = _school_inputs(tmp_path)
    pl.read_parquet(opportunities).with_columns(pl.lit(band).alias("snapshot_freshness")).write_parquet(opportunities)
    return {
        "school_opportunities_path": opportunities,
        "school_packages_path": packages,
        "school_snapshots_path": _snapshot_input(tmp_path, start_on_site=None),
        "report_as_of": "2026-08-07",
    }


def test_promote_leaves_an_upstream_band_alone(tmp_path: Path) -> None:
    """A corpus that bands its own rows keeps its own provenance — the fill coalesces, never
    overwrites."""
    _write_inputs(tmp_path)
    _, gold, _ = promote(tmp_path, **_school_band(tmp_path, FRESHNESS_STALE))

    kept = gold.filter(pl.col("lead_id") == "school:school-1")["snapshot_freshness"].to_list()
    assert kept == [FRESHNESS_STALE], "a band supplied by the source corpus must survive promotion"
    # The base-corpus rows, which carry no band of their own, are still filled from their date.
    assert gold.filter(pl.col("snapshot_freshness").is_null()).height == 0


def test_promote_rejects_an_unknown_band(tmp_path: Path) -> None:
    """A new upstream vocabulary must fail loudly. It would otherwise reach the page, match no
    entry in its band-to-label map, and render no pill — the silent failure this lane already had."""
    _write_inputs(tmp_path)
    with pytest.raises(ValueError, match="unknown snapshot_freshness band"):
        promote(tmp_path, **_school_band(tmp_path, "fresh_ish"))


def test_promote_reports_corpus_wide_freshness_counts(tmp_path: Path) -> None:
    """Coverage must carry the whole-corpus banding, not just the school lane's. The page's
    staleness sentence is built from these counts, so a school-only count under-reports it."""
    _dated(tmp_path, "2024-01-01")
    _, gold, coverage = promote(tmp_path)

    assert set(coverage["freshness_rows"]) == set(FRESHNESS_BANDS)
    assert sum(coverage["freshness_rows"].values()) + coverage["undated_rows"] == gold.height
    assert coverage["freshness_rows"][FRESHNESS_STALE] == 1


def test_promote_allows_an_undated_observation_to_stay_unbanded(tmp_path: Path) -> None:
    """No date means no honest band. It must stay null and be COUNTED, so the page can say how
    many rows its age statement does not cover instead of implying it covers all of them."""
    _write_inputs(tmp_path)
    frame = pl.read_parquet(tmp_path / SOURCE_FILES[0]).with_columns(pl.lit(None, dtype=pl.String).alias("source_date"))
    frame.write_parquet(tmp_path / SOURCE_FILES[0])
    _, gold, coverage = promote(tmp_path)

    assert coverage["undated_rows"] == 1
    assert gold.filter(pl.col("snapshot_freshness").is_null()).height == 1


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


def _school_inputs(tmp_path: Path, *, row_index: int = 4) -> tuple[Path, Path]:
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
                "reported_stage": "Stage 4",
                "engagement_window": "procurement_or_delivery_check",
                "published_scope": "Two SEN classrooms and refurbishment works",
                "source_attachment_url": "https://data.oireachtas.ie/school.xlsx",
                "source_sheet_or_table": "Current Projects",
                "source_row_index": row_index,
                "source_review_required": True,
                "snapshot_freshness": "recent_180_days",
            }
        ]
    ).write_parquet(opportunities)
    pl.DataFrame([{"opportunity_id": "school-1", "work_package": "sen_set_and_specialist_rooms"}]).write_parquet(
        packages
    )
    return opportunities, packages


def _snapshot_input(tmp_path: Path, *, start_on_site: str | None, row_index: int = 4) -> Path:
    snapshots = tmp_path / "school_project_snapshots.parquet"
    pl.DataFrame(
        [
            {
                "attachment_url": "https://data.oireachtas.ie/school.xlsx",
                "sheet_or_table": "Current Projects",
                "source_row_index": row_index,
                "stage_1_start_date": "2020-04-15 00:00:00",
                "stage_2a_start_date": "2022-04-11 00:00:00",
                "stage_2b_start_date": None,
                "stage_3_start_date": None,
                "stage_4_start_date": "2025-05-29 00:00:00",
                "start_on_site_date": start_on_site,
            }
        ]
    ).write_parquet(snapshots)
    return snapshots


def test_promote_carries_the_published_stage_dates_onto_school_leads(tmp_path: Path) -> None:
    _write_inputs(tmp_path)
    opportunities, packages = _school_inputs(tmp_path)
    snapshots = _snapshot_input(tmp_path, start_on_site="2025-05-29 00:00:00")

    _, gold, coverage = promote(
        tmp_path,
        school_opportunities_path=opportunities,
        school_packages_path=packages,
        school_snapshots_path=snapshots,
        report_as_of="2026-08-07",
    )

    school = gold.filter(pl.col("source_corpus") == "pq_school_project_table").row(0, named=True)
    assert school["stage_1_start_date"] == "2020-04-15"
    assert school["stage_2a_start_date"] == "2022-04-11"
    assert school["stage_2b_start_date"] is None
    assert school["stage_4_start_date"] == "2025-05-29"
    assert school["start_on_site_date"] == "2025-05-29"
    assert school["start_on_site_note"] is None
    assert coverage["school_stage_date_rows"]["start_on_site_date"] == 1
    assert coverage["school_start_on_site_unparsed_rows"] == 0
    # The non-school corpora carry no stage dates and must stay null, not blank.
    assert gold.filter(pl.col("source_corpus") != "pq_school_project_table")["stage_1_start_date"].null_count() == 3


def test_promote_keeps_non_date_on_site_wording_instead_of_dropping_it(tmp_path: Path) -> None:
    _write_inputs(tmp_path)
    opportunities, packages = _school_inputs(tmp_path)
    snapshots = _snapshot_input(tmp_path, start_on_site="TBC")

    _, gold, coverage = promote(
        tmp_path,
        school_opportunities_path=opportunities,
        school_packages_path=packages,
        school_snapshots_path=snapshots,
        report_as_of="2026-08-07",
    )

    school = gold.filter(pl.col("source_corpus") == "pq_school_project_table").row(0, named=True)
    assert school["start_on_site_date"] is None
    assert school["start_on_site_note"] == "TBC"
    assert coverage["school_start_on_site_unparsed_rows"] == 1


def test_promote_rejects_a_duplicated_stage_date_join_key(tmp_path: Path) -> None:
    _write_inputs(tmp_path)
    opportunities, packages = _school_inputs(tmp_path)
    snapshots = _snapshot_input(tmp_path, start_on_site="2025-05-29 00:00:00")
    doubled = pl.concat([pl.read_parquet(snapshots), pl.read_parquet(snapshots)])
    doubled.write_parquet(snapshots)

    with pytest.raises(ValueError, match="join key must be unique"):
        promote(
            tmp_path,
            school_opportunities_path=opportunities,
            school_packages_path=packages,
            school_snapshots_path=snapshots,
            report_as_of="2026-08-07",
        )


def test_school_source_requires_all_three_promotion_arguments(tmp_path: Path) -> None:
    _write_inputs(tmp_path)
    with pytest.raises(ValueError, match="must be supplied together"):
        promote(tmp_path, school_opportunities_path=tmp_path / "school.parquet")
