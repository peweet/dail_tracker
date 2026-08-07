"""Promote classified pre-tender observations into silver and gold contracts.

The upstream classifiers emit one parquet per source corpus. This stage does
not reclassify source text: it validates those decisions, preserves the full
evidence in silver, and emits a compact gold table for query/API consumers.

Pre-tender observations are a separate non-money grain. Amount text is source
wording only and is never parsed, normalised, or made sum-safe here.
"""

from __future__ import annotations

# isort: off
# Apply native thread caps before Polars loads. Ordering is the contract.
import services.runtime_env  # noqa: F401  # native thread caps before Polars
# isort: on

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from paths import PROJECT_ROOT
from services.coverage_io import save_coverage
from services.parquet_io import save_parquet

SOURCE_FILES = (
    "semistate_pre_tender_signals.parquet",
    "pq_pre_tender_signals.parquet",
    "council_part8_pre_tender_signals.parquet",
)

SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "parquet" / "pre_tender_lead_evidence.parquet"
GOLD_PATH = PROJECT_ROOT / "data" / "gold" / "parquet" / "pre_tender_leads.parquet"
COVERAGE_PATH = PROJECT_ROOT / "data" / "_meta" / "pre_tender_leads_coverage.json"

REQUIRED_COLUMNS = {
    "lead_id",
    "source_corpus",
    "source_record_id",
    "source_date",
    "date_precision",
    "buyer_or_sponsor",
    "reporting_area",
    "area_basis",
    "project_name",
    "sector",
    "likely_work_package",
    "published_stage",
    "normalized_stage",
    "stage_display_order",
    "amount_text",
    "amount_is_not_aggregable",
    "evidence_text",
    "source_url",
    "source_review_required",
    "current_status_verified",
    "tender_notice_status",
    "report_as_of",
    "classification_schema",
}

GOLD_COLUMNS = (
    "lead_id",
    "source_corpus",
    "source_record_id",
    "source_date",
    "date_precision",
    "buyer_or_sponsor",
    "reporting_area",
    "area_basis",
    "project_name",
    "sector",
    "likely_work_package",
    "published_stage",
    "normalized_stage",
    "stage_display_order",
    "amount_text",
    "amount_is_not_aggregable",
    "source_url",
    "source_review_required",
    "current_status_verified",
    "tender_notice_status",
    "report_as_of",
    "classification_schema",
    "school_roll_number",
    "snapshot_freshness",
)

SCHOOL_REQUIRED_COLUMNS = {
    "opportunity_id",
    "reporting_area",
    "reporting_area_basis",
    "school_name",
    "roll_number",
    "snapshot_date",
    "reported_stage",
    "engagement_window",
    "published_scope",
    "source_attachment_url",
    "source_review_required",
    "snapshot_freshness",
}

SCHOOL_PACKAGE_REQUIRED_COLUMNS = {"opportunity_id", "work_package"}

SCHOOL_STAGE_MAP = {
    "pre_tender_watch": ("tender_preparation", 10),
    "procurement_or_delivery_check": ("procurement_or_delivery_check", 20),
    "developed_design_watch": ("detailed_design", 30),
    "approved_works_pipeline": ("approved_works", 40),
    "early_design_pipeline": ("early_design", 50),
    "active_delivery_subcontracting_watch": ("active_delivery", 60),
}

WORK_PACKAGE_LABELS = {
    "sen_set_and_specialist_rooms": "SEN, SET and specialist rooms",
    "refurbishment_and_reconfiguration": "Refurbishment and reconfiguration",
    "energy_and_climate": "Energy and climate works",
    "new_school_or_extension": "New school or extension",
    "general_classrooms": "General classrooms",
    "roofing": "Roofing",
    "windows_and_doors": "Windows and doors",
    "modular_and_prefab": "Modular and prefab accommodation",
    "electrical_and_lighting": "Electrical and lighting",
    "toilets_and_sanitary": "Toilets and sanitary works",
    "staff_and_ancillary_spaces": "Staff and ancillary spaces",
    "arts_music_and_multimedia": "Arts, music and multimedia rooms",
    "science_labs_and_gas": "Science laboratories and gas",
    "technology_and_workshops": "Technology rooms and workshops",
    "pe_and_sports": "PE and sports facilities",
    "external_and_site_works": "External and site works",
    "heating_and_mechanical": "Heating and mechanical works",
    "fire_safety": "Fire safety works",
    "scope_not_detailed": "Scope not detailed",
}


def _clean_text(column: str) -> pl.Expr:
    return (
        pl.col(column)
        .cast(pl.String, strict=False)
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .replace("", None)
        .alias(column)
    )


def _clean_url() -> pl.Expr:
    return (
        _clean_text("source_url")
        .str.replace(r"^https:/([^/])", r"https://$1")
        .str.replace(r"^http:/([^/])", r"http://$1")
        .alias("source_url")
    )


def _school_pre_tender_rows(
    opportunities_path: Path,
    packages_path: Path,
    *,
    report_as_of: str,
) -> pl.DataFrame:
    """Map classified school-project observations onto the shared lead grain."""
    try:
        date.fromisoformat(report_as_of)
    except ValueError as exc:
        raise ValueError("report_as_of must be an ISO date") from exc

    opportunities = pl.read_parquet(opportunities_path)
    packages = pl.read_parquet(packages_path)
    missing = SCHOOL_REQUIRED_COLUMNS.difference(opportunities.columns)
    if missing:
        raise ValueError(f"{opportunities_path.name} is missing required columns: {sorted(missing)}")
    missing_packages = SCHOOL_PACKAGE_REQUIRED_COLUMNS.difference(packages.columns)
    if missing_packages:
        raise ValueError(f"{packages_path.name} is missing required columns: {sorted(missing_packages)}")
    if opportunities.select(pl.col("opportunity_id").is_duplicated().any()).item():
        raise ValueError("school opportunity_id must be unique at the observation grain")
    if opportunities.select(pl.col("school_name").is_null().any()).item():
        raise ValueError("school_name is required for every school observation")

    package_summary = (
        packages.select(
            "opportunity_id",
            pl.col("work_package")
            .cast(pl.String, strict=False)
            .replace_strict(WORK_PACKAGE_LABELS, default=pl.col("work_package"))
            .str.replace_all("_", " ")
            .alias("work_package_label"),
        )
        .filter(pl.col("work_package_label").is_not_null())
        .group_by("opportunity_id")
        .agg(pl.col("work_package_label").unique().sort().str.join(", ").alias("package_summary"))
    )
    stage_names = {key: value[0] for key, value in SCHOOL_STAGE_MAP.items()}
    stage_order = {key: value[1] for key, value in SCHOOL_STAGE_MAP.items()}
    rows = opportunities.join(package_summary, on="opportunity_id", how="left").with_columns(
        pl.concat_str([pl.lit("school:"), pl.col("opportunity_id")]).alias("lead_id"),
        pl.lit("pq_school_project_table").alias("source_corpus"),
        pl.col("opportunity_id").alias("source_record_id"),
        pl.col("snapshot_date").alias("source_date"),
        pl.lit("day").alias("date_precision"),
        pl.lit("Department of Education and Youth programme").alias("buyer_or_sponsor"),
        pl.col("reporting_area_basis").alias("area_basis"),
        pl.col("school_name").alias("project_name"),
        pl.lit("Schools and education").alias("sector"),
        pl.coalesce("package_summary", "published_scope").alias("likely_work_package"),
        pl.col("reported_stage").alias("published_stage"),
        pl.col("engagement_window")
        .replace_strict(stage_names, default=pl.lit("stage_not_stated"))
        .alias("normalized_stage"),
        pl.col("engagement_window")
        .replace_strict(stage_order, default=pl.lit(90))
        .cast(pl.Int64)
        .alias("stage_display_order"),
        pl.lit(None, dtype=pl.String).alias("amount_text"),
        pl.lit(True).alias("amount_is_not_aggregable"),
        pl.col("published_scope").alias("evidence_text"),
        pl.col("source_attachment_url").alias("source_url"),
        pl.lit(False).alias("current_status_verified"),
        pl.lit("not_checked_against_live_tenders").alias("tender_notice_status"),
        pl.lit(report_as_of).alias("report_as_of"),
        pl.lit("school-pre-tender-lead/1").alias("classification_schema"),
        pl.col("roll_number").alias("school_roll_number"),
    )
    return rows.select(sorted(REQUIRED_COLUMNS | {"school_roll_number", "snapshot_freshness"}))


def promote(
    input_dir: Path,
    *,
    school_opportunities_path: Path | None = None,
    school_packages_path: Path | None = None,
    report_as_of: str | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame, dict]:
    """Validate source-specific classified parquets and return silver, gold, coverage."""
    paths = [input_dir / name for name in SOURCE_FILES]
    missing = [str(path) for path in paths if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"missing classified input parquet(s): {', '.join(missing)}")

    frames: list[pl.DataFrame] = []
    source_rows: dict[str, int] = {}
    for path in paths:
        frame = pl.read_parquet(path)
        absent = REQUIRED_COLUMNS.difference(frame.columns)
        if absent:
            raise ValueError(f"{path.name} is missing required columns: {sorted(absent)}")
        frames.append(frame.select(sorted(REQUIRED_COLUMNS)))
        source_rows[path.name] = frame.height

    school_args = (school_opportunities_path, school_packages_path, report_as_of)
    if any(value is not None for value in school_args):
        if not all(value is not None for value in school_args):
            raise ValueError(
                "school_opportunities_path, school_packages_path and report_as_of must be supplied together"
            )
        assert school_opportunities_path is not None
        assert school_packages_path is not None
        assert report_as_of is not None
        school_rows = _school_pre_tender_rows(
            school_opportunities_path,
            school_packages_path,
            report_as_of=report_as_of,
        )
        frames.append(school_rows)
        source_rows[school_opportunities_path.name] = school_rows.height

    silver = pl.concat(frames, how="diagonal_relaxed")
    for column in ("school_roll_number", "snapshot_freshness"):
        if column not in silver.columns:
            silver = silver.with_columns(pl.lit(None, dtype=pl.String).alias(column))
    text_columns = sorted(
        REQUIRED_COLUMNS.difference(
            {
                "amount_is_not_aggregable",
                "current_status_verified",
                "source_review_required",
                "stage_display_order",
                "source_url",
            }
        )
    )
    silver = silver.with_columns(
        *[_clean_text(column) for column in [*text_columns, "school_roll_number", "snapshot_freshness"]],
        _clean_url(),
        pl.col("stage_display_order").cast(pl.Int64, strict=False),
        pl.col("amount_is_not_aggregable").cast(pl.Boolean, strict=False),
        pl.col("source_review_required").cast(pl.Boolean, strict=False),
        pl.col("current_status_verified").cast(pl.Boolean, strict=False),
    )
    silver = silver.with_columns(
        pl.col("tender_notice_status")
        .replace("not assessed in this pre-tender layer", "not_checked_against_live_tenders")
        .alias("tender_notice_status")
    )

    if silver.select(pl.col("lead_id").is_null().any()).item():
        raise ValueError("lead_id is required for every pre-tender observation")
    if silver.select(pl.col("lead_id").is_duplicated().any()).item():
        raise ValueError("lead_id must be unique at the observation grain")
    if silver.select((~pl.col("source_url").str.starts_with("http")).any()).item():
        raise ValueError("every pre-tender observation must retain an HTTP(S) source URL")
    if silver.select((~pl.col("amount_is_not_aggregable")).any()).item():
        raise ValueError("pre-tender amount text must remain explicitly non-aggregable")
    if silver.select(pl.col("current_status_verified").any()).item():
        raise ValueError("classified observations cannot claim current tender status")
    if silver.select((pl.col("tender_notice_status") != "not_checked_against_live_tenders").any()).item():
        raise ValueError("tender notice status must remain explicitly unchecked")

    # Gold intentionally excludes evidence_text. Full quotations and classifier
    # rationale remain in silver and do not cross the product API by default.
    gold = silver.select(GOLD_COLUMNS).sort(
        by=["stage_display_order", "source_date", "project_name"],
        descending=[False, True, False],
        nulls_last=True,
    )
    if gold.height != silver.height:
        raise ValueError("gold promotion changed the observation row count")

    area_specific = gold.filter(~pl.col("reporting_area").str.starts_with("Area not explicit")).height
    coverage = {
        "schema": "pre-tender-leads-coverage/1",
        "grain": "one dated source observation about one named project or procurement package",
        "input_directory": str(input_dir),
        "source_rows": source_rows,
        "silver_rows": silver.height,
        "gold_rows": gold.height,
        "area_specific_rows": area_specific,
        "current_status_verified_rows": 0,
        "school_rows": gold.filter(pl.col("source_corpus") == "pq_school_project_table").height,
        "school_recent_rows": gold.filter(
            (pl.col("source_corpus") == "pq_school_project_table") & (pl.col("snapshot_freshness") == "recent_180_days")
        ).height,
        "school_stale_rows": gold.filter(
            (pl.col("source_corpus") == "pq_school_project_table")
            & (pl.col("snapshot_freshness") == "stale_over_365_days")
        ).height,
        "money_contract": "amount_text is source wording and is never aggregable",
        "classification_contract": "upstream classifications preserved; no reclassification in promotion",
    }
    return silver, gold, coverage


def write_outputs(
    input_dir: Path,
    *,
    school_opportunities_path: Path | None = None,
    school_packages_path: Path | None = None,
    report_as_of: str | None = None,
    silver_path: Path = SILVER_PATH,
    gold_path: Path = GOLD_PATH,
    coverage_path: Path = COVERAGE_PATH,
) -> dict:
    silver, gold, coverage = promote(
        input_dir,
        school_opportunities_path=school_opportunities_path,
        school_packages_path=school_packages_path,
        report_as_of=report_as_of,
    )
    save_parquet(silver, silver_path, min_rows=1)
    save_parquet(gold, gold_path, min_rows=1)
    save_coverage(coverage, coverage_path)
    return {**coverage, "silver_path": str(silver_path), "gold_path": str(gold_path)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Promote classified pre-tender observations to silver and gold")
    parser.add_argument("--input-dir", type=Path, required=True, help="directory containing the classified parquets")
    parser.add_argument("--school-opportunities", type=Path)
    parser.add_argument("--school-packages", type=Path)
    parser.add_argument("--report-as-of", help="ISO date for the school-source status caveat")
    parser.add_argument("--silver", type=Path, default=SILVER_PATH)
    parser.add_argument("--gold", type=Path, default=GOLD_PATH)
    parser.add_argument("--coverage", type=Path, default=COVERAGE_PATH)
    args = parser.parse_args()
    result = write_outputs(
        args.input_dir,
        school_opportunities_path=args.school_opportunities,
        school_packages_path=args.school_packages,
        report_as_of=args.report_as_of,
        silver_path=args.silver,
        gold_path=args.gold,
        coverage_path=args.coverage,
    )
    print(result)


if __name__ == "__main__":
    main()
