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

# The Department publishes a stage-date column per approval gate. The opportunity
# classifier drops them, so they are joined back from the snapshot table here. A
# project with a past start-on-site date is already a building site, not a lead —
# without these columns nothing downstream can tell the two apart.
STAGE_DATE_COLUMNS = (
    "stage_1_start_date",
    "stage_2a_start_date",
    "stage_2b_start_date",
    "stage_3_start_date",
    "stage_4_start_date",
    "start_on_site_date",
)

# How old an observation is, banded. The school lane arrives already banded from its
# snapshot table; every other corpus arrived with the column null, so ~30% of gold rows
# carried no age signal at all and the page's staleness count under-reported the corpus
# (55 banded stale against 84 rows genuinely over a year old). The bands below are the
# SAME rule the school lane uses upstream — derived from source_date against report_as_of,
# it reproduces all 148 upstream school bands exactly, which is why filling the gap here
# is a completion of one vocabulary rather than the invention of a second one.
FRESHNESS_RECENT = "recent_180_days"
FRESHNESS_AGEING = "ageing_181_to_365_days"
FRESHNESS_STALE = "stale_over_365_days"
FRESHNESS_BANDS = (FRESHNESS_RECENT, FRESHNESS_AGEING, FRESHNESS_STALE)

SCHOOL_SNAPSHOT_JOIN_KEYS = ("attachment_url", "sheet_or_table", "source_row_index")

SCHOOL_SNAPSHOT_REQUIRED_COLUMNS = {*SCHOOL_SNAPSHOT_JOIN_KEYS, *STAGE_DATE_COLUMNS}

# Columns the school lane contributes that the other corpora do not have.
SCHOOL_EXTRA_COLUMNS = (
    "school_roll_number",
    "snapshot_freshness",
    *STAGE_DATE_COLUMNS,
    "start_on_site_note",
)

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
    *STAGE_DATE_COLUMNS,
    "start_on_site_note",
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


def _published_date_text(column: str) -> pl.Expr:
    """The published stage date as text, before any date parsing."""
    return pl.col(column).cast(pl.String, strict=False).str.strip_chars().replace("", None)


def _iso_date(column: str) -> pl.Expr:
    """An ISO date, or null when the cell holds something that is not one.

    The Department writes a real date in most cells but plain wording ("TBC") in
    others, so this never raises. Unparsed non-empty cells are counted in
    coverage instead of being dropped silently — a new vintage's wording has to
    show up somewhere.
    """
    return (
        _published_date_text(column)
        .str.slice(0, 10)
        .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
        .dt.to_string("%Y-%m-%d")
        .alias(column)
    )


def _derived_freshness() -> pl.Expr:
    """The age band implied by source_date against report_as_of, or null when either is
    unparseable. Used to FILL a null band, never to overwrite one a corpus supplied."""
    observed = pl.col("source_date").cast(pl.String, strict=False).str.to_date("%Y-%m-%d", strict=False)
    as_of = pl.col("report_as_of").cast(pl.String, strict=False).str.to_date("%Y-%m-%d", strict=False)
    age_days = (as_of - observed).dt.total_days()
    return (
        pl.when(observed.is_null() | as_of.is_null())
        .then(pl.lit(None, dtype=pl.String))
        .when(age_days <= 180)
        .then(pl.lit(FRESHNESS_RECENT))
        .when(age_days <= 365)
        .then(pl.lit(FRESHNESS_AGEING))
        .otherwise(pl.lit(FRESHNESS_STALE))
    )


def _join_stage_dates(opportunities: pl.DataFrame, snapshots_path: Path) -> pl.DataFrame:
    """Attach the per-gate stage dates the opportunity classifier drops."""
    snapshots = pl.read_parquet(snapshots_path)
    missing = SCHOOL_SNAPSHOT_REQUIRED_COLUMNS.difference(snapshots.columns)
    if missing:
        raise ValueError(f"{snapshots_path.name} is missing required columns: {sorted(missing)}")

    keys = list(SCHOOL_SNAPSHOT_JOIN_KEYS)
    snapshots = snapshots.select(*keys, *STAGE_DATE_COLUMNS)
    # A duplicated key would fan the observation grain out and publish one
    # school's dates against another's row, so it fails rather than joins.
    if snapshots.select(pl.struct(keys).is_duplicated().any()).item():
        raise ValueError(f"{snapshots_path.name} stage-date join key must be unique per snapshot row")

    joined = opportunities.join(
        snapshots.with_columns(
            *[_iso_date(column) for column in STAGE_DATE_COLUMNS],
            _published_date_text("start_on_site_date").alias("_on_site_published"),
        ),
        left_on=["source_attachment_url", "source_sheet_or_table", "source_row_index"],
        right_on=keys,
        how="left",
    )
    if joined.height != opportunities.height:
        raise ValueError("stage-date join changed the school observation row count")
    return joined.with_columns(
        pl.when(pl.col("start_on_site_date").is_null() & pl.col("_on_site_published").is_not_null())
        .then(pl.col("_on_site_published"))
        .otherwise(None)
        .alias("start_on_site_note")
    ).drop("_on_site_published")


def _school_pre_tender_rows(
    opportunities_path: Path,
    packages_path: Path,
    *,
    report_as_of: str,
    snapshots_path: Path | None = None,
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

    if snapshots_path is not None:
        absent_keys = {"source_sheet_or_table", "source_row_index"}.difference(opportunities.columns)
        if absent_keys:
            raise ValueError(
                f"{opportunities_path.name} cannot be joined to stage dates without: {sorted(absent_keys)}"
            )
        opportunities = _join_stage_dates(opportunities, snapshots_path)
    else:
        opportunities = opportunities.with_columns(
            *[pl.lit(None, dtype=pl.String).alias(column) for column in STAGE_DATE_COLUMNS],
            pl.lit(None, dtype=pl.String).alias("start_on_site_note"),
        )

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
    return rows.select(sorted(REQUIRED_COLUMNS | set(SCHOOL_EXTRA_COLUMNS)))


def promote(
    input_dir: Path,
    *,
    school_opportunities_path: Path | None = None,
    school_packages_path: Path | None = None,
    school_snapshots_path: Path | None = None,
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
            snapshots_path=school_snapshots_path,
        )
        frames.append(school_rows)
        source_rows[school_opportunities_path.name] = school_rows.height

    silver = pl.concat(frames, how="diagonal_relaxed")
    for column in SCHOOL_EXTRA_COLUMNS:
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
        *[_clean_text(column) for column in [*text_columns, *SCHOOL_EXTRA_COLUMNS]],
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
    # Fill the age band wherever a corpus did not supply one. Coalesce, not overwrite:
    # a corpus that bands its own rows upstream keeps its own provenance.
    silver = silver.with_columns(
        pl.coalesce(pl.col("snapshot_freshness"), _derived_freshness()).alias("snapshot_freshness")
    )
    unbanded_dated = silver.filter(pl.col("snapshot_freshness").is_null() & pl.col("source_date").is_not_null())
    if unbanded_dated.height:
        raise ValueError(
            f"{unbanded_dated.height} dated observation(s) carry no snapshot_freshness band — "
            "every dated row must carry an age signal or the page under-reports staleness"
        )
    bad_bands = set(silver["snapshot_freshness"].drop_nulls().unique()) - set(FRESHNESS_BANDS)
    if bad_bands:
        raise ValueError(f"unknown snapshot_freshness band(s): {sorted(bad_bands)}")

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
        # Corpus-wide age bands. The school_* counts above stay for continuity, but the
        # page's staleness caveat must be built from THESE — banding only the school lane
        # is what let 29 stale council rows go uncounted and unpilled.
        "freshness_rows": {band: gold.filter(pl.col("snapshot_freshness") == band).height for band in FRESHNESS_BANDS},
        "undated_rows": gold.filter(pl.col("source_date").is_null()).height,
        "school_stage_date_rows": {
            column: gold.filter(pl.col(column).is_not_null()).height for column in STAGE_DATE_COLUMNS
        },
        # Cells that held wording rather than a date ("TBC"). Counted so a new
        # vintage's phrasing is visible instead of silently becoming a null.
        "school_start_on_site_unparsed_rows": gold.filter(pl.col("start_on_site_note").is_not_null()).height,
        "money_contract": "amount_text is source wording and is never aggregable",
        "classification_contract": "upstream classifications preserved; no reclassification in promotion",
    }
    return silver, gold, coverage


def write_outputs(
    input_dir: Path,
    *,
    school_opportunities_path: Path | None = None,
    school_packages_path: Path | None = None,
    school_snapshots_path: Path | None = None,
    report_as_of: str | None = None,
    silver_path: Path = SILVER_PATH,
    gold_path: Path = GOLD_PATH,
    coverage_path: Path = COVERAGE_PATH,
) -> dict:
    silver, gold, coverage = promote(
        input_dir,
        school_opportunities_path=school_opportunities_path,
        school_packages_path=school_packages_path,
        school_snapshots_path=school_snapshots_path,
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
    parser.add_argument(
        "--school-snapshots",
        type=Path,
        help="school project snapshot parquet carrying the per-gate stage dates",
    )
    parser.add_argument("--report-as-of", help="ISO date for the school-source status caveat")
    parser.add_argument("--silver", type=Path, default=SILVER_PATH)
    parser.add_argument("--gold", type=Path, default=GOLD_PATH)
    parser.add_argument("--coverage", type=Path, default=COVERAGE_PATH)
    args = parser.parse_args()
    result = write_outputs(
        args.input_dir,
        school_opportunities_path=args.school_opportunities,
        school_packages_path=args.school_packages,
        school_snapshots_path=args.school_snapshots,
        report_as_of=args.report_as_of,
        silver_path=args.silver,
        gold_path=args.gold,
        coverage_path=args.coverage,
    )
    print(result)


if __name__ == "__main__":
    main()
