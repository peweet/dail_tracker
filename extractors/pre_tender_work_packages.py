"""Classify pre-tender observations into a provenance-carrying package dimension.

The output grain is one ``lead_id x package_code`` classification. A project may
therefore have several packages without being counted as several projects. Full
source text remains in silver; gold retains only the exact matched phrase and
the rule basis required to explain the classification.

CE-report candidates are classified into silver for review, but enter gold only
when their existing ``promotion_permitted`` gate is true.
"""

from __future__ import annotations

# isort: off
import services.runtime_env  # noqa: F401  # native thread caps before Polars
# isort: on

import argparse
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from paths import PROJECT_ROOT
from services.coverage_io import save_coverage
from services.parquet_io import save_parquet

PRE_TENDER_PATH = PROJECT_ROOT / "data" / "silver" / "parquet" / "pre_tender_lead_evidence.parquet"
CE_LEADS_PATH = PROJECT_ROOT / "data" / "gold" / "parquet" / "council_ce_report_leads.parquet"
SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "parquet" / "pre_tender_work_package_evidence.parquet"
GOLD_PATH = PROJECT_ROOT / "data" / "gold" / "parquet" / "pre_tender_work_packages.parquet"
COVERAGE_PATH = PROJECT_ROOT / "data" / "_meta" / "pre_tender_work_packages_coverage.json"


@dataclass(frozen=True)
class PackageRule:
    code: str
    label: str
    group: str
    pattern: str


# Patterns deliberately require a procurement-relevant phrase. In particular,
# bare "network" is not an IT signal because it commonly means roads, water or
# wastewater in this corpus.
PACKAGE_RULES = (
    PackageRule(
        "electrical_lighting_bems",
        "Electrical, lighting and BEMS",
        "Building services",
        r"(?i)\b(electrical(?:\s+works?)?|lighting(?:\s+(?:works?|upgrade|replacement))?|rewir(?:e|ing)|BEMS|building\s+energy\s+management|solar\s+PV|photovoltaic|EV\s+charg(?:er|ing))\b",
    ),
    PackageRule(
        "plumbing_sanitary",
        "Plumbing and sanitary works",
        "Building services",
        r"(?i)\b(plumb(?:er|ing)?|sanitary(?:\s+works?)?|toilet(?:s|\s+facilities|\s+upgrade)|domestic\s+water\s+services?|drainage\s+works?)\b",
    ),
    PackageRule(
        "mechanical_heating_hvac",
        "Mechanical, heating and HVAC",
        "Building services",
        r"(?i)\b(mechanical(?:\s+(?:works?|services?))?|heating(?:\s+works?)?|boilers?|heat\s+pumps?|HVAC|pumping\s+stations?|pumps?)\b",
    ),
    PackageRule(
        "construction_building_works",
        "Construction and building works",
        "Construction",
        r"(?i)\b(construction|building\s+works?|new\s+school|school\s+extension|new\s+build|civil\s+works?)\b",
    ),
    PackageRule(
        "refurbishment_fitout",
        "Refurbishment, reconfiguration and fit-out",
        "Construction",
        r"(?i)\b(refurbish(?:ment|ing)?|reconfiguration|fit[- ]?out|renovation|remediation)\b",
    ),
    PackageRule(
        "roof_windows_doors",
        "Roofing, windows and doors",
        "Building fabric",
        r"(?i)\b(roof(?:ing|\s+works?|\s+repairs?)?|windows?(?:\s+projects?|\s+replacement)?|doors?(?:\s+replacement)?)\b",
    ),
    PackageRule(
        "fire_safety",
        "Fire safety and compliance",
        "Building services",
        r"(?i)\b(fire\s+safety|fire\s+upgrade|fire\s+alarm|compartmentation)\b",
    ),
    PackageRule(
        "it_digital_systems",
        "IT, digital and software systems",
        "Technology",
        r"(?i)\b(ICT|information\s+technology|IT\s+services?|software|digital\s+(?:configuration|platform|system|services?)|cyber(?:security)?|cloud\s+(?:services?|platform)|data\s+(?:platform|system)|customer\s+systems?|telecommunications?|communications?\s+systems?|SCADA)\b",
    ),
    PackageRule(
        "security_cctv_access",
        "Security, CCTV and access control",
        "Technology",
        r"(?i)\b(CCTV|security\s+systems?|intruder\s+alarms?|access\s+control)\b",
    ),
    PackageRule(
        "design_qs_engineering",
        "Design, architecture, engineering and QS",
        "Professional services",
        r"(?i)\b(architect(?:s|ural)?|engineering|engineers?|quantity\s+survey(?:or|ing)|QS\s+services?|PSDP|design\s+team|consultancy)\b",
    ),
    PackageRule(
        "roads_public_realm",
        "Roads, resurfacing and public realm",
        "Civil and public realm",
        r"(?i)\b(roadworks?|road\s+upgrade|resurfacing|public\s+realm|streetscape|footpaths?|cycleways?)\b",
    ),
    PackageRule(
        "landscape_external_site",
        "Landscaping, external and site works",
        "Civil and public realm",
        r"(?i)\b(landscap(?:e|ing)|external\s+(?:environment|works?)|site\s+works?|parks?\s+works?)\b",
    ),
    PackageRule(
        "water_wastewater_civil",
        "Water, wastewater and process works",
        "Water infrastructure",
        r"(?i)\b(water\s+supply|water\s+network|wastewater|treatment\s+plant|trunk\s+main|reservoir|bioresources?|process\s+works?)\b",
    ),
    PackageRule(
        "facilities_maintenance",
        "Facilities repair and maintenance",
        "Facilities",
        r"(?i)\b(facilities\s+management|repairs?\s+and\s+maintenance|planned\s+maintenance|maintenance\s+services?)\b",
    ),
    PackageRule(
        "equipment_supply_install",
        "Equipment supply and installation",
        "Equipment",
        r"(?i)\b(equipment\s+(?:supply|installation)|supply\s+and\s+install(?:ation)?|plant\s+and\s+equipment)\b",
    ),
)

PRE_TENDER_REQUIRED = {
    "lead_id",
    "source_corpus",
    "source_record_id",
    "source_date",
    "reporting_area",
    "project_name",
    "evidence_text",
    "likely_work_package",
    "source_url",
    "source_review_required",
}

CE_REQUIRED = {
    "lead_id",
    "council",
    "report_title",
    "report_month",
    "reviewed_project_name",
    "quote",
    "lead_types",
    "source_url",
    "source_landing_url",
    "promotion_permitted",
}

GOLD_COLUMNS = (
    "package_row_id",
    "lead_id",
    "source_corpus",
    "source_record_id",
    "source_date",
    "reporting_area",
    "project_name",
    "package_code",
    "package_label",
    "package_group",
    "evidence_phrase",
    "matched_field",
    "classification_basis",
    "source_url",
    "source_review_required",
    "current_status_verified",
    "amount_is_not_aggregable",
    "classification_schema",
)


def _require_columns(frame: pl.DataFrame, required: set[str], name: str) -> None:
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(f"{name} is missing required columns: {sorted(missing)}")


def _source_rows(pre_tender_path: Path, ce_leads_path: Path | None) -> pl.DataFrame:
    pre = pl.read_parquet(pre_tender_path)
    _require_columns(pre, PRE_TENDER_REQUIRED, pre_tender_path.name)
    frames = [
        pre.select(
            "lead_id",
            "source_corpus",
            "source_record_id",
            "source_date",
            "reporting_area",
            "project_name",
            "evidence_text",
            pl.col("likely_work_package").alias("existing_package"),
            "source_url",
            "source_review_required",
            pl.lit(True).alias("promotion_permitted"),
        )
    ]
    if ce_leads_path is not None and ce_leads_path.is_file():
        ce = pl.read_parquet(ce_leads_path)
        _require_columns(ce, CE_REQUIRED, ce_leads_path.name)
        frames.append(
            ce.select(
                pl.concat_str([pl.lit("ce:"), pl.col("lead_id")]).alias("lead_id"),
                pl.lit("council_ce_report").alias("source_corpus"),
                pl.col("lead_id").alias("source_record_id"),
                pl.concat_str([pl.col("report_month"), pl.lit("-01")]).alias("source_date"),
                pl.col("council").alias("reporting_area"),
                pl.col("reviewed_project_name").alias("project_name"),
                pl.col("quote").alias("evidence_text"),
                pl.col("lead_types").list.join(", ").alias("existing_package"),
                pl.coalesce("source_url", "source_landing_url").alias("source_url"),
                (~pl.col("promotion_permitted")).alias("source_review_required"),
                "promotion_permitted",
            )
        )
    return pl.concat(frames, how="diagonal_relaxed").with_columns(
        pl.col("evidence_text").fill_null("").str.strip_chars(),
        pl.col("existing_package").fill_null("").str.strip_chars(),
        pl.col("source_url").fill_null("").str.strip_chars(),
    )


def classify_work_packages(
    pre_tender_path: Path,
    *,
    ce_leads_path: Path | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame, dict]:
    """Return silver classifications, promotion-gated gold rows and coverage."""
    sources = _source_rows(pre_tender_path, ce_leads_path)
    matches: list[pl.DataFrame] = []
    for rule in PACKAGE_RULES:
        evidence_matches = pl.col("evidence_text").str.contains(rule.pattern)
        existing_matches = pl.col("existing_package").str.contains(rule.pattern)
        matched = (
            sources.filter(evidence_matches | existing_matches)
            .with_columns(
                pl.lit(rule.code).alias("package_code"),
                pl.lit(rule.label).alias("package_label"),
                pl.lit(rule.group).alias("package_group"),
                pl.when(evidence_matches)
                .then(pl.col("evidence_text").str.extract(rule.pattern, 0))
                .otherwise(pl.col("existing_package").str.extract(rule.pattern, 0))
                .alias("evidence_phrase"),
                pl.when(evidence_matches)
                .then(pl.lit("evidence_text"))
                .otherwise(pl.lit("likely_work_package"))
                .alias("matched_field"),
                pl.when(evidence_matches)
                .then(pl.lit("source_literal_rule"))
                .otherwise(pl.lit("existing_classification_rule"))
                .alias("classification_basis"),
            )
            .with_columns(
                pl.concat_str([pl.col("lead_id"), pl.lit(":"), pl.col("package_code")]).alias("package_row_id"),
                pl.lit(False).alias("current_status_verified"),
                pl.lit(True).alias("amount_is_not_aggregable"),
                pl.lit("pre-tender-work-package/1").alias("classification_schema"),
            )
        )
        matches.append(matched)

    silver = pl.concat(matches, how="diagonal_relaxed").sort(["lead_id", "package_code"])
    if silver.select(pl.col("package_row_id").is_duplicated().any()).item():
        raise ValueError("package_row_id must be unique at lead x package grain")
    if silver.select(pl.col("evidence_phrase").is_null().any()).item():
        raise ValueError("every package classification must retain its matched phrase")
    if silver.select((~pl.col("source_url").str.starts_with("http")).any()).item():
        raise ValueError("every package classification must retain an HTTP(S) source URL")
    if silver.select(pl.col("current_status_verified").any()).item():
        raise ValueError("package classifications cannot claim current tender status")

    gold = silver.filter(pl.col("promotion_permitted")).select(GOLD_COLUMNS)
    counts = {
        row["package_code"]: row["len"] for row in gold.group_by("package_code").len().sort("package_code").to_dicts()
    }
    classified_source_ids = silver["lead_id"].unique().to_list()
    promoted_sources = sources.filter(pl.col("promotion_permitted"))
    classified_promoted_ids = gold["lead_id"].unique().to_list()
    coverage = {
        "schema": "pre-tender-work-packages-coverage/1",
        "grain": "one deterministic lead by work-package classification",
        "source_leads": sources.height,
        "silver_rows": silver.height,
        "gold_rows": gold.height,
        "classified_source_leads": len(classified_source_ids),
        "unclassified_source_leads": sources.filter(~pl.col("lead_id").is_in(classified_source_ids)).height,
        "promoted_source_leads": promoted_sources.height,
        "classified_promoted_leads": len(classified_promoted_ids),
        "unclassified_promoted_leads": promoted_sources.filter(
            ~pl.col("lead_id").is_in(classified_promoted_ids)
        ).height,
        "ce_candidate_rows": silver.filter(pl.col("source_corpus") == "council_ce_report").height,
        "ce_promoted_rows": gold.filter(pl.col("source_corpus") == "council_ce_report").height,
        "ce_candidate_leads": silver.filter(pl.col("source_corpus") == "council_ce_report")["lead_id"].n_unique(),
        "ce_promoted_leads": gold.filter(pl.col("source_corpus") == "council_ce_report")["lead_id"].n_unique(),
        "package_counts": counts,
        "classification_contract": "deterministic phrase rules; bare network is not an IT classification",
        "promotion_contract": "CE rows enter gold only when their existing promotion_permitted gate is true",
    }
    return silver, gold, coverage


def write_outputs(
    pre_tender_path: Path = PRE_TENDER_PATH,
    *,
    ce_leads_path: Path | None = CE_LEADS_PATH,
    silver_path: Path = SILVER_PATH,
    gold_path: Path = GOLD_PATH,
    coverage_path: Path = COVERAGE_PATH,
) -> dict:
    silver, gold, coverage = classify_work_packages(pre_tender_path, ce_leads_path=ce_leads_path)
    save_parquet(silver, silver_path, min_rows=1)
    save_parquet(gold, gold_path, min_rows=1)
    save_coverage(coverage, coverage_path)
    return {**coverage, "silver_path": str(silver_path), "gold_path": str(gold_path)}


def main() -> None:
    parser = argparse.ArgumentParser(description="Classify pre-tender observations into work packages")
    parser.add_argument("--pre-tender", type=Path, default=PRE_TENDER_PATH)
    parser.add_argument("--ce-leads", type=Path, default=CE_LEADS_PATH)
    parser.add_argument("--silver", type=Path, default=SILVER_PATH)
    parser.add_argument("--gold", type=Path, default=GOLD_PATH)
    parser.add_argument("--coverage", type=Path, default=COVERAGE_PATH)
    args = parser.parse_args()
    print(
        write_outputs(
            args.pre_tender,
            ce_leads_path=args.ce_leads,
            silver_path=args.silver,
            gold_path=args.gold,
            coverage_path=args.coverage,
        )
    )


if __name__ == "__main__":
    main()
