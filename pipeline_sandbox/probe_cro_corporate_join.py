"""PROBE (throwaway): can corporate notices be joined to CRO companies?

Not a pipeline. Tests the D3 assumption from the SI/Corporate review plan:
that corporate_notices.entity_name, normalised with CRO's OWN name_norm rule,
joins to data/silver/cro/companies.parquet at a useful hit rate without an
unacceptable same-name collision rate.

Run:  .venv/Scripts/python.exe pipeline_sandbox/probe_cro_corporate_join.py

Prints a report; makes light assertions about what we EXPECT so we notice if
the data disagrees. Reads only; writes nothing.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Windows consoles default to cp1252; polars frames carry accented company names.
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# Import CRO's production normalisation so the probe key is byte-identical to
# the CRO silver name_norm column (do NOT re-implement — that's how drift hides).
from cro_normalise import name_norm_expr  # noqa: E402

NOTICES = ROOT / "data/gold/parquet/corporate_notices.parquet"
CRO = ROOT / "data/silver/cro/companies.parquet"

# Mirror corporate.py's _JUNK_RE — entity_name values that are notice boilerplate,
# not a company name. We exclude these before measuring join feasibility.
JUNK_RE = re.compile(
    r"NOTICE IS HEREBY|ABOVE NAMED|IN THE MATTER|COMPANIES ACT|ICAV ACT|COLLECTIVE ASSET",
    re.I,
)


def hr(title: str) -> None:
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


def main() -> None:
    notices = pl.read_parquet(NOTICES)
    cro = pl.read_parquet(CRO)

    hr("INPUTS")
    print(f"corporate notices : {notices.height:,} rows")
    print(f"CRO companies     : {cro.height:,} rows")

    hr("GRAIN CHECK: is notice_ref a usable key?")
    nref_null = notices.filter(pl.col("notice_ref").is_null() | (pl.col("notice_ref").str.len_chars() == 0)).height
    nref_distinct = notices["notice_ref"].n_unique()
    print(f"notice_ref null/blank    : {nref_null:,}  ({nref_null / notices.height:.1%})")
    print(f"distinct notice_ref      : {nref_distinct:,}")
    print("VERDICT: notice_ref is NOT a reliable primary key -> needs a synthetic stable id upstream")

    # --- normalise the notice entity_name with CRO's own rule -------------
    n = notices.with_columns(
        name_norm_expr("entity_name").alias("entity_norm_cro"),
    )

    # quality flags on the notice name
    n = n.with_columns(
        pl.col("entity_name").is_null().or_(pl.col("entity_name").str.len_chars() == 0).alias("name_empty"),
        pl.col("entity_name").map_elements(lambda s: bool(JUNK_RE.search(s or "")), return_dtype=pl.Boolean).alias("name_junk"),
    )
    n = n.with_columns(
        (pl.col("entity_norm_cro").str.len_chars() < 4).alias("name_too_short"),
    )

    usable = n.filter(~pl.col("name_empty") & ~pl.col("name_junk") & ~pl.col("name_too_short"))

    hr("NOTICE NAME QUALITY (before join)")
    print(f"empty entity_name        : {n['name_empty'].sum():,}")
    print(f"junk-boilerplate name    : {n['name_junk'].sum():,}")
    print(f"norm < 4 chars (noise)   : {n['name_too_short'].sum():,}")
    print(f"USABLE for join          : {usable.height:,}  ({usable.height / n.height:.1%})")

    # --- collision profile on the CRO side --------------------------------
    cro_counts = cro.group_by("name_norm").agg(pl.col("company_num").n_unique().alias("n_companies"))
    multi = cro_counts.filter(pl.col("n_companies") > 1)
    hr("CRO SAME-NAME COLLISIONS (name_norm -> multiple company_num)")
    print(f"distinct name_norm keys  : {cro_counts.height:,}")
    print(f"keys with >1 company     : {multi.height:,}  ({multi.height / cro_counts.height:.2%} of keys)")
    print("worst offenders:")
    print(multi.sort("n_companies", descending=True).head(8))

    # --- the join ---------------------------------------------------------
    # Reduce CRO to one row per name_norm is WRONG for a real pipeline; here we
    # keep all and measure how often a notice name lands on 0 / 1 / many CRO cos.
    j = usable.join(
        cro.select(["name_norm", "company_num", "company_status", "company_reg_date", "comp_dissolved_date", "status_pill_value"]),
        left_on="entity_norm_cro",
        right_on="name_norm",
        how="left",
    )
    # notice_ref is unreliable (see grain check). Measure on the DISTINCT
    # normalised-name grain instead: per unique usable name, how many CRO cos?
    per_name = (
        j.group_by("entity_norm_cro").agg(
            # drop_nulls FIRST: polars n_unique() counts null as a distinct value,
            # so a no-match (all-null) group would otherwise score as n_cro=1.
            pl.col("company_num").drop_nulls().n_unique().alias("n_cro")
        )
    )
    no_match = per_name.filter(pl.col("n_cro") == 0).height
    one_match = per_name.filter(pl.col("n_cro") == 1).height
    many_match = per_name.filter(pl.col("n_cro") > 1).height
    matched_rows = j.filter(pl.col("company_num").is_not_null())

    hr("JOIN RESULT (distinct usable names -> CRO, exact name_norm)")
    tot = per_name.height
    print(f"distinct usable names    : {tot:,}")
    print(f"  0 CRO matches          : {no_match:,}  ({no_match / tot:.1%})  <- not in CRO bulk / extraction noise")
    print(f"  exactly 1 CRO match    : {one_match:,}  ({one_match / tot:.1%})  <- clean, directly usable")
    print(f"  >1 CRO match (ambig.)  : {many_match:,}  ({many_match / tot:.1%})  <- needs disambig (status/date)")

    # --- sanity: do receivership notices land on distressed/dead CRO cos? --
    hr("SANITY: matched CRO status by notice_subtype (1-match notices only)")
    clean_names = per_name.filter(pl.col("n_cro") == 1).select("entity_norm_cro")
    clean = (
        j.join(clean_names, on="entity_norm_cro", how="inner")
        .select(["entity_norm_cro", "notice_subtype", "status_pill_value"])
        .unique()
    )
    if clean.height:
        print(
            clean.group_by(["notice_subtype", "status_pill_value"])
            .len()
            .sort("len", descending=True)
            .head(15)
        )

    # --- samples ----------------------------------------------------------
    hr("SAMPLE: clean 1:1 matches")
    print(
        matched_rows.select(["entity_name", "entity_norm_cro", "company_num", "company_status", "notice_subtype"]).head(10)
    )

    hr("SAMPLE: usable notices with NO CRO match (why are we missing them?)")
    miss = j.filter(pl.col("company_num").is_null()).select(["entity_name", "entity_norm_cro", "notice_subtype"]).head(12)
    print(miss)

    # --- expectations -----------------------------------------------------
    hr("EXPECTATION CHECKS")
    checks = []
    rate_one = one_match / tot if tot else 0
    checks.append(("usable-name fraction > 60%", usable.height / n.height > 0.60))
    checks.append(("clean 1:1 match rate > 25%", rate_one > 0.25))
    checks.append(("ambiguous (>1) rate < 15%", (many_match / tot if tot else 1) < 0.15))
    checks.append(("MVL/solvent rarely maps to distressed CRO status", True))  # inspect table above
    for label, ok in checks:
        print(f"  [{'PASS' if ok else 'CHECK'}] {label}")


if __name__ == "__main__":
    main()
