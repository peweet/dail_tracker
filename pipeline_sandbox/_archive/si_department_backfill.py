"""SI department backfill via responsible_actor crosswalk — SANDBOX demo.

NOT an LRC enrichment. A separate, internal data-quality probe: 56.8% of SIs have
a NULL si_department. 647 of those carry a populated si_responsible_actor whose
string the existing department mapping didn't recognise (renamed/short-form
portfolio titles). This deterministically backfills department from the actor.

Two confidence tiers (the distinction matters under the no-inference-in-UI rule):
  - high   : a renamed/short-form portfolio that maps 1:1 to a current dept, or a
             body whose remit sits unambiguously in one dept (Revenue->finance,
             court rules committees->justice). This is normalisation, not inference.
  - ambiguous : the actor spans >1 modern dept (e.g. "Communications" = culture OR
             climate historically) — backfilled but FLAGGED, never UI-asserted.
  - (unmappable: "The Taoiseach"/"The Government" have NO dept in the 18-vocab —
     left NULL on purpose. Don't force a label.)

Writes a sandbox parquet only; gold untouched.
Reads : data/gold/parquet/statutory_instruments.parquet
Writes: pipeline_sandbox/_lrc_output/si_department_backfill.parquet
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

GOLD = ROOT / "data/gold/parquet/statutory_instruments.parquet"
OUT = ROOT / "pipeline_sandbox/_lrc_output/si_department_backfill.parquet"

# Ordered (keyword regex, dept, tier). First match wins — order matters:
# specific bodies before generic "Minister for X" portfolio words.
CROSSWALK: list[tuple[str, str, str]] = [
    # --- bodies / committees with an unambiguous home dept (high) ---
    (r"central bank", "finance", "high"),
    (r"revenue commissioners", "finance", "high"),
    (r"courts? rules committee|superior courts rules", "justice", "high"),
    (r"commission for communications regulation", "culture_communications_sport", "high"),
    # --- renamed / short-form ministerial portfolios (high = normalisation) ---
    (r"\bjobs?\b|for business", "enterprise", "high"),          # Jobs/Business -> Enterprise
    (r"\benterprise\b", "enterprise", "high"),
    (r"\bagri", "agriculture", "high"),                          # incl. "Agriulture" typo
    (r"\bfinance\b", "finance", "high"),
    (r"public expenditure|for public\b", "public_expenditure", "high"),
    (r"social protection|employment affairs", "social_protection", "high"),
    (r"\bhealth\b", "health", "high"),
    (r"\bjustice\b", "justice", "high"),
    (r"housing|local government", "housing", "high"),
    (r"\bdefence\b", "defence", "high"),
    (r"foreign affairs", "foreign_affairs", "high"),
    (r"further and higher|further and$", "further_higher_education", "high"),
    (r"\beducation\b", "education", "high"),
    (r"rural and community|rural and$", "rural_community", "high"),
    (r"\btransport\b", "transport", "high"),
    (r"\bchildren\b", "children_disability_equality", "high"),
    (r"arts|culture|tourism|media|gaeltacht|sport", "culture_communications_sport", "high"),
    # --- genuinely ambiguous: flagged, not UI-asserted ---
    (r"communications", "culture_communications_sport", "ambiguous"),  # could be climate_energy_environment
    (r"\benvironment\b|climate", "climate_energy_environment", "ambiguous"),
]


def classify(actor: str | None) -> tuple[str | None, str | None]:
    if not actor:
        return None, None
    a = actor.lower()
    for pat, dept, tier in CROSSWALK:
        if re.search(pat, a):
            return dept, tier
    return None, None  # Taoiseach / Government / truncated -> stay null


def main() -> None:
    g = pl.read_parquet(GOLD)
    before_null = g["si_department"].null_count()

    target = g.filter(pl.col("si_department").is_null() & pl.col("si_responsible_actor").is_not_null())
    mapped = target.with_columns(
        pl.col("si_responsible_actor")
        .map_elements(lambda a: classify(a)[0], return_dtype=pl.String)
        .alias("dept_backfill"),
        pl.col("si_responsible_actor")
        .map_elements(lambda a: classify(a)[1], return_dtype=pl.String)
        .alias("dept_backfill_tier"),
    )
    filled = mapped.filter(pl.col("dept_backfill").is_not_null())

    print(f"null-dept rows with an actor (candidates): {target.height}")
    print(f"  backfilled                              : {filled.height}")
    print(f"  ...high confidence                      : {filled.filter(pl.col('dept_backfill_tier')=='high').height}")
    print(f"  ...ambiguous (flagged)                  : {filled.filter(pl.col('dept_backfill_tier')=='ambiguous').height}")
    left = mapped.filter(pl.col("dept_backfill").is_null())
    print(f"  left NULL (no dept in vocab / truncated): {left.height}")
    print("    unmapped actors:")
    for r in left["si_responsible_actor"].value_counts(sort=True).head(8).iter_rows(named=True):
        print(f"      x{r['count']:3d}  {r['si_responsible_actor'][:50]}")

    new_null = before_null - filled.height
    print(f"\ndepartment null rate: {before_null}/{g.height} = {before_null/g.height:.1%}"
          f"  ->  {new_null}/{g.height} = {new_null/g.height:.1%}")
    high_null = before_null - filled.filter(pl.col("dept_backfill_tier") == "high").height
    print(f"  (high-confidence only)              ->  {high_null/g.height:.1%}")

    print("\nbackfill distribution by dept:")
    print(filled["dept_backfill"].value_counts(sort=True))

    # write the demo artifact: the SIs that gained a department
    out = filled.select(
        "si_year", "si_number", "si_title", "si_responsible_actor",
        "dept_backfill", "dept_backfill_tier",
    )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    out.write_parquet(OUT, compression="zstd", compression_level=3, statistics=True)
    print(f"\nwrote {OUT.relative_to(ROOT)} ({out.height} rows)")
    print("\nsample fills:")
    print(out.head(10))


if __name__ == "__main__":
    main()
