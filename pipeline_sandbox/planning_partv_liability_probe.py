"""SANDBOX probe: Part V social/affordable-housing LIABILITY across the national applications corpus.

Part V (Planning & Development Act 2000, as amended) obligates a social/affordable housing share on
residential developments of **9 or more units OR any housing on a site >0.1 ha**. The reservation was
raised 10%->20% by the Affordable Housing Act 2021 (20% for land bought >=1 Aug 2021; 10% for land
bought 2015-2021 until 2026, then 20% for all).

What this probe can and cannot do (HONEST scope):
  - It flags LIABILITY via the **unit limb only** (NumResidentialUnits >= 9). This is a conservative
    FLOOR: NumResidentialUnits is null on ~44% of rows, so true liability is higher.
  - The **>0.1 ha limb is NOT computed**: AreaofSite is unit-inconsistent across councils (one-off
    sites read ~0.3 = hectares, but other rows read in m² + garbage up to 1e10). Using it would need a
    per-council unit-normalisation pass (a separate DQ task) — flagged, not faked.
  - It flags LIABILITY, not COMPLIANCE: the Part V agreement / exemption certificate is not in the feed.
  - Historic threshold simplification: pre-2021 the area limb caught many <9-unit schemes, so unit-only
    further undercounts earlier years. Reported by year so the trend is visible, not a single rate.

Source-of-truth = data/silver/parquet/planning_applications_silver.parquet (495,632 rows).
Output: printed summary + pipeline_sandbox/_planning_output/partv_liability_summary.json
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
PARQUET = ROOT / "data" / "silver" / "parquet" / "planning_applications_silver.parquet"
OUT_DIR = ROOT / "pipeline_sandbox" / "_planning_output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

UNIT_THRESHOLD = 9  # current Part V unit limb

con = duckdb.connect()
P = str(PARQUET).replace("\\", "/")


def df(sql: str):
    return con.execute(sql).df()


def one(sql: str):
    return con.execute(sql).fetchone()[0]

total = one(f"SELECT count(*) FROM '{P}'")

# --- coverage caveats -----------------------------------------------------------------------------
units_nonnull = one(f"SELECT count(NumResidentialUnits) FROM '{P}'")
units_pos = one(f"SELECT count(*) FROM '{P}' WHERE NumResidentialUnits > 0")

# --- liability (unit limb) ------------------------------------------------------------------------
liable = one(f"SELECT count(*) FROM '{P}' WHERE NumResidentialUnits >= {UNIT_THRESHOLD}")
band_5_8 = one(f"SELECT count(*) FROM '{P}' WHERE NumResidentialUnits BETWEEN 5 AND 8")  # area-limb-dependent

# decided-outcome split for liable schemes vs all decided
liable_outcome = df(f"""
  SELECT decision_category, count(*) n
  FROM '{P}' WHERE NumResidentialUnits >= {UNIT_THRESHOLD}
  GROUP BY 1 ORDER BY n DESC
""")

def grant_rate(where_extra: str) -> tuple[int, int, float]:
    dec = one(f"SELECT count(*) FROM '{P}' WHERE decision_category IN ('granted','granted_conditional','refused') {where_extra}")
    gr = one(f"SELECT count(*) FROM '{P}' WHERE decision_category IN ('granted','granted_conditional') {where_extra}")
    return gr, dec, (100 * gr / dec if dec else 0.0)

g_liable = grant_rate(f"AND NumResidentialUnits >= {UNIT_THRESHOLD}")
g_all = grant_rate("")

# by year received (focus on the 20% era boundary 2021-09)
by_year = df(f"""
  SELECT EXTRACT(year FROM ReceivedDate) yr, count(*) AS n_liable,
         sum(CASE WHEN decision_category IN ('granted','granted_conditional') THEN 1 ELSE 0 END) AS n_granted
  FROM '{P}'
  WHERE NumResidentialUnits >= {UNIT_THRESHOLD} AND ReceivedDate IS NOT NULL
    AND EXTRACT(year FROM ReceivedDate) BETWEEN 2015 AND 2026
  GROUP BY 1 ORDER BY 1
""")

# top councils by liable count
by_council = df(f"""
  SELECT PlanningAuthority, count(*) AS n_liable,
         round(100.0*sum(CASE WHEN decision_category IN ('granted','granted_conditional') THEN 1 ELSE 0 END)
               / NULLIF(sum(CASE WHEN decision_category IN ('granted','granted_conditional','refused') THEN 1 ELSE 0 END),0),1) grant_pct
  FROM '{P}' WHERE NumResidentialUnits >= {UNIT_THRESHOLD}
  GROUP BY 1 ORDER BY n_liable DESC LIMIT 12
""")

# largest schemes (sanity)
biggest = df(f"""
  SELECT PlanningAuthority, NumResidentialUnits, decision_category, EXTRACT(year FROM ReceivedDate) yr
  FROM '{P}' WHERE NumResidentialUnits >= {UNIT_THRESHOLD}
  ORDER BY NumResidentialUnits DESC LIMIT 8
""")

# ---- report --------------------------------------------------------------------------------------
print(f"Corpus: {total:,} applications")
print(f"NumResidentialUnits coverage: {units_nonnull:,} non-null ({100*units_nonnull/total:.1f}%), "
      f"{units_pos:,} with >0 ({100*units_pos/total:.1f}%)\n")
print(f"PART V LIABLE (unit limb, >= {UNIT_THRESHOLD} units): {liable:,}  [conservative FLOOR — units null on "
      f"{100*(total-units_nonnull)/total:.0f}% of rows]")
print(f"  + 5-8 unit band (liable only via the >0.1 ha limb / pre-2021 thresholds): {band_5_8:,}")
print(f"\nGrant rate (decided): liable schemes {g_liable[2]:.1f}% ({g_liable[0]:,}/{g_liable[1]:,})  "
      f"vs all apps {g_all[2]:.1f}% ({g_all[0]:,}/{g_all[1]:,})")
print("\nLiable-scheme outcomes:\n", liable_outcome.to_string(index=False))
print("\nLiable schemes by year received (2015-2026):\n", by_year.to_string(index=False))
print("\nTop councils by liable-scheme count:\n", by_council.to_string(index=False))
print("\nLargest schemes (sanity):\n", biggest.to_string(index=False))

summary = {
    "corpus_rows": total,
    "unit_threshold": UNIT_THRESHOLD,
    "units_coverage_pct": round(100 * units_nonnull / total, 1),
    "partv_liable_unit_limb": liable,
    "band_5_8_area_limb_dependent": band_5_8,
    "grant_rate_liable_pct": round(g_liable[2], 1),
    "grant_rate_all_pct": round(g_all[2], 1),
    "by_year": by_year.to_dict("records"),
    "top_councils": by_council.to_dict("records"),
    "caveats": [
        "unit limb only; NumResidentialUnits null on ~44% of rows -> conservative floor",
        "AreaofSite unit-inconsistent (ha vs m2 vs garbage) -> >0.1ha limb NOT computed",
        "flags liability, not compliance (Part V agreement/exemption not in feed)",
        "pre-2021 area limb caught <9-unit schemes -> earlier years further undercounted",
    ],
}
(OUT_DIR / "partv_liability_summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
print(f"\nwrote {OUT_DIR / 'partv_liability_summary.json'}")
