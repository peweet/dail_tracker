"""SANDBOX: normalise the unit-inconsistent `AreaofSite` field to m², per council, and re-run the
Part V liability count WITH the >0.1 ha area limb (lifting the unit-limb-only floor from the prior probe).

WHY: AreaofSite is a DOUBLE recorded in DIFFERENT UNITS by different councils. Verified empirically:
  - 26 councils record HECTARES (one-off-house site reference ~0.1-0.4 — exactly right for a rural plot).
  - 4 Dublin-region councils record METRES² (Dublin City, South Dublin, Fingal, Dún Laoghaire-Rathdown
    — small-scheme site medians 260-720, i.e. m²).
  - Cork County records NO AreaofSite at all (all null/0) — cannot normalise; area limb N/A there.

METHOD (data-driven, self-validating):
  - per council, reference = median one-off-house AreaofSite, else median of 1-4 unit schemes.
  - ref < 5  -> hectares ; ref > 50 -> metres² ; in between -> ambiguous (none expected) ; no data -> none.
  - convert to site_m2 (ha -> ×10 000). Quarantine implausible: keep 10 m² .. 5,000,000 m² (=500 ha);
    anything outside is data-entry garbage and is NOT used for the area limb (flagged, not converted).
  - assert the split is {ha:26, m2:4, none:1} as a tripwire so a data refresh can't silently drift.

Part V liability (lifted): NumResidentialUnits >= 9  OR  (NumResidentialUnits 5-8 AND site_m2 > 1000).
Still a FLOOR (units null on ~44% of rows; null-unit residential schemes on >0.1 ha not added here).

Outputs: pipeline_sandbox/_planning_output/areaofsite_unit_map.json  (reusable normalisation key)
         pipeline_sandbox/_planning_output/partv_liability_area_limb.json
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
P = str(ROOT / "data" / "silver" / "parquet" / "planning_applications_silver.parquet").replace("\\", "/")
OUT = ROOT / "pipeline_sandbox" / "_planning_output"
OUT.mkdir(parents=True, exist_ok=True)
c = duckdb.connect()

# --- 1. per-council unit reference ----------------------------------------------------------------
ref = c.execute(f"""
  SELECT PlanningAuthority AS la,
    median(AreaofSite) FILTER (WHERE is_one_off_house AND AreaofSite>0)                      AS oneoff_med,
    median(AreaofSite) FILTER (WHERE AreaofSite>0 AND NumResidentialUnits BETWEEN 1 AND 4)   AS small_med,
    median(AreaofSite) FILTER (WHERE AreaofSite>0)                                           AS all_med,
    count(*)           FILTER (WHERE AreaofSite>0)                                           AS n_area
  FROM '{P}' GROUP BY 1 ORDER BY 1
""").df()

unit_map = {}
for r in ref.itertuples():
    # reference priority: one-off median (cleanest known-scale) -> 1-4 unit median -> overall median
    if r.oneoff_med == r.oneoff_med:
        refv, basis = r.oneoff_med, "one_off_median"
    elif r.small_med == r.small_med:
        refv, basis = r.small_med, "small_scheme_median"
    else:
        refv, basis = r.all_med, "overall_median"
    if r.n_area == 0 or refv != refv:
        unit, factor = "none", None
    elif refv < 5:
        unit, factor = "hectares", 10000
    elif refv > 50:
        unit, factor = "metres2", 1
    else:
        unit, factor = "ambiguous", None
    unit_map[r.la] = {"unit": unit, "to_m2_factor": factor, "reference_value": None if refv != refv else round(float(refv), 3),
                      "basis": basis, "n_area_rows": int(r.n_area)}

split = {}
for v in unit_map.values():
    split[v["unit"]] = split.get(v["unit"], 0) + 1
print("unit split:", split)
assert split.get("hectares") == 26 and split.get("metres2") == 4 and split.get("none") == 1, \
    f"unit split drifted from expected 26 ha / 4 m² / 1 none: {split}"
assert "ambiguous" not in split, f"ambiguous councils appeared: {split}"

(OUT / "areaofsite_unit_map.json").write_text(json.dumps(unit_map, indent=2), encoding="utf-8")

# --- 2. normalised site_m2 + Part V liability (area limb) -----------------------------------------
ha = "','".join(la for la, v in unit_map.items() if v["unit"] == "hectares")
m2 = "','".join(la for la, v in unit_map.items() if v["unit"] == "metres2")
# CASE -> site_m2 (NULL where unit unknown or implausible after conversion)
site_m2 = f"""
  CASE WHEN AreaofSite IS NULL OR AreaofSite<=0 THEN NULL
       WHEN PlanningAuthority IN ('{ha}') THEN AreaofSite*10000
       WHEN PlanningAuthority IN ('{m2}') THEN AreaofSite
       ELSE NULL END
"""
con_view = f"SELECT *, ({site_m2}) AS site_m2 FROM '{P}'"
c.execute(f"CREATE TEMP VIEW v AS SELECT *, CASE WHEN site_m2 BETWEEN 10 AND 5000000 THEN site_m2 ELSE NULL END AS site_m2_clean FROM ({con_view})")

def one(sql): return c.execute(sql).fetchone()[0]
total = one("SELECT count(*) FROM v")
quarantined = one("SELECT count(*) FROM v WHERE site_m2 IS NOT NULL AND site_m2_clean IS NULL")
unit_limb = one("SELECT count(*) FROM v WHERE NumResidentialUnits>=9")
area_added = one("SELECT count(*) FROM v WHERE NumResidentialUnits BETWEEN 5 AND 8 AND site_m2_clean>1000")
band_5_8_total = one("SELECT count(*) FROM v WHERE NumResidentialUnits BETWEEN 5 AND 8")
liable = one("SELECT count(*) FROM v WHERE NumResidentialUnits>=9 OR (NumResidentialUnits BETWEEN 5 AND 8 AND site_m2_clean>1000)")

def grate(w):
    d=one(f"SELECT count(*) FROM v WHERE decision_category IN ('granted','granted_conditional','refused') AND ({w})")
    g=one(f"SELECT count(*) FROM v WHERE decision_category IN ('granted','granted_conditional') AND ({w})")
    return g,d,round(100*g/d,1) if d else 0.0
g_liable=grate("NumResidentialUnits>=9 OR (NumResidentialUnits BETWEEN 5 AND 8 AND site_m2_clean>1000)")

print(f"\nPart V liability WITH area limb:")
print(f"  unit limb (>=9 units):                 {unit_limb:,}")
print(f"  + area limb (5-8 units & >0.1 ha):     {area_added:,}  (of {band_5_8_total:,} in the 5-8 band)")
print(f"  = total liable (lifted floor):         {liable:,}")
print(f"  grant rate of liable schemes:          {g_liable[2]}% ({g_liable[0]:,}/{g_liable[1]:,})")
print(f"  AreaofSite values quarantined (implausible after unit-convert): {quarantined:,}")

summary = {
    "unit_split": split,
    "unit_limb_9plus": unit_limb,
    "area_limb_added_5_8_over_0_1ha": area_added,
    "band_5_8_total": band_5_8_total,
    "total_liable_with_area_limb": liable,
    "prev_unit_limb_only_floor": unit_limb,
    "grant_rate_liable_pct": g_liable[2],
    "areaofsite_quarantined": quarantined,
    "caveats": [
        "site_m2 = AreaofSite normalised per-council (26 ha→×10000, 4 m²→×1, Cork County none)",
        "still a FLOOR: NumResidentialUnits null on ~44% of rows; null-unit residential schemes on >0.1 ha not added",
        "Cork County has no AreaofSite -> its 5-8 band cannot gain the area limb",
        "liability not compliance; quarantine window 10 m²..5,000,000 m²",
    ],
}
(OUT / "partv_liability_area_limb.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
print(f"\nwrote {OUT/'areaofsite_unit_map.json'}\nwrote {OUT/'partv_liability_area_limb.json'}")
