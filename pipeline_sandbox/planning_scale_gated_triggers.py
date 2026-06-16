"""SANDBOX: standards-vs-outcome for SCALE-GATED obligations beyond Part V.

Generalises the Part V probe: several DM obligations switch on above a development-size threshold, and
those thresholds are computable from the feed (NumResidentialUnits, FloorArea, and the per-council
unit-normalised site_m2 from planning_areaofsite_normalise.py). For each trigger we flag the applications
that CROSS the threshold (= owe the obligation) and report count + grant rate + leading councils.

Each trigger carries an explicit `source` + `confidence`, because they are NOT equally authoritative:
  - part_v        statutory   (PDA 2000 Part V; verified earlier)
  - childcare     guideline   (DoEHLG Childcare Facilities Guidelines 2001 = 1 facility/20 places per 75
                               dwellings; the "75 units" figure is verbatim in our own matrix for
                               Kildare/Mayo/Monaghan/Cork City etc.)
  - social_infra  council     (community & social-infrastructure audit at 50+ units — e.g. Fingal DMSO80,
                               Dublin City — verbatim in matrix; NOT national, a common council threshold)
  - eia_resi      statutory*  (mandatory EIA, Planning & Development Regs 2001 Sch.5 Pt2 Class 10(b):
                               commonly cited as >500 dwellings OR urban development site >10 ha.
                               *EXACT FIGURE NOT VERIFIED against Schedule 5 in this pass — flagged.)
  - tta           proxy       (Traffic & Transport Assessment; TII TTA Guidelines are trip-based, here
                               proxied at >=200 dwellings — a SCALE PROXY, not a council/statutory number)

CAVEATS (same family as the Part V probe): NumResidentialUnits null on ~44% of rows -> every count is a
FLOOR; flags liability/obligation, not compliance; commercial-only schemes (FloorArea-driven TTA/EIA
thresholds) are NOT modelled here — residential unit/site limbs only.

Outputs: pipeline_sandbox/_planning_output/scale_gated_triggers_summary.json
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
P = str(ROOT / "data" / "silver" / "parquet" / "planning_applications_silver.parquet").replace("\\", "/")
OUT = ROOT / "pipeline_sandbox" / "_planning_output"
UNIT_MAP = OUT / "areaofsite_unit_map.json"
c = duckdb.connect()

# build site_m2 from the per-council unit map (run planning_areaofsite_normalise.py first)
um = json.loads(UNIT_MAP.read_text(encoding="utf-8"))
ha = "','".join(la for la, v in um.items() if v["unit"] == "hectares")
m2 = "','".join(la for la, v in um.items() if v["unit"] == "metres2")
site_m2 = (f"CASE WHEN AreaofSite IS NULL OR AreaofSite<=0 THEN NULL "
           f"WHEN PlanningAuthority IN ('{ha}') THEN AreaofSite*10000 "
           f"WHEN PlanningAuthority IN ('{m2}') THEN AreaofSite ELSE NULL END")
c.execute(f"""CREATE TEMP VIEW v AS
  SELECT *, CASE WHEN ({site_m2}) BETWEEN 10 AND 5000000 THEN ({site_m2}) ELSE NULL END AS site_m2_clean
  FROM '{P}'""")

GRANT = "decision_category IN ('granted','granted_conditional')"
DECIDED = "decision_category IN ('granted','granted_conditional','refused')"

TRIGGERS = [
    ("part_v", "Part V social/affordable housing",
     "NumResidentialUnits>=9 OR (NumResidentialUnits BETWEEN 5 AND 8 AND site_m2_clean>1000)",
     ">=9 units, or 5-8 units on >0.1 ha", "PDA 2000 Part V (as amended)", "statutory"),
    ("childcare", "Childcare facility provision",
     "NumResidentialUnits>=75",
     ">=75 dwellings (1 facility / 20 places)", "DoEHLG Childcare Facilities Guidelines 2001", "guideline"),
    ("social_infra", "Community & social-infrastructure audit",
     "NumResidentialUnits>=50",
     ">=50 dwellings", "Council DM standard (e.g. Fingal DMSO80 / Dublin City)", "council-common"),
    # EIA: dwellings limb ONLY. The Sch.5 ">10 ha urban development" limb is NOT applied — gating on
    # site area alone wrongly catches large NON-urban sites (agricultural/extractive), and the feed has
    # no clean "urban development" type flag to restrict it. So this is the residential dwellings limb only.
    ("eia_resi", "Mandatory EIA (residential)",
     "NumResidentialUnits>=500",
     ">=500 dwellings (area limb excluded)", "P&D Regs 2001 Sch.5 Pt2 Class 10(b)", "statutory-UNVERIFIED-figure"),
    ("tta", "Traffic & Transport Assessment",
     "NumResidentialUnits>=200",
     ">=200 dwellings (scale proxy)", "TII TTA Guidelines (trip-based; proxied)", "guideline-proxy"),
]


def one(sql: str):
    return c.execute(sql).fetchone()[0]

total = one("SELECT count(*) FROM v")
results = []
print(f"Corpus: {total:,} applications  (NumResidentialUnits null on ~44% -> all counts are FLOORS)\n")
print(f"{'trigger':<34}{'threshold':<34}{'liable':>8}{'grant%':>8}  confidence")
print("-" * 110)
for key, label, pred, thr, source, conf in TRIGGERS:
    liable = one(f"SELECT count(*) FROM v WHERE {pred}")
    dec = one(f"SELECT count(*) FROM v WHERE ({pred}) AND {DECIDED}")
    gr = one(f"SELECT count(*) FROM v WHERE ({pred}) AND {GRANT}")
    grant_pct = round(100 * gr / dec, 1) if dec else None
    top = c.execute(f"""SELECT PlanningAuthority, count(*) n FROM v WHERE {pred}
                        GROUP BY 1 ORDER BY n DESC LIMIT 3""").fetchall()
    print(f"{label:<34}{thr:<34}{liable:>8,}{(str(grant_pct)+'%' if grant_pct is not None else '—'):>8}  {conf}")
    results.append({"key": key, "label": label, "threshold": thr, "source": source, "confidence": conf,
                    "liable": liable, "grant_rate_pct": grant_pct,
                    "top_councils": [{"la": t[0], "n": t[1]} for t in top]})

# obligation stacking — how many of the 4 non-Part-V scale triggers a scheme crosses
stack = c.execute(f"""
  SELECT (CASE WHEN NumResidentialUnits>=50 THEN 1 ELSE 0 END
        + CASE WHEN NumResidentialUnits>=75 THEN 1 ELSE 0 END
        + CASE WHEN NumResidentialUnits>=200 THEN 1 ELSE 0 END
        + CASE WHEN NumResidentialUnits>=500 THEN 1 ELSE 0 END) AS n_triggers,
        count(*) n
  FROM v WHERE NumResidentialUnits>=50
  GROUP BY 1 ORDER BY 1
""").df()
print("\nObligation stacking (residential schemes >=50 units / >=10 ha — how many of the 4 scale triggers each crosses):")
print(stack.to_string(index=False))

summary = {"corpus_rows": total, "triggers": results,
           "obligation_stacking": stack.to_dict("records"),
           "caveats": [
               "NumResidentialUnits null on ~44% of rows -> every count is a floor",
               "obligation/liability, not compliance",
               "residential unit limbs only; FloorArea-driven commercial TTA/EIA thresholds not modelled",
               "eia_resi >=500 dwellings figure NOT verified against Schedule 5; the >10 ha urban-development limb is EXCLUDED (area alone catches non-urban sites, no clean type flag in feed)",
           ]}
(OUT / "scale_gated_triggers_summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
print(f"\nwrote {OUT / 'scale_gated_triggers_summary.json'}")
