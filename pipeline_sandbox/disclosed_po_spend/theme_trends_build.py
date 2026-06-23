"""WHAT CAN BE DETERMINED #3 - CATEGORY / SPEND THEMES OVER TIME.

Sandbox-only. READ-ONLY against:
  data/raw_bq/bq-results-20260619-122315-1781871808837.csv  (disclosed national PO/payments-over-EUR-20k)
  data/gold/parquet/procurement_payments_fact.parquet        (our parsed fact, for per-body semantics)

Writes ONLY under pipeline_sandbox/disclosed_po_spend/.

Theme classification = keyword regex over Description (and Supplier as fallback for a few themes).
Each row is assigned to AT MOST ONE theme (first match wins, priority-ordered) so the theme x year
matrix never double-counts a euro line.

CRITICAL SEMANTICS RULE (from prior workflow): payment-list bodies and PO-commitment bodies must NOT be
summed as one "spend" figure. We split every theme x year cell by regime:
  - payment_actual  : money actually paid (HSE, OPW, Education, DCEDIY, DECC, Revenue, TII, ...)
  - po_committed    : purchase-order commitments raised (Justice, Defence, NTA, Tusla, all LAs, ...)
  - aggregated_rollup : utility/regulator per-category quarterly buckets, NOT EUR-20k line items
                        (Irish Water, EirGrid, Gas Networks, Central Bank, ESB, RTE) -> EXCLUDED from
                        any theme spend; reported separately as a memo only.
Regime per body = our fact's amount_semantics where the body is anchored; else the blank-PO heuristic
(>=95% blank PO -> payment-ish, else po-ish) flagged regime_source='heuristic_uncertain'.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")
from pathlib import Path
import polars as pl

ROOT = Path("c:/Users/pglyn/PycharmProjects/dail_extractor")
SRC = ROOT / "data/raw_bq/bq-results-20260619-122315-1781871808837.csv"
FACT = ROOT / "data/gold/parquet/procurement_payments_fact.parquet"
OUT = ROOT / "pipeline_sandbox/disclosed_po_spend"

# ---- load ----
df = pl.read_csv(SRC, schema_overrides={"Total": pl.Float64, "Year": pl.Int64})

# clean entity: strip 'Agency : ' and 'Section 38 : ' (incl U+202F narrow no-break space variant)
df = df.with_columns(
    pl.col("entity")
    .str.replace_all(" ", " ")
    .str.replace(r"^\s*Agency\s*:\s*", "")
    .str.replace(r"^\s*Section 38\s*:\s*", "")
    .str.strip_chars()
    .alias("entity_clean")
)

# ---- per-body regime ----
# 1) anchor from our fact: map BQ entity_clean -> our publisher amount_semantics.
fact = pl.read_parquet(FACT, columns=["publisher_name", "amount_semantics"])
fact_sem = fact.group_by("publisher_name").agg(pl.col("amount_semantics").first())

# crosswalk BQ entity_clean -> our publisher_name (only the meaningful/large ones; rest fall to heuristic)
XWALK = {
    "Health Service Executive": "Health Service Executive",
    "Transport Infrastructure Ireland": "Transport Infrastructure Ireland",
    "Department of Children, Equality, Disability, Integration and Youth": "Department of Children, Disability and Equality",
    "Department of Education": "Department of Education and Youth",
    "Office of Public Works": "Office of Public Works",
    "Department of Justice": "Department of Justice, Home Affairs and Migration",
    "SEAI": "Sustainable Energy Authority of Ireland (SEAI)",
    "Department of Defence": "Department of Defence",
    "National Transport Authority": "National Transport Authority",
    "TUSLA": "Tusla – Child and Family Agency",
    "Department of the Environment, Climate and Communications": "Dept of Climate, Energy and the Environment",
    "Department of Agriculture, Food and the Marine": "Department of Agriculture, Food and the Marine",
    "Revenue": "Revenue Commissioners",
    "Department of Transport": "Department of Transport",
    "Department of Social Protection": "Department of Social Protection",
    "Department of Foreign Affairs": "Department of Foreign Affairs and Trade",
    "Department of Health": "Department of Health",
    "Department of Finance": "Department of Finance",
    "Bord Bia": "Bord Bia",
    "Enterprise Ireland": "Enterprise Ireland",
    "Teagasc": "Teagasc",
    "Marine Institute": "Marine Institute",
    "Irish Prison Service": "Irish Prison Service",
    "The Courts Services": "Courts Service of Ireland",
    "National Paediatric Hospital Board": "National Paediatric Hospital Development Board",
    "Pobal": "Pobal",
    "Section 38 : Beaumont Hospital": "Beaumont Hospital",
    "Beaumont Hospital": "Beaumont Hospital",
    # councils (BQ long name -> our short)
    "Cork City Council": "Cork City",
    "Cork County Council": "Cork County",
    "Limerick City and County Council": "Limerick",
    "Mayo County Council": "Mayo",
    "South Dublin County Council": "South Dublin",
    "Meath County Council": "Meath",
    "Fingal County Council": "Fingal",
    "Galway County Council": "Galway County",
    "Galway City Council": "Galway City",
    "Kildare County Council": "Kildare",
    "Donegal County Council": "Donegal",
    "Waterford City and County Council": "Waterford",
    "Wexford County Council": "Wexford",
    "Kilkenny County Council": "Kilkenny",
    "Clare County Council": "Clare",
    "Wicklow County Council": "Wicklow",
    "Westmeath County Council": "Westmeath",
    "Monaghan County Council": "Monaghan",
    "Sligo County Council": "Sligo",
    "Longford County Council": "Longford",
    "Laois County Council": "Laois",
    "Offaly County Council": "Offaly",
    "Leitrim County Council": "Leitrim",
}

# bodies that are aggregated category roll-ups (NOT EUR-20k line items): utilities + commercial semis
AGG_ROLLUP = {
    "Irish Water", "Eirgrid", "Gas Networks Ireland", "Central Bank of Ireland",
    "ESB", "RTE",
}

sem_map = dict(zip(fact_sem["publisher_name"], fact_sem["amount_semantics"]))

# build per-entity regime table
ent_stats = (
    df.group_by("entity_clean")
    .agg(
        pl.len().alias("rows"),
        (pl.col("PO").fill_null("").str.strip_chars() == "").mean().alias("blank_po_frac"),
    )
)


def regime_for(entity_clean: str) -> tuple[str, str]:
    if entity_clean in AGG_ROLLUP:
        return ("aggregated_rollup", "rollup_known")
    ours = XWALK.get(entity_clean)
    if ours and ours in sem_map:
        return (sem_map[ours], "fact_anchored")
    return ("__heuristic__", "heuristic_uncertain")


reg_rows = []
for r in ent_stats.iter_rows(named=True):
    ec = r["entity_clean"]
    reg, src = regime_for(ec)
    if reg == "__heuristic__":
        reg = "payment_actual" if r["blank_po_frac"] >= 0.95 else "po_committed"
    reg_rows.append({"entity_clean": ec, "regime": reg, "regime_source": src,
                     "blank_po_frac": r["blank_po_frac"], "body_rows": r["rows"]})
reg_df = pl.DataFrame(reg_rows)
reg_df.write_csv(OUT / "body_regime_crosswalk.csv")

df = df.join(reg_df.select("entity_clean", "regime", "regime_source"), on="entity_clean", how="left")

# ---- theme classifier (priority-ordered, first match wins) ----
# patterns over Description (lowercased). A few themes also look at Supplier.
THEMES = [
    # (theme, description_regex, supplier_regex_or_None)
    # asylum/IP/Ukraine: DCEDIY + DoJ migration lines. NOTE: 'emergency accommodation' is
    # deliberately EXCLUDED here -> it is council HOMELESSNESS spend (DCC EUR 238m etc.), a
    # different theme; it gets its own bucket below so it is not mis-attributed to asylum.
    ("asylum_ip_ukraine_accommodation",
     r"(ukraine|ukr |\bukr\b|ip accom|ipas|international protection|asylum|direct provision|providing accommodation|provision of accommodation and services|refugee|beneficiaries of temporary protection|\bbotp\b|separated children seeking)",
     None),
    ("homeless_emergency_accommodation",
     r"(emergency accommodation|homeless|family hub|private emergency|temporary emergency accom)",
     None),
    ("agency_locum_temporary_staff",
     r"(agency staff|agency nurs|agency work|locum|temporary staff|temp staff|temporary clerical|relief staff|substitute|bank staff|recruitment agenc|contract staff|external (it )?resourc)",
     None),
    ("management_consultancy",
     r"(management consult|consultancy|consulting services|advisory services|business consult|strategy consult|organisational review)",
     None),
    ("legal_solicitor_barrister",
     r"(legal (services|fees|costs|advice)|legal serv|solicitor|barrister|counsel fee|legal counsel|conveyanc|legal expenses|\blaw \b|legal & |legal and )",
     None),
    ("medical_drugs_pharma",
     r"(drugs? & medicin|drugs and medicin|medicine|pharmac|medical suppl|medical equip|surgical|vaccine|blood product|clinical suppl|laborator|reagent|diagnostic|patient|prosthe|orthotic|dental suppl)",
     None),
    ("it_software_ict",
     r"(\bict\b|software|\bit \b|information (and )?(comms?|communicat)|telecom|computer|hardware|licen[sc]e.*soft|cloud|data centre|cyber|server|network infrastruct|managed it|it servic|digital service)",
     None),
    ("construction_building",
     r"(construction|building (works|maintenance|modif|project)|capital (works|contract|project)|civil (work|engineer)|refurbish|school building|housing construction|road (works|grant|construction)|main contract|capital contract|works/maintenance|engineering (professional )?serv|infrastructur|capital works)",
     None),
    ("energy_electricity_gas",
     r"(electricit|\bgas\b|energy (suppl|cost|cred|warmer)|natural gas|heating (oil|fuel)|\bfuel\b|esb |better energy|warmer homes|utilities|power (suppl|generat))",
     None),
    ("cleaning_facilities",
     r"(cleaning|facilit(y|ies) (management|services|maintenance)|janitor|waste (collect|disposal|management|services)|sludge|grounds maintenance|pest control|landscap|caretak|catering|canteen)",
     None),
    ("transport_fleet",
     r"(\bfleet\b|vehicle|\bbus\b|public transport|rail |\btaxi\b|helicopter|air corps|aircraft|haulage|courier|fuel for vehicle|car hire|transport service)",
     None),
    ("security",
     r"(security (services|guard|staff)|\bcctv\b|access control|manned guard|alarm monitor|cash in transit|garda |patrol)",
     None),
    ("advertising_pr_media",
     r"(advertis|public relations|\bpr \b|media (buying|campaign)|marketing|communications campaign|publicity|promotion|press)",
     None),
    ("rent_property",
     r"(\brent\b|rental|lease|accommodation lease|property (rent|lease)|office accommodation)",
     None),
]

desc_l = pl.col("Description").fill_null("").str.to_lowercase()
sup_l = pl.col("Supplier").fill_null("").str.to_lowercase()

theme_expr = pl.lit("other")
for theme, dre, sre in reversed(THEMES):  # reverse so first listed wins via successive when/otherwise
    cond = desc_l.str.contains(dre)
    if sre:
        cond = cond | sup_l.str.contains(sre)
    theme_expr = pl.when(cond).then(pl.lit(theme)).otherwise(theme_expr)

df = df.with_columns(theme_expr.alias("theme"))

# ---- theme x year x regime matrix ----
mat = (
    df.group_by("theme", "Year", "regime")
    .agg(pl.col("Total").sum().alias("gross_eur"), pl.len().alias("rows"))
    .sort("theme", "Year", "regime")
)
mat.write_csv(OUT / "theme_year_regime_long.csv")

# wide: theme x year (gross) for the main task deliverable, with regime split as separate sheets.
# Primary deliverable theme_trends.csv = theme x year, line-value gross, with a regime-mix note column.
yrs = sorted([y for y in df["Year"].unique().to_list() if y is not None])


def pivot_for(frame: pl.DataFrame, label: str) -> pl.DataFrame:
    p = (
        frame.group_by("theme", "Year").agg(pl.col("Total").sum().alias("g"))
        .pivot(values="g", index="theme", on="Year")
    )
    # ensure all year cols present, ordered
    for y in yrs:
        if str(y) not in p.columns:
            p = p.with_columns(pl.lit(None).cast(pl.Float64).alias(str(y)))
    p = p.select(["theme"] + [str(y) for y in yrs])
    p = p.with_columns(pl.lit(label).alias("regime"))
    return p


# regime-specific pivots (these ARE comparable spend within regime)
pay = pivot_for(df.filter(pl.col("regime") == "payment_actual"), "payment_actual")
po = pivot_for(df.filter(pl.col("regime") == "po_committed"), "po_committed")
rollup = pivot_for(df.filter(pl.col("regime") == "aggregated_rollup"), "aggregated_rollup")
allmix = pivot_for(df, "ALL_gross_line_value_NOT_spend")

combined = pl.concat([pay, po, rollup, allmix]).select(
    ["regime", "theme"] + [str(y) for y in yrs]
)
combined.write_csv(OUT / "theme_trends.csv")

# ---- early vs late trend table (2018 vs 2024 comparable full years) within each regime ----
EARLY, LATE = "2018", "2024"
trend_rows = []
for label, frame in [("payment_actual", df.filter(pl.col("regime") == "payment_actual")),
                     ("po_committed", df.filter(pl.col("regime") == "po_committed")),
                     ("aggregated_rollup", df.filter(pl.col("regime") == "aggregated_rollup")),
                     ("ALL_gross_line_value", df)]:
    p = pivot_for(frame, label)
    for row in p.iter_rows(named=True):
        e = row.get(EARLY) or 0.0
        l = row.get(LATE) or 0.0
        # also full-series total + peak year
        series = {y: (row.get(str(y)) or 0.0) for y in yrs}
        total = sum(series.values())
        peak_year = max(series, key=series.get) if series else None
        delta = l - e
        pct = (l / e - 1.0) * 100 if e > 0 else None
        trend_rows.append({
            "regime": label, "theme": row["theme"],
            "y2018": round(e, 0), "y2024": round(l, 0),
            "delta_2018_2024": round(delta, 0),
            "pct_2018_2024": round(pct, 1) if pct is not None else None,
            "series_total": round(total, 0), "peak_year": peak_year,
        })
trend = pl.DataFrame(trend_rows).sort("regime", "series_total", descending=[False, True])
trend.write_csv(OUT / "theme_trend_early_late.csv")

# ---- print summaries for the agent ----
print("=== rows by regime ===")
print(df.group_by("regime").agg(pl.col("Total").sum().alias("gross"), pl.len().alias("rows")).sort("gross", descending=True))

print("\n=== theme gross by regime (series total) ===")
with pl.Config(tbl_rows=80, fmt_str_lengths=45):
    print(trend.filter(pl.col("regime") != "ALL_gross_line_value")
          .select("regime", "theme", "y2018", "y2024", "pct_2018_2024", "series_total"))

print("\n=== 'other' (unclassified) share check ===")
oth = df.group_by(pl.col("theme") == "other").agg(pl.col("Total").sum().alias("g"), pl.len().alias("n"))
print(oth)

print("\n=== ASYLUM/IP/UKRAINE by year (payment_actual = real spend, DCEDIY-led) ===")
asy = (df.filter(pl.col("theme") == "asylum_ip_ukraine_accommodation")
       .group_by("Year", "regime").agg(pl.col("Total").sum().alias("g"), pl.len().alias("n")).sort("Year", "regime"))
with pl.Config(tbl_rows=60):
    print(asy)

print("\n=== top bodies inside asylum theme ===")
print(df.filter(pl.col("theme") == "asylum_ip_ukraine_accommodation")
      .group_by("entity_clean", "regime").agg(pl.col("Total").sum().alias("g")).sort("g", descending=True).head(8))

print("\nWROTE:")
for f in ["theme_trends.csv", "theme_year_regime_long.csv", "theme_trend_early_late.csv", "body_regime_crosswalk.csv"]:
    print(" ", OUT / f)
