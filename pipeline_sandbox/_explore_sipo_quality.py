"""THROWAWAY data-quality exploration for the SIPO expenses fact.
Hunts outliers/anomalies BEYOND the flags, to (a) judge trustworthiness and
(b) decide the unit-test invariants. Reads the promoted gold parquet.

Run: ./.venv/Scripts/python.exe pipeline_sandbox/_explore_sipo_quality.py
"""
import sys; sys.stdout.reconfigure(encoding="utf-8")
import re
from pathlib import Path
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
df = pl.read_parquet(ROOT / "data/gold/parquet/sipo_expenses_fact.parquet")
constit = pl.read_parquet(ROOT / "data/gold/parquet/ec_constituency_pop_2022.parquet")
VALID_CONST = set(constit["constituency_name"].to_list())
LIMITS = {3: 38900, 4: 48600, 5: 58350}


def hr(t): print(f"\n{'='*70}\n{t}\n{'='*70}")


hr("SHAPE")
print(f"{df.height} rows, {df['party'].n_unique()} parties")
print("columns:", df.columns)

hr("1. FLAG DISTRIBUTION")
print(df["flag"].value_counts().sort("count", descending=True))

hr("2. CONSTITUENCY VALIDITY (must be in the 43 closed set)")
bad = df.filter(~pl.col("constituency").is_in(list(VALID_CONST)))
print(f"rows with constituency NOT in closed set: {bad.height}")
if bad.height:
    print(bad.select("party", "candidate_name", "constituency").head(10) if "candidate_name" in df.columns else bad.select("party","constituency").head(10))

hr("3. CONSTITUENCY OVER-COUNTS per party (>3 candidates in one constituency = likely mis-match)")
name_col = "candidate_name" if "candidate_name" in df.columns else "candidate_name_raw"
oc = (df.group_by("party", "constituency").len()
      .filter(pl.col("len") > 3).sort("len", descending=True))
print(oc if oc.height else "none > 3")

hr("4. DUPLICATE CANDIDATES across parties (a person runs for ONE party)")
dup = (df.group_by(name_col).agg(pl.col("party").n_unique().alias("n_parties"),
                                 pl.col("party").unique().alias("parties"))
       .filter(pl.col("n_parties") > 1))
print(f"names in >1 party: {dup.height}")
if dup.height:
    print(dup.head(15))

hr("5. NAME QUALITY (non-person-looking names: digits, very short, header-like)")
suspicious = df.filter(
    (pl.col(name_col).str.len_chars() < 4)
    | pl.col(name_col).str.contains(r"\d")
    | pl.col(name_col).str.to_lowercase().is_in(["total", "candidate name", "constituency", "name", ""])
)
print(f"suspicious names: {suspicious.height}")
if suspicious.height:
    print(suspicious.select("party", name_col, "constituency", "expenditure_eur").head(20))

hr("6. AMOUNT INVARIANTS")
le = df.filter(pl.col("expenditure_eur").is_not_null())
print(f"expenditure > statutory_limit (impossible): {le.filter(pl.col('expenditure_eur') > pl.col('statutory_limit_eur')).height}")
print(f"expenditure > assigned*1.02: {le.filter((pl.col('amount_assigned_eur').is_not_null()) & (pl.col('expenditure_eur') > pl.col('amount_assigned_eur')*1.02)).height}")
print(f"assigned > statutory_limit (impossible): {df.filter((pl.col('amount_assigned_eur').is_not_null()) & (pl.col('amount_assigned_eur') > pl.col('statutory_limit_eur'))).height}")
print(f"expenditure == 0 (valid zero spend): {le.filter(pl.col('expenditure_eur')==0).height}")
print(f"expenditure negative: {le.filter(pl.col('expenditure_eur')<0).height}")
print(f"expenditure tiny (0<x<1): {le.filter((pl.col('expenditure_eur')>0)&(pl.col('expenditure_eur')<1)).height}")

hr("7. THE FLAGGED ROWS (over_limit + spend_gt_assigned) — are they genuine garbage?")
flagged = df.filter(pl.col("flag").is_in(["over_limit_verify", "spend_gt_assigned_verify"]))
print(flagged.select("party", name_col, "constituency", "amount_assigned_eur", "expenditure_eur", "statutory_limit_eur", "flag"))

hr("8. ASSIGNED-AMOUNT DISTRIBUTION per party (should be a small set of tiers)")
for p in df["party"].unique().sort():
    vals = df.filter((pl.col("party")==p) & pl.col("amount_assigned_eur").is_not_null())["amount_assigned_eur"]
    uniq = sorted(set(vals.to_list()))
    print(f"  {p}: {len(uniq)} distinct assigned values{' ' if len(uniq)<=8 else ' (sample) '}{uniq[:8]}")

hr("9. COVERAGE: candidates per party + constituency coverage")
cov = df.group_by("party").agg(
    pl.len().alias("rows"),
    pl.col("constituency").n_unique().alias("constituencies"),
    pl.col("expenditure_eur").is_not_null().sum().alias("with_amt"),
).sort("rows", descending=True)
print(cov)

hr("10. CONFIDENCE OUTLIERS (lowest 10 by expenditure_confidence)")
print(df.filter(pl.col("expenditure_confidence").is_not_null())
      .sort("expenditure_confidence").select("party", name_col, "expenditure_eur", "expenditure_confidence", "flag").head(10))

hr("11. EXACT expenditure == assigned (spent EXACTLY the assignment — suspicious?)")
exact = le.filter((pl.col("amount_assigned_eur").is_not_null()) & (pl.col("expenditure_eur")==pl.col("amount_assigned_eur")))
print(f"count: {exact.height}")
if exact.height:
    print(exact.select("party", name_col, "amount_assigned_eur", "expenditure_eur").head(10))
