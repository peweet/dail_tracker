import sys
sys.stdout.reconfigure(encoding="utf-8")
import re
import polars as pl

SRC = "c:/Users/pglyn/PycharmProjects/dail_extractor/data/raw_bq/bq-results-20260619-122315-1781871808837.csv"
OUT = "c:/Users/pglyn/PycharmProjects/dail_extractor/pipeline_sandbox/disclosed_po_spend"

df = pl.read_csv(SRC, schema_overrides={"Total": pl.Float64, "Year": pl.Int64})
print("rows", df.height, "cols", df.columns)

# --- normalise body + supplier ---
def norm_body(col):
    return (
        col.str.replace(r"(?i)^\s*Agency\s*:\s*", "")
           .str.strip_chars()
    )

# rough supplier normalisation: lowercase, strip legal suffixes/punct, collapse ws
SUFFIX = r"\b(ltd|limited|plc|llp|lp|teo|teoranta|uc|dac|clg|company|co|incorporated|inc|the)\b"
def norm_supplier_expr(col):
    e = col.str.to_lowercase()
    e = e.str.replace_all(r"[\.\,&/'\"()\-]", " ")
    e = e.str.replace_all(SUFFIX, " ")
    e = e.str.replace_all(r"\s+", " ")
    e = e.str.strip_chars()
    return e

df = df.with_columns(
    norm_body(pl.col("entity")).alias("body"),
    norm_supplier_expr(pl.col("Supplier")).alias("supplier_norm"),
    pl.col("Supplier").str.strip_chars().alias("supplier_raw"),
)
# drop empty / null suppliers from supplier-level analysis
df = df.with_columns(
    pl.when(pl.col("supplier_norm").str.len_chars() == 0)
      .then(None)
      .otherwise(pl.col("supplier_norm"))
      .alias("supplier_norm")
)

total_gross = df["Total"].sum()
print("total gross all rows EUR", total_gross)

# --- detect regime per body: share of blank PO ---
regime = (
    df.with_columns((pl.col("PO").is_null() | (pl.col("PO").str.strip_chars().str.len_chars() == 0)).alias("po_blank"))
    .group_by("body")
    .agg(
        pl.col("po_blank").mean().alias("blank_po_share"),
        pl.col("Total").sum().alias("body_gross"),
        pl.len().alias("n_rows"),
    )
    .with_columns(
        pl.when(pl.col("blank_po_share") >= 0.5).then(pl.lit("payments"))
          .otherwise(pl.lit("purchase_orders")).alias("regime")
    )
    .sort("body_gross", descending=True)
)
regime.write_csv(f"{OUT}/body_regime.csv")
print(regime.head(20))

# ============================================================
# 1) TOP SUPPLIERS overall by gross line value + n_bodies
# ============================================================
sup = df.filter(pl.col("supplier_norm").is_not_null())
top_suppliers = (
    sup.group_by("supplier_norm")
    .agg(
        pl.col("Total").sum().alias("gross_eur"),
        pl.col("body").n_unique().alias("n_bodies"),
        pl.len().alias("n_lines"),
        pl.col("supplier_raw").mode().first().alias("example_raw"),
    )
    .sort("gross_eur", descending=True)
)
top_suppliers.head(60).write_csv(f"{OUT}/top_suppliers_overall.csv")
print("\n=== TOP 15 SUPPLIERS OVERALL (gross line value, mixes regimes) ===")
print(top_suppliers.head(15))

# ============================================================
# 2) Supplier concentration WITHIN selected large bodies
# ============================================================
# pick largest bodies, but ensure named anchors included
target_substrings = [
    "health service executive", "irish water", "uisce", "dublin city council",
    "office of public works", "opw", "children", "disability", "equality",
    "transport infrastructure", "defence", "eirgrid", "gas networks",
    "garda", "central bank", "nama", "national asset", "education and training",
]
body_gross = regime.select("body", "body_gross", "regime")
# top 12 by gross
big_bodies = body_gross.head(20)["body"].to_list()
# add anchors found by substring
all_bodies = body_gross["body"].to_list()
for ss in target_substrings:
    for b in all_bodies:
        if ss in b.lower() and b not in big_bodies:
            big_bodies.append(b)
            break

conc_rows = []
for b in big_bodies:
    bdf = sup.filter(pl.col("body") == b)
    bg = bdf["Total"].sum()
    if bg <= 0:
        continue
    by_sup = (
        bdf.group_by("supplier_norm")
        .agg(pl.col("Total").sum().alias("g"), pl.col("supplier_raw").mode().first().alias("raw"))
        .sort("g", descending=True)
    )
    top5 = by_sup.head(5)["g"].sum()
    top_sup_raw = by_sup["raw"][0]
    top_sup_g = by_sup["g"][0]
    n_sup = by_sup.height
    reg = regime.filter(pl.col("body") == b)["regime"][0]
    conc_rows.append({
        "body": b,
        "regime": reg,
        "body_gross_eur": bg,
        "n_suppliers": n_sup,
        "top_supplier": top_sup_raw,
        "top_supplier_gross_eur": top_sup_g,
        "top1_share_pct": round(100 * top_sup_g / bg, 2),
        "top5_share_pct": round(100 * top5 / bg, 2),
    })

conc = pl.DataFrame(conc_rows).sort("body_gross_eur", descending=True)
conc.write_csv(f"{OUT}/body_concentration.csv")
print("\n=== SUPPLIER CONCENTRATION WITHIN BODY (regime-safe) ===")
print(conc)

# ============================================================
# 3) Biggest single individual lines
# ============================================================
biggest = (
    df.select("body", "supplier_raw", "Total", "Description", "Year", "QTR", "PO")
    .sort("Total", descending=True)
    .head(40)
)
biggest.write_csv(f"{OUT}/biggest_lines.csv")
print("\n=== BIGGEST SINGLE LINES ===")
with pl.Config(fmt_str_lengths=80, tbl_rows=15):
    print(biggest.head(15))

# ============================================================
# Combined market_structure.csv (fuller breakdown)
# ============================================================
top_suppliers.head(100).with_columns(pl.lit("top_supplier_overall").alias("section")).select(
    "section",
    pl.col("example_raw").alias("name"),
    pl.col("gross_eur"),
    pl.col("n_bodies"),
    pl.col("n_lines"),
).write_csv(f"{OUT}/market_structure.csv")
print("\nwrote market_structure.csv")
