import sys
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl

SRC = "c:/Users/pglyn/PycharmProjects/dail_extractor/data/raw_bq/bq-results-20260619-122315-1781871808837.csv"
OUT = "c:/Users/pglyn/PycharmProjects/dail_extractor/pipeline_sandbox/disclosed_po_spend"

df = pl.read_csv(SRC, schema_overrides={"Total": pl.Float64, "Year": pl.Int64})
SUFFIX = r"\b(ltd|limited|plc|llp|lp|teo|teoranta|uc|dac|clg|company|co|incorporated|inc|the)\b"
df = df.with_columns(
    pl.col("entity").str.replace(r"(?i)^\s*Agency\s*:\s*", "").str.strip_chars().alias("body"),
    pl.col("Supplier").str.strip_chars().alias("supplier_raw"),
)
df = df.with_columns(
    pl.col("supplier_raw").str.to_lowercase()
      .str.replace_all(r"[\.\,&/'\"()\-]", " ")
      .str.replace_all(SUFFIX, " ")
      .str.replace_all(r"\s+", " ").str.strip_chars().alias("supplier_norm")
)

# regime per body
regime = (
    df.with_columns((pl.col("PO").is_null() | (pl.col("PO").str.strip_chars().str.len_chars() == 0)).alias("po_blank"))
    .group_by("body")
    .agg(pl.col("po_blank").mean().alias("blank_po_share"), pl.col("Total").sum().alias("body_gross"))
    .with_columns(pl.when(pl.col("blank_po_share") >= 0.5).then(pl.lit("payments")).otherwise(pl.lit("purchase_orders")).alias("regime"))
)

# selected genuinely-large bodies (manual anchor list, all multi-100m+)
selected = [
    "Health Service Executive",
    "Irish Water",
    "Transport Infrastructure Ireland",
    "Department of Children, Equality, Disability, Integration and Youth",  # may differ
    "Dublin City Council",
    "Department of Education",
    "Office of Public Works",
    "Department of Justice",
    "An Garda Síochána",
    "National Transport Authority",
    "Department of Defence",
    "TUSLA",
    "Revenue",
    "Central Bank of Ireland",
]
all_bodies = regime["body"].to_list()
def resolve(name):
    if name in all_bodies:
        return name
    # substring fallback
    key = name.split(",")[0].lower()
    for b in all_bodies:
        if key in b.lower():
            return b
    return None

rows = []
for name in selected:
    b = resolve(name)
    if b is None:
        print("MISSING", name); continue
    bdf = df.filter(pl.col("body") == b)
    full_gross = bdf["Total"].sum()
    # supplier-attributable portion = rows with a real named supplier
    named = bdf.filter(pl.col("supplier_norm").is_not_null() & (pl.col("supplier_norm").str.len_chars() > 1))
    attributable = named["Total"].sum()
    by_sup = (named.group_by("supplier_norm")
              .agg(pl.col("Total").sum().alias("g"), pl.col("supplier_raw").mode().first().alias("raw"))
              .sort("g", descending=True))
    if by_sup.height == 0:
        print(f"NO NAMED SUPPLIERS (category-rollup only): {b}  gross={full_gross:,.0f}")
        rows.append({
            "body": b, "regime": regime.filter(pl.col("body")==b)["regime"][0],
            "body_gross_eur": round(full_gross,0), "named_supplier_attributable_eur": 0.0,
            "pct_attributable": 0.0, "n_suppliers": 0, "top_supplier": "(category rollups only - no named suppliers)",
            "top_supplier_gross_eur": 0.0, "top1_share_of_body_pct": 0.0,
            "top5_share_of_body_pct": 0.0, "top5_share_of_attributable_pct": 0.0,
        })
        continue
    top1g = by_sup["g"][0]; top1raw = by_sup["raw"][0]
    top5g = by_sup.head(5)["g"].sum()
    reg = regime.filter(pl.col("body") == b)["regime"][0]
    rows.append({
        "body": b,
        "regime": reg,
        "body_gross_eur": round(full_gross, 0),
        "named_supplier_attributable_eur": round(attributable, 0),
        "pct_attributable": round(100*attributable/full_gross, 1),
        "n_suppliers": by_sup.height,
        "top_supplier": top1raw,
        "top_supplier_gross_eur": round(top1g, 0),
        "top1_share_of_body_pct": round(100*top1g/full_gross, 2),
        "top5_share_of_body_pct": round(100*top5g/full_gross, 2),
        "top5_share_of_attributable_pct": round(100*top5g/attributable, 2),
    })

conc = pl.DataFrame(rows).sort("body_gross_eur", descending=True)
conc.write_csv(f"{OUT}/body_concentration_final.csv")
with pl.Config(tbl_rows=30, fmt_str_lengths=42, tbl_width_chars=240):
    print(conc)
