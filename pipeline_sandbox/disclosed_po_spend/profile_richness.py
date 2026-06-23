import sys
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl
import re

SRC = r"data/raw_bq/bq-results-20260619-122315-1781871808837.csv"
OUT = r"pipeline_sandbox/disclosed_po_spend"

df = pl.read_csv(SRC, schema_overrides={"Total": pl.Float64, "Year": pl.Int64})
n = df.height
print(f"ROWS={n}  COLS={df.columns}")

# strip 'Agency : ' prefix on entity
df = df.with_columns(
    pl.col("entity").str.replace(r"^\s*Agency\s*:\s*", "", literal=False).str.strip_chars().alias("entity_clean")
)

# ---------------- BLANK RATES ----------------
def blank_rate(col):
    s = df[col]
    if s.dtype == pl.Utf8:
        blank = df.select(
            (pl.col(col).is_null() | (pl.col(col).str.strip_chars() == "")).sum()
        ).item()
    else:
        blank = df.select(pl.col(col).is_null().sum()).item()
    return round(100.0 * blank / n, 4)

blank = {c: blank_rate(c) for c in ["PO", "Supplier", "Total", "Description", "QTR", "Year", "entity"]}
print("\n=== BLANK RATES (%) ===")
for c, v in blank.items():
    print(f"  {c:14s} {v}")

# ---------------- SUPPLIER ----------------
distinct_raw_sup = df.select(pl.col("Supplier").n_unique()).item()
print(f"\nDISTINCT RAW SUPPLIER = {distinct_raw_sup}")

def norm_sup(expr):
    e = expr.str.to_lowercase().str.strip_chars()
    e = e.str.replace_all(r"[^\w\s]", " ")           # strip punctuation
    e = e.str.replace_all(r"\s+", " ").str.strip_chars()
    # strip trailing ltd/limited/plc (possibly repeated)
    e = e.str.replace_all(r"\s+(ltd|limited|plc)$", "")
    e = e.str.replace_all(r"\s+(ltd|limited|plc)$", "")
    e = e.str.strip_chars()
    return e

df = df.with_columns(norm_sup(pl.col("Supplier")).alias("sup_norm"))
# treat empty norm as null for the distinct count of real suppliers
df_sup = df.filter(pl.col("sup_norm").is_not_null() & (pl.col("sup_norm") != ""))
distinct_norm_sup = df_sup.select(pl.col("sup_norm").n_unique()).item()
print(f"DISTINCT NORM SUPPLIER = {distinct_norm_sup}")

# fragmentation: norm -> count of distinct raw variants
frag = (
    df_sup.group_by("sup_norm")
    .agg(pl.col("Supplier").n_unique().alias("n_variants"))
    .filter(pl.col("n_variants") > 1)
    .sort("n_variants", descending=True)
)
n_frag = frag.height
print(f"\nNORM NAMES mapping to >1 raw variant = {n_frag}")
print("TOP 12 FRAGMENTED:")
top_frag = frag.head(12)
for r in top_frag.iter_rows(named=True):
    print(f"  {r['n_variants']:4d}  {r['sup_norm']}")

# show example variants for the worst few
print("\nEXAMPLE VARIANTS (worst 5):")
for r in frag.head(5).iter_rows(named=True):
    variants = df_sup.filter(pl.col("sup_norm") == r["sup_norm"]).select("Supplier").unique().head(8)
    vs = [x[0] for x in variants.iter_rows()]
    print(f"  [{r['sup_norm']}] -> {vs}")

# ---------------- DESCRIPTION ----------------
distinct_desc = df.select(pl.col("Description").n_unique()).item()
print(f"\nDISTINCT DESCRIPTION = {distinct_desc}")

desc_freq = (
    df.group_by("Description")
    .agg(pl.len().alias("rows"), pl.col("Total").sum().alias("gross"))
    .sort("rows", descending=True)
)
print("\nTOP 15 DESC BY FREQUENCY:")
for r in desc_freq.head(15).iter_rows(named=True):
    d = (r["Description"] or "")[:55]
    print(f"  {r['rows']:8d}  {d}")

print("\nTOP 15 DESC BY GROSS VALUE:")
for r in desc_freq.sort("gross", descending=True).head(15).iter_rows(named=True):
    d = (r["Description"] or "")[:55]
    print(f"  {r['gross']:18,.0f}  {d}")

# Coded vs free text heuristics: avg length, capitalisation pattern, reuse ratio
desc_nonblank = df.filter(pl.col("Description").is_not_null() & (pl.col("Description").str.strip_chars() != ""))
avg_len = desc_nonblank.select(pl.col("Description").str.len_chars().mean()).item()
reuse_ratio = distinct_desc / max(desc_nonblank.height, 1)
print(f"\nDESC avg_len={avg_len:.1f}  distinct/nonblank_rows={reuse_ratio:.5f}  (low=>controlled vocab)")

# per-body: is description a bounded vocab? count distinct desc per body and rows per body
body_desc = (
    df.group_by("entity_clean")
    .agg(
        pl.len().alias("rows"),
        pl.col("Description").n_unique().alias("distinct_desc"),
    )
    .with_columns((pl.col("distinct_desc") / pl.col("rows")).alias("desc_reuse"))
    .sort("rows", descending=True)
)
print("\nDESC reuse by body (sample big bodies): low desc_reuse => coded vocab")
for r in body_desc.head(12).iter_rows(named=True):
    print(f"  rows={r['rows']:8d} distinct_desc={r['distinct_desc']:7d} reuse={r['desc_reuse']:.4f}  {r['entity_clean'][:40]}")

# ---------------- PER-BODY DENSITY ----------------
# also compute PO blank rate per body to flag payment-list vs PO-commitment semantics
density = (
    df.group_by("entity_clean")
    .agg(
        pl.len().alias("rows"),
        pl.col("Total").sum().alias("gross_line_value"),
        pl.col("sup_norm").filter(pl.col("sup_norm") != "").n_unique().alias("distinct_suppliers_norm"),
        pl.col("Supplier").n_unique().alias("distinct_suppliers_raw"),
        pl.col("Description").n_unique().alias("distinct_descriptions"),
        (100.0 * (pl.col("PO").is_null() | (pl.col("PO").str.strip_chars() == "")).sum() / pl.len()).alias("po_blank_pct"),
        pl.col("Year").min().alias("year_min"),
        pl.col("Year").max().alias("year_max"),
    )
    .sort("gross_line_value", descending=True)
)
density.write_csv(f"{OUT}/richness_detail.csv")
print(f"\nWROTE per-body density: {OUT}/richness_detail.csv  ({density.height} bodies)")

print("\nTOP 10 BODIES BY GROSS:")
for r in density.head(10).iter_rows(named=True):
    print(f"  {r['gross_line_value']:16,.0f}  rows={r['rows']:8d} sup={r['distinct_suppliers_norm']:6d} desc={r['distinct_descriptions']:6d} po_blank={r['po_blank_pct']:5.1f}%  {r['entity_clean'][:38]}")

print("\nTHINNEST 10 BODIES (fewest rows):")
for r in density.sort("rows").head(10).iter_rows(named=True):
    print(f"  rows={r['rows']:6d} gross={r['gross_line_value']:14,.0f} sup={r['distinct_suppliers_norm']:5d} desc={r['distinct_descriptions']:5d}  {r['entity_clean'][:38]}")

# spread stats
print("\nDENSITY SPREAD:")
for col in ["rows", "distinct_suppliers_norm", "distinct_descriptions"]:
    s = density[col]
    print(f"  {col:26s} min={s.min():>8} median={int(s.median()):>8} max={s.max():>10}")

# desc_reuse distribution to classify coded vs free text globally
print("\nDESC_REUSE distribution across bodies (low=coded, high=free text):")
dr = body_desc["desc_reuse"]
print(f"  min={dr.min():.4f} p25={dr.quantile(0.25):.4f} median={dr.median():.4f} p75={dr.quantile(0.75):.4f} max={dr.max():.4f}")
n_coded = body_desc.filter(pl.col("desc_reuse") < 0.05).height
n_free = body_desc.filter(pl.col("desc_reuse") > 0.5).height
print(f"  bodies desc_reuse<0.05 (coded-ish) = {n_coded}")
print(f"  bodies desc_reuse>0.5  (free-ish)   = {n_free}")

# Year range + entity count
print(f"\nYEAR range: {df['Year'].min()}..{df['Year'].max()}  distinct entities (clean)={df['entity_clean'].n_unique()}")
