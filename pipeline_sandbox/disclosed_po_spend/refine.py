import sys
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl

SRC = "c:/Users/pglyn/PycharmProjects/dail_extractor/data/raw_bq/bq-results-20260619-122315-1781871808837.csv"
OUT = "c:/Users/pglyn/PycharmProjects/dail_extractor/pipeline_sandbox/disclosed_po_spend"

df = pl.read_csv(SRC, schema_overrides={"Total": pl.Float64, "Year": pl.Int64})
df = df.with_columns(
    pl.col("entity").str.replace(r"(?i)^\s*Agency\s*:\s*", "").str.strip_chars().alias("body"),
    pl.col("Supplier").str.strip_chars().alias("supplier_raw"),
)

# biggest lines WITH a real named supplier (exclude blank-supplier category rollups)
named = df.filter(
    pl.col("supplier_raw").is_not_null() & (pl.col("supplier_raw").str.len_chars() > 1)
)
biggest_named = (
    named.select("body", "supplier_raw", "Total", "Description", "Year", "QTR", "PO")
    .sort("Total", descending=True)
    .head(40)
)
biggest_named.write_csv(f"{OUT}/biggest_named_lines.csv")
with pl.Config(fmt_str_lengths=70, tbl_rows=15, tbl_width_chars=200):
    print("=== BIGGEST SINGLE LINES WITH A NAMED SUPPLIER ===")
    print(biggest_named.head(15))

# how much of total gross is blank-supplier category rollup
blank_sup = df.filter(pl.col("supplier_raw").is_null() | (pl.col("supplier_raw").str.len_chars() <= 1))
print("\nblank-supplier rows:", blank_sup.height, "gross:", blank_sup["Total"].sum())
print("which bodies use blank-supplier rollups (top):")
print(
    blank_sup.group_by("body").agg(pl.col("Total").sum().alias("g"), pl.len().alias("n"))
    .sort("g", descending=True).head(10)
)

# full concentration table print
conc = pl.read_csv(f"{OUT}/body_concentration.csv")
with pl.Config(tbl_rows=30, fmt_str_lengths=45, tbl_width_chars=200):
    print("\n=== FULL CONCENTRATION TABLE ===")
    print(conc.sort("body_gross_eur", descending=True))
