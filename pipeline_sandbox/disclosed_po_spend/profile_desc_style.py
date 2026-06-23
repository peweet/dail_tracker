import sys
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl

SRC = r"data/raw_bq/bq-results-20260619-122315-1781871808837.csv"
df = pl.read_csv(SRC, schema_overrides={"Total": pl.Float64, "Year": pl.Int64})
df = df.with_columns(
    pl.col("entity").str.replace(r"^\s*Agency\s*:\s*", "").str.strip_chars().alias("entity_clean")
)

# Where do blank suppliers concentrate? (bodies with 0 distinct norm sup are aggregate publishers)
sup_blank = df.filter(pl.col("Supplier").is_null() | (pl.col("Supplier").str.strip_chars() == ""))
print(f"Blank-supplier rows: {sup_blank.height}")
print("Top bodies by blank-supplier rows:")
bb = sup_blank.group_by("entity_clean").agg(pl.len().alias("n")).sort("n", descending=True).head(10)
for r in bb.iter_rows(named=True):
    print(f"  {r['n']:6d}  {r['entity_clean'][:45]}")

# Bodies that publish NO supplier at all (utility-style aggregate disclosures)
nosup = (
    df.group_by("entity_clean")
    .agg(
        pl.len().alias("rows"),
        pl.col("Supplier").filter(pl.col("Supplier").str.strip_chars() != "").n_unique().alias("sup"),
    )
    .filter(pl.col("sup") == 0)
    .sort("rows", descending=True)
)
print(f"\nBodies publishing ZERO supplier names = {nosup.height}")
for r in nosup.head(15).iter_rows(named=True):
    print(f"  rows={r['rows']:6d}  {r['entity_clean'][:45]}")

# Description style: sample distinct descriptions for a 'coded' body vs a 'free text' body
print("\n--- South Dublin County Council (reuse 0.005 -> coded vocab) distinct desc sample ---")
sd = df.filter(pl.col("entity_clean") == "South Dublin County Council").select("Description").unique().head(20)
for r in sd.iter_rows():
    print(f"   {r[0]!r}")

# Find the most free-text body among large ones (Dept Agriculture had 0.23)
print("\n--- Dept Agriculture (reuse 0.23) distinct desc sample (free-ish?) ---")
ag = df.filter(pl.col("entity_clean").str.contains("Agriculture")).select("Description").unique().head(20)
for r in ag.iter_rows():
    d = r[0] or ""
    print(f"   {d[:80]!r}")

# Global: how many descriptions used by >1 body (shared controlled terms) vs unique-to-one-body
desc_bodies = (
    df.filter(pl.col("Description").is_not_null() & (pl.col("Description").str.strip_chars() != ""))
    .group_by("Description")
    .agg(pl.col("entity_clean").n_unique().alias("n_bodies"), pl.len().alias("rows"))
)
shared = desc_bodies.filter(pl.col("n_bodies") > 1).height
single = desc_bodies.filter(pl.col("n_bodies") == 1).height
print(f"\nDistinct descriptions shared across >1 body = {shared}")
print(f"Distinct descriptions used by only ONE body = {single}")
# rows covered by single-body (free-text-ish) descriptions
rows_single = desc_bodies.filter(pl.col("n_bodies") == 1)["rows"].sum()
rows_shared = desc_bodies.filter(pl.col("n_bodies") > 1)["rows"].sum()
print(f"Rows under shared desc = {rows_shared}  ;  rows under single-body desc = {rows_single}")

# how many descriptions look like ALLCAPS code-ish vs Title/sentence
nb = df.filter(pl.col("Description").is_not_null() & (pl.col("Description").str.strip_chars() != ""))
allcaps = nb.filter(pl.col("Description") == pl.col("Description").str.to_uppercase()).height
print(f"\nALLCAPS descriptions rows = {allcaps} ({100*allcaps/nb.height:.1f}%)")
