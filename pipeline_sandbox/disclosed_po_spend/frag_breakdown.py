import sys
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl

SRC = r"data/raw_bq/bq-results-20260619-122315-1781871808837.csv"
df = pl.read_csv(SRC, schema_overrides={"Total": pl.Float64, "Year": pl.Int64})

def norm_sup(expr):
    e = expr.str.to_lowercase().str.strip_chars()
    e = e.str.replace_all(r"[^\w\s]", " ")
    e = e.str.replace_all(r"\s+", " ").str.strip_chars()
    e = e.str.replace_all(r"\s+(ltd|limited|plc)$", "")
    e = e.str.replace_all(r"\s+(ltd|limited|plc)$", "")
    return e.str.strip_chars()

df = df.with_columns(norm_sup(pl.col("Supplier")).alias("sup_norm"))
ds = df.filter(pl.col("sup_norm").is_not_null() & (pl.col("sup_norm") != ""))

# raw distinct ignoring case/whitespace only (no suffix strip) - to see how much is pure suffix/whitespace
def ws_only(expr):
    return expr.str.to_lowercase().str.replace_all(r"\s+", " ").str.strip_chars()
df2 = df.with_columns(ws_only(pl.col("Supplier")).alias("sup_ws"))
ds2 = df2.filter(pl.col("sup_ws").is_not_null() & (pl.col("sup_ws") != ""))
print("distinct raw Supplier        :", df.select(pl.col('Supplier').n_unique()).item())
print("distinct after ws-normalise  :", ds2.select(pl.col('sup_ws').n_unique()).item())
print("distinct after full normalise:", ds.select(pl.col('sup_norm').n_unique()).item())

frag = (ds.group_by("sup_norm").agg(pl.col("Supplier").n_unique().alias("nv")).filter(pl.col("nv") > 1))
print("norm names with >1 raw variant:", frag.height)
print("max variants:", frag['nv'].max(), " sum extra variants:", (frag['nv'].sum() - frag.height))
