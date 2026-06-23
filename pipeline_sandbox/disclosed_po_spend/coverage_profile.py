import sys
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl

RAW = "c:/Users/pglyn/PycharmProjects/dail_extractor/data/raw_bq/bq-results-20260619-122315-1781871808837.csv"
FACT = "c:/Users/pglyn/PycharmProjects/dail_extractor/data/gold/parquet/procurement_payments_fact.parquet"
OUT = "c:/Users/pglyn/PycharmProjects/dail_extractor/pipeline_sandbox/disclosed_po_spend/per_body_coverage.csv"

df = pl.read_csv(RAW, schema_overrides={"Total": pl.Float64, "Year": pl.Int64})
print("RAW rows:", df.height)

# strip "Agency :" prefix
df = df.with_columns(
    pl.col("entity").str.replace(r"^Agency\s*:\s*", "").str.strip_chars().alias("body")
)
print("distinct bodies:", df["body"].n_unique())

# --- quarter index helper: year*4 + qnum ---
df = df.with_columns(
    pl.col("QTR").str.extract(r"q(\d)").cast(pl.Int64).alias("qnum")
)
# sanity
print("QTR distinct:", sorted(df["QTR"].unique().to_list()))
print("qnum nulls:", df["qnum"].null_count())
df = df.with_columns((pl.col("Year") * 4 + (pl.col("qnum") - 1)).alias("qidx"))

# ---------- OVERALL TIMELINE ----------
timeline = (
    df.group_by("Year")
    .agg(pl.len().alias("rows"), pl.col("Total").sum().alias("gross_eur"))
    .sort("Year")
)
print("\n=== OVERALL TIMELINE (rows, gross per Year) ===")
for r in timeline.iter_rows(named=True):
    print(f"  {r['Year']}: rows={r['rows']:>8}  gross_eur={r['gross_eur']:>20,.0f}")

# ---------- PO'S BLANK PCT (semantics flag) ----------
blank = (
    df.with_columns((pl.col("PO").fill_null("").str.strip_chars() == "").alias("po_blank"))
    .group_by("body")
    .agg(
        pl.col("po_blank").mean().alias("po_blank_pct"),
        pl.len().alias("n_rows"),
    )
)

# ---------- PER BODY COVERAGE ----------
qpb = (
    df.group_by("body")
    .agg(
        pl.col("year_quarter").min().alias("first_q"),
        pl.col("year_quarter").max().alias("last_q"),
        pl.col("qidx").n_unique().alias("n_quarters"),
        pl.col("qidx").min().alias("min_qidx"),
        pl.col("qidx").max().alias("max_qidx"),
        pl.col("Total").sum().alias("gross"),
        pl.len().alias("rows"),
        pl.col("Year").min().alias("min_year"),
        pl.col("Year").max().alias("max_year"),
    )
    .with_columns(
        (pl.col("max_qidx") - pl.col("min_qidx") + 1).alias("span_quarters")
    )
    .with_columns(
        (pl.col("n_quarters") / pl.col("span_quarters")).alias("continuity_pct")
    )
    .join(blank.select("body", "po_blank_pct"), on="body", how="left")
    .sort("first_q", "body")
)

# semantics label
qpb = qpb.with_columns(
    pl.when(pl.col("po_blank_pct") >= 0.9).then(pl.lit("PAYMENT-list"))
    .when(pl.col("po_blank_pct") <= 0.1).then(pl.lit("PO-commitment"))
    .otherwise(pl.lit("mixed"))
    .alias("semantics")
)

# write full CSV
out = qpb.select(
    "body", "first_q", "last_q", "n_quarters", "span_quarters",
    "continuity_pct", "gross", "rows", "po_blank_pct", "semantics",
    "min_year", "max_year",
).sort("first_q", "body")
out.write_csv(OUT)
print("\nWROTE:", OUT, "rows:", out.height)

# ---------- LONGEST CONTINUOUS RUNS ----------
print("\n=== LONGEST RUNS (by n_quarters, then continuity) ===")
longest = qpb.sort(["n_quarters", "continuity_pct"], descending=[True, True]).head(20)
for r in longest.iter_rows(named=True):
    print(f"  {r['body'][:48]:<48} {r['first_q']}..{r['last_q']}  "
          f"nq={r['n_quarters']:>3} span={r['span_quarters']:>3} "
          f"cont={r['continuity_pct']*100:5.1f}% [{r['semantics']}] gross={r['gross']:,.0f}")

# ---------- SPARSE / ONE-OFF ----------
print("\n=== SPARSE / ONE-OFF (1-2 quarters present) ===")
sparse = qpb.filter(pl.col("n_quarters") <= 2).sort(["n_quarters", "gross"], descending=[False, True])
print("count <=2 quarters:", sparse.height)
for r in sparse.head(25).iter_rows(named=True):
    print(f"  {r['body'][:48]:<48} nq={r['n_quarters']} {r['first_q']}..{r['last_q']} gross={r['gross']:,.0f}")

# ---------- COMPARE TO OUR FACT (earlier history) ----------
print("\n=== COMPARING TO PARSED FACT ===")
fact = pl.read_parquet(FACT, columns=["publisher_name", "year"])
fact_min = (
    fact.filter(pl.col("year").is_not_null())
    .group_by("publisher_name")
    .agg(pl.col("year").min().alias("fact_min_year"), pl.col("year").max().alias("fact_max_year"))
)
print("fact publishers:", fact_min.height)

# normalise both sides for join: lower + strip
def norm(c):
    return pl.col(c).str.to_lowercase().str.replace_all(r"[^a-z0-9]+", " ").str.strip_chars()

bq_min = qpb.select("body", "min_year", "max_year", "rows", "gross").with_columns(norm("body").alias("k"))
fact_n = fact_min.with_columns(norm("publisher_name").alias("k"))

joined = bq_min.join(fact_n, on="k", how="inner")
print("name-matched bodies:", joined.height)
earlier = joined.filter(pl.col("min_year") < pl.col("fact_min_year")).sort("body")
print("\n=== BODIES WHERE DISCLOSED REACHES EARLIER YEARS THAN OUR FACT ===")
for r in earlier.iter_rows(named=True):
    print(f"  {r['body'][:42]:<42} disclosed_min={r['min_year']}  fact_min={r['fact_min_year']}  "
          f"(disclosed reaches back {r['fact_min_year']-r['min_year']} yrs earlier)")

# HSE explicit check
print("\n=== HSE explicit ===")
hse_bq = qpb.filter(pl.col("body").str.to_lowercase().str.contains("health service|hse"))
for r in hse_bq.iter_rows(named=True):
    print(f"  BQ body='{r['body']}' first={r['first_q']} last={r['last_q']} min_year={r['min_year']} max_year={r['max_year']} nq={r['n_quarters']}")
hse_fact = fact_min.filter(pl.col("publisher_name").str.to_lowercase().str.contains("health service|hse"))
for r in hse_fact.iter_rows(named=True):
    print(f"  FACT pub='{r['publisher_name']}' min_year={r['fact_min_year']} max_year={r['fact_max_year']}")

# what HSE quarters exist in BQ (to confirm 2017q3..2020q2 + 2025q4 + 2026q1)
hse_rows = df.filter(pl.col("body").str.to_lowercase().str.contains("health service|hse"))
print("\nHSE distinct year_quarter present in BQ:")
hq = sorted(hse_rows["year_quarter"].unique().to_list())
print(" ", hq)
