import sys
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl

OUT = "c:/Users/pglyn/PycharmProjects/dail_extractor/pipeline_sandbox/disclosed_po_spend"

top = pl.read_csv(f"{OUT}/top_suppliers_overall.csv")
conc = pl.read_csv(f"{OUT}/body_concentration_final.csv")
big = pl.read_csv(f"{OUT}/biggest_named_lines.csv")

# Section 1: top suppliers overall
s1 = top.head(50).select(
    pl.lit("1_top_supplier_overall_GROSS_LINE_VALUE_mixes_regimes").alias("section"),
    pl.col("example_raw").alias("name"),
    pl.col("body").alias("body").cast(pl.Utf8) if "body" in top.columns else pl.lit("").alias("body"),
    pl.col("gross_eur").round(0).alias("amount_eur"),
    pl.col("n_bodies").cast(pl.Int64).alias("n_bodies_or_share"),
    pl.lit("").alias("description"),
) if "body" not in top.columns else top.head(50).select(
    pl.lit("1_top_supplier_overall_GROSS_LINE_VALUE_mixes_regimes").alias("section"),
    pl.col("example_raw").alias("name"),
    pl.lit("ALL_BODIES").alias("body"),
    pl.col("gross_eur").round(0).alias("amount_eur"),
    pl.col("n_bodies").cast(pl.Int64).alias("n_bodies_or_share"),
    pl.lit("").alias("description"),
)

# Section 2: concentration within body (regime-safe)
s2 = conc.select(
    pl.lit("2_concentration_within_body_REGIME_SAFE").alias("section"),
    pl.col("top_supplier").alias("name"),
    pl.col("body"),
    pl.col("top5_share_of_body_pct").alias("amount_eur"),
    pl.lit(None).cast(pl.Int64).alias("n_bodies_or_share"),
    ("top1=" + pl.col("top_supplier") + " @ " + pl.col("top1_share_of_body_pct").cast(pl.Utf8) + "%; body_gross_eur=" + pl.col("body_gross_eur").cast(pl.Utf8) + "; pct_attributable=" + pl.col("pct_attributable").cast(pl.Utf8)).alias("description"),
)

# Section 3: biggest named lines
s3 = big.head(20).select(
    pl.lit("3_biggest_named_single_lines").alias("section"),
    pl.col("supplier_raw").alias("name"),
    pl.col("body"),
    pl.col("Total").round(0).alias("amount_eur"),
    pl.lit(None).cast(pl.Int64).alias("n_bodies_or_share"),
    (pl.col("Description").fill_null("") + " [" + pl.col("Year").cast(pl.Utf8) + " " + pl.col("QTR").fill_null("") + "]").alias("description"),
)

combined = pl.concat([s1.with_columns(pl.col("amount_eur").cast(pl.Float64)),
                      s2.with_columns(pl.col("amount_eur").cast(pl.Float64)),
                      s3.with_columns(pl.col("amount_eur").cast(pl.Float64))])
combined.write_csv(f"{OUT}/market_structure.csv")
print("wrote market_structure.csv rows", combined.height)
print(combined.head(5))
print("...")
print(combined.tail(5))
