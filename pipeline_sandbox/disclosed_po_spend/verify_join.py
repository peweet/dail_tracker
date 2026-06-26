import sys
sys.stdout.reconfigure(encoding="utf-8")
import re
import polars as pl

DROP = {"LTD","LIMITED","PLC","LLP","LLC","UC","DAC","CLG","ULC","TEORANTA","TEO","CPT",
        "INC","GMBH","BV","AG","SA","PTY","COMPANY","CO","GROUP","HOLDINGS","IRELAND",
        "INTERNATIONAL"}

def norm(s):
    if s is None:
        return ""
    s = str(s).upper()
    # drop trailing T/A ...
    s = re.split(r"\bT/?A\b", s)[0]
    s = s.replace("&", " ")
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    toks = [t for t in s.split() if t and t not in DROP]
    return " ".join(toks)

# --- Disclosed payee file ---
pay = pl.read_csv("data/raw_bq/bq-results-20260619-122315-1781871808837.csv", infer_schema_length=10000)
pay = pay.with_columns(pl.col("Supplier").map_elements(norm, return_dtype=pl.Utf8).alias("sup_norm"))
pay_agg = (pay.filter(pl.col("sup_norm") != "")
           .group_by("sup_norm")
           .agg(pl.col("Total").sum().alias("gross"),
                pl.len().alias("n_rows"),
                pl.col("entity").n_unique().alias("n_entities"))
           .filter(pl.col("gross") >= 1_000_000))  # large payees
print("large disclosed payees:", pay_agg.height)

# --- Diaries ---
dia = pl.read_parquet("data/gold/parquet/ministerial_diary_org_mentions.parquet")
dia = dia.with_columns(pl.col("matched_org_name").map_elements(norm, return_dtype=pl.Utf8).alias("sup_norm"))
dia_agg = (dia.filter(pl.col("sup_norm") != "")
           .group_by("sup_norm")
           .agg(pl.len().alias("n_meetings"),
                pl.col("minister").n_unique().alias("n_ministers")))

# --- Lobbying ---
lob = pl.read_parquet("data/gold/parquet/top_lobbyist_organisations.parquet")
lob = lob.with_columns(pl.col("lobbyist_name").map_elements(norm, return_dtype=pl.Utf8).alias("sup_norm"))
lob_agg = (lob.filter(pl.col("sup_norm") != "")
           .group_by("sup_norm")
           .agg(pl.col("returns_filed").sum().alias("returns")))
cli = pl.read_parquet("data/gold/parquet/top_client_companies.parquet")
cli = cli.with_columns(pl.col("client_name").map_elements(norm, return_dtype=pl.Utf8).alias("sup_norm"))
cli_agg = (cli.filter(pl.col("sup_norm") != "")
           .group_by("sup_norm").agg(pl.col("return_count").sum().alias("client_returns")))

lob_keys = set(lob_agg["sup_norm"].to_list()) | set(cli_agg["sup_norm"].to_list())

# --- Triple join ---
cand = (pay_agg
        .join(dia_agg, on="sup_norm", how="inner")
        .filter(pl.col("sup_norm").is_in(list(lob_keys))))
cand = cand.sort("gross", descending=True)
print("CANDIDATES (large payee + diary + lobbying):", cand.height)
with pl.Config(tbl_rows=100, fmt_str_lengths=60, tbl_width_chars=200):
    print(cand.with_columns((pl.col("gross")/1e6).round(2).alias("gross_eur_m")).select(
        ["sup_norm","gross_eur_m","n_rows","n_entities","n_meetings","n_ministers"]))
