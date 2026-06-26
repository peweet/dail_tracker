import sys, re
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl

DROP = {"LTD","LIMITED","PLC","LLP","LLC","UC","DAC","CLG","ULC","TEORANTA","TEO","CPT","INC","GMBH","BV","AG","SA","PTY","COMPANY","CO","GROUP","HOLDINGS","IRELAND","INTERNATIONAL"}
def norm(s):
    if s is None: return ""
    s = str(s).upper()
    s = re.sub(r"\bT/?A\b.*$", "", s)
    s = s.replace("&", " ")
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    return " ".join(t for t in s.split() if t and t not in DROP)

d = pl.read_parquet("data/gold/parquet/ministerial_diary_org_mentions.parquet")
d = d.with_columns(pl.col("matched_org_name").map_elements(norm, return_dtype=pl.Utf8).alias("sup_norm"))

print("=== matched_org_name containing token 'TONE' (raw substring, case-insensitive) ===")
sub = d.filter(pl.col("matched_org_name").str.to_uppercase().str.contains("TONE"))
with pl.Config(fmt_str_lengths=120, tbl_rows=60):
    print(sub.select(["matched_org_name","sup_norm"]).unique().sort("matched_org_name"))

print("\n=== sup_norm containing 'TONE' token ===")
tok = d.filter(pl.col("sup_norm").str.contains(r"\bTONE\b"))
with pl.Config(fmt_str_lengths=200, tbl_rows=60):
    print(tok.select(["entry_date","minister","subject","matched_org_name","sup_norm"]).unique().sort("matched_org_name"))

# Lobbying
print("\n=== LOBBYING orgs/clients containing TONE ===")
lo = pl.read_parquet("data/gold/parquet/top_lobbyist_organisations.parquet")
print("lobbyist cols:", lo.columns)
lh = lo.filter(pl.col("lobbyist_name").str.to_uppercase().str.contains("TONE"))
with pl.Config(fmt_str_lengths=120, tbl_rows=40):
    print(lh)
cl = pl.read_parquet("data/gold/parquet/top_client_companies.parquet")
print("client cols:", cl.columns)
ch = cl.filter(pl.col("client_name").str.to_uppercase().str.contains("TONE"))
with pl.Config(fmt_str_lengths=120, tbl_rows=40):
    print(ch)
