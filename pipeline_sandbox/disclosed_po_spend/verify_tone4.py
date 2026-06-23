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
# any diary org containing 'wolfe' or bare 'tone'
m = d.filter(pl.col("matched_org_name").str.to_uppercase().str.contains("WOLFE") | (pl.col("matched_org_name").str.to_uppercase()=="TONE"))
print("=== diary rows for WOLFE / bare TONE ===", m.height)
with pl.Config(fmt_str_lengths=200, tbl_rows=40):
    print(m.select(["entry_date","minister","subject","matched_org_name"]))

lo = pl.read_parquet("data/gold/parquet/top_lobbyist_organisations.parquet")
cl = pl.read_parquet("data/gold/parquet/top_client_companies.parquet")
print("\nlobbyist WOLFE/TONE:", lo.filter(pl.col("lobbyist_name").str.to_uppercase().str.contains("WOLFE")).height,
      lo.filter(pl.col("lobbyist_name").str.to_uppercase()=="TONE").height)
print("client WOLFE/TONE:", cl.filter(pl.col("client_name").str.to_uppercase().str.contains("WOLFE")).height,
      cl.filter(pl.col("client_name").str.to_uppercase()=="TONE").height)
