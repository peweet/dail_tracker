import sys, re
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl

DROP = {"LTD","LIMITED","PLC","LLP","LLC","UC","DAC","CLG","ULC","TEORANTA","TEO","CPT","INC","GMBH","BV","AG","SA","PTY","COMPANY","CO","GROUP","HOLDINGS","IRELAND","INTERNATIONAL"}

def norm(s):
    if s is None:
        return ""
    s = str(s).upper()
    # drop trailing T/A ...
    s = re.sub(r"\bT/?A\b.*$", "", s)
    s = s.replace("&", " ")
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    toks = [t for t in s.split() if t and t not in DROP]
    return " ".join(toks)

target = "TONE"
print("norm(TONE) =", repr(norm(target)))

# Diary
d = pl.read_parquet("data/gold/parquet/ministerial_diary_org_mentions.parquet")
print("diary cols:", d.columns)
print("diary rows:", d.height)
d = d.with_columns(pl.col("matched_org_name").map_elements(norm, return_dtype=pl.Utf8).alias("sup_norm"))
hit = d.filter(pl.col("sup_norm") == norm(target))
print("\n=== DIARY rows where sup_norm == 'TONE' ===")
print("count:", hit.height)
with pl.Config(fmt_str_lengths=200, tbl_rows=200, tbl_cols=10):
    print(hit.select(["entry_date","minister","subject","matched_org_name","match_confidence"]).sort("entry_date"))
