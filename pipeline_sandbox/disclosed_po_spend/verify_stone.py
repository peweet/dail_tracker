import sys, re
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl

DROP = {"LTD","LIMITED","PLC","LLP","LLC","UC","DAC","CLG","ULC","TEORANTA","TEO","CPT","INC","GMBH","BV","AG","SA","PTY","COMPANY","CO","GROUP","HOLDINGS","IRELAND","INTERNATIONAL"}

def norm(s):
    if s is None:
        return ""
    s = str(s).upper()
    # drop trailing T/A ...
    s = re.sub(r"\bT/A\b.*$", "", s)
    s = s.replace("&", " ")
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    toks = [t for t in s.split() if t and t not in DROP]
    return " ".join(toks)

# Diary
d = pl.read_parquet("data/gold/parquet/ministerial_diary_org_mentions.parquet")
print("DIARY COLS:", d.columns)
print("DIARY ROWS:", d.height)
d = d.with_columns(pl.col("matched_org_name").map_elements(norm, return_dtype=pl.Utf8).alias("sup_norm"))

# Find all sup_norm containing STONE
stone = d.filter(pl.col("sup_norm").str.contains("STONE"))
print("\n=== ALL diary rows with STONE in normalised org name ===")
print("rows:", stone.height)
vc = stone.group_by("matched_org_name","sup_norm").agg(pl.len().alias("n")).sort("n", descending=True)
with pl.Config(tbl_rows=100, fmt_str_lengths=120, tbl_width_chars=240):
    print(vc)
