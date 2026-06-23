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
    s = re.split(r"\bT/?A\b", s)[0]
    s = s.replace("&", " ")
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    toks = [t for t in s.split() if t and t not in DROP]
    return " ".join(toks)

dia = pl.read_parquet("data/gold/parquet/ministerial_diary_org_mentions.parquet")
dia = dia.with_columns(pl.col("matched_org_name").map_elements(norm, return_dtype=pl.Utf8).alias("sup_norm"))

pay = pl.read_csv("data/raw_bq/bq-results-20260619-122315-1781871808837.csv", infer_schema_length=10000)
pay = pay.with_columns(pl.col("Supplier").map_elements(norm, return_dtype=pl.Utf8).alias("sup_norm"))

lob = pl.read_parquet("data/gold/parquet/top_lobbyist_organisations.parquet")
lob = lob.with_columns(pl.col("lobbyist_name").map_elements(norm, return_dtype=pl.Utf8).alias("sup_norm"))

targets = [t.strip() for t in sys.argv[1].split("|")]
for t in targets:
    print("\n" + "="*90)
    print("FIRM:", t)
    print("="*90)
    d = dia.filter(pl.col("sup_norm") == t).sort("entry_date")
    print(f"-- DIARY rows ({d.height}) raw matched_org_name values:",
          sorted(set(d["matched_org_name"].to_list())))
    with pl.Config(tbl_rows=60, fmt_str_lengths=110, tbl_width_chars=240):
        print(d.select(["entry_date","minister","matched_org_name","subject","match_confidence"]))
    # payment nature
    p = pay.filter(pl.col("sup_norm") == t)
    descs = (p.group_by("Description").agg(pl.col("Total").sum().alias("tot"), pl.len().alias("n"))
             .sort("tot", descending=True).head(8))
    ents = (p.group_by("entity").agg(pl.col("Total").sum().alias("tot")).sort("tot", descending=True).head(8))
    sups = sorted(set(p["Supplier"].to_list()))[:8]
    print("-- raw Supplier samples:", sups)
    with pl.Config(tbl_rows=10, fmt_str_lengths=60):
        print("-- top Descriptions:"); print(descs)
        print("-- top paying entities:"); print(ents)
    # lobbying activity text
    l = lob.filter(pl.col("sup_norm") == t)
    if l.height:
        with pl.Config(tbl_rows=10, fmt_str_lengths=200):
            print("-- LOBBYING:")
            print(l.select(["lobbyist_name","returns_filed","main_activities_of_organisation"]))
    else:
        print("-- LOBBYING: only matched as CLIENT (top_client_companies), no org-register row")
