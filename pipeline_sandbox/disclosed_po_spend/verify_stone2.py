import sys, re
sys.stdout.reconfigure(encoding="utf-8")
import polars as pl

DROP = {"LTD","LIMITED","PLC","LLP","LLC","UC","DAC","CLG","ULC","TEORANTA","TEO","CPT","INC","GMBH","BV","AG","SA","PTY","COMPANY","CO","GROUP","HOLDINGS","IRELAND","INTERNATIONAL"}
def norm(s):
    if s is None: return ""
    s = str(s).upper()
    s = re.sub(r"\bT/A\b.*$", "", s)
    s = s.replace("&", " ")
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    toks = [t for t in s.split() if t and t not in DROP]
    return " ".join(toks)

d = pl.read_parquet("data/gold/parquet/ministerial_diary_org_mentions.parquet")
d = d.with_columns(pl.col("matched_org_name").map_elements(norm, return_dtype=pl.Utf8).alias("sup_norm"))
stone = d.filter(pl.col("sup_norm").str.contains("STONE")).sort("entry_date")
with pl.Config(tbl_rows=50, fmt_str_lengths=300, tbl_width_chars=400):
    print(stone.select("entry_date","minister_display","subject","matched_org_name","match_confidence","match_method","gaz_origin"))

print("\n=== DISCLOSED CSV suppliers with STONE ===")
import csv
hits = {}
with open("data/raw_bq/bq-results-20260619-122315-1781871808837.csv", encoding="utf-8") as f:
    r = csv.DictReader(f)
    cols = r.fieldnames
    print("CSV COLS:", cols)
    for row in r:
        sup = row.get("Supplier","")
        n = norm(sup)
        if "STONE" in n:
            key = (sup, n, row.get("entity",""))
            try:
                tot = float(row.get("Total","0") or 0)
            except:
                tot = 0.0
            if key not in hits:
                hits[key] = {"count":0,"total":0.0,"descs":set()}
            hits[key]["count"]+=1
            hits[key]["total"]+=tot
            desc = row.get("Description","")
            if desc: hits[key]["descs"].add(desc[:80])
for key, v in sorted(hits.items(), key=lambda x:-x[1]["total"]):
    print(f"\nSupplier={key[0]!r} norm={key[1]!r} entity={key[2]!r}")
    print(f"  lines={v['count']} total={v['total']:,.0f}")
    for desc in list(v["descs"])[:8]:
        print(f"    - {desc}")
