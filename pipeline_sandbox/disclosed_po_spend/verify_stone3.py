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
    return " ".join(t for t in s.split() if t and t not in DROP)

# Totals for ROADSTONE in disclosed file
import csv
roadstone_total=0.0; roadstone_lines=0; entities=set()
with open("data/raw_bq/bq-results-20260619-122315-1781871808837.csv", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        if norm(row.get("Supplier","")) == "ROADSTONE":
            try: roadstone_total += float(row.get("Total","0") or 0)
            except: pass
            roadstone_lines += 1
            entities.add(row.get("entity",""))
print(f"ROADSTONE disclosed: lines={roadstone_lines} total=EUR{roadstone_total:,.0f} across {len(entities)} entities")
print("entities:", sorted(entities))

# Lobbying register
print("\n=== LOBBYING: lobbyist orgs matching STONE ===")
lo = pl.read_parquet("data/gold/parquet/top_lobbyist_organisations.parquet")
lo = lo.with_columns(pl.col("lobbyist_name").map_elements(norm, return_dtype=pl.Utf8).alias("n"))
hit = lo.filter(pl.col("n").str.contains("STONE"))
with pl.Config(tbl_rows=50, fmt_str_lengths=200, tbl_width_chars=300):
    print(hit.select("lobbyist_name","returns_filed","main_activities_of_organisation"))

print("\n=== LOBBYING: client companies matching STONE ===")
cc = pl.read_parquet("data/gold/parquet/top_client_companies.parquet")
cc = cc.with_columns(pl.col("client_name").map_elements(norm, return_dtype=pl.Utf8).alias("n"))
hitc = cc.filter(pl.col("n").str.contains("STONE"))
with pl.Config(tbl_rows=50, fmt_str_lengths=200, tbl_width_chars=300):
    print(hitc.select("client_name","return_count"))

# exact ROADSTONE and KEYSTONE PROCUREMENT
print("\nROADSTONE exact in lobbyist:", lo.filter(pl.col("n")=="ROADSTONE").height)
print("ROADSTONE exact in clients:", cc.filter(pl.col("n")=="ROADSTONE").height)
print("KEYSTONE PROCUREMENT in lobbyist:", lo.filter(pl.col("n")=="KEYSTONE PROCUREMENT").height)
print("KEYSTONE PROCUREMENT in clients:", cc.filter(pl.col("n")=="KEYSTONE PROCUREMENT").height)
# Keystone in disclosed?
print("\nKEYSTONE PROCUREMENT in disclosed file?")
with open("data/raw_bq/bq-results-20260619-122315-1781871808837.csv", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        if "KEYSTONE" in norm(row.get("Supplier","")):
            print("  ", row.get("Supplier"), norm(row.get("Supplier")), row.get("entity"))
            break
    else:
        print("  none")
