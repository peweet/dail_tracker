import sys; sys.stdout.reconfigure(encoding='utf-8')
import re, polars as pl

DROP = {"LTD","LIMITED","PLC","LLP","LLC","UC","DAC","CLG","ULC","TEORANTA","TEO","CPT","INC","GMBH","BV","AG","SA","PTY","COMPANY","CO","GROUP","HOLDINGS","IRELAND","INTERNATIONAL"}

def norm(s):
    if s is None: return ""
    s = str(s).upper()
    s = re.sub(r"\bT/?A\b.*$", "", s)  # drop trailing T/A ...
    s = s.replace("&", " ")
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    toks = [t for t in s.split() if t and t not in DROP]
    return " ".join(toks)

firms = ["ROADSTONE","ACCENTURE","DELOITTE","PFIZER","VODAFONE","IBM","KPMG","MASON HAYES CURRAN","MCCANN FITZGERALD","GILEAD SCIENCES","GRANT THORNTON","WILLIAM FRY","MAZARS","STAYCITY","BON SECOURS HEALTH SYSTEM","HORSE SPORT","MEDTRONIC","VIRGIN MEDIA","MUSGRAVE","AMAZON WEB SERVICES","AER LINGUS","NOVARTIS","GRAFTON ARCHITECTS","EVERSHEDS SUTHERLAND","IRISH MANUFACTURING RESEARCH"]

diary = pl.read_parquet('data/gold/parquet/ministerial_diary_org_mentions.parquet')
diary = diary.with_columns(pl.col("matched_org_name").map_elements(norm, return_dtype=pl.Utf8).alias("on_norm"))

target = sys.argv[1] if len(sys.argv)>1 else None
for f in firms:
    if target and f != target: continue
    sub = diary.filter(pl.col("on_norm")==f)
    print("="*90)
    print(f"FIRM: {f}  rows={sub.height}")
    if sub.height==0:
        # try contains
        print("  NO EXACT on_norm match. distinct on_norm containing token:")
        tok = f.split()[0]
        cand = diary.filter(pl.col("on_norm").str.contains(tok)).select("matched_org_name","on_norm").unique()
        print(cand.head(20))
        continue
    print("  distinct matched_org_name:", sub.select("matched_org_name").unique().to_series().to_list())
    print("  ministers:", sorted([x or "?" for x in sub.select("minister_display").unique().to_series().to_list()]))
    for r in sub.sort("entry_date").iter_rows(named=True):
        subj = (r["subject"] or "").replace("\n"," ").strip()[:170]
        md = (r["minister_display"] or r["minister"] or "?")
        print(f"  [{r['entry_date']}] {md:14} conf={r['match_confidence'] or '?':6} src={(r['match_method'] or '?'):18} | {subj}")
