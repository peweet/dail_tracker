"""Adversarial verification: dump ALL diary rows per firm with full subjects + lobbying + payment nature."""
import sys
sys.stdout.reconfigure(encoding="utf-8")
import re
import polars as pl

BASE = "data/gold/parquet/"
CSV = "data/raw_bq/bq-results-20260619-122315-1781871808837.csv"

SUFFIXES = {"LTD","LIMITED","PLC","LLP","LLC","UC","DAC","CLG","ULC","TEORANTA","TEO","CPT",
            "INC","GMBH","BV","AG","SA","PTY","COMPANY","CO","GROUP","HOLDINGS","IRELAND","INTERNATIONAL"}
TA_RE = re.compile(r"\b(T/A|TRADING AS|T A)\b.*$", re.I)
def norm(s):
    if s is None: return ""
    s = TA_RE.sub("", s.upper()).replace("&"," ")
    s = re.sub(r"[^A-Z0-9 ]"," ", s)
    return " ".join(t for t in s.split() if t and t not in SUFFIXES).strip()

FIRMS = sys.argv[1:]  # list of sup_norm to dump

dia = pl.read_parquet(BASE + "ministerial_diary_org_mentions.parquet").with_columns(
    pl.col("matched_org_name").map_elements(norm, return_dtype=pl.Utf8).alias("sn"))
lob = pl.read_parquet(BASE + "top_lobbyist_organisations.parquet").with_columns(
    pl.col("lobbyist_name").map_elements(norm, return_dtype=pl.Utf8).alias("sn"))
cli = pl.read_parquet(BASE + "top_client_companies.parquet").with_columns(
    pl.col("client_name").map_elements(norm, return_dtype=pl.Utf8).alias("sn"))
df = pl.read_csv(CSV, schema_overrides={"Total": pl.Float64, "Year": pl.Int64}).with_columns(
    pl.col("entity").str.replace(r"^Agency\s*:\s*","").str.strip_chars().alias("body"),
    pl.col("Supplier").map_elements(norm, return_dtype=pl.Utf8).alias("sn"))

for n in FIRMS:
    print(f"\n{'='*90}\n### {n}\n{'='*90}")
    dd = dia.filter(pl.col("sn") == n).sort("entry_date")
    print(f"-- DIARY: {dd.height} rows, matched_org_names = {sorted(set(dd['matched_org_name'].to_list()))}")
    print(f"-- gaz_origin/key = {sorted(set((str(a),str(b)) for a,b in zip(dd['gaz_origin'].to_list(), dd['gazetteer_key'].to_list())))}")
    for r in dd.iter_rows(named=True):
        print(f"   {r['entry_date']} | {str(r['minister'] or '?'):12} | {r['match_confidence']:6} | {str(r['subject'])[:110]}")
    # lobbying
    lr = lob.filter(pl.col("sn") == n)
    cr = cli.filter(pl.col("sn") == n)
    print(f"-- LOBBY as_lobbyist: {lr.height} rows")
    for r in lr.iter_rows(named=True):
        print(f"     name='{r['lobbyist_name']}' returns={r.get('returns_filed')} activity={str(r.get('main_activities_of_organisation'))[:90]}")
    print(f"-- LOBBY as_client: {cr.height} rows")
    for r in cr.iter_rows(named=True):
        print(f"     name='{r['client_name']}' returns={r.get('return_count')}")
    # payment nature
    pay = df.filter(pl.col("sn") == n)
    if pay.height:
        tot = pay["Total"].sum()
        print(f"-- PAYMENTS: {pay.height} lines, gross=€{tot/1e6:.1f}m, distinct bodies={pay['body'].n_unique()}")
        descs = pay.group_by("Description").agg(pl.col("Total").sum().alias("t")).sort("t", descending=True).head(8)
        for r in descs.iter_rows(named=True):
            print(f"     €{r['t']/1e6:7.2f}m | {str(r['Description'])[:80]}")
        print(f"   raw suppliers: {sorted(set(pay['Supplier'].to_list()))[:6]}")
        bodies = pay.group_by("body").agg(pl.col("Total").sum().alias("t")).sort("t", descending=True).head(6)
        print("   top paying bodies:")
        for r in bodies.iter_rows(named=True):
            print(f"     €{r['t']/1e6:7.2f}m | {r['body']}")
