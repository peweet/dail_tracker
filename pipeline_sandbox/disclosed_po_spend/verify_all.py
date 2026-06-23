"""Full adversarial dump of every firm: diary subjects, lobbying activity, payment nature."""
import sys
sys.stdout.reconfigure(encoding="utf-8")
import re
import polars as pl

BASE = "data/gold/parquet/"
CSV = "data/raw_bq/bq-results-20260619-122315-1781871808837.csv"

DROP = {"LTD","LIMITED","PLC","LLP","LLC","UC","DAC","CLG","ULC","TEORANTA","TEO","CPT",
        "INC","GMBH","BV","AG","SA","PTY","COMPANY","CO","GROUP","HOLDINGS","IRELAND",
        "INTERNATIONAL"}
def norm(s):
    if s is None: return ""
    s = str(s).upper()
    s = re.sub(r"\bT/?A\b.*$", " ", s)
    s = s.replace("&", " ")
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    return " ".join(t for t in s.split() if t and t not in DROP)

FIRMS = [
    "ROADSTONE","ACCENTURE","DELOITTE","PFIZER","VODAFONE","IBM","KPMG",
    "MASON HAYES CURRAN","MCCANN FITZGERALD","GILEAD SCIENCES","GRANT THORNTON",
    "WILLIAM FRY","MAZARS","STAYCITY","BON SECOURS HEALTH SYSTEM","HORSE SPORT",
    "MEDTRONIC","VIRGIN MEDIA","MUSGRAVE","AMAZON WEB SERVICES","AER LINGUS",
    "NOVARTIS","GRAFTON ARCHITECTS","EVERSHEDS SUTHERLAND","IRISH MANUFACTURING RESEARCH",
]

dia = pl.read_parquet(BASE + "ministerial_diary_org_mentions.parquet").with_columns(
    pl.col("matched_org_name").map_elements(norm, return_dtype=pl.Utf8).alias("sn"))
lob = pl.read_parquet(BASE + "top_lobbyist_organisations.parquet").with_columns(
    pl.col("lobbyist_name").map_elements(norm, return_dtype=pl.Utf8).alias("sn"))
cli = pl.read_parquet(BASE + "top_client_companies.parquet").with_columns(
    pl.col("client_name").map_elements(norm, return_dtype=pl.Utf8).alias("sn"))
df = pl.read_csv(CSV, schema_overrides={"Total": pl.Float64}, infer_schema_length=10000).with_columns(
    pl.col("entity").str.replace(r"^Agency\s*:\s*","").str.strip_chars().alias("body"),
    pl.col("Supplier").map_elements(norm, return_dtype=pl.Utf8).alias("sn"))

target = sys.argv[1:] if len(sys.argv) > 1 else FIRMS
for n in target:
    print(f"\n{'='*95}\n### {n}\n{'='*95}")
    dd = dia.filter(pl.col("sn") == n).sort("entry_date")
    names = sorted(set(dd['matched_org_name'].to_list()))
    print(f"-- DIARY: {dd.height} rows | matched_org_names={names}")
    print(f"-- gaz_origin={sorted(set(str(x) for x in dd['gaz_origin'].to_list()))} | gazetteer_key={sorted(set(str(x) for x in dd['gazetteer_key'].to_list()))}")
    for r in dd.iter_rows(named=True):
        print(f"   {r['entry_date']} | {str(r['minister'] or '?'):14} | {r['match_confidence']:6} | {str(r['subject'])[:130]}")
    lr = lob.filter(pl.col("sn") == n)
    cr = cli.filter(pl.col("sn") == n)
    print(f"-- LOBBY as_lobbyist: {lr.height}")
    for r in lr.iter_rows(named=True):
        print(f"     '{r['lobbyist_name']}' returns={r.get('returns_filed')} pols={r.get('distinct_politicians_targeted')} act={str(r.get('main_activities_of_organisation'))[:120]}")
    print(f"-- LOBBY as_client: {cr.height}")
    for r in cr.iter_rows(named=True):
        print(f"     '{r['client_name']}' returns={r.get('return_count')} firms={r.get('distinct_lobbyist_firms')} pols={r.get('distinct_politicians_targeted')} areas={r.get('distinct_policy_areas')}")
    pay = df.filter(pl.col("sn") == n)
    if pay.height:
        tot = pay["Total"].sum()
        print(f"-- PAYMENTS: {pay.height} lines, gross=EUR{tot/1e6:.1f}m, bodies={pay['body'].n_unique()}")
        descs = pay.group_by("Description").agg(pl.col("Total").sum().alias("t")).sort("t", descending=True).head(6)
        for r in descs.iter_rows(named=True):
            print(f"     EUR{r['t']/1e6:7.2f}m | {str(r['Description'])[:85]}")
        bodies = pay.group_by("body").agg(pl.col("Total").sum().alias("t")).sort("t", descending=True).head(6)
        print("   top paying bodies:")
        for r in bodies.iter_rows(named=True):
            print(f"     EUR{r['t']/1e6:7.2f}m | {r['body']}")
        print(f"   raw suppliers sample: {sorted(set(pay['Supplier'].to_list()))[:5]}")
