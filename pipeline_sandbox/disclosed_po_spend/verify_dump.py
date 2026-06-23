"""ADVERSARIAL verification dump (read-only). For each firm: ALL diary rows (full subjects),
lobbying detail, payment-nature descriptions. Writes nothing to gold."""
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

FIRMS = [
    "ROADSTONE","ACCENTURE","DELOITTE","PFIZER","VODAFONE","IBM","KPMG","MASON HAYES CURRAN",
    "MCCANN FITZGERALD","GILEAD SCIENCES","GRANT THORNTON","WILLIAM FRY","MAZARS","STAYCITY",
    "BON SECOURS HEALTH SYSTEM","HORSE SPORT","MEDTRONIC","VIRGIN MEDIA","MUSGRAVE",
    "AMAZON WEB SERVICES","AER LINGUS","NOVARTIS","GRAFTON ARCHITECTS","EVERSHEDS SUTHERLAND",
    "IRISH MANUFACTURING RESEARCH",
]

dia = pl.read_parquet(BASE + "ministerial_diary_org_mentions.parquet").with_columns(
    pl.col("matched_org_name").map_elements(norm, return_dtype=pl.Utf8).alias("sn"))
lob = pl.read_parquet(BASE + "top_lobbyist_organisations.parquet").with_columns(
    pl.col("lobbyist_name").map_elements(norm, return_dtype=pl.Utf8).alias("sn"))
cli = pl.read_parquet(BASE + "top_client_companies.parquet").with_columns(
    pl.col("client_name").map_elements(norm, return_dtype=pl.Utf8).alias("sn"))
df = pl.read_csv(CSV, schema_overrides={"Total": pl.Float64, "Year": pl.Int64}).with_columns(
    pl.col("entity").str.replace(r"^Agency\s*:\s*","").str.strip_chars().alias("body"),
    pl.col("Supplier").map_elements(norm, return_dtype=pl.Utf8).alias("sn"))

target = sys.argv[1] if len(sys.argv) > 1 else None
for n in FIRMS:
    if target and target.upper() not in n: continue
    dd = dia.filter(pl.col("sn") == n).sort("entry_date")
    print("\n" + "="*100)
    print(f"### {n}  ({dd.height} diary rows)")
    print("="*100)
    # full subjects, dedup by (date,minister,subject)
    seen = set()
    for r in dd.iter_rows(named=True):
        key = (str(r["entry_date"]), r["minister"], r["subject"])
        if key in seen: continue
        seen.add(key)
        print(f"  {r['entry_date']} | {str(r['minister'] or '?'):12} | conf={r['match_confidence']:6} | matched_as='{r['matched_org_name']}' | gaz={r['gazetteer_key']}")
        print(f"      SUBJ: {r['subject']}")
    print(f"  -- distinct ministers: {sorted(set(x for x in dd['minister'].to_list() if x))}")
    print(f"  -- match_confidence values: {sorted(set(dd['match_confidence'].to_list()))}")
    print(f"  -- matched_org_name values: {sorted(set(dd['matched_org_name'].to_list()))}")
    # lobbying
    lr = lob.filter(pl.col("sn") == n)
    cr = cli.filter(pl.col("sn") == n)
    print(f"  -- LOBBYING as-lobbyist: {lr.height} rows")
    for r in lr.iter_rows(named=True):
        print(f"        name='{r['lobbyist_name']}' returns={r['returns_filed']} pols={r['distinct_politicians_targeted']} areas={r['distinct_policy_areas']} activity={str(r['main_activities_of_organisation'])[:120]}")
    print(f"  -- LOBBYING as-client: {cr.height} rows")
    for r in cr.iter_rows(named=True):
        print(f"        name='{r['client_name']}' returns={r['return_count']} lobbyists={r['distinct_lobbyist_firms']} pols={r['distinct_politicians_targeted']} areas={r['distinct_policy_areas']}")
    # payment nature: top descriptions + paying bodies
    pay = df.filter(pl.col("sn") == n)
    if pay.height:
        bodies = sorted(pay["body"].unique().to_list())
        print(f"  -- PAYMENTS: {pay.height} lines, gross=€{pay['Total'].sum()/1e6:.1f}m, {len(bodies)} bodies")
        descs = pay.group_by("Description").agg(pl.col("Total").sum().alias("t")).sort("t", descending=True).head(8)
        for r in descs.iter_rows(named=True):
            print(f"        €{r['t']/1e6:7.2f}m  {str(r['Description'])[:90]}")
        print(f"        bodies: {bodies[:15]}")
