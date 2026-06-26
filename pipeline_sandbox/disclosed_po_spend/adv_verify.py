import sys
sys.stdout.reconfigure(encoding="utf-8")
import re
import pandas as pd

BASE = "C:/Users/pglyn/PycharmProjects/dail_extractor/data/gold/parquet/"
CSV = "C:/Users/pglyn/PycharmProjects/dail_extractor/data/raw_bq/bq-results-20260619-122315-1781871808837.csv"
OUT = "C:/Users/pglyn/PycharmProjects/dail_extractor/pipeline_sandbox/disclosed_po_spend/"

# VERBATIM normaliser from prompt
DROP = {"LTD","LIMITED","PLC","LLP","LLC","UC","DAC","CLG","ULC","TEORANTA","TEO","CPT",
        "INC","GMBH","BV","AG","SA","PTY","COMPANY","CO","GROUP","HOLDINGS","IRELAND",
        "INTERNATIONAL"}
def norm(s):
    if s is None: return ""
    s = str(s).upper()
    s = re.sub(r"\bT/?A\b.*$", " ", s)
    s = s.replace("&", " ")
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    toks = [t for t in s.split() if t and t not in DROP]
    return " ".join(toks)

def dnorm(s):
    if s is None: return ""
    return re.sub(r"[^A-Z0-9 ]"," ", str(s).upper()).replace("  "," ").strip()

ev = pd.read_csv(OUT + "xref_lobbied_then_paid_evidence.csv")
firms = list(ev["sup_norm"])

# diary
dia = pd.read_parquet(BASE + "ministerial_diary_org_mentions.parquet")
dia["sn"] = dia["matched_org_name"].map(norm)

# minister->depts
eng = pd.read_parquet(BASE + "ministerial_diary_engagements.parquet", columns=["minister","department"])
min_depts = eng.dropna(subset=["minister"]).groupby("minister")["department"].apply(lambda s: sorted(set(x for x in s if x))).to_dict()

# lobbying
lob = pd.read_parquet(BASE + "top_lobbyist_organisations.parquet")
lob["sn"] = lob["lobbyist_name"].map(norm)
cli = pd.read_parquet(BASE + "top_client_companies.parquet")
cli["sn"] = cli["client_name"].map(norm)

# payments
pay = pd.read_csv(CSV)
pay["sn"] = pay["Supplier"].map(norm)
pay["body"] = pay["entity"].astype(str).str.replace(r"^Agency\s*:\s*","",regex=True).str.strip()

target = sys.argv[1] if len(sys.argv) > 1 else None
for n in firms:
    if target and n != target: continue
    print("\n" + "="*100)
    print(f"FIRM sup_norm = {n!r}")
    dd = dia[dia["sn"] == n].sort_values("entry_date")
    print(f"  diary rows: {len(dd)}  | distinct matched_org_name: {sorted(set(dd['matched_org_name']))}")
    print(f"  ministers: {sorted(set(x for x in dd['minister'] if x))}")
    print(f"  match_conf: {sorted(set(dd['match_confidence']))} | match_method: {sorted(set(dd['match_method']))} | gaz_origin: {sorted(set(dd['gaz_origin']))}")
    print("  --- FULL DIARY SUBJECTS (date | minister | conf | method | subject) ---")
    for _, r in dd.iterrows():
        print(f"   {r['entry_date']} | {str(r['minister'] or '?'):12} | {r['match_confidence']:6} | {r['match_method']:12} | {str(r['subject'])[:160]}")
    # lobbying
    lr = lob[lob["sn"] == n]
    cr = cli[cli["sn"] == n]
    print(f"  LOBBYING as_lobbyist rows={len(lr)} returns={int(lr['returns_filed'].sum()) if len(lr) else 0}")
    for _, r in lr.iterrows():
        act = str(r.get('main_activities_of_organisation',''))[:200]
        print(f"      lobbyist_name={r['lobbyist_name']!r} returns={r['returns_filed']} act={act}")
    print(f"  LOBBYING as_client rows={len(cr)} returns={int(cr['return_count'].sum()) if len(cr) else 0}")
    for _, r in cr.iterrows():
        print(f"      client_name={r['client_name']!r} returns={r['return_count']}")
    # payments
    pp = pay[pay["sn"] == n]
    bodies = sorted(set(pp["body"]))
    print(f"  PAYMENTS rows={len(pp)} bodies={len(bodies)} total_gross=EUR{pp['Total'].sum()/1e6:.1f}m")
    print(f"      bodies: {bodies[:40]}")
    # top descriptions
    descs = pp.groupby("Description")["Total"].sum().sort_values(ascending=False).head(8)
    print("      top descriptions by gross:")
    for desc, tot in descs.items():
        print(f"        EUR{tot/1e6:7.2f}m  {str(desc)[:90]}")
    # dept-remit
    paying_d = {dnorm(b) for b in bodies}
    mins = sorted(set(x for x in dd['minister'] if x))
    remit = set()
    for m in mins:
        for dep in min_depts.get(m, []):
            dn = dnorm(dep)
            if dn and any(dn == pb or (len(dn) > 8 and dn in pb) for pb in paying_d):
                remit.add(f"{m}~{dep}")
    print(f"  DEPT-REMIT match: {sorted(remit)}")
    # show what depts each met minister is associated with
    for m in mins:
        print(f"      minister {m} depts: {min_depts.get(m, [])}")
