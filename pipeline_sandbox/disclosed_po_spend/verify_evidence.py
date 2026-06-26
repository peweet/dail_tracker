"""
Pull the underlying EVIDENCE behind the top private 'lobbied-then-paid' claims so each can be
checked (real diary meeting w/ date + match-confidence; real lobbying-register row; and the
STRONG test: is the firm paid by the SAME department whose minister it met — dept-remit match).
Read-only on gold; writes only xref_lobbied_then_paid_evidence.* under this folder.
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")
import re
import polars as pl

BASE = "data/gold/parquet/"
CSV = "data/raw_bq/bq-results-20260619-122315-1781871808837.csv"
OUT = "pipeline_sandbox/disclosed_po_spend/"

SUFFIXES = {"LTD","LIMITED","PLC","LLP","LLC","UC","DAC","CLG","ULC","TEORANTA","TEO","CPT",
            "INC","GMBH","BV","AG","SA","PTY","COMPANY","CO","GROUP","HOLDINGS","IRELAND","INTERNATIONAL"}
TA_RE = re.compile(r"\b(T/A|TRADING AS|T A)\b.*$", re.I)
def norm(s):
    if s is None: return ""
    s = TA_RE.sub("", s.upper()).replace("&"," ")
    s = re.sub(r"[^A-Z0-9 ]"," ", s)
    return " ".join(t for t in s.split() if t and t not in SUFFIXES).strip()
def dnorm(s):  # department/body normaliser for remit matching
    if s is None: return ""
    return re.sub(r"[^A-Z0-9 ]"," ", s.upper()).replace("  "," ").strip()

triple = pl.read_parquet(OUT + "xref_lobbied_then_paid.parquet").filter(pl.col("vendor_class") == "private")
firms = triple.sort("gross_eur", descending=True).head(25)

# disclosed paying bodies per firm
df = pl.read_csv(CSV, schema_overrides={"Total": pl.Float64, "Year": pl.Int64}).with_columns(
    pl.col("entity").str.replace(r"^Agency\s*:\s*","").str.strip_chars().alias("body"),
    pl.col("Supplier").map_elements(norm, return_dtype=pl.Utf8).alias("sn"),
)

# diary detail
dia = pl.read_parquet(BASE + "ministerial_diary_org_mentions.parquet").with_columns(
    pl.col("matched_org_name").map_elements(norm, return_dtype=pl.Utf8).alias("sn"))
# minister -> departments they appear under (remit)
eng = pl.read_parquet(BASE + "ministerial_diary_engagements.parquet", columns=["minister","department"])
min_depts = {r["minister"]: set(x for x in r["depts"] if x)
             for r in eng.group_by("minister").agg(pl.col("department").unique().alias("depts")).iter_rows(named=True)}

# lobbying detail
lob = pl.read_parquet(BASE + "top_lobbyist_organisations.parquet").with_columns(pl.col("lobbyist_name").map_elements(norm, return_dtype=pl.Utf8).alias("sn"))
cli = pl.read_parquet(BASE + "top_client_companies.parquet").with_columns(pl.col("client_name").map_elements(norm, return_dtype=pl.Utf8).alias("sn"))
# awards authorities
awd = pl.read_parquet(BASE + "procurement_awards.parquet", columns=["supplier","Contracting Authority"]).with_columns(pl.col("supplier").map_elements(norm, return_dtype=pl.Utf8).alias("sn"))

rows = []
for f in firms.iter_rows(named=True):
    n = f["sup_norm"]; raw = f["example_raw"]
    paying = sorted(df.filter(pl.col("sn") == n)["body"].unique().to_list())
    paying_d = {dnorm(b) for b in paying}
    dd = dia.filter(pl.col("sn") == n)
    # meetings with date + confidence
    meets = dd.select(["entry_date","minister","subject","match_confidence","matched_org_name"]).sort("entry_date")
    mins = sorted(x for x in set(dd["minister"].to_list()) if x)
    # dept-remit: does any met-minister's department appear among paying bodies?
    remit_hits = set()
    for m in mins:
        for dep in min_depts.get(m, set()):
            dn = dnorm(dep)
            if dn and any(dn == pb or (len(dn) > 8 and dn in pb) for pb in paying_d):
                remit_hits.add(f"{m}~{dep}")
    # lobbying rows
    lr = lob.filter(pl.col("sn") == n)
    cr = cli.filter(pl.col("sn") == n)
    # award authorities that ALSO pay them (award->payment realisation within same body)
    auths = set(dnorm(a) for a in awd.filter(pl.col("sn") == n)["Contracting Authority"].to_list())
    auth_pay_overlap = sorted({a for a in auths if a and any(a == pb or (len(a) > 8 and a in pb) for pb in paying_d)})

    rows.append({
        "firm": raw, "sup_norm": n, "gross_eur": f["gross_eur"], "n_paying_bodies": len(paying),
        "n_diary_meetings": dd.height,
        "diary_match_conf": (", ".join(sorted(set(x for x in dd["match_confidence"].to_list() if x))) if dd.height else None),
        "diary_subjects_sample": " || ".join(str(x)[:50] for x in dd["subject"].to_list()[:3]),
        "diary_matched_names": ", ".join(sorted(set(x for x in dd["matched_org_name"].to_list() if x))[:3]),
        "ministers_met": ", ".join(mins),
        "dept_remit_matches": "; ".join(sorted(remit_hits)) or None,
        "lobby_returns_as_lobbyist": (int(lr["returns_filed"].sum()) if lr.height else 0),
        "lobby_returns_as_client": (int(cr["return_count"].sum()) if cr.height else 0),
        "award_authorities_that_also_pay": "; ".join(auth_pay_overlap[:5]) or None,
    })
    print(f"\n=== {raw}  (€{f['gross_eur']/1e6:.0f}m gross, {len(paying)} bodies) ===")
    print(f"  diary: {dd.height} meetings, conf={rows[-1]['diary_match_conf']}, matched_as='{rows[-1]['diary_matched_names']}'")
    if dd.height:
        for r in meets.head(4).iter_rows(named=True):
            print(f"     {r['entry_date']} | {str(r['minister'] or '?'):14} | conf={r['match_confidence']} | {str(r['subject'])[:60]}")
    print(f"  lobbying: as_lobbyist returns={rows[-1]['lobby_returns_as_lobbyist']}, as_client returns={rows[-1]['lobby_returns_as_client']}")
    print(f"  DEPT-REMIT (met minister whose dept pays them): {rows[-1]['dept_remit_matches']}")
    print(f"  award authority that ALSO pays them: {rows[-1]['award_authorities_that_also_pay']}")

ev = pl.DataFrame(rows)
ev.write_parquet(OUT + "xref_lobbied_then_paid_evidence.parquet")
ev.write_csv(OUT + "xref_lobbied_then_paid_evidence.csv")
print("\n\nWROTE xref_lobbied_then_paid_evidence.parquet/.csv  (top 25 private firms)")
print("dept-remit matches:", ev.filter(pl.col('dept_remit_matches').is_not_null()).height, "/ 25")
print("award-authority-also-pays:", ev.filter(pl.col('award_authorities_that_also_pay').is_not_null()).height, "/ 25")
