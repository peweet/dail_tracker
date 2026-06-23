"""
Cross-corpus leverage build (SANDBOX, read-only on gold; writes only under this folder).

Follows pipeline_sandbox/disclosed_po_spend/cross_corpus_leverage.md:
joins the disclosed EUR-20k supplier spine to lobbying / ministerial-diary / procurement-award
/ CRO corpora and emits the feature tables. Public bodies are EXCLUDED (inter-body transfers,
honesty limit #3). No euro is summed across PO/payment regimes; gross is gross-line-value only.

Outputs (parquet + a couple of CSV previews) into this folder:
  xref_supplier_master.parquet   one row per recurring PRIVATE disclosed supplier + footprint + corpus flags + CRO
  xref_lobbied_then_paid.parquet the triple: in lobbying AND ministerial diary AND a large disclosed payee
  xref_award_to_payment.parquet  firms with BOTH a tendered award and a disclosed payment line
"""
import sys
sys.stdout.reconfigure(encoding="utf-8")
import re
import polars as pl

BASE = "data/gold/parquet/"
CSV = "data/raw_bq/bq-results-20260619-122315-1781871808837.csv"
OUT = "pipeline_sandbox/disclosed_po_spend/"

# --- normaliser (identical to cross_corpus_leverage.md's, for match-rate consistency) ---
SUFFIXES = {
    "LTD", "LIMITED", "PLC", "LLP", "LLC", "UC", "DAC", "CLG", "ULC",
    "TEORANTA", "TEO", "CPT", "INC", "GMBH", "BV", "AG", "SA", "PTY",
    "COMPANY", "CO", "GROUP", "HOLDINGS", "IRELAND", "INTERNATIONAL",
}
TA_RE = re.compile(r"\b(T/A|TRADING AS|T A)\b.*$", re.I)

def norm(s):
    if s is None:
        return ""
    s = s.upper()
    s = TA_RE.sub("", s)
    s = s.replace("&", " ")
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    toks = [t for t in s.split() if t and t not in SUFFIXES]
    return " ".join(toks).strip()

def ncol(expr_col, alias="j_norm"):
    return pl.col(expr_col).map_elements(norm, return_dtype=pl.Utf8).alias(alias)

# ============ disclosed supplier spine ============
df = pl.read_csv(CSV, schema_overrides={"Total": pl.Float64, "Year": pl.Int64})
df = df.with_columns(
    pl.col("entity").str.replace(r"^Agency\s*:\s*", "").str.strip_chars().alias("body"),
    ncol("Supplier", "sup_norm"),
)
df = df.filter(pl.col("sup_norm") != "")

sup = (
    df.group_by("sup_norm")
    .agg(
        pl.col("Total").sum().alias("gross_eur"),
        pl.len().alias("n_rows"),
        pl.col("body").n_unique().alias("n_bodies"),
        pl.col("Supplier").mode().first().alias("example_raw"),
        pl.col("body").unique().sort().alias("bodies"),
    )
)

# ============ PUBLIC-BODY exclusion set (inter-body transfers, not market vendors) ============
body_norms = {norm(b) for b in df["body"].unique().to_list()}
fact_pub = pl.read_parquet(BASE + "procurement_payments_fact.parquet", columns=["publisher_name"])
pub_norms = body_norms | {norm(b) for b in fact_pub["publisher_name"].unique().to_list()}
awd_full = pl.read_parquet(
    BASE + "procurement_awards.parquet",
    columns=["supplier", "Contracting Authority", "value_eur", "value_safe_to_sum", "is_public_body", "Tender ID"],
).with_columns(ncol("supplier"))
pub_norms |= set(awd_full.filter(pl.col("is_public_body"))["j_norm"].to_list())
# a few obvious public/semi-state self-payees the flags miss
pub_norms |= {norm(x) for x in ["ESB", "C&AG", "HEA", "OPW", "Revenue", "An Garda Siochana", "Central Bank of Ireland"]}
pub_norms.discard("")

sup = sup.with_columns(pl.col("sup_norm").is_in(list(pub_norms)).alias("is_public_body"))

# candidate PRIVATE suppliers: recurring (>=2 bodies) OR material (gross >= 1m), exclude public bodies
cand = sup.filter((~pl.col("is_public_body")) & ((pl.col("n_bodies") >= 2) | (pl.col("gross_eur") >= 1_000_000)))
print(f"disclosed suppliers: {sup.height:,} | public-body-excluded: {sup['is_public_body'].sum():,} | candidate private set: {cand.height:,}")

# ============ corpus key-sets + detail ============
lob = pl.read_parquet(BASE + "top_lobbyist_organisations.parquet").with_columns(ncol("lobbyist_name"))
lob_norms = set(lob.filter(pl.col("j_norm") != "")["j_norm"].to_list())
cli = pl.read_parquet(BASE + "top_client_companies.parquet").with_columns(ncol("client_name"))
cli_norms = set(cli.filter(pl.col("j_norm") != "")["j_norm"].to_list())
lobby_all = lob_norms | cli_norms

# diaries: curated org-influence table (has ministers_met) + raw org mentions
dci = pl.read_parquet(BASE + "diary_company_influence.parquet").with_columns(ncol("organisation"))
dci_norms = set(dci.filter(pl.col("j_norm") != "")["j_norm"].to_list())
dia = pl.read_parquet(BASE + "ministerial_diary_org_mentions.parquet").with_columns(ncol("matched_org_name"))
dia_norms = set(dia.filter(pl.col("j_norm") != "")["j_norm"].to_list())
diary_all = dci_norms | dia_norms
# map norm -> ministers met (from curated table), and norm -> set of ministers from raw mentions
dci_min = {r["j_norm"]: r["ministers_met"] for r in dci.iter_rows(named=True) if r["j_norm"]}
dia_min = (dia.filter(pl.col("j_norm") != "").group_by("j_norm")
           .agg(pl.col("minister").unique().alias("mins"), pl.col("entry_date").min().alias("first"), pl.col("entry_date").max().alias("last")))
dia_min_map = {r["j_norm"]: r for r in dia_min.iter_rows(named=True)}

# awards
awd_norms = set(awd_full.filter(pl.col("j_norm") != "")["j_norm"].to_list())
awd_agg = (awd_full.filter(pl.col("j_norm") != "").group_by("j_norm")
           .agg(pl.len().alias("award_rows"),
                pl.col("Contracting Authority").n_unique().alias("award_authorities"),
                pl.col("value_eur").filter(pl.col("value_safe_to_sum")).sum().alias("award_value_eur_safe")))
awd_map = {r["j_norm"]: r for r in awd_agg.iter_rows(named=True)}

# CRO
crom = pl.read_parquet(BASE + "procurement_supplier_cro_match.parquet").with_columns(ncol("supplier"))
crom_norms = set(crom.filter(pl.col("j_norm") != "")["j_norm"].to_list())
crom_map = {r["j_norm"]: r for r in crom.filter(pl.col("j_norm") != "").iter_rows(named=True)}

# charities register (NGOs receiving state grants — a DIFFERENT relationship to procurement)
cha = pl.read_parquet(BASE + "charities_enriched.parquet").with_columns(ncol("registered_charity_name"))
cha_norms = set(cha.filter(pl.col("j_norm") != "")["j_norm"].to_list())

# our parsed fact (already-seen + pre-resolved cro)
fact = pl.read_parquet(BASE + "procurement_payments_fact.parquet", columns=["supplier_raw", "cro_company_num"]).with_columns(ncol("supplier_raw"))
fact_norms = set(fact.filter(pl.col("j_norm") != "")["j_norm"].to_list())

# ============ assemble master ============
def flag(s, uni):
    return pl.col(s).is_in(list(uni))

master = cand.with_columns(
    flag("sup_norm", lob_norms).alias("in_lobbyist"),
    flag("sup_norm", cli_norms).alias("in_lobby_client"),
    flag("sup_norm", diary_all).alias("in_diary"),
    flag("sup_norm", awd_norms).alias("in_award"),
    flag("sup_norm", crom_norms).alias("in_cro"),
    flag("sup_norm", fact_norms).alias("in_our_fact"),
).with_columns(
    (pl.col("in_lobbyist") | pl.col("in_lobby_client")).alias("in_lobbying"),
)
# attach award detail
master = master.with_columns(
    pl.col("sup_norm").map_elements(lambda n: (awd_map.get(n) or {}).get("award_rows"), return_dtype=pl.Int64).alias("award_rows"),
    pl.col("sup_norm").map_elements(lambda n: (awd_map.get(n) or {}).get("award_authorities"), return_dtype=pl.Int64).alias("award_authorities"),
    pl.col("sup_norm").map_elements(lambda n: (str(v) if (v := (crom_map.get(n) or {}).get("company_num")) is not None else None), return_dtype=pl.Utf8).alias("cro_company_num"),
    pl.col("sup_norm").map_elements(lambda n: (crom_map.get(n) or {}).get("company_status"), return_dtype=pl.Utf8).alias("cro_status"),
    pl.col("sup_norm").map_elements(lambda n: (crom_map.get(n) or {}).get("match_confidence"), return_dtype=pl.Float64).alias("cro_confidence"),
)
master = master.with_columns(
    (pl.col("in_lobbying").cast(pl.Int8) + pl.col("in_diary").cast(pl.Int8) + pl.col("in_award").cast(pl.Int8) + pl.col("in_cro").cast(pl.Int8)).alias("n_corpora"),
)

# vendor_class: separate genuine private vendors from semi-states / public institutions
# (these lobby & are paid but are NOT private market suppliers — keep, but tag so the
#  "private vendor lobbied-then-paid" lens can exclude them).
SEMISTATE = {norm(x) for x in [
    "An Post", "Gas Networks Ireland", "Bord na Mona", "ESB", "Electricity Supply Board",
    "EirGrid", "Irish Water", "Uisce Eireann", "Coillte", "RTE", "Raidio Teilifis Eireann",
    "DAA", "Bord Gais", "Bord Gais Energy", "Bord Bia", "Enterprise Ireland", "IDA Ireland",
    "An Bord Pleanala", "Bord Iascaigh Mhara", "Irish Rail", "Iarnrod Eireann", "Bus Eireann",
    "Dublin Bus", "Transport Infrastructure Ireland", "VHI", "Vhi Healthcare",
]}
INSTITUTION_KW = ["UNIVERSITY", "COLLEGE", "INSTITUTE OF TECHNOLOGY", "TECHNOLOGICAL UNIVERSITY",
                  "ROYAL COLLEGE", "TRINITY", "HOSPITAL", "HEALTH SERVICE", "ETB",
                  "EDUCATION AND TRAINING BOARD", "INSTITUTE FOR"]
FINANCIAL = {norm(x) for x in [
    "AIB", "Allied Irish Banks", "Bank of Ireland", "Barclays Bank", "Ulster Bank",
    "Permanent TSB", "PTSB", "KBC Bank", "Citibank", "Bank of America", "JP Morgan",
]}
CHARITY_NGO = {norm(x) for x in [
    "Respond", "Peter McVerry Trust", "Merchants Quay Ireland", "Educate Together",
    "Focus Ireland", "Simon Community", "Society of St Vincent de Paul", "Barnardos",
    "Rehab Group", "Enable Ireland", "Crosscare", "Dublin Simon Community",
]}
def vclass(s):
    n, raw = s["sup_norm"], (s["example_raw"] or "").upper()
    if n in SEMISTATE:
        return "semi_state"
    if n in FINANCIAL or raw.endswith(" BANK") or " BANK " in raw:
        return "financial"
    if n in CHARITY_NGO or n in cha_norms or "TRUST" in raw or "FOUNDATION" in raw:
        return "charity_ngo"
    if any(k in raw for k in INSTITUTION_KW):
        return "institution"
    return "private"
master = master.with_columns(
    pl.struct(["sup_norm", "example_raw"]).map_elements(vclass, return_dtype=pl.Utf8).alias("vendor_class"),
)
master.sort("gross_eur", descending=True).write_parquet(OUT + "xref_supplier_master.parquet")

# ============ lobbied-then-paid (the triple) ============
triple = master.filter(pl.col("in_lobbying") & pl.col("in_diary")).sort("gross_eur", descending=True)
def ministers_for(n):
    # ALWAYS names, from the per-mention diary table (never the curated count)
    r = dia_min_map.get(n)
    if r and r["mins"]:
        names = sorted({x for x in r["mins"] if x})
        return ", ".join(names) if names else None
    return None
triple = triple.with_columns(
    pl.col("sup_norm").map_elements(ministers_for, return_dtype=pl.Utf8).alias("ministers_met"),
).with_columns(
    pl.col("ministers_met").str.count_matches(",").add(1).fill_null(0).alias("n_ministers_met"),
)
triple.select(["example_raw","sup_norm","vendor_class","gross_eur","n_bodies","in_lobbyist","in_lobby_client",
               "award_rows","award_authorities","cro_company_num","cro_confidence","n_ministers_met","ministers_met","bodies"]
              ).write_parquet(OUT + "xref_lobbied_then_paid.parquet")

# ============ award-to-payment bridge ============
bridge = (master.filter(pl.col("in_award"))
          .select(["example_raw","sup_norm","gross_eur","n_bodies","award_rows","award_authorities","cro_company_num"])
          .sort("gross_eur", descending=True))
bridge.write_parquet(OUT + "xref_award_to_payment.parquet")

# ============ match-rate sanity vs the documented floor (top-90 candidate set) ============
top50 = sup.filter(~pl.col("is_public_body")).sort("gross_eur", descending=True).head(50)
cross40 = sup.filter(~pl.col("is_public_body")).sort(["n_bodies","gross_eur"], descending=[True,True]).head(40)
doc_cand = set(pl.concat([top50, cross40]).unique(subset=["sup_norm"])["sup_norm"].to_list())
def rate(s, uni): h=sum(1 for x in s if x in uni); return h, round(100*h/max(1,len(s)),1)
print(f"\n=== match-rate sanity (private top-by-gross+by-bodies set, n={len(doc_cand)}) ===")
for lbl, uni in [("awards", awd_norms),("CRO", crom_norms),("lobbying(any)", lobby_all),("diaries", diary_all),("our-fact", fact_norms)]:
    h,p = rate(doc_cand, uni); print(f"  {lbl:14}: {h}/{len(doc_cand)} = {p}%")

print(f"\nWROTE:")
print(f"  xref_supplier_master.parquet      rows={master.height:,}  (n_corpora>=2: {master.filter(pl.col('n_corpora')>=2).height:,})")
print(f"  xref_lobbied_then_paid.parquet    rows={triple.height:,}")
print(f"  xref_award_to_payment.parquet     rows={bridge.height:,}")
print("  lobbied-then-paid by vendor_class:", triple.group_by("vendor_class").len().sort("len", descending=True).to_dicts())
print("\n=== LOBBIED-THEN-PAID — PRIVATE vendors only, top 25 by disclosed gross (gross=gross-line-value NOT spend) ===")
with pl.Config(tbl_rows=27, fmt_str_lengths=30, tbl_width_chars=210):
    print(triple.filter(pl.col("vendor_class") == "private").select(
        ["example_raw","gross_eur","n_bodies","in_lobbyist","in_lobby_client","award_rows","cro_company_num","n_ministers_met","ministers_met"]).head(25))
