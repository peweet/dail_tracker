import sys
sys.stdout.reconfigure(encoding="utf-8")
import re
import polars as pl

BASE = "data/gold/parquet/"
CSV = "data/raw_bq/bq-results-20260619-122315-1781871808837.csv"

# --- rough normaliser mimicking our gold supplier_normalised convention ---
SUFFIXES = {
    "LTD", "LIMITED", "PLC", "LLP", "LLC", "UC", "DAC", "CLG", "ULC",
    "TEORANTA", "TEO", "CPT", "INC", "GMBH", "BV", "AG", "SA", "PTY",
    "COMPANY", "CO", "GROUP", "HOLDINGS", "IRELAND", "INTERNATIONAL",
}
TA_RE = re.compile(r"\b(T/A|TRADING AS|T A)\b.*$", re.I)

def norm(s: str) -> str:
    if s is None:
        return ""
    s = s.upper()
    s = TA_RE.sub("", s)            # drop trailing trading-as
    s = s.replace("&", " ")
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    toks = [t for t in s.split() if t and t not in SUFFIXES]
    return " ".join(toks).strip()

# --- load disclosed CSV ---
df = pl.read_csv(CSV, schema_overrides={"Total": pl.Float64, "Year": pl.Int64})
# strip Agency : prefix
df = df.with_columns(
    pl.col("entity").str.replace(r"^Agency\s*:\s*", "").alias("body")
)
df = df.with_columns(
    pl.col("Supplier").map_elements(norm, return_dtype=pl.Utf8).alias("sup_norm")
)
df = df.filter(pl.col("sup_norm") != "")

print("Disclosed rows:", df.height, "bodies:", df["body"].n_unique())

# --- top ~50 suppliers by gross (rough-normalised). NOTE gross mixes PO+payment semantics; caveat. ---
sup = (
    df.group_by("sup_norm")
    .agg(
        pl.col("Total").sum().alias("gross_eur"),
        pl.len().alias("n_rows"),
        pl.col("body").n_unique().alias("n_bodies"),
        pl.col("Supplier").first().alias("example_raw"),
    )
    .sort("gross_eur", descending=True)
)
top50 = sup.head(50)
print("\n=== TOP 50 SUPPLIERS BY GROSS (rough-norm; gross mixes PO+payment, NOT spend) ===")
with pl.Config(tbl_rows=60, fmt_str_lengths=40, tbl_width_chars=160):
    print(top50.select(["sup_norm", "gross_eur", "n_rows", "n_bodies", "example_raw"]))

# --- top cross-body suppliers (whole-of-government footprint) ---
crossbody = sup.sort(["n_bodies", "gross_eur"], descending=[True, True]).head(40)
print("\n=== TOP CROSS-BODY SUPPLIERS (most distinct bodies) ===")
with pl.Config(tbl_rows=45, fmt_str_lengths=40, tbl_width_chars=160):
    print(crossbody.select(["sup_norm", "n_bodies", "gross_eur", "n_rows", "example_raw"]))

# --- candidate set for joins: union of top50-by-gross + top40-crossbody ---
cand = (
    pl.concat([top50, crossbody]).unique(subset=["sup_norm"]).sort("gross_eur", descending=True)
)
cand_norms = set(cand["sup_norm"].to_list())
print("\nCandidate suppliers for join tests:", len(cand_norms))

# ============ JOIN TARGETS ============

def load_norm(path, src_col, out="j_norm"):
    d = pl.read_parquet(BASE + path)
    d = d.with_columns(pl.col(src_col).map_elements(norm, return_dtype=pl.Utf8).alias(out))
    return d

# (a) Lobbying register — lobbyist orgs + client companies
lob = load_norm("top_lobbyist_organisations.parquet", "lobbyist_name")
lob_norms = set(lob.filter(pl.col("j_norm") != "")["j_norm"].to_list())
cli = load_norm("top_client_companies.parquet", "client_name")
cli_norms = set(cli.filter(pl.col("j_norm") != "")["j_norm"].to_list())
lobby_all = lob_norms | cli_norms

# (b) Ministerial diaries — orgs ministers met
dia = load_norm("ministerial_diary_org_mentions.parquet", "matched_org_name")
dia_norms = set(dia.filter(pl.col("j_norm") != "")["j_norm"].to_list())
# also diary_company_influence (curated org list w/ awards/paid)
dci = load_norm("diary_company_influence.parquet", "organisation")
dci_norms = set(dci.filter(pl.col("j_norm") != "")["j_norm"].to_list())
diary_all = dia_norms | dci_norms

# (c) Procurement awards (eTenders/TED winners)
awd = pl.read_parquet(BASE + "procurement_awards.parquet",
                      columns=["supplier", "supplier_norm", "value_eur", "Contracting Authority", "Tender ID"])
# supplier_norm already uppercased in gold; re-run our norm on the raw supplier for consistency
awd = awd.with_columns(pl.col("supplier").map_elements(norm, return_dtype=pl.Utf8).alias("j_norm"))
awd_norms = set(awd.filter(pl.col("j_norm") != "")["j_norm"].to_list())

# (d) CRO — via our supplier_cro_match + our fact's cro_company_num
crom = load_norm("procurement_supplier_cro_match.parquet", "supplier")
crom_norms = set(crom.filter(pl.col("j_norm") != "")["j_norm"].to_list())

# charities (state-adjacent firms can be charities)
cha = load_norm("charities_enriched.parquet", "registered_charity_name")
cha_norms = set(cha.filter(pl.col("j_norm") != "")["j_norm"].to_list())

# our own fact suppliers (does disclosed supplier already appear in our parsed corpus?)
fact = pl.read_parquet(BASE + "procurement_payments_fact.parquet",
                       columns=["supplier_raw", "supplier_normalised", "cro_company_num", "publisher_name"])
fact = fact.with_columns(pl.col("supplier_raw").map_elements(norm, return_dtype=pl.Utf8).alias("j_norm"))
fact_norms = set(fact.filter(pl.col("j_norm") != "")["j_norm"].to_list())
fact_with_cro = set(fact.filter((pl.col("cro_company_num").is_not_null()) & (pl.col("j_norm") != ""))["j_norm"].to_list())

# ============ MATCH RATES over candidate set ============
def rate(s, universe):
    hit = sum(1 for x in s if x in universe)
    return hit, round(100 * hit / max(1, len(s)), 1)

print("\n=== MATCH RATES (candidate top-suppliers set, n=%d) ===" % len(cand_norms))
for label, uni in [
    ("(a) lobbying register (lobbyist OR client)", lobby_all),
    ("    ...lobbyist-org only", lob_norms),
    ("    ...client-company only", cli_norms),
    ("(b) ministerial diaries (orgs met)", diary_all),
    ("(c) procurement AWARDS (eTenders/TED winner)", awd_norms),
    ("(d) CRO via supplier_cro_match", crom_norms),
    ("    our fact suppliers (already parsed)", fact_norms),
    ("    our fact rows w/ resolved cro_company_num", fact_with_cro),
    ("    charities register", cha_norms),
]:
    h, p = rate(cand_norms, uni)
    print(f"  {label}: {h}/{len(cand_norms)} = {p}%")

# ============ CONCRETE LINKAGES (per candidate, which corpora) ============
print("\n=== CONCRETE LINKAGES (top candidates -> corpora) ===")
rows = []
for r in cand.iter_rows(named=True):
    n = r["sup_norm"]
    hits = []
    if n in lob_norms: hits.append("lobbyist")
    if n in cli_norms: hits.append("lobby-client")
    if n in diary_all: hits.append("diary")
    if n in awd_norms: hits.append("award")
    if n in crom_norms: hits.append("cro")
    if n in fact_norms: hits.append("our-fact")
    if n in cha_norms: hits.append("charity")
    rows.append((r["example_raw"], n, r["gross_eur"], r["n_bodies"], "+".join(hits) if hits else "-"))

link = pl.DataFrame(rows, schema=["example_raw","sup_norm","gross_eur","n_bodies","corpora"], orient="row")
with pl.Config(tbl_rows=90, fmt_str_lengths=42, tbl_width_chars=170):
    print(link.sort("gross_eur", descending=True))

# ============ AWARD->PAYMENT realisation evidence for a few firms ============
print("\n=== AWARD vs PAYMENT cross-evidence (sample firms with BOTH award + disclosed payment) ===")
both = [n for n in cand_norms if n in awd_norms]
for n in sorted(both, key=lambda x: -sup.filter(pl.col('sup_norm')==x)['gross_eur'][0])[:15]:
    awd_val = awd.filter(pl.col("j_norm") == n)["value_eur"].sum()
    awd_cnt = awd.filter(pl.col("j_norm") == n).height
    auths = awd.filter(pl.col("j_norm") == n)["Contracting Authority"].n_unique()
    disc = sup.filter(pl.col("sup_norm") == n)
    disc_gross = disc["gross_eur"][0]
    disc_bodies = disc["n_bodies"][0]
    raw = disc["example_raw"][0]
    print(f"  {raw[:45]:45} | awards: {awd_cnt} rows / {auths} authorities / EUR {awd_val:,.0f} | disclosed gross: EUR {disc_gross:,.0f} across {disc_bodies} bodies")

# ============ Lobbied-a-minister AND paid: cross firms ============
print("\n=== LOBBIED + DIARY + PAID (disclosed) firms ===")
for r in cand.iter_rows(named=True):
    n = r["sup_norm"]
    if (n in lobby_all) and (n in diary_all):
        print(f"  {r['example_raw'][:45]:45} | gross EUR {r['gross_eur']:,.0f} / {r['n_bodies']} bodies | lobby+diary+disclosed-payment")
