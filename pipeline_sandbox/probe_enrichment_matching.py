"""EXPLORATION PROBE (2026-06-12): what can the new enrichment-round data match?

Read-only prototype — no parquet written, no pipeline touched. Four experiments:

  1. Diary subjects x lobbying-org gazetteer (the planned §7.2 fuzzy match)
  2. Diary<->lobbying CORROBORATION window-join (org + minister + period)
  3. CBI enforcement parties x CRO register + lobbying register
  4. EU TAM beneficiaries x CRO (via company number) + lobbying register

Run: .venv/Scripts/python.exe pipeline_sandbox/probe_enrichment_matching.py
"""

from __future__ import annotations

import re
import sys
import unicodedata
from collections import defaultdict

import polars as pl

sys.stdout.reconfigure(encoding="utf-8")

ENR = "data/sandbox/enrichment/"
LOB = "data/silver/lobbying/parquet/"

_SUFFIX_RE = re.compile(
    r"\b(limited|ltd|plc|p\.l\.c|dac|d\.a\.c|clg|uc|ulc|icav|teoranta|teo|cga|"
    r"company|co|holdings|group|ireland|irish|the)\b\.?",
)
_PUNCT_RE = re.compile(r"[^a-z0-9 ]")

# tokens too generic to anchor a single-token match
STOP = set(
    ["ireland", "irish", "national", "association", "federation", "society", "institute", "council", "group", "alliance", "forum", "network", "centre", "center", "office", "board", "union", "college", "university", "services", "service", "association", "department", "minister", "meeting", "launch", "visit", "interview", "company", "limited", "association", "irelands", "new", "event", "opening", "members", "association", "enterprise", "chambers", "chamber", "local", "capital", "business", "holdings", "management", "partners", "technology", "energy", "media", "communications", "development"]
)
# a single-token org name only counts when the subject reads like an engagement
CUE_RE = re.compile(
    r"\b(meeting|meet|mtg|call|phonecall|visit|launch|opening|reception|roundtable|"
    r"briefing|dinner|lunch|breakfast|address|speech|event|presentation|signing)\b"
)


def norm(s: str) -> str:
    s = unicodedata.normalize("NFD", s.lower())
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.replace("&", " and ")
    s = _PUNCT_RE.sub(" ", s)
    s = _SUFFIX_RE.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


def hr(title: str) -> None:
    print(f"\n{'=' * 78}\n{title}\n{'=' * 78}")


# ---------------------------------------------------------------- load sources
entries = pl.read_parquet(ENR + "ministerial_diary_entries.parquet")
cbi = pl.read_parquet(ENR + "cbi_enforcement_actions.parquet")
tam = pl.read_parquet(ENR + "eu_tam_ireland_awards.parquet")
isif = pl.read_parquet(ENR + "isif_portfolio.parquet")

lobbyists = (
    (pl.scan_parquet(LOB + "returns_master.parquet").select("lobbyist_name").unique().collect())["lobbyist_name"]
    .drop_nulls()
    .to_list()
)
clients = (
    (pl.scan_parquet(LOB + "client_company_returns_detail.parquet").select("client_name").unique().collect())[
        "client_name"
    ]
    .drop_nulls()
    .to_list()
)
cro = pl.scan_parquet("data/silver/cro/companies.parquet")

print(
    f"sources: {len(entries)} diary entries | {len(lobbyists)} lobbyist orgs | "
    f"{len(clients)} client orgs | {cbi.height} CBI actions | {tam.height} TAM awards"
)

# ------------------------------------------------- gazetteer + token index
gaz: dict[str, tuple[str, str]] = {}  # norm -> (display, source)
for name in lobbyists:
    n = norm(name)
    if len(n) >= 4:
        gaz.setdefault(n, (name, "lobbyist"))
for name in clients:
    if len(name) > 120:  # client field sometimes holds a paragraph, skip
        continue
    n = norm(name)
    if len(n) >= 4:
        gaz.setdefault(n, (name, "client"))


# Round-1 finding: suffix-stripping collapses names like "Enterprise Holdings
# Limited" / "Chambers Ireland" / "Local Ireland" to ONE generic token, which
# then matches department prose, person surnames ("Minister Chambers") and
# travel lines. Tiering (plan §7.2 said exactly this):
#   high   = >=2-token normalised name found in subject
#   medium = single distinctive token (len>=6, non-STOP) + engagement cue
def anchor_tier(n: str) -> str | None:
    toks = n.split()
    if len(toks) >= 2:
        return "high"
    if len(toks) == 1 and len(toks[0]) >= 6 and toks[0] not in STOP:
        return "medium"
    return None


tiers = {n: anchor_tier(n) for n in gaz}
gaz = {n: v for n, v in gaz.items() if tiers[n]}
token_index: dict[str, set[str]] = defaultdict(set)
for n in gaz:
    for t in n.split():
        if len(t) >= 4 and t not in STOP:
            token_index[t].add(n)

print(f"gazetteer: {len(gaz)} normalised orgs, {len(token_index)} index tokens")

# ============================================================ EXP 1: diary match
hr("EXP 1 — diary subjects x lobbying-org gazetteer")
matches = []
for row in entries.iter_rows(named=True):
    subj_n = " " + norm(row["subject"] or "") + " "
    has_cue = bool(CUE_RE.search((row["subject"] or "").lower()))
    cands: set[str] = set()
    for t in subj_n.split():
        cands |= token_index.get(t, set())
    for cand in cands:
        tier = tiers[cand]
        if tier == "medium" and not has_cue:
            continue
        if f" {cand} " in subj_n:
            matches.append(
                {
                    "entry_date": row["entry_date"],
                    "minister": row["minister"],
                    "subject": row["subject"],
                    "org_norm": cand,
                    "org": gaz[cand][0],
                    "gaz_source": gaz[cand][1],
                    "tier": tier,
                }
            )
m = pl.DataFrame(matches)
if m.is_empty():
    print("NO MATCHES — check normalisation")
else:
    n_entries = m["subject"].n_unique()
    print(
        f"{len(m)} mentions | {n_entries} distinct entries matched "
        f"({100 * n_entries / len(entries):.1f}% of {len(entries)} entries)"
    )
    print("tier split:", m.group_by("tier").len().to_dicts())
    print("\ntop matched orgs (DISTINCT entries):")
    print(
        m.group_by("org")
        .agg(pl.col("subject").n_unique().alias("n_entries"))
        .sort("n_entries", descending=True)
        .head(15)
    )
    print("\ntop (minister, org) pairs:")
    print(
        m.group_by("minister", "org").agg(pl.col("subject").n_unique().alias("n")).sort("n", descending=True).head(12)
    )
    print("\nrandom sample of 15 mentions (precision eyeball):")
    for r in m.sample(min(15, len(m)), seed=42).iter_rows(named=True):
        print(f'  [{r["entry_date"]}] {r["minister"]}: "{r["subject"][:70]}"  ->  {r["org"][:45]} ({r["gaz_source"]})')

# ===================================== EXP 2: corroboration window-join
hr("EXP 2 — diary<->lobbying corroboration (same org + same minister + period)")
if not m.is_empty():
    pol = (
        pl.scan_parquet(LOB + "politician_returns_detail.parquet")
        .select("full_name", "position", "lobbyist_name", "primary_key", "lobbying_period_start_date")
        .with_columns(pl.col("lobbyist_name").map_elements(norm, return_dtype=pl.String).alias("org_norm"))
        .collect()
    )
    ends = pl.scan_parquet(LOB + "returns_master.parquet").select("primary_key", "lobbying_period_end_date").collect()
    pol = pol.join(ends, on="primary_key", how="left").with_columns(
        pl.col("full_name").str.to_lowercase().str.extract(r"(\S+)$").alias("surname"),
        pl.col("lobbying_period_start_date").dt.date().alias("p_start"),
        pl.col("lobbying_period_end_date").dt.date().alias("p_end"),
    )
    md = m.filter(pl.col("minister").is_not_null()).with_columns(pl.col("minister").str.to_lowercase().alias("surname"))
    cor = md.join(pol, on=["org_norm", "surname"], how="inner").filter(
        (pl.col("entry_date") >= pl.col("p_start")) & (pl.col("entry_date") <= pl.col("p_end"))
    )
    print(
        f"corroborated mention-return pairs: {len(cor)} | distinct diary entries: "
        f"{cor['subject'].n_unique() if len(cor) else 0} | distinct returns: "
        f"{cor['primary_key'].n_unique() if len(cor) else 0}"
    )
    if len(cor):
        # surname-only identity joins collide (Emer Higgins vs Alice-Mary/Clodagh
        # Higgins) — count how many distinct full_names each diary surname hit
        ids = cor.group_by("minister").agg(pl.col("full_name").n_unique().alias("n_full_names"))
        print("identity fan-out per diary surname (1 = clean, >1 = collision):")
        print(ids.sort("n_full_names", descending=True).head(8).to_dicts())
    if len(cor):
        print("\nsample corroborations:")
        seen = set()
        for r in cor.iter_rows(named=True):
            k = (r["subject"], r["org"])
            if k in seen:
                continue
            seen.add(k)
            print(f'  [{r["entry_date"]}] Min. {r["minister"]} diary: "{r["subject"][:58]}"')
            print(
                f"      <-> return {r['primary_key']} by {r['lobbyist_name'][:50]} "
                f"(period {r['p_start']}..{r['p_end']}, names {r['full_name']})"
            )
            if len(seen) >= 10:
                break

# ===================================== EXP 3: CBI x CRO + lobbying
hr("EXP 3 — CBI enforcement parties x CRO register + lobbying register")
cro_names = (
    cro.select("company_num", "company_name", "company_status")
    .with_columns(pl.col("company_name").map_elements(norm, return_dtype=pl.String).alias("name_norm"))
    .collect()
)
cbi_n = cbi.filter(pl.col("party_name").is_not_null()).with_columns(
    pl.col("party_name").map_elements(norm, return_dtype=pl.String).alias("name_norm")
)
cbi_cro = cbi_n.join(cro_names, on="name_norm", how="left")
hit = cbi_cro.filter(pl.col("company_num").is_not_null())
print(
    f"CBI parties exact-norm matched to CRO: {hit['party_name'].n_unique()}/{cbi_n.height} "
    f"({100 * hit['party_name'].n_unique() / cbi_n.height:.0f}%)"
)
lob_norms = {norm(x) for x in lobbyists}
cbi_lob = cbi_n.filter(pl.col("name_norm").is_in(list(lob_norms)))
print(f"CBI-sanctioned parties ALSO on lobbying register (exact-norm): {cbi_lob.height}")
for r in cbi_lob.sort("fine_amount_eur", descending=True, nulls_last=True).head(10).iter_rows(named=True):
    amt = f"€{r['fine_amount_eur']:,.0f}" if r["fine_amount_eur"] else "no-fine-parsed"
    print(f"  - {r['party_name'][:55]} | {r['notice_date']} | {amt}")

# ===================================== EXP 4: TAM x CRO + lobbying
hr("EXP 4 — TAM beneficiaries x CRO (company number) + lobbying register")
tam_n = tam.with_columns(pl.col("beneficiary_name").map_elements(norm, return_dtype=pl.String).alias("name_norm"))
with_num = tam_n.filter(pl.col("cro_company_num").is_not_null())
cro_nums = set(cro_names["company_num"].cast(pl.String).str.strip_chars("0").to_list())
tam_join = with_num.with_columns(
    pl.col("cro_company_num").str.strip_chars("0").is_in(list(cro_nums)).alias("num_in_cro")
)
ok = tam_join.filter(pl.col("num_in_cro"))
print(f"TAM rows with a CRO-shaped national id: {with_num.height}/{tam.height}")
print(f"  ...whose number exists in the CRO register: {ok.height} ({100 * ok.height / max(1, with_num.height):.0f}%)")
# name-vs-number agreement spot check
spot = ok.join(
    cro_names.with_columns(pl.col("company_num").cast(pl.String).str.strip_chars("0")),
    left_on=pl.col("cro_company_num").str.strip_chars("0"),
    right_on="company_num",
    how="inner",
)
agree = spot.filter(pl.col("name_norm") == pl.col("name_norm_right"))
print(
    f"  name==CRO-name exact agreement on joined rows: {agree.height}/{spot.height} "
    f"({100 * agree.height / max(1, spot.height):.0f}%) — rest are alias/trading-name drift"
)
tam_lob = tam_n.filter(pl.col("name_norm").is_in(list(lob_norms)))
print(
    f"\nTAM beneficiaries ALSO on lobbying register (exact-norm): "
    f"{tam_lob['beneficiary_name'].n_unique()} orgs / {tam_lob.height} awards"
)
print(
    tam_lob.group_by("beneficiary_name")
    .agg(
        pl.len().alias("n_awards"),
        pl.col("granting_authority").n_unique().alias("n_authorities"),
    )
    .sort("n_awards", descending=True)
    .head(12)
)

isif_lob = isif.with_columns(
    pl.col("investee_name").map_elements(norm, return_dtype=pl.String).alias("name_norm")
).filter(pl.col("name_norm").is_in(list(lob_norms)))
print(f"\nISIF investees also on lobbying register: {isif_lob.height}/213")
for r in isif_lob.head(8).iter_rows(named=True):
    print(f"  - {r['investee_name']}")
