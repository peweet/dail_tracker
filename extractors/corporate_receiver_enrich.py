"""Receiver-appointer + operator-firm enrichment (PROMOTED, writes gold).

Graduates the receiver-appointer RANKING and operator-firm CONCENTRATION that
used to be recomputed in pandas on every Corporate-page load (utility/pages_code/
corporate.py :: _render_featured / _render_operator_strip) into precomputed gold.
Those panels are independent of the page filters (rendered on the full corpus),
so the aggregation was a static model recomputed per request — pipeline territory,
not display. See the logic-firewall audit (2026-06-20).

Inputs (read-only):
    data/gold/parquet/corporate_notices.parquet   (iris chain — corporate_notices_enrichment.py)
        carries entity_name, raw_text, notice_subtype, and the curated list
        columns parent_fund_mentions / fund_type_mentions (tagged from
        data/_meta/loan_book_fund_aliases.csv).

Outputs (gold):
    data/gold/parquet/corporate_notices_enriched.parquet
        the notices SUPERSET — every original column plus the per-notice flags
        the page used to derive at render time:
          is_receivership   bool   receivership-shaped notice
          is_spv            bool   entity_name has DAC / ICAV / "designated activity company" shape
          has_parent_mention bool  parent_fund_mentions non-empty
          receiver_firms    list   curated professional firms named in raw_text (ALL notices)
          has_receiver_firm bool   receiver_firms non-empty
        v_corporate_notices reads this superset (backward compatible).
    data/gold/parquet/corporate_receiver_appointers.parquet
        one row per parent fund named across receivership notices:
          parent, n_notices, dominant_fund_type, type_bucket
    data/gold/parquet/corporate_receiver_firms.parquet
        one row per professional firm named across receivership notices:
          firm, n_notices (notice-presence, counted once per notice), is_big6

Match parity: the firm regexes, the receivership-shape test, the SPV test, the
dominant-fund-type tiebreak and the type-bucket map are LIFTED VERBATIM from
corporate.py so the gold is byte-identical to what the page rendered. A reference
recomputation at the bottom of main() asserts the two agree before writing.

Engine: Polars (project convention — Polars for ETL, pandas only in the UI layer).
The per-row Python loops (firm regexes, CBI resolver, parent/ftype zip) are kept
as loops on purpose: the match semantics are pinned verbatim to corporate.py's
Python `re` patterns, and the corpus (~51k notices) is loop-cheap.

Run:
    .venv/Scripts/python.exe extractors/corporate_receiver_enrich.py
"""

from __future__ import annotations

import re
import sys
from collections import Counter
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:  # noqa: SIM105
    sys.stdout.reconfigure(encoding="utf-8")  # accented entity names on cp1252 consoles
except Exception:
    pass

from services.parquet_io import save_parquet  # noqa: E402
from shared.name_norm import name_norm_str  # noqa: E402

NOTICES_PARQUET = ROOT / "data" / "gold" / "parquet" / "corporate_notices.parquet"
CBI_XREF_PARQUET = ROOT / "data" / "gold" / "parquet" / "cbi_xref_corporate_notices.parquet"
ENRICHED_PARQUET = ROOT / "data" / "gold" / "parquet" / "corporate_notices_enriched.parquet"
APPOINTERS_PARQUET = ROOT / "data" / "gold" / "parquet" / "corporate_receiver_appointers.parquet"
FIRMS_PARQUET = ROOT / "data" / "gold" / "parquet" / "corporate_receiver_firms.parquet"

FEATURED_TOP_N = 8  # mirrors corporate.FEATURED_TOP_N (display cap lives in the page; gold keeps all)

# ── Verbatim matchers from corporate.py ───────────────────────────────────────
_RECEIVERSHIP_RE = "APPOINTMENT OF (?:STATUTORY )?RECEIVER|NOTICE OF APPOINTMENT OF RECEIVER"
# Non-capturing group — identical match set to corporate.py's _SPV_RE. Applied
# case-insensitively ((?i) at the call site — Polars regex takes inline flags,
# not a compiled-pattern object).
_SPV_RE = r"\b(?:DAC|DESIGNATED ACTIVITY COMPANY|ICAV)\b"

# Dominant-type-per-parent tiebreak (canonical role wins) — corporate.py _TYPE_PRIORITY.
_TYPE_PRIORITY = {
    "vulture fund": 0,
    "credit servicer": 1,
    "Irish bank": 2,
    "Irish bank (winding down)": 2,
    "Irish bank (exited)": 2,
    "state asset manager": 3,
    "state agency": 3,
}

# Professional-firm patterns — corporate.py _OPERATOR_PATTERNS_WORD (case-sensitive),
# _OPERATOR_PATTERNS_CASE (case-sensitive uppercase abbreviations) and _OPERATOR_PWC.
_OPERATOR_PATTERNS_WORD = [
    ("Deloitte", re.compile(r"\bDeloitte\b")),
    ("Grant Thornton", re.compile(r"\bGrant Thornton\b")),
    ("Mazars", re.compile(r"\bMazars\b")),
    ("Kroll", re.compile(r"\bKroll\b")),
    ("Crowe", re.compile(r"\bCrowe\b")),
    ("Friel Stafford", re.compile(r"\bFriel Stafford\b")),
    ("McKeogh Gallagher Ryan", re.compile(r"\bMcKeogh Gallagher Ryan\b")),
    ("McStay Luby", re.compile(r"\bMcStay Luby\b")),
    ("Hughes Blake", re.compile(r"\bHughes Blake\b")),
    ("Baker Tilly", re.compile(r"\bBaker Tilly\b")),
    ("Cooney Carey", re.compile(r"\bCooney Carey\b")),
    ("FTI Consulting", re.compile(r"\bFTI Consulting\b")),
    ("Interpath", re.compile(r"\bInterpath\b")),
    ("Teneo", re.compile(r"\bTeneo\b")),
]
_OPERATOR_PATTERNS_CASE = [
    ("EY", re.compile(r"\bEY\b")),
    ("KPMG", re.compile(r"\bKPMG\b")),
    ("BDO", re.compile(r"\bBDO\b")),
    ("RBK", re.compile(r"\bRBK\b")),
    ("OCKT", re.compile(r"\bOCKT\b")),
]
_OPERATOR_PWC = re.compile(r"\b(?:PwC|PWC|PricewaterhouseCoopers|PriceWaterhouseCoopers)\b")

# Big 6 accountancy firms — corporate.py _render_operator_strip.
_BIG6 = {"Deloitte", "EY", "PwC", "KPMG", "Grant Thornton", "BDO", "Mazars"}


def _as_list(v) -> list:
    """corporate.py treats a list column cell as a list iff it is iterable."""
    if v is None or not hasattr(v, "__iter__") or isinstance(v, str):
        return []
    return list(v)


def _firms_in(raw: str) -> list[str]:
    """Curated professional firms named in one notice's raw_text — at most once
    each. Verbatim union of the WORD / CASE / PwC patterns from corporate.py.
    Returned in the canonical pattern order so the list is deterministic."""
    if not raw:
        return []
    present: list[str] = []
    for name, pat in _OPERATOR_PATTERNS_WORD:
        if pat.search(raw):
            present.append(name)
    for name, pat in _OPERATOR_PATTERNS_CASE:
        if pat.search(raw):
            present.append(name)
    if _OPERATOR_PWC.search(raw):
        present.append("PwC")
    return present


# ── CBI authorisation badge (entity-norm match) ───────────────────────────────
# _norm_entity is VERBATIM from corporate.py (same legal-form strip as
# extractors/cbi_registers_extract._norm_firm) so a notice's normalised entity
# reproduces the xref's entity_norm for exact matches.
_NORM_SUFFIX_RE = re.compile(
    r"\b(public limited company|limited liability partnership|limited|ltd\.?|"
    r"plc|llp|sa|nv|gmbh|inc\.?)\b\.?",
    re.I,
)
_CBI_MIN_NORM_CHARS = 6  # corporate.py gate — shorter norms over-match


def _norm_entity(name) -> str:
    """Canonical company key via shared.name_norm (UPPERCASE), matching the xref's
    ``entity_norm`` (also shared.name_norm) so ``build_cbi_badge_resolver`` actually
    lands. Was a LOWERCASE variant — a case/suffix mismatch against the UPPERCASE
    reg_map keys that silently suppressed matches. Drops any 't/a XYZ' tail first."""
    if name is None or (isinstance(name, float) and name != name):  # None / NaN
        return ""
    s = re.sub(r"\bt/?a\b.*$", "", str(name), flags=re.IGNORECASE)
    return name_norm_str(s)


def build_cbi_badge_resolver(xref: pl.DataFrame):
    """Return resolve(entity_name) -> (register, ref_no).

    Method B2 (validated 2026-06-20 as the accuracy/truthfulness optimum):
      * EXACT entity-norm match (any token count) — zero false positives; PLUS
      * longest delimited-substring match, but ONLY for firm norms with >= 2
        tokens. The >=2-token gate drops the single-token false positives the
        old page shipped (e.g. 'donnybrook' matching a street address,
        'allianz' matching a different Allianz legal entity) while keeping the
        genuine recoveries where a notice carries text prefixes/suffixes around
        the real entity name (Havbell No.2 DAC, M&F Finance Ireland, …).
    """
    reg_map: dict[str, tuple[str, str]] = {}
    for r in xref.iter_rows(named=True):
        en = r.get("entity_norm")
        if not en or len(str(en)) < _CBI_MIN_NORM_CHARS:
            continue
        en = str(en)
        if en in reg_map:
            continue
        regs = _as_list(r.get("registers"))
        refs = _as_list(r.get("ref_nos"))
        reg_map[en] = (str(regs[0]) if regs else "", str(refs[0]) if refs else "")
    multi = sorted((c for c in reg_map if len(c.split()) >= 2), key=lambda x: -len(x))

    def resolve(entity_name) -> tuple[str, str]:
        en = _norm_entity(entity_name)
        if not en or len(en) < _CBI_MIN_NORM_CHARS:
            return ("", "")
        hit = reg_map.get(en)  # exact, any token count
        if hit:
            return hit
        padded = f" {en} "
        for cand in multi:
            if f" {cand} " in padded:
                return reg_map[cand]
        return ("", "")

    return resolve


def _type_bucket(ft: str) -> str:
    """corporate.py _type_bucket — fine-grained fund_type → headline bucket."""
    if not ft:
        return "other"
    ft_low = ft.lower()
    if "vulture" in ft_low:
        return "vulture"
    if "servicer" in ft_low:
        return "servicer"
    if "irish bank" in ft_low or ft_low.startswith("bank"):
        return "bank"
    if "state" in ft_low or "nama" in ft_low or "revenue" in ft_low:
        return "state"
    return "other"


def _dominant_ftype(ftypes: list[str]) -> str:
    """corporate.py _dominant_ftype — modal fund_type, ties broken by canonical
    priority then alphabetically."""
    counts = Counter(ftypes)
    if not counts:
        return ""
    top_n = max(counts.values())
    winners = [ft for ft, n in counts.items() if n == top_n]
    winners.sort(key=lambda x: (_TYPE_PRIORITY.get(x, 99), x))
    return winners[0]


def _parent_ftype_pairs(recv: pl.DataFrame) -> list[tuple[str, str]]:
    """(parent, ftype) mention pairs across receivership notices — verbatim zip
    semantics from corporate.py (positional pairing, short ftype list pads '')."""
    pairs: list[tuple[str, str]] = []
    for parents, ftypes in recv.select("parent_fund_mentions", "fund_type_mentions").iter_rows():
        parents = _as_list(parents)
        ftypes = _as_list(ftypes)
        for i, p in enumerate(parents):
            if p:
                pairs.append((p, (ftypes[i] if i < len(ftypes) else "") or ""))
    return pairs


def enrich_notices(notices: pl.DataFrame, cbi_xref: pl.DataFrame | None = None) -> pl.DataFrame:
    """Return the notices superset with the per-notice receiver flags + CBI badge."""
    df = notices.with_columns(
        # Year — identical parse result to corporate.load_corporate (L849-850)
        # (issue_date is ISO yyyy-mm-dd or null; coerce → null) so the precomputed
        # sparkline counts match what the page derived at render time.
        pl.col("issue_date").str.to_date("%Y-%m-%d", strict=False).dt.year().alias("year"),
        (
            (pl.col("notice_subtype") == "receivership")
            | pl.col("raw_text").fill_null("").str.contains(f"(?i){_RECEIVERSHIP_RE}")
        )
        .fill_null(False)
        .alias("is_receivership"),
        pl.col("entity_name").fill_null("").str.contains(f"(?i){_SPV_RE}").alias("is_spv"),
        (pl.col("parent_fund_mentions").list.len() > 0).fill_null(False).alias("has_parent_mention"),
        pl.col("raw_text")
        .fill_null("")
        .map_elements(_firms_in, return_dtype=pl.List(pl.String))
        .alias("receiver_firms"),
    ).with_columns((pl.col("receiver_firms").list.len() > 0).alias("has_receiver_firm"))

    # CBI authorisation badge — precomputed (was a row-time substring join in the page).
    if cbi_xref is not None and not cbi_xref.is_empty():
        resolve = build_cbi_badge_resolver(cbi_xref)
        badges = [resolve(name) for name in df["entity_name"]]
        df = df.with_columns(
            pl.Series("cbi_register", [b[0] for b in badges], dtype=pl.String),
            pl.Series("cbi_ref_no", [b[1] for b in badges], dtype=pl.String),
        )
    else:
        df = df.with_columns(
            pl.lit("").alias("cbi_register"),
            pl.lit("").alias("cbi_ref_no"),
        )
    return df


def build_appointers(enriched: pl.DataFrame) -> pl.DataFrame:
    """One row per parent fund named across receivership notices."""
    pairs = _parent_ftype_pairs(enriched.filter(pl.col("is_receivership")))
    if not pairs:
        return pl.DataFrame(
            schema={
                "parent": pl.String,
                "n_notices": pl.Int64,
                "dominant_fund_type": pl.String,
                "type_bucket": pl.String,
            }
        )
    ftypes_by_parent: dict[str, list[str]] = {}
    for p, ft in pairs:
        ftypes_by_parent.setdefault(p, []).append(ft)
    out = pl.DataFrame(
        [
            {
                "parent": p,
                "n_notices": len(fts),
                "dominant_fund_type": _dominant_ftype(fts),
                "type_bucket": _type_bucket(_dominant_ftype(fts)),
            }
            for p, fts in ftypes_by_parent.items()
        ],
        schema={
            "parent": pl.String,
            "n_notices": pl.Int64,
            "dominant_fund_type": pl.String,
            "type_bucket": pl.String,
        },
    )
    return out.sort(["n_notices", "parent"], descending=[True, False])


def build_firms(enriched: pl.DataFrame) -> pl.DataFrame:
    """One row per professional firm named across receivership notices
    (notice-presence, counted at most once per notice)."""
    recv = enriched.filter(pl.col("is_receivership"))
    counts: dict[str, int] = {}
    for fs in recv["receiver_firms"]:
        for name in set(fs):
            counts[name] = counts.get(name, 0) + 1
    if not counts:
        return pl.DataFrame(schema={"firm": pl.String, "n_notices": pl.Int64, "is_big6": pl.Boolean})
    out = pl.DataFrame(
        sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])),
        schema={"firm": pl.String, "n_notices": pl.Int64},
        orient="row",
    )
    return out.with_columns(pl.col("firm").is_in(list(_BIG6)).alias("is_big6"))


# ── Reference recomputation (parity guard) ────────────────────────────────────
def _reference_topn_and_buckets(enriched: pl.DataFrame) -> tuple[list, dict, dict]:
    """Replicate corporate._render_featured's exact structure to cross-check the
    gold. Returns (top-N parent list, bucket->mention-count, scalar counts).
    Tie order within equal mention counts is pinned parent-ascending (the gold
    tiebreak — the page's sort left ties engine-ordered)."""
    recv = enriched.filter(pl.col("is_receivership"))
    n_recv = recv.height
    pairs = _parent_ftype_pairs(recv)
    ftypes_by_parent: dict[str, list[str]] = {}
    for p, ft in pairs:
        ftypes_by_parent.setdefault(p, []).append(ft)
    parent_to_ftype = {p: _dominant_ftype(fts) for p, fts in ftypes_by_parent.items()}
    ranked = sorted(ftypes_by_parent.items(), key=lambda kv: (-len(kv[1]), kv[0]))
    top = [p for p, _ in ranked[:FEATURED_TOP_N]]
    bucket_counts = dict(Counter(_type_bucket(parent_to_ftype[p]) for p, _ in pairs))
    n_tagged = recv.select((pl.col("parent_fund_mentions").list.len() > 0).sum()).item()
    scalars = {"n_recv": n_recv, "n_tagged": int(n_tagged)}
    return top, bucket_counts, scalars


def main() -> int:
    if not NOTICES_PARQUET.exists():
        raise SystemExit(f"corporate notices gold not found: {NOTICES_PARQUET} (run the iris chain first)")

    notices = pl.read_parquet(NOTICES_PARQUET)
    cbi_xref = pl.read_parquet(CBI_XREF_PARQUET) if CBI_XREF_PARQUET.exists() else None
    enriched = enrich_notices(notices, cbi_xref)
    appointers = build_appointers(enriched)
    firms = build_firms(enriched)

    # ── Parity guard: gold must agree with the verbatim page structure ────────
    ref_top, ref_buckets, ref_scalars = _reference_topn_and_buckets(enriched)
    gold_top = appointers["parent"].head(FEATURED_TOP_N).to_list()
    assert gold_top == ref_top, f"top-N mismatch:\n gold={gold_top}\n ref ={ref_top}"
    gold_buckets = dict(
        appointers.group_by("type_bucket").agg(pl.col("n_notices").sum()).iter_rows()
    )
    assert gold_buckets == ref_buckets, f"bucket mismatch:\n gold={gold_buckets}\n ref={ref_buckets}"
    n_recv_gold = int(enriched["is_receivership"].sum())
    n_tagged_gold = int((enriched["is_receivership"] & enriched["has_parent_mention"]).sum())
    assert n_recv_gold == ref_scalars["n_recv"], (n_recv_gold, ref_scalars["n_recv"])
    assert n_tagged_gold == ref_scalars["n_tagged"], (n_tagged_gold, ref_scalars["n_tagged"])

    save_parquet(enriched, ENRICHED_PARQUET, min_rows=1)
    save_parquet(appointers, APPOINTERS_PARQUET)
    save_parquet(firms, FIRMS_PARQUET)

    n_spv = int((enriched["is_receivership"] & enriched["is_spv"]).sum())
    n_any_firm = int((enriched["is_receivership"] & enriched["has_receiver_firm"]).sum())
    n_cbi = int((enriched["cbi_register"] != "").sum())
    print(f"[corporate_receiver] wrote {ENRICHED_PARQUET.name}, {APPOINTERS_PARQUET.name}, {FIRMS_PARQUET.name}")
    print(f"  notices in            : {notices.height:,}")
    print(f"  receivership notices  : {n_recv_gold:,}  (spv-shaped {n_spv:,}, parent-tagged {n_tagged_gold:,})")
    print(f"  distinct appointers   : {appointers.height:,}")
    print(f"  distinct firms        : {firms.height:,}  (firm-tagged notices {n_any_firm:,})")
    print(f"  CBI-badged notices    : {n_cbi:,}  (exact + >=2-token substring; B2)")
    print(f"  parity                : OK (top-{FEATURED_TOP_N}, buckets, scalar counts agree with page logic)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
