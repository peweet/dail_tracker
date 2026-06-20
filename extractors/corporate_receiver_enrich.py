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

Run:
    .venv/Scripts/python.exe extractors/corporate_receiver_enrich.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

try:  # noqa: SIM105
    sys.stdout.reconfigure(encoding="utf-8")  # accented entity names on cp1252 consoles
except Exception:
    pass

from services.parquet_io import save_parquet  # noqa: E402

NOTICES_PARQUET = ROOT / "data" / "gold" / "parquet" / "corporate_notices.parquet"
ENRICHED_PARQUET = ROOT / "data" / "gold" / "parquet" / "corporate_notices_enriched.parquet"
APPOINTERS_PARQUET = ROOT / "data" / "gold" / "parquet" / "corporate_receiver_appointers.parquet"
FIRMS_PARQUET = ROOT / "data" / "gold" / "parquet" / "corporate_receiver_firms.parquet"

FEATURED_TOP_N = 8  # mirrors corporate.FEATURED_TOP_N (display cap lives in the page; gold keeps all)

# ── Verbatim matchers from corporate.py ───────────────────────────────────────
_RECEIVERSHIP_RE = "APPOINTMENT OF (?:STATUTORY )?RECEIVER|NOTICE OF APPOINTMENT OF RECEIVER"
_SPV_RE = re.compile(r"\b(DAC|DESIGNATED ACTIVITY COMPANY|ICAV)\b", re.I)

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


def _dominant_ftype(s: pd.Series) -> str:
    """corporate.py _dominant_ftype — modal fund_type, ties broken by canonical
    priority then alphabetically."""
    counts = s.value_counts()
    if counts.empty:
        return ""
    top_n = counts.iloc[0]
    winners = counts[counts == top_n].index.tolist()
    winners.sort(key=lambda x: (_TYPE_PRIORITY.get(x, 99), x))
    return winners[0]


def enrich_notices(notices: pd.DataFrame) -> pd.DataFrame:
    """Return the notices superset with the per-notice receiver flags appended."""
    df = notices.copy()
    # Year — identical parse to corporate.load_corporate (L849-850) so the
    # precomputed sparkline counts match what the page derived at render time.
    df["year"] = pd.to_datetime(df["issue_date"], errors="coerce").dt.year
    raw = df["raw_text"].fillna("").astype(str)
    ent = df["entity_name"].fillna("").astype(str)
    df["is_receivership"] = (df["notice_subtype"] == "receivership") | raw.str.contains(
        _RECEIVERSHIP_RE, case=False, regex=True, na=False
    )
    df["is_spv"] = ent.str.contains(_SPV_RE, regex=True, na=False)
    df["has_parent_mention"] = df["parent_fund_mentions"].apply(lambda x: len(_as_list(x)) > 0)
    df["receiver_firms"] = raw.apply(_firms_in)
    df["has_receiver_firm"] = df["receiver_firms"].apply(lambda fs: len(fs) > 0)
    return df


def build_appointers(enriched: pd.DataFrame) -> pd.DataFrame:
    """One row per parent fund named across receivership notices."""
    recv = enriched[enriched["is_receivership"]]
    rows: list[dict] = []
    for _, r in recv.iterrows():
        parents = _as_list(r.get("parent_fund_mentions"))
        ftypes = _as_list(r.get("fund_type_mentions"))
        for i, p in enumerate(parents):
            if p:
                rows.append({"parent": p, "ftype": (ftypes[i] if i < len(ftypes) else "") or ""})
    if not rows:
        return pd.DataFrame(columns=["parent", "n_notices", "dominant_fund_type", "type_bucket"])
    pdf = pd.DataFrame(rows)
    parent_to_ftype = pdf.groupby("parent")["ftype"].agg(_dominant_ftype).to_dict()
    out = (
        pdf.groupby("parent")
        .size()
        .rename("n_notices")
        .reset_index()
        .assign(dominant_fund_type=lambda d: d["parent"].map(parent_to_ftype))
        .sort_values(["n_notices", "parent"], ascending=[False, True])
        .reset_index(drop=True)
    )
    out["type_bucket"] = out["dominant_fund_type"].map(_type_bucket)
    return out[["parent", "n_notices", "dominant_fund_type", "type_bucket"]]


def build_firms(enriched: pd.DataFrame) -> pd.DataFrame:
    """One row per professional firm named across receivership notices
    (notice-presence, counted at most once per notice)."""
    recv = enriched[enriched["is_receivership"]]
    counts: dict[str, int] = {}
    for fs in recv["receiver_firms"]:
        for name in set(fs):
            counts[name] = counts.get(name, 0) + 1
    if not counts:
        return pd.DataFrame(columns=["firm", "n_notices", "is_big6"])
    out = (
        pd.DataFrame(sorted(counts.items(), key=lambda kv: (-kv[1], kv[0])), columns=["firm", "n_notices"])
        .reset_index(drop=True)
    )
    out["is_big6"] = out["firm"].isin(_BIG6)
    return out


# ── Reference recomputation (parity guard) ────────────────────────────────────
def _reference_topn_and_buckets(enriched: pd.DataFrame) -> tuple[list, dict, dict]:
    """Replicate corporate._render_featured's exact structure to cross-check the
    gold. Returns (top-N parent list, bucket->mention-count, scalar counts)."""
    recv = enriched[enriched["is_receivership"]]
    n_recv = len(recv)
    parent_rows: list[dict] = []
    for _, r in recv.iterrows():
        parents = _as_list(r.get("parent_fund_mentions"))
        ftypes = _as_list(r.get("fund_type_mentions"))
        for i, p in enumerate(parents):
            if p:
                parent_rows.append({"parent": p, "ftype": (ftypes[i] if i < len(ftypes) else "") or ""})
    pdf = pd.DataFrame(parent_rows)
    parent_to_ftype = pdf.groupby("parent")["ftype"].agg(_dominant_ftype).to_dict()
    top = (
        pdf.groupby("parent").size().rename("n").reset_index()
        .assign(ftype=lambda d: d["parent"].map(parent_to_ftype))
        .sort_values("n", ascending=False)
        .head(FEATURED_TOP_N)
        .set_index("parent")
    )
    pdf["bucket"] = pdf["parent"].map(parent_to_ftype).map(_type_bucket)
    bucket_counts = pdf["bucket"].value_counts().to_dict()
    n_tagged = int(recv["parent_fund_mentions"].apply(lambda x: len(_as_list(x)) > 0).sum())
    scalars = {"n_recv": n_recv, "n_tagged": n_tagged}
    return list(top.index), bucket_counts, scalars


def main() -> int:
    if not NOTICES_PARQUET.exists():
        raise SystemExit(f"corporate notices gold not found: {NOTICES_PARQUET} (run the iris chain first)")

    notices = pd.read_parquet(NOTICES_PARQUET)
    enriched = enrich_notices(notices)
    appointers = build_appointers(enriched)
    firms = build_firms(enriched)

    # ── Parity guard: gold must agree with the verbatim page structure ────────
    ref_top, ref_buckets, ref_scalars = _reference_topn_and_buckets(enriched)
    gold_top = appointers["parent"].head(FEATURED_TOP_N).tolist()
    assert gold_top == ref_top, f"top-N mismatch:\n gold={gold_top}\n ref ={ref_top}"
    gold_buckets = appointers.groupby("type_bucket")["n_notices"].sum().to_dict()
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
    print(f"[corporate_receiver] wrote {ENRICHED_PARQUET.name}, {APPOINTERS_PARQUET.name}, {FIRMS_PARQUET.name}")
    print(f"  notices in            : {len(notices):,}")
    print(f"  receivership notices  : {n_recv_gold:,}  (spv-shaped {n_spv:,}, parent-tagged {n_tagged_gold:,})")
    print(f"  distinct appointers   : {len(appointers):,}")
    print(f"  distinct firms        : {len(firms):,}  (firm-tagged notices {n_any_firm:,})")
    print(f"  parity                : OK (top-{FEATURED_TOP_N}, buckets, scalar counts agree with page logic)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
