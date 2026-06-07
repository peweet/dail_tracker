"""
iris_si_bill_enrichment.py

Matches Statutory Instruments (from the Iris Oifigiúil ETL) to their enabling
bills. Writes one row per matched (bill, SI) to
data/gold/parquet/bill_statutory_instruments.parquet.

Matching tuning (Jaccard >= 0.40, +/-3yr window, SI year >= 2018, taxonomy
confidence >= 0.5) — lifted verbatim from the statutory_instruments page that
this enrichment graduated.

Outputs:
  - si_minister_named   - first/last name extracted from SI raw_text
                          where the standard 'The Minister for X,
                          Firstname Lastname, in exercise...' formula
                          appears. Coverage modest (~5-15% in current data).
  - sponsor_unique_member_code - free piggyback for Route B (TD page).

The unmatched SIs are written to a sibling file under silver _meta/ for
the coverage gate (not consumed by views).
"""

from __future__ import annotations

import logging
import re
import string

import pandas as pd

from config import DATA_DIR, GOLD_PARQUET_DIR, SILVER_DIR, SILVER_PARQUET_DIR
from services.parquet_io import save_parquet

logger = logging.getLogger(__name__)

_SI_CSV = SILVER_DIR / "iris_oifigiuil" / "iris_si_taxonomy.csv"
_SPONSORS = SILVER_PARQUET_DIR / "sponsors.parquet"
_OUT = GOLD_PARQUET_DIR / "bill_statutory_instruments.parquet"
_OUT_UNM = SILVER_DIR / "_meta" / "bill_statutory_instruments_unmatched.parquet"
# Hand-curated table of pre-2014 enabling Acts so SIs citing them (e.g.
# European Communities Act 1972, Health Act 1947, Taxes Consolidation Act
# 1997) attribute to the real primary Act instead of falling unmatched OR
# being mis-attributed to a same-name 2014+ Amendment Bill. Synthetic
# bill_ids are "act_<year>_<slug>" so they're visually distinct from
# real "<year>_<bill_no>" ids and don't collide with v_legislation_index.
_PRE2014_ACTS = DATA_DIR / "_meta" / "pre2014_acts.csv"

# Constants lifted verbatim from statutory_instruments.py
SI_YEAR_FLOOR = 2018
MIN_TAXO_CONFIDENCE = 0.5
MATCH_THRESHOLD = 0.40
MATCH_YEAR_WINDOW = 3
_TITLE_STOP = {"act", "bill", "of", "the", "and", "an", "a", "for", "to", "in", "no", "no.", "amendment"}

# Three patterns covering the common SI signing formulas. Most SIs do NOT
# name the minister (they say 'The Minister for X, in exercise...' with no
# name); coverage with the strictest pattern is ~1-2% of SIs - this is
# a 'show when present' bonus, not the headline. Names are clean
# (Helen McEntee, Leo Varadkar etc.).
_NAMED_MIN_PATTERNS = (
    # "The Minister for X, FirstName LastName, in exercise/hereby"
    re.compile(
        r"The Minister for [A-Z][^,\n]{2,80}?,\s*"
        r"([A-Z][a-z]+(?:\s+[A-Z][a-zA-Z']+){1,2})"
        r"(?:,?\s+T\.?D\.?)?\s*,\s*(?:in\s+exercise|hereby)"
    ),
    # "FirstName LastName, T.D., Minister for" (signing formula)
    re.compile(
        r"\b([A-Z][a-z]+(?:\s+[A-Z][a-zA-Z']+){1,2})\s*,?\s*"
        r"T\.?D\.?\s*,\s*Minister\s+(?:for|of)"
    ),
    # "I, FirstName LastName, T.D., Minister..." (rare warrant form)
    re.compile(
        r"^I,\s*([A-Z][a-z]+(?:\s+[A-Z][a-zA-Z']+){1,2})\s*,?\s*"
        r"T\.?D\.?\s*,\s*Minister",
        re.MULTILINE,
    ),
)

_HONORIFIC_RE = re.compile(r"^(Mr|Ms|Mrs|Dr|Professor|Prof)\.?\s+", re.IGNORECASE)


def _title_tokens(s: str) -> set[str]:
    if not s:
        return set()
    out: set[str] = set()
    for w in s.lower().split():
        t = w.strip(string.punctuation)
        if t and t not in _TITLE_STOP:
            out.add(t)
    return out


def _extract_named_minister(text) -> str | None:
    if not isinstance(text, str):
        return None
    for pat in _NAMED_MIN_PATTERNS:
        m = pat.search(text)
        if m:
            name = m.group(1).strip()
            # Strip leading 'Mr'/'Ms'/'Dr' honorifics for clean display.
            return _HONORIFIC_RE.sub("", name) or None
    return None


def load_si() -> pd.DataFrame:
    df = pd.read_csv(_SI_CSV, low_memory=False)
    df = df[df["notice_category"] == "statutory_instrument"]
    df = df[~df["is_quarantined"].fillna(False).astype(bool)]
    df = df[df["si_number"].notna() & df["si_year"].notna() & df["title"].notna()]
    df["si_year"] = df["si_year"].astype(int)
    df["si_number"] = df["si_number"].astype(int)
    df = df[df["si_year"] >= SI_YEAR_FLOOR]
    df = df[df["si_taxonomy_confidence"].fillna(0) >= MIN_TAXO_CONFIDENCE]
    df["si_id"] = df["si_year"].astype(str) + "-" + df["si_number"].astype(str).str.zfill(3)
    df["issue_date"] = pd.to_datetime(df["issue_date"], errors="coerce")
    df["si_minister_named"] = df["raw_text"].map(_extract_named_minister)
    return df.drop_duplicates(subset=["si_id"]).reset_index(drop=True)


def load_bills() -> pd.DataFrame:
    df = pd.read_parquet(_SPONSORS)
    df["bill_year_num"] = pd.to_numeric(df["bill_year"], errors="coerce")
    df["bill_no_num"] = pd.to_numeric(df["bill_no"], errors="coerce")
    df = df.dropna(subset=["bill_no_num", "bill_year_num", "short_title_en"])
    df["bill_id"] = df["bill_year_num"].astype(int).astype(str) + "_" + df["bill_no_num"].astype(int).astype(str)
    keep = [
        "bill_id",
        "short_title_en",
        "bill_year",
        "bill_no",
        "status",
        "bill_url",
        "bill_year_num",
        "bill_no_num",
        "sponsor_by_show_as",
        "unique_member_code",
    ]
    keep = [c for c in keep if c in df.columns]
    return df[keep].drop_duplicates(subset=["bill_id"]).reset_index(drop=True)


_PARENT_RE = re.compile(r"([A-Z][^,()\n]{2,80}?\bAct\s+(\d{4}))")
# Leading filler phrases the parser sometimes captures before the real
# Act name (e.g. "These Regulations amend the Health Act 1947"). We
# strip these before looking up the curated table; the residue then
# matches the table key e.g. 'health'.
_PARENT_PREFIX_RE = re.compile(
    r"^(these regulations (?:amend|revoke|prescribe)(?: under)? the |"
    r"this order may be cited as the |"
    r"under the |the )",
)


def _normalise_parent(name: str) -> str:
    """Reduce an extracted Act name to the curated table's residue form:
    lowercase, drop trailing ' Act NNNN', strip leading filler clauses,
    strip non-alphanumeric, collapse whitespace."""
    s = name.lower()
    s = re.sub(r"\s*\bact\b.*$", "", s)
    s = _PARENT_PREFIX_RE.sub("", s)
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def load_pre2014_acts() -> dict[tuple[str, int], dict]:
    """Load the curated pre-2014 Acts table keyed on (residue, year).
    Missing file returns {} so the enrichment still runs without it."""
    if not _PRE2014_ACTS.exists():
        logger.warning("pre-2014 Acts table not found: %s", _PRE2014_ACTS)
        return {}
    df = pd.read_csv(_PRE2014_ACTS)
    out: dict[tuple[str, int], dict] = {}
    for r in df.to_dict("records"):
        key = (str(r["match_residue"]).strip().lower(), int(r["act_year"]))
        out[key] = {
            "bill_id": r["canonical_bill_id"],
            "bill_title": r["act_short_title"],
            "policy_domain": r.get("policy_domain"),
        }
    return out


def match_si_to_bill(si_df: pd.DataFrame, bills_df: pd.DataFrame) -> pd.DataFrame:
    """Year-bucketed token-set Jaccard match. Lifted verbatim from
    statutory_instruments.match_si_to_bill - same regex, same threshold,
    same year window. Returns si_df + four match columns plus the
    sponsor_unique_member_code carry-through (new).

    Pre-pass: try the curated pre-2014 Acts table first. If the SI cites a
    known pre-2014 Act, attribute directly (synthetic bill_id) and skip
    the Jaccard step. This recovers SIs whose enabling Act predates the
    bills table (2014) — e.g. European Communities Act 1972."""
    parent_re = _PARENT_RE
    pre2014 = load_pre2014_acts()
    logger.info("pre-2014 Acts table: %d curated entries", len(pre2014))

    bills = bills_df.dropna(subset=["short_title_en"]).copy()
    bills["_tokens"] = bills["short_title_en"].astype(str).map(_title_tokens)

    bills_by_year: dict[int, list[dict]] = {}
    for rec in bills.to_dict("records"):
        y = rec.get("bill_year_num")
        if pd.notna(y):
            bills_by_year.setdefault(int(y), []).append(rec)

    def candidates(year_hint: int) -> list[dict]:
        out: list[dict] = []
        for dy in range(-MATCH_YEAR_WINDOW, MATCH_YEAR_WINDOW + 1):
            out.extend(bills_by_year.get(year_hint + dy, []))
        # Tie-break: prefer the non-Amendment bill, then the lower bill_no.
        # The `best()` loop below uses strict `>` so whichever candidate is
        # seen first wins ties — sorting here makes that deterministic AND
        # journalistically correct (SIs under "Data Protection Act 2018"
        # should attribute to the primary 2018 Act, not the Amendment Act
        # whose token-residue happens to be identical).
        out.sort(
            key=lambda r: (
                "amendment" in (r.get("short_title_en") or "").lower(),
                r.get("bill_no_num") or 0,
            )
        )
        return out

    def best(parent_text) -> pd.Series:
        if not isinstance(parent_text, str):
            return pd.Series([None, None, None, None, 0.0])
        m = parent_re.search(parent_text)
        if not m:
            return pd.Series([None, None, None, None, 0.0])
        year = int(m.group(2))

        # Pre-2014 lookup pass — exact (residue, year) match in the
        # curated table. Always preferred over the Jaccard fallback
        # because it's hand-verified.
        residue = _normalise_parent(m.group(1))
        hit = pre2014.get((residue, year))
        if hit is not None:
            return pd.Series(
                [
                    hit["bill_id"],
                    hit["bill_title"],
                    None,  # no bill_url — pre-2014 Act has no bills.parquet row
                    None,  # no sponsor_unique_member_code
                    1.0,  # curated hit = perfect score
                ]
            )

        ref = _title_tokens(m.group(1))
        if not ref:
            return pd.Series([None, None, None, None, 0.0])
        cands = candidates(year)
        if not cands:
            return pd.Series([None, None, None, None, 0.0])
        best_row, best_score = None, 0.0
        for c in cands:
            bt = c["_tokens"]
            if not bt:
                continue
            inter = len(ref & bt)
            if inter == 0:
                continue
            score = inter / len(ref | bt)
            if score > best_score:
                best_score, best_row = score, c
        if best_row is None or best_score < MATCH_THRESHOLD:
            return pd.Series([None, None, None, None, round(best_score, 2)])
        return pd.Series(
            [
                best_row["bill_id"],
                best_row["short_title_en"],
                best_row.get("bill_url"),
                best_row.get("unique_member_code"),
                round(best_score, 2),
            ]
        )

    matched = si_df["si_parent_legislation"].apply(best)
    matched.columns = [
        "matched_bill_id",
        "matched_bill_title",
        "matched_bill_url",
        "matched_sponsor_code",
        "match_score",
    ]
    return pd.concat([si_df.reset_index(drop=True), matched.reset_index(drop=True)], axis=1)


def run() -> tuple[int, int]:
    si = load_si()
    bills = load_bills()
    most_recent = si["issue_date"].max()
    logger.info(
        "Iris SI source: %s | %d clean SIs | most recent issue %s",
        _SI_CSV,
        len(si),
        most_recent.date().isoformat() if pd.notna(most_recent) else "?",
    )

    joined = match_si_to_bill(si, bills)
    matched = joined[joined["matched_bill_id"].notna()].copy()
    unmatched = joined[joined["matched_bill_id"].isna()].copy()
    eu_neg = ("none_detected", "")

    out = pd.DataFrame(
        {
            "bill_id": matched["matched_bill_id"],
            "bill_short_title": matched["matched_bill_title"],
            "sponsor_unique_member_code": matched["matched_sponsor_code"],
            "si_year": matched["si_year"],
            "si_number": matched["si_number"],
            "si_id": matched["si_id"],
            "si_title": matched["title"],
            "si_signed_date": matched["issue_date"].dt.date,
            "si_minister": matched["si_responsible_actor"],
            "si_minister_named": matched["si_minister_named"],
            "si_policy_domain": matched["si_policy_domain_primary"],
            "si_policy_domains_all": matched["si_policy_domains"],
            "si_operation": matched["si_operation_primary"],
            "si_operation_flags": matched["si_operation_flags"],
            "si_form": matched["si_form"],
            "si_eu_relationship": matched["si_eu_relationship"],
            "si_is_eu": ~matched["si_eu_relationship"].fillna("").isin(eu_neg),
            "eisb_url": matched["eisb_url"],
            "iris_source_pdf": matched["source_file"],
            "match_score": matched["match_score"],
        }
    )
    save_parquet(out, _OUT)

    unm = pd.DataFrame(
        {
            "si_id": unmatched["si_id"],
            "si_parent_text": unmatched["si_parent_legislation"],
            "best_score": unmatched["match_score"],
        }
    )
    save_parquet(unm, _OUT_UNM)

    matched_count = len(out)
    total = len(si)
    named = int(out["si_minister_named"].notna().sum())
    logger.info(
        "bill_statutory_instruments: %d/%d SIs matched (%.1f%%) | named-minister extracted on %d (%.1f%%)",
        matched_count,
        total,
        100 * matched_count / total,
        named,
        100 * named / total,
    )
    return matched_count, total


if __name__ == "__main__":
    try:
        from services.logging_setup import setup_logging

        setup_logging()
    except Exception:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
