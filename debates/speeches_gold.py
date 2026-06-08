"""
speeches_gold.py

Stage 3 of the debates integration: turn the parsed silver speeches into the
gold `speeches_fact` table — the member-attributed, query-ready fact that the
app and MCP read.

Two enrichments on top of silver (debates/speech_parse.py):
  1. Member identity — left-join the <TLCPerson>-resolved `unique_member_code`
     to the member registries (Dáil + Seanad) for member_name / party /
     constituency. Deterministic; unresolved contributions (presiding officer
     referenced obliquely, the rare visitor) keep null identity rather than
     being dropped.
  2. Language flag — `is_irish` / `irish_score`. This is language IDENTIFICATION
     (structural, deterministic), NOT interpretation: it counts how much of a
     contribution is Irish (fada-bearing or distinctively-Irish function words)
     over a minimum length, so short procedural Irish (chair titles like
     "An Cathaoirleach") is not mistaken for a Gaeilge contribution. Lives in the
     pipeline, never the UI (logic firewall).

Grain unchanged: one row per contribution. `house` is taken from the speech's
own chamber (where it was said), independent of the member's registry house — a
member can speak in both chambers under one canonical code (e.g. Seán Kyne).

Input  : data/silver/parquet/speeches.parquet  (+ member registries)
Output : data/gold/parquet/speeches_fact.parquet

Run standalone:
  python -m debates.speeches_gold
"""

from __future__ import annotations

import logging
import re
import sys

import pandas as pd

from config import (
    GOLD_SPEECHES_FACT_FULL_PARQUET,
    GOLD_SPEECHES_FACT_PARQUET,
    SILVER_PARQUET_DIR,
    SILVER_SPEECHES_PARQUET,
)
from services.parquet_io import save_parquet

# Committed-slice policy (see config.GOLD_SPEECHES_FACT_PARQUET): the gitignored
# full fact carries every year + full text; the committed Cloud slice is capped to
# recent years and a truncated excerpt so it stays well under GitHub's 100MB limit.
_COMMITTED_YEAR_FLOOR = 2020
_EXCERPT_CHARS = 300

logger = logging.getLogger(__name__)

_DAIL_REGISTRY = SILVER_PARQUET_DIR / "flattened_members.parquet"
_SEANAD_REGISTRY = SILVER_PARQUET_DIR / "flattened_seanad_members.parquet"

_REGISTRY_COLS = ["unique_member_code", "full_name", "party", "constituency_name"]

# Irish-language detection ----------------------------------------------------
# Distinctively-Irish high-frequency FUNCTION words. Deliberately excludes:
#   - common English collisions (a/an/do/is/go/as/me/sin/air),
#   - proper/content nouns that appear inside English speeches (Éireann,
#     Gaeltacht, Gaeilge, Teachta, Aire) — those are about Irish, not in Irish.
# Function words are the discriminator: a fada alone fires on proper nouns
# ("Dáil Éireann", "Ó Murchú") in otherwise-English turns; requiring function
# words separates a Gaeilge contribution from an English one that names them.
_IRISH_FUNCTION_WORDS = frozenset(
    ["agus", "atá", "bhfuil", "bhí", "beidh", "chomh", "chun", "cén", "dom", "duit", "faoi", "freisin", "gach", "leat", "liom", "maith", "mar", "muid", "níl", "níos", "raibh", "sibh", "siad", "táim", "tá", "uirthi", "anois", "ansin", "anseo", "conas", "aon", "seo", "gabhaim", "buíochas", "leis", "ní", "ag", "ar", "leo", "orm", "ort", "dúirt", "déanamh"]
)
_FADA_RE = re.compile(r"[áéíóúÁÉÍÓÚ]")
_WORD_RE = re.compile(r"[A-Za-zÁÉÍÓÚáéíóú]+")

_IRISH_MIN_WORDS = 10  # below this, a turn is too short to classify (procedural)
_IRISH_THRESHOLD = 0.25  # share of Irish-signal tokens to flag a contribution
_IRISH_MIN_FUNCWORDS = 2  # require real function words, not just fada proper nouns


def _irish_stats(text: str) -> tuple[float, int]:
    """Return (irish_signal_score, function_word_count) for a contribution.

    score = share of tokens that are fada-bearing OR Irish function words;
    function_word_count counts only the function words (the discriminator that
    keeps fada-laden English proper nouns from scoring as Gaeilge).
    """
    words = _WORD_RE.findall(text or "")
    if len(words) < _IRISH_MIN_WORDS:
        return 0.0, 0
    func = sum(1 for w in words if w.lower() in _IRISH_FUNCTION_WORDS)
    signal = sum(1 for w in words if _FADA_RE.search(w) or w.lower() in _IRISH_FUNCTION_WORDS)
    return round(signal / len(words), 4), func


def irish_score(text: str) -> float:
    """Irish-signal share of a contribution (0.0 for sub-minimum-length turns)."""
    return _irish_stats(text)[0]


def is_irish_speech(text: str) -> bool:
    """True iff a contribution is delivered substantially in Irish.

    Needs both a high signal share AND real function words, so an English turn
    that merely names "Dáil Éireann" or a member with a fada is not flagged.
    """
    score, func = _irish_stats(text)
    return score >= _IRISH_THRESHOLD and func >= _IRISH_MIN_FUNCWORDS


def _member_map() -> pd.DataFrame:
    """Union the Dáil + Seanad registries to one code -> identity lookup.

    Deduped on unique_member_code (a person elected to both houses carries one
    canonical code); keeps the first non-null identity seen.
    """
    frames = []
    for path in (_DAIL_REGISTRY, _SEANAD_REGISTRY):
        if path.exists():
            d = pd.read_parquet(path)
            keep = [c for c in _REGISTRY_COLS if c in d.columns]
            frames.append(d[keep])
        else:
            logger.warning("speeches_gold: registry %s missing", path)
    if not frames:
        return pd.DataFrame(columns=_REGISTRY_COLS)
    reg = pd.concat(frames, ignore_index=True)
    reg = reg.dropna(subset=["unique_member_code"]).drop_duplicates(subset=["unique_member_code"], keep="first")
    return reg.rename(columns={"full_name": "member_name", "constituency_name": "constituency"})


def build_speeches_fact(speeches: pd.DataFrame, members: pd.DataFrame) -> pd.DataFrame:
    """Pure transform: silver speeches + member map -> gold speeches_fact."""
    if speeches.empty:
        return speeches.copy()

    df = speeches.copy()
    df["house"] = df["chamber"].map({"dail": "Dáil", "seanad": "Seanad"}).fillna(df["chamber"])
    df["word_count"] = df["speech_text"].fillna("").map(lambda t: len(_WORD_RE.findall(t)))
    df["irish_score"] = df["speech_text"].fillna("").map(irish_score)
    df["is_irish"] = df["speech_text"].fillna("").map(is_irish_speech)

    # blank code -> NA so the join leaves identity null rather than matching ''
    df["unique_member_code"] = df["unique_member_code"].replace("", pd.NA)
    out = df.merge(members, on="unique_member_code", how="left")

    out["year"] = pd.to_datetime(out["date"], errors="coerce").dt.year
    sort_cols = [c for c in ["date", "chamber", "contribution_order"] if c in out.columns]
    return out.sort_values(sort_cols, ascending=[False, True, True][: len(sort_cols)]).reset_index(drop=True)


def build_committed_slice(fact: pd.DataFrame) -> pd.DataFrame:
    """The lite, Cloud-committable slice of the full fact.

    Same schema (so views/UI are identical) but capped to recent years and with
    `speech_text` truncated to an excerpt — speech_text is ~98% of the parquet,
    so this is what keeps the committed file under GitHub's 100MB limit. Views
    fall back to this slice only when the full (gitignored) fact is absent
    (i.e. on a fresh Cloud clone); locally the full fact + full-text search win.
    """
    if fact.empty:
        return fact.copy()
    lite = fact[pd.to_numeric(fact["year"], errors="coerce") >= _COMMITTED_YEAR_FLOOR].copy()
    txt = lite["speech_text"].fillna("")
    truncated = txt.str.len() > _EXCERPT_CHARS
    lite["speech_text"] = txt.where(~truncated, txt.str.slice(0, _EXCERPT_CHARS).str.rstrip() + "…")
    return lite


def run() -> int:
    if not SILVER_SPEECHES_PARQUET.exists():
        logger.warning("speeches_gold: silver %s missing — run debates.speech_parse first", SILVER_SPEECHES_PARQUET)
        return 0

    speeches = pd.read_parquet(SILVER_SPEECHES_PARQUET)
    fact = build_speeches_fact(speeches, _member_map())
    if fact.empty:
        logger.warning("speeches_gold: 0 rows produced")
        return 0

    resolved = fact["member_name"].notna().sum()
    irish = int(fact["is_irish"].sum())
    logger.info(
        "speeches_gold: rows=%d resolved_identity=%d/%d (%.0f%%) irish_flagged=%d members=%d houses=%s",
        len(fact),
        resolved,
        len(fact),
        100 * resolved / len(fact),
        irish,
        fact["unique_member_code"].nunique(),
        fact["house"].value_counts().to_dict(),
    )
    # Full fact (all years, full text) — gitignored; local + API + full-text search.
    save_parquet(fact, GOLD_SPEECHES_FACT_FULL_PARQUET)
    logger.info("speeches_gold: wrote FULL %s (%d rows)", GOLD_SPEECHES_FACT_FULL_PARQUET, len(fact))
    # Lite slice (>= %d, excerpt) — committed for GitHub/Streamlit Cloud.
    lite = build_committed_slice(fact)
    save_parquet(lite, GOLD_SPEECHES_FACT_PARQUET)
    logger.info(
        "speeches_gold: wrote COMMITTED slice %s (%d rows, >=%d, %d-char excerpt)",
        GOLD_SPEECHES_FACT_PARQUET,
        len(lite),
        _COMMITTED_YEAR_FLOOR,
        _EXCERPT_CHARS,
    )
    return len(fact)


def main() -> int:
    from services.logging_setup import setup_logging

    setup_logging()
    return 0 if run() >= 0 else 1


if __name__ == "__main__":
    sys.exit(main())
