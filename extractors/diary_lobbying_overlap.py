"""Diary × lobbying-register OVERLAP — the strict, display-grade join (sandbox).

Takes the noisy ``diary_org_mentions`` co-occurrence table (extractors/diary_org_match.py)
and distils it to the *defensible* signal: a lobbying-registered organisation that a
minister actually MET, with a corroboration flag for "this org also lobbied this same
minister". This is the table a front-end "Ministers met these registered interests"
feature should sit on — NOT the raw mentions, which are too noisy to ship.

Strictness (each filter removes a measured noise class — see project_ministerial_diaries):
  1. KEEP both tiers,      — the ``match_confidence`` column (high|medium) is carried through
     carry confidence        so the front-end can lead with HIGH (≥2-token name found verbatim,
                             96.3% measured precision) and mark MEDIUM as lower-confidence.
                             DO NOT drop MEDIUM: a single distinctive token + an engagement cue
                             is exactly how every single-word GLOBAL BRAND lands here — "Google",
                             "Insurance" (Insurance Ireland), "Vodafone", "Pfizer" all normalise
                             to one token, so a HIGH-only filter silently deletes the marquee
                             names. (Generic single tokens are already gated upstream by the
                             STOP list + length floor + engagement-cue requirement.)
  2. drop entry_class      — travel + media. "Return flight from Brussels Aer Lingus" and
     {travel, media}        "Virgin Media Tonight Show" are the org name appearing in a
                             flight/photocall line, not a meeting. This is the single
                             biggest contaminant (Aer Lingus is ~all travel).
  3. PERSON_DENYLIST       — individual lobbyists / advisers / journalists whose 2-token
                             personal name passed the org gazetteer (e.g. Dónall Geoghegan,
                             a ministerial adviser, 177×). STOPGAP until the historical-
                             member-name list lands (diary_org_match.py docstring follow-up);
                             current-Dáil TDs are already excluded upstream.

Corroboration ("lobbied AND met the same minister") joins each surviving meeting to
data/silver/lobbying/parquet/lobby_break_down_by_politician.parquet (the named-target
table: one row per return × politician × activity) on normalised org-name AND minister
SURNAME. Surname keys are crude (the diary minister is a filename guess like "Ryans") so a
True here is indicative, not proof — and a common surname can collide. NEVER read this as
influence/causation: it evidences ACCESS (lobbied + met), there is no outcome variable.

Outputs -> data/sandbox/enrichment/
  diary_lobbying_overlap.parquet        one row per (meeting × matched org); the detail grain
  diary_lobbying_overlap_ranked.parquet one row per org: who met ministers most + corroboration

Run: .venv/Scripts/python.exe extractors/diary_lobbying_overlap.py
"""

from __future__ import annotations

import logging
import re
import unicodedata
from pathlib import Path

import polars as pl

from extractors.diary_org_match import norm  # identical org/subject normaliser
from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

log = logging.getLogger(__name__)

ENR = Path("data/sandbox/enrichment")
ENTRIES = ENR / "ministerial_diary_entries.parquet"
MENTIONS = ENR / "diary_org_mentions.parquet"
POL = Path("data/silver/lobbying/parquet/lobby_break_down_by_politician.parquet")
OUT_DETAIL = ENR / "diary_lobbying_overlap.parquet"
OUT_RANKED = ENR / "diary_lobbying_overlap_ranked.parquet"

# diary entry classes that are NOT a meeting with the named org (the org name rides a
# travel/photocall line). Everything else (external_meeting, govt_business, other,
# internal_dept, constituency, oireachtas) can legitimately host a real engagement.
EXCLUDE_CLASSES = {"travel", "media"}

# Government bodies that leak into the gazetteer via the client_name field — a department
# meeting a minister is not an outside interest lobbying. Drop by name prefix/keyword.
_GOV_BODY_RE = re.compile(
    r"^(department of|office of|houses of the oireachtas|an roinn)\b|\bgovernment\b",
    re.IGNORECASE,
)

# Vetted individual-name false positives (persons, not orgs) that cleared the org
# gazetteer's current-TD exclusion. Match on the verbatim display name. Stopgap — a
# historical-member + adviser name list would generalise this.
PERSON_DENYLIST = {
    "Dónall Geoghegan", "David Kelly", "Harry McGee", "Dave Fallon", "Patrick Costello",
    "Tony O'Brien", "Brian Carroll", "Tara Farrell", "Brendan Griffin", "John O'Neill",
    "Marc Coleman", "Seamus Quinn", "Pat Fitzpatrick", "Conor Kelly", "John Lynch",
    "Niall O'Connor", "bryan lynam",
}


def surname_key(name: str | None) -> str:
    """Crude surname key: ASCII-folded last token, trailing possessive 's' stripped.

    Aligns the diary minister filename-guess ("Ryans", "Brownes") with the lobbying
    register full_name ("Eamon Ryan"). Deliberately lossy — collisions possible.
    """
    if not name:
        return ""
    toks = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode().lower().split()
    if not toks:
        return ""
    last = toks[-1]
    return last[:-1] if last.endswith("s") and len(last) > 4 else last


def main() -> int:
    setup_standalone_logging("diary_lobbying_overlap")
    for p in (ENTRIES, MENTIONS, POL):
        if not p.exists():
            log.error("missing input: %s", p)
            return 1

    entries = pl.read_parquet(ENTRIES)
    if "entry_id" not in entries.columns or "entry_class" not in entries.columns:
        log.error(
            "entries parquet lacks entry_id/entry_class — re-run diary_entry_classify.py "
            "then diary_org_match.py before this builder (they stamp those columns)."
        )
        return 1

    mentions = pl.read_parquet(MENTIONS)
    overlap = (
        mentions.filter(~pl.col("matched_org_name").is_in(list(PERSON_DENYLIST)))
        .filter(~pl.col("matched_org_name").str.contains("(?i)" + _GOV_BODY_RE.pattern))
        .join(entries.select(["entry_id", "entry_class", "department"]), on="entry_id", how="left")
        .filter(~pl.col("entry_class").is_in(list(EXCLUDE_CLASSES)))
        .with_columns([
            pl.col("matched_org_name").map_elements(norm, return_dtype=pl.Utf8).alias("org_nk"),
            pl.col("minister").map_elements(surname_key, return_dtype=pl.Utf8).alias("min_sk"),
        ])
    )
    if overlap.is_empty():
        log.error("no overlap rows survived the strict filters — check inputs")
        return 1

    # corroboration: (org_nk, minister surname) pairs the register says were lobbied as a Minister
    pol = pl.read_parquet(POL).with_columns([
        pl.col("lobbyist_name").map_elements(norm, return_dtype=pl.Utf8).alias("org_nk"),
        pl.col("full_name").map_elements(surname_key, return_dtype=pl.Utf8).alias("min_sk"),
    ])
    pol_min = pol.filter(pl.col("position").fill_null("").str.contains("Minister"))
    lobbied_pairs = pol_min.select(["org_nk", "min_sk"]).unique().with_columns(pl.lit(True).alias("lobbied_same_minister"))
    returns_per_org = (
        pol.group_by("org_nk").agg(pl.col("lobby_url").n_unique().alias("total_lobbying_returns"))
    )

    overlap = (
        overlap.join(lobbied_pairs, on=["org_nk", "min_sk"], how="left")
        .with_columns(pl.col("lobbied_same_minister").fill_null(False))  # noqa: FBT003
        .join(returns_per_org, on="org_nk", how="left")
        .with_columns(pl.col("total_lobbying_returns").fill_null(0))
    )
    save_parquet(overlap, OUT_DETAIL)
    overlap.write_csv(OUT_DETAIL.with_suffix(".csv"))

    # org-level ranking: who met ministers most (DISTINCT meetings — explosion-safe)
    ranked = (
        overlap.group_by("matched_org_name")
        .agg([
            pl.col("entry_id").n_unique().alias("meetings"),
            pl.col("min_sk").filter(pl.col("min_sk") != "").n_unique().alias("ministers_met"),
            pl.col("min_sk").filter(pl.col("lobbied_same_minister")).n_unique().alias("ministers_lobbied_and_met"),
            pl.col("total_lobbying_returns").max().alias("total_lobbying_returns"),
            pl.col("entry_date").min().alias("first_meeting"),
            pl.col("entry_date").max().alias("last_meeting"),
        ])
        .sort(["meetings", "ministers_met"], descending=True)
    )
    save_parquet(ranked, OUT_RANKED)
    ranked.write_csv(OUT_RANKED.with_suffix(".csv"))

    log.info(
        "OVERLAP: %d meetings | %d distinct orgs | %d distinct ministers | corroborated (lobbied+met same min): %d",
        overlap["entry_id"].n_unique(),
        overlap["matched_org_name"].n_unique(),
        overlap.filter(pl.col("min_sk") != "")["min_sk"].n_unique(),
        overlap.filter(pl.col("lobbied_same_minister")).height,
    )
    log.info("Top 25 organisations by minister meetings:")
    log.info("  %-42s %8s %5s %12s %8s", "organisation", "meetings", "mins", "lobbied&met", "returns")
    for r in ranked.head(25).iter_rows(named=True):
        log.info(
            "  %-42s %8d %5d %12d %8d",
            r["matched_org_name"][:42], r["meetings"], r["ministers_met"],
            r["ministers_lobbied_and_met"], r["total_lobbying_returns"],
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
