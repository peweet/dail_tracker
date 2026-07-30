"""Scale figures for the Support page — how much of the public record is here.

Three counts, all derived at run time from the repo's own metadata and
registers. Nothing is hard-coded: a figure typed into markup is true on the day
it is typed and quietly wrong after the next refresh.

Cheap by construction — two small metadata files plus two aggregate queries.
No parquet rows are materialised.

  * ``data/_meta/fact_cards.json``    — one card per silver/gold parquet
    (name, layer, rows); the same index the MCP's ``list_datasets`` serves.
  * ``data/_meta/source_cadence.csv`` — the monitored-source registry
    (``tools/migration/check_source_cadence.py`` is its traffic light).

── What "records published" counts, and what it deliberately excludes ───────
Summing every gold row would over-claim, so three classes are dropped. Each
exclusion is named here so the figure is auditable rather than asserted:

  1. DUPLICATE PROJECTIONS — the same records published twice under two names.
     ``speeches_fact`` is a subset of ``speeches_fact_full``;
     ``corporate_notices_enriched`` is 1:1 with ``corporate_notices``;
     ``si_lrc_enrichment_summary`` is 1:1 with ``statutory_instruments``;
     the openview schedule re-cuts the openview cases.
  2. AGGREGATES — rankings, scorecards, breakdowns, wide pivots. Derived
     summaries of records already counted, not records in their own right.
  3. CSO REFERENCE STATISTICS — price and housing series ingested to deflate
     and contextualise money figures. Real published rows, but upstream
     statistics rather than the accountability record this site exists to
     publish. Including them would inflate the headline by roughly half, and
     ``cso_hpm03`` alone would become the largest single "record" on the site.

Silver is excluded throughout: gold is the published layer.

── What "public officials tracked" counts ──────────────────────────────────
Two office-holder registers, each counted on its own person key and then
ADDED, never joined. Cross-register person matching is this repo's documented
trap (two key spaces, and 0 = not-matched ≠ absent), so no reconciliation is
attempted:

  * Oireachtas — distinct ``unique_member_code`` in ``member_terms`` (sitting
    AND former TDs and senators). That parquet is the superset: unioning it
    with the four other member parquets returns the same distinct count.
  * Judiciary  — distinct ``judge_name`` in ``judiciary_bench``.

Adding them is safe because the sets are disjoint in fact — an Irish judge
cannot hold an Oireachtas seat — and the page discloses the split beside the
total, so a reader can check the sum instead of trusting it.

DELIBERATELY EXCLUDED: ``public_appointments`` (state-board appointees). It has
no person key — most rows carry a blank appointee, and many pack several people
into one semicolon-joined string with a separate ``appointee_count``. Any
"distinct people" figure from it would be a guess dressed as a count.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_FACT_CARDS = _ROOT / "data/_meta/fact_cards.json"
_SOURCE_CADENCE = _ROOT / "data/_meta/source_cadence.csv"
_MEMBER_TERMS = _ROOT / "data/silver/parquet/member_terms.parquet"
_JUDICIARY_BENCH = _ROOT / "data/gold/parquet/judiciary_bench.parquet"

# Same underlying records already counted under another name (see §1 above).
_DUPLICATE_PROJECTIONS = frozenset(
    {
        "speeches_fact",
        "corporate_notices_enriched",
        "si_lrc_enrichment_summary",
        "judicial_legal_diary_openview_schedule",
    }
)

# Name fragments that mark a derived summary rather than a record (see §2).
_AGGREGATE_MARKERS = (
    "_summary",
    "_rankings",
    "_breakdown",
    "_wide",
    "_scorecard",
    "leaderboard",
    "top_",
    "_counts",
    "_mix",
    "_overlap",
    "_gaps",
    "deflator",
    "_profile",
    "persistence",
    "distinct_orgs",
    "most_",
)

_REFERENCE_PREFIX = "cso_"  # upstream statistics, not our record (see §3)


@dataclass(frozen=True)
class SupportStats:
    """The three scale figures, with exact counts kept beside the rounded
    display value so the page can show both."""

    records: int
    record_datasets: int
    sources: int
    publishers: int
    oireachtas_members: int
    judges: int

    @property
    def officials(self) -> int:
        """Two disjoint registers added, never joined — see the module header."""
        return self.oireachtas_members + self.judges

    @property
    def records_display(self) -> str:
        """Rounded DOWN — never round a claim upward.

        2,606,405 -> "2.6m". The exact count travels in the sub-label, so the
        rounding stays visible instead of hidden.
        """
        if self.records >= 1_000_000:
            return f"{self.records // 100_000 / 10:.1f}m"
        if self.records >= 1_000:
            return f"{self.records // 1_000}k"
        return str(self.records)


def _is_record_fact(name: str, layer: str) -> bool:
    if layer != "gold":
        return False
    if name in _DUPLICATE_PROJECTIONS:
        return False
    if name.startswith(_REFERENCE_PREFIX):
        return False
    return not any(marker in name for marker in _AGGREGATE_MARKERS)


@lru_cache(maxsize=1)
def _count_officials() -> tuple[int, int] | None:
    """(distinct Oireachtas members, distinct judges), or None if unreadable.

    Two aggregate queries over two small parquets — no rows are materialised.
    Cached because the counts move only when the ETL reruns, which restarts
    the app anyway.
    """
    if not (_MEMBER_TERMS.exists() and _JUDICIARY_BENCH.exists()):
        return None
    try:
        import duckdb

        con = duckdb.connect()
        try:
            members = con.execute(
                f"SELECT count(DISTINCT unique_member_code) FROM '{_MEMBER_TERMS.as_posix()}'"
            ).fetchone()[0]
            judges = con.execute(
                f"SELECT count(DISTINCT judge_name) FROM '{_JUDICIARY_BENCH.as_posix()}'"
            ).fetchone()[0]
        finally:
            con.close()
    except Exception:
        return None
    return int(members), int(judges)


@lru_cache(maxsize=1)
def load_support_stats() -> SupportStats | None:
    """Return the figures, or None if any input is unreadable.

    None is a real answer and callers must render it as such rather than
    printing a guess. A page arguing that every figure carries its source
    cannot invent its own — and on Streamlit Cloud a parquet genuinely can be
    absent, which is exactly when a fabricated number would ship.
    """
    try:
        facts = json.loads(_FACT_CARDS.read_text(encoding="utf-8"))["facts"]
    except (OSError, KeyError, ValueError):
        return None

    kept = {
        name: int(card.get("rows") or 0)
        for name, card in facts.items()
        if _is_record_fact(name, card.get("layer", ""))
    }
    if not kept:
        return None

    try:
        with open(_SOURCE_CADENCE, encoding="utf-8-sig", newline="") as fh:
            rows = list(csv.DictReader(fh))
    except OSError:
        return None

    # source_id is one monitored FEED; a publisher can expose several (Carlow
    # files both an annual financial statement and a payments return), so the
    # two counts genuinely differ and both are worth showing.
    feeds = {r["source_id"] for r in rows if r.get("source_id")}
    publishers = {r["name"] for r in rows if r.get("name")}
    if not feeds:
        return None

    officials = _count_officials()
    if officials is None:
        return None
    members, judges = officials

    return SupportStats(
        records=sum(kept.values()),
        record_datasets=len(kept),
        sources=len(feeds),
        publishers=len(publishers),
        oireachtas_members=members,
        judges=judges,
    )
