"""Real figures for the support page's cost strip. SANDBOX ONLY.

Computed at RUN TIME from the repo's own metadata, never hard-coded — a figure
typed into markup is true on the day it is typed and quietly wrong thereafter.
Both sources are small metadata files, not facts: no parquet is scanned.

  * ``data/_meta/fact_cards.json``   — one card per silver/gold parquet
    (name, layer, rows). Same index the MCP's list_datasets serves.
  * ``data/_meta/source_cadence.csv`` — the monitored-source registry
    (`tools/migration/check_source_cadence.py` is its traffic light).

FIREWALL: in production this is data access and belongs in
``utility/data_access/``, with the page rendering from a registered contract.
It sits here because the whole feature is unwired.

── What "records published" counts, and what it deliberately excludes ───────
Counting every gold row would over-claim, so three classes are dropped. Each
exclusion is named here so the figure is auditable rather than asserted:

  1. DUPLICATE PROJECTIONS — the same underlying records published twice.
     `speeches_fact` is a subset of `speeches_fact_full`;
     `corporate_notices_enriched` is 1:1 with `corporate_notices`;
     `si_lrc_enrichment_summary` is 1:1 with `statutory_instruments`;
     `judicial_legal_diary_openview_schedule` re-cuts the openview cases.
  2. AGGREGATES — rankings, scorecards, breakdowns, wide pivots. Derived
     summaries of records already counted, not records themselves.
  3. CSO REFERENCE STATISTICS — price/housing series ingested to deflate and
     contextualise money figures. Real published rows, but upstream statistics
     rather than the accountability record this site exists to publish.
     Counting them would inflate the figure by ~49%.

Silver is excluded throughout: gold is the published layer.

── What "public officials tracked" counts ──────────────────────────────────
Two office-holder registers, each counted on its own person key and then
added — NOT joined. Cross-register person matching is this repo's documented
trap (see the join map: two key spaces, ORG vs PERSON, and 0 = not-matched
≠ absent), so no attempt is made to reconcile them:

  * Oireachtas — distinct `unique_member_code` in `member_terms` (sitting AND
    former TDs and senators). The code is the canonical person key; the other
    member parquets are subsets of it, verified by union.
  * Judiciary  — distinct `judge_name` in `judiciary_bench`.

Adding them is safe because the sets are disjoint in fact: an Irish judge
cannot simultaneously hold an Oireachtas seat. The sub-label discloses the
split, so the total is auditable rather than asserted.

DELIBERATELY EXCLUDED: `public_appointments` (state-board appointees). It has
no person key — 470 of 1,155 rows have a blank appointee, and 160 rows hold
several people in one semicolon-joined string. Any "distinct people" figure
from it would be a guess dressed as a count.
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
    """Everything the cost strip needs, with the exact counts kept alongside
    the rounded display value so the page can show both."""

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
        rounding is visible rather than hidden.
        """
        if self.records >= 1_000_000:
            return f"{self.records // 100_000 / 10:.1f}m"
        if self.records >= 1_000:
            return f"{self.records // 1_000}k"
        return str(self.records)


@lru_cache(maxsize=1)
def _count_officials() -> tuple[int, int] | None:
    """(distinct Oireachtas members, distinct judges), or None if unreadable.

    Two aggregate queries over two small parquets — no rows are materialised.
    Cached: the counts change only when the ETL reruns, which restarts the app.
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


def _is_record_fact(name: str, layer: str) -> bool:
    if layer != "gold":
        return False
    if name in _DUPLICATE_PROJECTIONS:
        return False
    if name.startswith(_REFERENCE_PREFIX):
        return False
    return not any(marker in name for marker in _AGGREGATE_MARKERS)


def load_stats() -> SupportStats | None:
    """Return the figures, or None if either metadata file is unreadable.

    None is a real answer: the caller must fall back to placeholders rather
    than print a guess. A support page that invents its own numbers would
    undercut the exact claim it is making.
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
    # two counts are genuinely different and both are worth showing.
    feeds = {r["source_id"] for r in rows if r.get("source_id")}
    publishers = {r["name"] for r in rows if r.get("name")}

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
