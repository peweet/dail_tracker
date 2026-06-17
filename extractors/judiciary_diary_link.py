"""judiciary_diary_link.py — link Legal Diary judge strings to the bench roster.

The daily Legal Diary names judges SURNAME-ONLY ("Ms Justice Butler") while the
bench roster keys on forename+surname ("una butler"), so the exact judge_key join
covers only ~2/3 of diary judges. This extractor builds the missing bridge as a
PIPELINE-owned mapping table so the judge profile page can show what is listed
before each judge (sql_views/judiciary/judiciary_judge_diary.sql et al.) without
any name-matching logic in the UI layer.

MATCHING (honest-or-nothing — a wrong judge attribution is worse than a gap):
  1. exact         cleaned diary string normalises to a roster judge_key verbatim
                   (full-name diary entries, e.g. "Mr Justice Micheál O'Connell").
  2. surname-court the surname resolves to exactly ONE roster judge within the
                   diary row's court (Central Criminal Court scopes to High Court
                   judges; Court of Appeal (Criminal) to the Court of Appeal).
  3. surname-unique surname unique across the whole bench (court missing only).
  REFUSED, never guessed:
    * ambiguous surnames within scope ("Mr Justice Collins" with two Justices
      Collins in reach) -> unmatched;
    * honorific conflicts — if "Mr Justice X" AND "Ms Justice X" both resolve to
      the SAME roster judge, the roster is missing one of two real people, so
      every surname-based match for that judge is withdrawn (kept only if exact);
    * office titles ("The President", "The Chief Justice") — offices, not names.

PRIVACY: this publishes nothing new — it links two already-public gold sets
(officials sitting in public function <-> the anonymised Tier C diary). No party
data, no workload framing; counts stay list-density, never judge performance.

OUTPUT
  data/gold/parquet/judiciary_diary_judge_map.parquet   one row per distinct
      (diary judge string x court) pair, matched AND unmatched (judge_key NULL),
      so coverage is queryable and honest.
  data/_meta/judiciary_diary_link_coverage.json

Run (also invoked automatically at the end of legal_diary_extract.py):
  ./.venv/Scripts/python.exe extractors/judiciary_diary_link.py
"""

from __future__ import annotations

import contextlib
import json
import logging
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from config import DATA_DIR, GOLD_PARQUET_DIR  # noqa: E402
from extractors.judiciary_bench_extract import _load_aliases, normalise_key  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

logger = logging.getLogger(__name__)

LINK_VERSION = "1.1.0"  # 1.1.0: OpenView sources + panel-member explosion (capped)
# A judge string with more members than a full court is a term-roster / callover notice,
# not a sitting panel — the Supreme Court sits at most 7, so 9 is a safe panel ceiling.
_PANEL_CAP = 9
MAP_PATH = GOLD_PARQUET_DIR / "judiciary_diary_judge_map.parquet"
COVERAGE_PATH = DATA_DIR / "_meta" / "judiciary_diary_link_coverage.json"

# Diary courts whose judges sit on a different roster court.
_COURT_SCOPE = {
    "Central Criminal Court": "High Court",
    "Court of Appeal (Criminal)": "Court of Appeal",
}

# Diary judge strings carry parse debris: a courtroom/time suffix glued onto the
# name line, parentheticals, or a trailing list code. Stripped BEFORE keying.
_IN_COURT_RE = re.compile(r"\s+in court\b.*$", re.I)
_PARENS_RE = re.compile(r"\([^)]*\)")
_TIME_RE = re.compile(r"\s*-?\s*\d{1,2}[:.]\d{2}\s*(?:am|pm)?\s*$", re.I)
_TRAIL_JUNK_RE = re.compile(r"\s+adv\.?$", re.I)
_OFFICE_RE = re.compile(r"^the\s+(president|chief justice)\b", re.I)
_FEM_RE = re.compile(r"^(ms|mrs|madam)\s+justice\b|^her honour\b", re.I)
_MASC_RE = re.compile(r"^mr\s+justice\b|^his honour\b", re.I)


def _clean(judge: str) -> str:
    s = _IN_COURT_RE.sub("", str(judge))
    s = _PARENS_RE.sub(" ", s)
    s = _TIME_RE.sub("", s)
    s = _TRAIL_JUNK_RE.sub("", s)
    return re.sub(r"\s+", " ", s).strip(" -:.,")


def _gender(judge: str) -> str | None:
    if _FEM_RE.search(judge):
        return "f"
    if _MASC_RE.search(judge):
        return "m"
    return None


def _current_gender_by_recency(latest_by_gender: dict[str, str]) -> str | None:
    """Given {gender: latest_diary_date} for one judge_key carrying BOTH genders (two
    same-surname people), return the gender of the CURRENT roster judge — the one whose
    matches reach the later date (the sitting judge holds the most recent / forward-
    scheduled dates; the departed namesake only old ones). Returns None when the two
    latest dates TIE — we then can't tell them apart, so the caller withdraws both
    (honest-or-nothing). Dates are ISO 'YYYY-MM-DD' strings, lexically comparable."""
    (g_a, d_a), (g_b, d_b) = sorted(latest_by_gender.items(), key=lambda kv: kv[1], reverse=True)[:2]
    return g_a if d_a > d_b else None


def build_map() -> pl.DataFrame:
    bench = pl.read_parquet(GOLD_PARQUET_DIR / "judiciary_bench.parquet")
    aliases = _load_aliases()
    # roster lookup: judge_key -> (name, court); surname index per court and bench-wide
    roster = [
        {
            "judge_key": r["judge_key"],
            "judge_name": r["judge_name"],
            "bench_court": r["court"],
            "surname": (r["judge_key"].split() or [""])[-1],
        }
        for r in bench.select(["judge_key", "judge_name", "court"]).iter_rows(named=True)
    ]
    by_key = {r["judge_key"]: r for r in roster}

    def _candidates(surname: str, court: str | None) -> list[dict]:
        if court:
            scope = _COURT_SCOPE.get(court, court)
            return [r for r in roster if r["surname"] == surname and r["bench_court"] == scope]
        return [r for r in roster if r["surname"] == surname]

    # distinct (judge string, court) pairs across BOTH diary pipelines: the .docx gold
    # and the OpenView gold (extractors/legal_diary_openview_extract.py — Circuit + the
    # higher courts' history). The string formats overlap, so the same map serves both.
    # Also track the LATEST diary_date each (judge string, court) appears on — the recency
    # signal that disambiguates two same-surname judges of different genders (the current
    # judge holds the most recent / forward-scheduled sittings; the departed one only old
    # ones). See the honorific guard below.
    pairs: set[tuple[str, str | None]] = set()
    pair_maxdate: dict[tuple[str, str | None], str] = {}
    for fname in (
        "judicial_legal_diary_schedule.parquet",
        "judicial_legal_diary_cases.parquet",
        "judicial_legal_diary_openview_schedule.parquet",
        "judicial_legal_diary_openview_cases.parquet",
    ):
        path = GOLD_PARQUET_DIR / fname
        if not path.exists():
            continue
        df = (
            pl.read_parquet(path)
            .select(["judge", "court", "diary_date"])
            .group_by(["judge", "court"])
            .agg(pl.col("diary_date").max().alias("maxd"))
        )
        for j, c, d in df.iter_rows():
            if j is None:
                continue
            pairs.add((j, c))
            if d and d > pair_maxdate.get((j, c), ""):
                pair_maxdate[(j, c)] = d

    def _resolve(member: str, court: str | None) -> tuple[dict | None, str]:
        """One judge substring -> (roster hit | None, match_method)."""
        cleaned = _clean(member)
        if _OFFICE_RE.match(cleaned):
            return None, "office-title"
        key = normalise_key(cleaned, aliases)
        if not key:
            return None, "no-candidate"
        if key in by_key:
            return by_key[key], "exact"
        cands = _candidates(key.split()[-1], court)
        if len(cands) == 1:
            return cands[0], ("surname-court" if court else "surname-unique")
        return None, ("ambiguous" if len(cands) > 1 else "no-candidate")

    rows: list[dict] = []
    for judge, court in sorted(pairs, key=lambda p: (str(p[1]), p[0])):
        # Supreme / Court of Appeal sit as PANELS, joined "A & B & C" in the gold judge
        # string. Per the panel-attribution decision, a panel matter belongs to EVERY
        # member, so we emit one map row per member (same diary string + court, distinct
        # judge_key). The judge_diary/sittings views JOIN on (judge, court), so the matter
        # fans out to each member's profile. A single-judge string yields exactly one row.
        # BUT a string naming MORE than a full court (cap = 9; the Supreme Court sits at
        # most 7) is a term-roster / callover NOTICE, not a panel — fanning its matters to
        # 28 judges would be wrong, so it is left unmatched rather than exploded.
        members = [m.strip() for m in judge.split(" & ")] if " & " in judge else [judge]
        if len(members) > _PANEL_CAP:
            rows.append(
                {
                    "judge": judge,
                    "court": court,
                    "judge_key": None,
                    "judge_name": None,
                    "bench_court": None,
                    "match_method": "roster-notice",
                    "_member": judge,
                    "_gender": None,
                    "_maxd": pair_maxdate.get((judge, court), ""),
                }
            )
            continue
        for member in members:
            hit, method = _resolve(member, court)
            rows.append(
                {
                    "judge": judge,  # the ORIGINAL (possibly panel) string — the join key
                    "court": court,
                    "judge_key": hit["judge_key"] if hit else None,
                    "judge_name": hit["judge_name"] if hit else None,
                    "bench_court": hit["bench_court"] if hit else None,
                    "match_method": method,
                    "_member": member,  # for the gender guard below; dropped before output
                    "_gender": _gender(member),
                    "_maxd": pair_maxdate.get((judge, court), ""),
                }
            )

    # honorific-conflict guard, gender-disambiguated by recency.
    # The diary names higher-court judges SURNAME-ONLY, so a surname that the roster has
    # once ("Butler" -> Ms Justice Nuala Butler) but the diary carries under BOTH genders
    # ("Mr Justice Butler" AND "Ms Justice Butler", over an 8-year history) is really TWO
    # people — the current roster judge plus a departed namesake. The honorific tells them
    # apart, and RECENCY says which is current: the sitting judge holds the latest (and
    # forward-scheduled) dates; the departed one only old ones. So per conflicted judge_key
    # we keep the gender whose matches reach the LATEST diary_date (the current judge) and
    # withdraw the other gender's strings as a different, off-roster person. Only when the
    # two genders' latest dates TIE (can't tell which is current) do we fall back to the
    # old all-or-nothing withdrawal. Exact (full-name) matches are always kept.
    latest_by_key_gender: dict[str, dict[str, str]] = {}
    for r in rows:
        if r["judge_key"] and r["_gender"]:
            g2d = latest_by_key_gender.setdefault(r["judge_key"], {})
            if r["_maxd"] > g2d.get(r["_gender"], ""):
                g2d[r["_gender"]] = r["_maxd"]
    # per conflicted key: the current gender (strictly-latest), or None if tied/ambiguous
    current_gender: dict[str, str | None] = {}
    for k, g2d in latest_by_key_gender.items():
        if len(g2d) >= 2:
            current_gender[k] = _current_gender_by_recency(g2d)

    n_disambig = n_conflict = 0
    for r in rows:
        cur = current_gender.get(r["judge_key"]) if r["judge_key"] else "__no_conflict__"
        if r["judge_key"] not in current_gender or r["match_method"] == "exact":
            continue  # no conflict for this key (or an unambiguous full-name match)
        if cur is None:  # tied recency — fall back to conservative all-or-nothing
            r.update(judge_key=None, judge_name=None, bench_court=None, match_method="honorific-conflict")
            n_conflict += 1
        elif r["_gender"] and r["_gender"] != cur:  # the departed namesake's strings
            r.update(judge_key=None, judge_name=None, bench_court=None, match_method="honorific-other-person")
            n_disambig += 1
    if n_conflict or n_disambig:
        logger.warning(
            "honorific guard: kept current-gender by recency (%d off-roster-namesake withdrawn), "
            "%d withdrawn on tied recency",
            n_disambig,
            n_conflict,
        )

    for r in rows:
        del r["_member"], r["_gender"], r["_maxd"]
    return pl.DataFrame(
        rows,
        schema={
            "judge": pl.Utf8,
            "court": pl.Utf8,
            "judge_key": pl.Utf8,
            "judge_name": pl.Utf8,
            "bench_court": pl.Utf8,
            "match_method": pl.Utf8,
        },
    )


def _row_level_rate(map_df: pl.DataFrame, fname: str) -> dict:
    path = GOLD_PARQUET_DIR / fname
    if not path.exists():
        return {}
    df = pl.read_parquet(path).filter(pl.col("judge").is_not_null())
    matched = map_df.filter(pl.col("judge_key").is_not_null()).select(["judge", "court"])
    n_hit = df.join(matched, on=["judge", "court"], how="semi").height
    return {"rows_with_judge": df.height, "rows_matched": n_hit}


def run() -> int:
    bench_path = GOLD_PARQUET_DIR / "judiciary_bench.parquet"
    if not bench_path.exists():
        logger.error("bench roster missing (%s) — run judiciary_bench_extract.py first.", bench_path)
        return 1
    map_df = build_map()
    if map_df.is_empty():
        logger.error("no diary judge strings found — run legal_diary_extract.py first.")
        return 1
    save_parquet(map_df, MAP_PATH)

    n_matched = map_df.filter(pl.col("judge_key").is_not_null()).height
    methods = {m: n for m, n in map_df.group_by("match_method").len().iter_rows()}
    coverage = {
        "link_version": LINK_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "judge_court_pairs": map_df.height,
        "matched_pairs": n_matched,
        "match_methods": methods,
        "unmatched": map_df.filter(pl.col("judge_key").is_null()).select(["judge", "court", "match_method"]).to_dicts(),
        "row_level": {
            "cases": _row_level_rate(map_df, "judicial_legal_diary_cases.parquet"),
            "schedule": _row_level_rate(map_df, "judicial_legal_diary_schedule.parquet"),
        },
        "note": (
            "Honest-or-nothing matching: ambiguous surnames, honorific conflicts and "
            "office titles are left unmatched rather than guessed."
        ),
    }
    COVERAGE_PATH.parent.mkdir(parents=True, exist_ok=True)
    COVERAGE_PATH.write_text(json.dumps(coverage, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(
        "diary judge map: %d/%d pairs matched (%s) -> %s",
        n_matched,
        map_df.height,
        methods,
        MAP_PATH,
    )
    return 0


def main() -> int:
    from services.logging_setup import setup_standalone_logging

    setup_standalone_logging("judiciary_diary_link")
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
