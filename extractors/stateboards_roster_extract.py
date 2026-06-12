"""State Boards register extractor — current roster of every state board, by department.

SOURCE: https://membership.stateboards.ie/ (DPER / OGCIO, static server-rendered
Django site, no auth, no JS). Three-level hierarchy:

    /en/                              -> 20 department links
    /en/department/<Department>/      -> board links per department
    /en/board/<Board>/                -> metadata <p> + one HTML <table> of members

Per-member fields published: name, first appointed, reappointed, expiry date,
position type, basis of appointment. Per-board: legal basis (link), maximum
positions, gender balance. There is NO occupation/employer field — the
"who is this person outside the board" layer in GOLD comes exclusively from
data/_meta/stateboards_wikidata_curated.csv, a HAND-CURATED identity list
(one row per verified member name). Automated name-matching was tried and
REMOVED: ~1 in 4 auto-matches was the wrong person (hurlers on the Medical
Council). wikidata/stateboards_wikidata_enrich.py only regenerates the
candidate queue for human review — it never feeds gold directly.

This register complements (does not duplicate) the Iris public-appointments
spine: Iris captures appointment EVENTS; this is the CURRENT ROSTER plus the
public-body universe (~250 bodies) behind Public-Body-Profile joins.
Scoping/verdict: doc/PUBLIC_RECORD_SOURCES_REVIEW.md (shortlist #2).

Reads  : membership.stateboards.ie (HTML cached to data/bronze/stateboards/)
         data/_meta/stateboards_wikidata_curated.csv      (hand-curated, tracked)
Writes : data/silver/parquet/stateboards_roster.parquet   (one row per seat)
         data/silver/parquet/stateboards_boards.parquet   (one row per board)
         data/gold/parquet/stateboards_roster.parquet     (silver + curated identities)
         data/_meta/stateboards_coverage.json             (fetch/parse health)

PRIVACY: board members are public office-holders; only role + body + term is
published and only that is stored. No contact data, no DOB.

Usage:
    python extractors/stateboards_roster_extract.py --max-boards 3      # smoke
    python extractors/stateboards_roster_extract.py                     # full
    python extractors/stateboards_roster_extract.py --use-cache         # replay bronze
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import re
import sys
import time
from datetime import date, datetime
from pathlib import Path
from urllib.parse import unquote, urljoin

import polars as pl
import requests
from bs4 import BeautifulSoup

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
from services.parquet_io import save_parquet  # noqa: E402

logger = logging.getLogger(__name__)

BASE = "https://membership.stateboards.ie"
INDEX_URL = f"{BASE}/en/"
_H = {"User-Agent": "Mozilla/5.0 (dail-tracker research; stateboards roster)"}
_SLEEP = 0.25  # polite gap between requests; ~250 pages => ~90s full run

BRONZE_DIR = _ROOT / "data" / "bronze" / "stateboards"
OUT_ROSTER = _ROOT / "data" / "silver" / "parquet" / "stateboards_roster.parquet"
OUT_BOARDS = _ROOT / "data" / "silver" / "parquet" / "stateboards_boards.parquet"
OUT_GOLD = _ROOT / "data" / "gold" / "parquet" / "stateboards_roster.parquet"
OUT_COVERAGE = _ROOT / "data" / "_meta" / "stateboards_coverage.json"
CURATED_CSV = _ROOT / "data" / "_meta" / "stateboards_wikidata_curated.csv"

# Hand-curated identity columns appended to gold (all nullable; populated only
# for member names a human verified in CURATED_CSV).
_CURATED_COLS = (
    "wikidata_qid",
    "wikidata_url",
    "wikidata_label",
    "wikidata_description",
    "wikidata_occupations",
    "wikidata_employers",
    "wikidata_positions_held",
    "wikidata_curation_note",
)

# The member table as published (header text, lowercased) -> our column name.
_COL_MAP = {
    "name": "member_name",
    "first appointed": "first_appointed_raw",
    "reappointed": "reappointed_raw",
    "expiry date": "expiry_date_raw",
    "position type": "position_type",
    "basis of appointment": "basis_of_appointment",
}


# ---------------------------------------------------------------------------
# Fetch (bronze-cached)
# ---------------------------------------------------------------------------


def _cache_slug(url: str) -> str:
    """Stable filesystem-safe filename for one page's bronze HTML."""
    tail = unquote(url.replace(BASE, "")).strip("/")
    slug = re.sub(r"[^A-Za-z0-9]+", "_", tail).strip("_").lower() or "index"
    return f"{slug}.html"


def fetch_html(session: requests.Session, url: str, *, use_cache: bool) -> str | None:
    """GET one page; cache raw HTML to bronze. ``use_cache`` replays bronze
    without touching the network (dev / deterministic re-parse)."""
    cache = BRONZE_DIR / _cache_slug(url)
    if use_cache and cache.exists():
        return cache.read_text(encoding="utf-8")
    for attempt in (1, 2, 3):
        try:
            r = session.get(url, headers=_H, timeout=30)
            r.raise_for_status()
            BRONZE_DIR.mkdir(parents=True, exist_ok=True)
            cache.write_text(r.text, encoding="utf-8")
            time.sleep(_SLEEP)
            return r.text
        except Exception as exc:  # noqa: BLE001 — retry transient HTTP/TLS causes
            logger.warning("fetch attempt %d/3 failed for %s: %s", attempt, url, exc)
            time.sleep(2 * attempt)
    return None


# ---------------------------------------------------------------------------
# Parse — pure functions, unit-testable, no network
# ---------------------------------------------------------------------------


def parse_link_list(html: str, href_prefix: str) -> list[tuple[str, str]]:
    """(text, absolute_url) for every main-content <a> whose href starts with
    ``href_prefix`` — used for both the department index and dept board lists."""
    soup = BeautifulSoup(html, "html.parser")
    main = soup.find(id="main-content") or soup
    out: list[tuple[str, str]] = []
    for a in main.find_all("a", href=True):
        href = a["href"]
        if unquote(href).startswith(href_prefix):
            out.append((a.get_text(strip=True), urljoin(BASE, href)))
    return out


def parse_dmy(raw: str | None) -> date | None:
    """Dates are published dd/mm/yyyy; anything else (blank, prose) -> None."""
    s = (raw or "").strip()
    if not s:
        return None
    try:
        return datetime.strptime(s, "%d/%m/%Y").date()
    except ValueError:
        return None


def _board_meta(soup: BeautifulSoup) -> dict:
    """Legal basis / max positions / gender balance from the metadata <p>.
    Every field is optional — some boards publish none of them."""
    meta: dict = {
        "legal_basis": None,
        "legal_basis_url": None,
        "max_positions": None,
        "gender_female_n": None,
        "gender_male_n": None,
        "gender_female_pct": None,
        "gender_male_pct": None,
    }
    main = soup.find(id="main-content") or soup
    for b in main.find_all("b"):
        label = b.get_text(strip=True).rstrip(":").strip().lower()
        # The value is the run of siblings up to the next <br>/<b>.
        parts: list[str] = []
        href: str | None = None
        for sib in b.next_siblings:
            name = getattr(sib, "name", None)
            if name in ("br", "b"):
                break
            if name == "a":
                href = sib.get("href")
            parts.append(sib.get_text(" ", strip=True) if name else str(sib).strip())
        value = " ".join(p for p in parts if p).strip()
        if label == "legal basis":
            meta["legal_basis"] = value or None
            meta["legal_basis_url"] = href
        elif label == "maximum number of positions":
            m = re.search(r"\d+", value)
            meta["max_positions"] = int(m.group()) if m else None
        elif label == "gender balance numbers":
            for sex, key in (("Female", "gender_female_n"), ("Male", "gender_male_n")):
                m = re.search(rf"{sex}\s*\((\d+)\)", value)
                meta[key] = int(m.group(1)) if m else None
        elif label == "gender balance percentage":
            for sex, key in (("Female", "gender_female_pct"), ("Male", "gender_male_pct")):
                m = re.search(rf"{sex}\s*\((\d+(?:\.\d+)?)%\)", value)
                meta[key] = float(m.group(1)) if m else None
    return meta


def parse_board(html: str, department: str, body: str, url: str) -> tuple[dict, list[dict]]:
    """One board page -> (board row, member rows). Header-driven so a board
    with reordered/missing columns degrades to nulls, not misaligned data."""
    soup = BeautifulSoup(html, "html.parser")
    h1 = soup.find("h1")
    body_full = h1.get_text(strip=True) if h1 else body

    board = {
        "department": department,
        "body": body,
        "body_full": body_full,
        "source_url": url,
        **_board_meta(soup),
    }

    members: list[dict] = []
    table = (soup.find(id="main-content") or soup).find("table")
    if table is not None:
        headers = [th.get_text(strip=True).lower() for th in table.find_all("th")]
        cols = [_COL_MAP.get(h) for h in headers]
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if not tds:
                continue
            row = dict.fromkeys(_COL_MAP.values())
            for col, td in zip(cols, tds, strict=False):
                if col:
                    row[col] = td.get_text(" ", strip=True) or None
            if not row["member_name"]:
                continue
            row.update(
                department=department,
                body=body,
                body_full=body_full,
                source_url=url,
                first_appointed=parse_dmy(row.pop("first_appointed_raw")),
                reappointed=parse_dmy(row.pop("reappointed_raw")),
                expiry_date=parse_dmy(row.pop("expiry_date_raw")),
            )
            members.append(row)
    board["members_listed"] = len(members)
    return board, members


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

_ROSTER_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "department": pl.Utf8,
    "body": pl.Utf8,
    "body_full": pl.Utf8,
    "member_name": pl.Utf8,
    "position_type": pl.Utf8,
    "basis_of_appointment": pl.Utf8,
    "first_appointed": pl.Date,
    "reappointed": pl.Date,
    "expiry_date": pl.Date,
    "source_url": pl.Utf8,
}

_BOARDS_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "department": pl.Utf8,
    "body": pl.Utf8,
    "body_full": pl.Utf8,
    "legal_basis": pl.Utf8,
    "legal_basis_url": pl.Utf8,
    "max_positions": pl.Int64,
    "gender_female_n": pl.Int64,
    "gender_male_n": pl.Int64,
    "gender_female_pct": pl.Float64,
    "gender_male_pct": pl.Float64,
    "members_listed": pl.Int64,
    "source_url": pl.Utf8,
}


def scrape(*, use_cache: bool, max_boards: int = 0, only_dept: str = "") -> tuple[pl.DataFrame, pl.DataFrame, dict]:
    session = requests.Session()
    failures: list[str] = []

    index_html = fetch_html(session, INDEX_URL, use_cache=use_cache)
    if index_html is None:
        raise SystemExit("stateboards: could not fetch the department index — aborting (no partial write)")
    departments = parse_link_list(index_html, "/en/department/")
    if only_dept:
        departments = [d for d in departments if only_dept.lower() in d[0].lower()]
    logger.info("stateboards: %d departments", len(departments))

    board_rows: list[dict] = []
    member_rows: list[dict] = []
    n_boards = 0
    for dept_name, dept_url in departments:
        dept_html = fetch_html(session, dept_url, use_cache=use_cache)
        if dept_html is None:
            failures.append(dept_url)
            continue
        boards = parse_link_list(dept_html, "/en/board/")
        for body, board_url in boards:
            if max_boards and n_boards >= max_boards:
                break
            html = fetch_html(session, board_url, use_cache=use_cache)
            if html is None:
                failures.append(board_url)
                continue
            board, members = parse_board(html, dept_name, body, board_url)
            board_rows.append(board)
            member_rows.extend(members)
            n_boards += 1
        if max_boards and n_boards >= max_boards:
            break

    roster = pl.DataFrame(member_rows, schema=_ROSTER_SCHEMA).sort(["department", "body", "member_name"])
    boards = pl.DataFrame(board_rows, schema=_BOARDS_SCHEMA).sort(["department", "body"])
    coverage = {
        "departments": len(departments),
        "boards": boards.height,
        "members": roster.height,
        "boards_without_members": int((boards["members_listed"] == 0).sum()) if boards.height else 0,
        "fetch_failures": failures,
        "retrieved_at": date.today().isoformat(),
        "source": INDEX_URL,
    }
    return roster, boards, coverage


def apply_curated(roster: pl.DataFrame, curated: pl.DataFrame | None) -> pl.DataFrame:
    """Gold frame: silver roster + hand-curated identity columns. ``curated``
    is the data/_meta CSV (or None when absent on a fresh clone) — names not in
    it get nulls. No automated matching here, ever: the auto name-matcher this
    replaced tagged ~1 in 4 members with the wrong same-named person."""
    if curated is not None and curated.height:
        cur = curated.rename({"curation_note": "wikidata_curation_note"}).select("member_name", *_CURATED_COLS)
        # CSV blanks (e.g. no employer on the Wikidata item) -> proper nulls.
        cur = cur.with_columns(pl.when(pl.col(c) == "").then(None).otherwise(pl.col(c)).alias(c) for c in _CURATED_COLS)
        gold = roster.join(cur, on="member_name", how="left")
    else:
        gold = roster.with_columns(pl.lit(None, dtype=pl.Utf8).alias(c) for c in _CURATED_COLS)
    return gold.sort(["department", "body", "member_name"])


def build_gold(roster: pl.DataFrame) -> None:
    curated = pl.read_csv(CURATED_CSV, infer_schema_length=0) if CURATED_CSV.exists() else None
    if curated is None:
        logger.warning("stateboards: %s missing — gold gets no curated identities", CURATED_CSV.name)
    gold = apply_curated(roster, curated)
    save_parquet(gold, OUT_GOLD)
    n = int(gold["wikidata_qid"].is_not_null().sum())
    print(f"wrote {OUT_GOLD.name} ({gold.height} rows, {n} seats with curated identity)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--max-boards", type=int, default=0, help="smoke run: stop after N boards, skip writes")
    ap.add_argument("--only-dept", default="", help="substring filter on department name")
    ap.add_argument("--use-cache", action="store_true", help="replay bronze HTML instead of fetching")
    args = ap.parse_args()

    roster, boards, coverage = scrape(use_cache=args.use_cache, max_boards=args.max_boards, only_dept=args.only_dept)
    print(
        f"stateboards: {coverage['departments']} depts, {coverage['boards']} boards, "
        f"{coverage['members']} seats, {len(coverage['fetch_failures'])} fetch failures"
    )

    smoke = bool(args.max_boards or args.only_dept)
    if smoke:
        print(roster.head(10))
        print("smoke run (--max-boards/--only-dept) — nothing written")
        return

    if coverage["fetch_failures"]:
        logger.warning(
            "stateboards: %d pages failed to fetch: %s", len(coverage["fetch_failures"]), coverage["fetch_failures"][:5]
        )
    save_parquet(roster, OUT_ROSTER)
    save_parquet(boards, OUT_BOARDS)
    OUT_COVERAGE.parent.mkdir(parents=True, exist_ok=True)
    OUT_COVERAGE.write_text(json.dumps(coverage, indent=2), encoding="utf-8")
    print(f"wrote {OUT_ROSTER.name} ({roster.height} rows), {OUT_BOARDS.name} ({boards.height} rows)")

    build_gold(roster)


if __name__ == "__main__":
    from services.logging_setup import setup_standalone_logging

    setup_standalone_logging("stateboards_roster_extract")
    main()
