"""Per-member news-mentions extractor → silver ``news_mentions.parquet``.

Promoted from ``pipeline_sandbox/news_mentions/`` (2026-06-19). One Google-News RSS *search per
member* (exact-quoted name, Irish locale, recent window), so EVERY member is queried by name and
the article only needs to CONTAIN the name (matches the body, not just the headline). This is the
high-coverage successor to the sandbox's 41-feed headline scan (which caught ~6% of members); the
per-member search reaches ~98% of sitting members.

ACCUMULATES: each run appends to the silver parquet and dedups on (article_url, member), keeping
the earliest-seen copy, so the corpus grows over time instead of resetting to the current RSS
window. ALL matching/classification logic lives here (logic-firewall clean — the view is a plain
SELECT and the page only renders).

Source of truth for the roster is the silver historic rosters (current + former members), so the
extractor has no sandbox dependency. Surfaces through ``v_member_news_mentions`` on the member page.

CAVEAT (common names): a quoted search for "John Murphy" can return any John Murphy. The Irish
locale + recent window reduce this; ``match_in_title`` flags the high-confidence headline hits.
Each row is a NAME MATCH, not an assertion the article is about this politician.

Run:
  ./.venv/Scripts/python.exe extractors/news_mentions_extract.py --limit 8         # smoke test
  ./.venv/Scripts/python.exe extractors/news_mentions_extract.py --current-only    # sitting members
  ./.venv/Scripts/python.exe extractors/news_mentions_extract.py --days 30         # full, 30-day window
"""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
import time
import unicodedata
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import polars as pl
import requests
from dateutil import parser as dateparser

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

SILVER = ROOT / "data/silver/parquet"
OUT = SILVER / "news_mentions.parquet"
ROSTERS = [(SILVER / "historic_members_dail.parquet", "Dail"), (SILVER / "historic_members_seanad.parquet", "Seanad")]

GN_BASE = "https://news.google.com/rss/search"
GN_PARAMS = {"hl": "en-IE", "gl": "IE", "ceid": "IE:en"}
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/rss+xml, application/xml, text/xml, */*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
}

# Map a publisher (Google-News <source>) to the UI tier palette; default 'national'. Matched on a
# case-insensitive substring, so "The Irish Times" → national.
_TIER_BY_SOURCE = {
    "national": (
        "rté",
        "rte",
        "irish times",
        "irish independent",
        "independent.ie",
        "the journal",
        "thejournal",
        "breakingnews",
        "irish examiner",
        "business post",
        "newstalk",
        "rte.ie",
        "extra.ie",
        "irish mirror",
        "irish daily",
        "gript",
    ),
    "specialist": (
        "agriland",
        "thedetail",
        "thecurrency",
        "the currency",
        "law society",
        "silicon",
        "farmers journal",
        "construction",
    ),
    "local_radio": (
        "fm",
        "radio",
        "highland",
        "midwest",
        "ocean fm",
        "tipp fm",
        "kfm",
        "c103",
        "live 95",
        "red fm",
        "shannonside",
        "northern sound",
    ),
    "local_paper": (
        "echo",
        "nationalist",
        "leader",
        "post",
        "people",
        "advertiser",
        "champion",
        "star",
        "herald",
        "chronicle",
        "observer",
        "gazette",
        "journal",
        "tribune",
        "democrat",
        "courier",
        "today",
        "connaught",
        "mayo",
        "kerryman",
    ),
}


def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def norm(s: str) -> str:
    s = strip_accents(s or "").lower()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _clean(x: str) -> str:
    x = re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1", x or "", flags=re.S)
    return re.sub(r"<[^>]+>", " ", x).strip()


def _parse_date(raw: str):
    if not raw:
        return None
    try:
        return parsedate_to_datetime(raw)
    except Exception:  # noqa: BLE001
        try:
            return dateparser.parse(raw)
        except Exception:  # noqa: BLE001
            return None


def load_members() -> list[dict]:
    """Current + former members from the silver historic rosters (no sandbox dependency)."""
    rows: list[dict] = []
    cols = ["unique_member_code", "first_name", "last_name", "full_name", "party", "constituency_name", "is_current"]
    for path, house in ROSTERS:
        if not path.exists():
            continue
        df = pl.read_parquet(path).select([c for c in cols if c in pl.read_parquet(path).columns]).unique()
        for r in df.iter_rows(named=True):
            r["house"] = house
            r["is_current"] = bool(r.get("is_current"))
            rows.append(r)
    return rows


def gn_search_url(name: str, days: int) -> str:
    """Build the Google-News RSS search URL for an exact-quoted member name + recency window."""
    q = f'"{name}" when:{days}d'
    return f"{GN_BASE}?{urllib.parse.urlencode({'q': q, **GN_PARAMS})}"


def source_tier(outlet: str) -> str:
    o = (outlet or "").lower()
    for tier, needles in _TIER_BY_SOURCE.items():
        if any(n in o for n in needles):
            return tier
    return "national"


def parse_gn_items(content: bytes) -> list[dict]:
    """Parse a Google-News RSS search response into {title, link, outlet, pubDate} dicts.

    GN item title is "Headline - Source"; the <source> element carries the clean outlet name, so the
    trailing " - Source" is stripped from the displayed title. Pure → unit-tested."""
    out: list[dict] = []
    try:
        root = ET.fromstring(content)
    except ET.ParseError:
        return out
    for it in root.iter():
        if it.tag.split("}")[-1] != "item":
            continue
        d: dict = {}
        for ch in it:
            ct = ch.tag.split("}")[-1]
            if ct == "title":
                d["title"] = _clean(ch.text or "")
            elif ct == "link":
                d["link"] = (ch.text or "").strip()
            elif ct == "pubDate":
                d["pubDate"] = (ch.text or "").strip()
            elif ct == "source":
                d["outlet"] = _clean(ch.text or "")
        title, outlet = d.get("title", ""), d.get("outlet", "")
        if outlet and title.endswith(f" - {outlet}"):
            title = title[: -(len(outlet) + 3)].strip()
        if title:
            out.append({"title": title, "link": d.get("link", ""), "outlet": outlet, "pubDate": d.get("pubDate", "")})
    return out


def fetch_member(name: str, days: int, timeout: int = 20) -> list[dict]:
    r = requests.get(gn_search_url(name, days), headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return parse_gn_items(r.content)


def rows_for_member(m: dict, items: list[dict], fetched_at: datetime) -> list[dict]:
    """Turn one member's search hits into output rows. Google already matched the article to the
    quoted name, so this only sets ``match_in_title`` and drops the rare loosely-related result that
    names neither the full name nor the surname in the headline AND has no link (Google drift)."""
    full = norm(m.get("full_name") or "")
    surname = norm(m.get("last_name") or (full.split()[-1] if full else ""))
    out = []
    for it in items:
        ntitle = f" {norm(it['title'])} "
        in_title = bool(full and f" {full} " in ntitle)
        if not in_title and surname and f" {surname} " not in ntitle and not it.get("link"):
            continue
        link = it.get("link", "")
        out.append(
            {
                "article_id": hashlib.sha1((link or it["title"]).encode("utf-8")).hexdigest()[:16],
                "unique_member_code": m["unique_member_code"],
                "matched_name": m.get("full_name") or m["unique_member_code"],
                "party": m.get("party"),
                "constituency": m.get("constituency_name"),
                "house": m.get("house", "Dail"),
                "is_current": bool(m.get("is_current", True)),
                "outlet": it.get("outlet") or "Google News",
                "outlet_tier": source_tier(it.get("outlet", "")),
                "article_title": it["title"],
                "article_url": link,
                "published_at": (_parse_date(it.get("pubDate", "")) or fetched_at).astimezone(UTC).replace(tzinfo=None),
                "match_in_title": in_title,
                "fetched_at": fetched_at.replace(tzinfo=None),
            }
        )
    return out


def accumulate(new: pl.DataFrame) -> pl.DataFrame:
    """Append new rows to the existing silver parquet and dedup on (article_url, member), keeping the
    EARLIEST fetched copy so first-seen dates stay stable across refreshes."""
    if OUT.exists():
        try:
            new = pl.concat([pl.read_parquet(OUT), new], how="diagonal_relaxed")
        except Exception as e:  # noqa: BLE001 — a corrupt parquet must not lose the new pull
            print(f"  WARN could not read existing parquet ({type(e).__name__}); writing fresh")
    if new.height:
        new = new.sort("fetched_at").unique(subset=["article_url", "unique_member_code"], keep="first")
    return new


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30, help="recency window (Google 'when:Nd')")
    ap.add_argument("--limit", type=int, default=0, help="only the first N members (smoke test)")
    ap.add_argument("--current-only", action="store_true", help="sitting members only")
    ap.add_argument("--delay", type=float, default=1.3, help="seconds between requests (politeness)")
    args = ap.parse_args()

    members = load_members()
    if args.current_only:
        members = [m for m in members if m.get("is_current")]
    seen, uniq = set(), []
    for m in members:
        c = m["unique_member_code"]
        if c in seen or not (m.get("full_name") or (m.get("first_name") and m.get("last_name"))):
            continue
        seen.add(c)
        uniq.append(m)
    if args.limit:
        uniq = uniq[: args.limit]
    if not uniq:
        raise SystemExit("no members loaded — is the silver historic roster built?")

    print(f"Searching {len(uniq)} members · window={args.days}d · delay={args.delay}s")
    fetched_at = datetime.now(UTC)
    rows: list[dict] = []
    ok = fail = empty = 0
    for i, m in enumerate(uniq, 1):
        name = m.get("full_name") or f"{m['first_name']} {m['last_name']}"
        try:
            mrows = rows_for_member(m, fetch_member(name, args.days), fetched_at)
            rows.extend(mrows)
            ok += 1
            empty += not mrows
        except Exception as e:  # noqa: BLE001 — one bad member must not kill the run
            fail += 1
            print(f"  FAIL {name}: {type(e).__name__}")
        if i % 25 == 0:
            print(f"  ...{i}/{len(uniq)}  rows so far={len(rows)}  (ok={ok} empty={empty} fail={fail})")
        time.sleep(args.delay)

    new = pl.DataFrame(rows) if rows else pl.DataFrame()
    merged = accumulate(new)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(merged, OUT)
    nm = new["unique_member_code"].n_unique() if new.height else 0
    tot = merged["unique_member_code"].n_unique() if merged.height else 0
    print(f"\nthis run: {new.height} rows / {nm} members (searched {len(uniq)}, {empty} empty, {fail} failed)")
    print(
        f"accumulated: {merged.height} rows / {tot} members / "
        f"{merged['article_id'].n_unique() if merged.height else 0} articles -> {OUT}"
    )


if __name__ == "__main__":
    main()
