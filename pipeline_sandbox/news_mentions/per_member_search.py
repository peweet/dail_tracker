"""Per-member news-search extractor — the high-coverage successor to ``extract.py``.

WHY: ``extract.py`` scans ~41 general RSS feeds' HEADLINES and only catches whoever happens
to be named in a current headline → ~6% of members (30/475). This flips the model: it issues
ONE Google-News RSS *search per member* (the exact quoted name, Irish locale, recent window),
so EVERY member is queried by name and the article need only contain the name (Google matches
the body, not just the headline). Result: coverage jumps from "who's in a headline" to "every
member with any recent coverage".

It also ACCUMULATES: each run appends to the sandbox parquet and dedups on
(article_url, unique_member_code), so the corpus grows over time instead of resetting to the
current RSS window (the snapshot limitation of extract.py).

Writes the SAME schema + SAME file (news_mentions_sandbox.parquet) the demo/UI reads, so it is
a drop-in data upgrade. All matching logic lives here (logic-firewall clean).

CAVEAT (common names): a quoted search for "John Murphy" can return any John Murphy. The Irish
locale + the recent window reduce this; a future pass can add a party/constituency boost term.
Each row is a *name match*, not an assertion the article is about this politician.

Run:
  python pipeline_sandbox/news_mentions/per_member_search.py --limit 8          # smoke test
  python pipeline_sandbox/news_mentions/per_member_search.py --days 30          # full, last 30d
  python pipeline_sandbox/news_mentions/per_member_search.py --current-only     # sitting members only
"""
from __future__ import annotations

import argparse
import hashlib
import sys
import time
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from extract import _clean, _parse_date, load_members, norm  # noqa: E402

OUT = Path(__file__).resolve().parent / "news_mentions_sandbox.parquet"

# Google News RSS search endpoint (public, no key). Irish edition so results bias to IE outlets.
GN_BASE = "https://news.google.com/rss/search"
GN_PARAMS = {"hl": "en-IE", "gl": "IE", "ceid": "IE:en"}
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/rss+xml, application/xml, text/xml, */*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
}

# Map a publisher (Google-News <source>) to the demo's tier palette; default 'national'. The
# source name is matched case-insensitively on a substring, so "The Irish Times" → national.
_TIER_BY_SOURCE = {
    "national": ("rté", "rte", "irish times", "irish independent", "independent.ie", "the journal",
                 "thejournal", "breakingnews", "irish examiner", "business post", "newstalk",
                 "rte.ie", "the irish times", "extra.ie", "irish mirror", "irish daily"),
    "specialist": ("agriland", "thedetail", "thecurrency", "the currency", "law society", "silicon",
                   "med", "farmers journal", "construction"),
    "local_radio": ("fm", "radio", "highland", "midwest", "ocean fm", "tipp fm", "kfm", "c103",
                    "live 95", "red fm", "shannonside", "northern sound"),
    "local_paper": ("echo", "nationalist", "leader", "post", "people", "advertiser", "champion",
                    "star", "herald", "chronicle", "observer", "gazette", "journal", "tribune",
                    "democrat", "examiner", "courier", "today", "connaught", "mayo", "kerryman"),
}


def gn_search_url(name: str, days: int) -> str:
    """Build the Google-News RSS search URL for an exact-quoted member name + recency window."""
    q = f'"{name}" when:{days}d'
    return f"{GN_BASE}?{urllib.parse.urlencode({'q': q, **GN_PARAMS})}"


def _source_tier(outlet: str) -> str:
    o = (outlet or "").lower()
    for tier, needles in _TIER_BY_SOURCE.items():
        if any(n in o for n in needles):
            return tier
    return "national"


def parse_gn_items(content: bytes) -> list[dict]:
    """Parse a Google-News RSS search response into {title, link, outlet, pubDate} dicts.

    GN item title is "Headline - Source"; the <source> element carries the clean outlet name, so
    the trailing " - Source" is stripped from the displayed title. Pure → unit-tested.
    """
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
        title = d.get("title", "")
        outlet = d.get("outlet", "")
        if outlet and title.endswith(f" - {outlet}"):
            title = title[: -(len(outlet) + 3)].strip()
        if title:
            out.append({"title": title, "link": d.get("link", ""), "outlet": outlet,
                        "pubDate": d.get("pubDate", "")})
    return out


def fetch_member(name: str, days: int, timeout: int = 20) -> list[dict]:
    r = requests.get(gn_search_url(name, days), headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return parse_gn_items(r.content)


def _rows_for_member(m: dict, items: list[dict], fetched_at: datetime) -> list[dict]:
    """Turn one member's search hits into output rows. Keeps an item only if the member's
    normalised full name appears in the (normalised) headline OR the query name itself — Google
    already matched the article to the quoted name, so this only sets match_in_title and trims
    the rare loosely-related result that doesn't mention the name at all."""
    full = norm(m.get("full_name") or "")
    surname = norm(m.get("last_name") or (full.split()[-1] if full else ""))
    rows = []
    for it in items:
        ntitle = f" {norm(it['title'])} "
        in_title = bool(full and f" {full} " in ntitle)
        # Drop a result that mentions neither the full name nor the surname in the headline AND has
        # no usable link — almost always Google topic-drift; everything else is retained.
        if not in_title and surname and f" {surname} " not in ntitle and not it.get("link"):
            continue
        link = it.get("link", "")
        rows.append({
            "article_id": hashlib.sha1((link or it["title"]).encode("utf-8")).hexdigest()[:16],
            "unique_member_code": m["unique_member_code"],
            "matched_name": m.get("full_name") or m["unique_member_code"],
            "party": m.get("party"),
            "constituency": m.get("constituency_name"),
            "house": m.get("house", "Dail"),
            "is_current": bool(m.get("is_current", True)),
            "outlet": it.get("outlet") or "Google News",
            "outlet_tier": _source_tier(it.get("outlet", "")),
            "article_title": it["title"],
            "article_url": link,
            "published_at": (_parse_date(it.get("pubDate", "")) or fetched_at).astimezone(UTC).replace(tzinfo=None),
            "match_in_title": in_title,
            "fetched_at": fetched_at.replace(tzinfo=None),
        })
    return rows


def _accumulate(new: pl.DataFrame) -> pl.DataFrame:
    """Append new rows to the existing sandbox parquet and dedup on (article_url, member),
    keeping the EARLIEST fetched copy so first-seen dates stay stable."""
    if OUT.exists():
        try:
            old = pl.read_parquet(OUT)
            new = pl.concat([old, new], how="diagonal_relaxed")
        except Exception as e:  # noqa: BLE001 — a corrupt/old parquet must not lose the new pull
            print(f"  WARN could not read existing parquet ({type(e).__name__}); writing fresh")
    if new.height:
        new = new.sort("fetched_at").unique(subset=["article_url", "unique_member_code"], keep="first")
    return new


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30, help="recency window (Google 'when:Nd')")
    ap.add_argument("--limit", type=int, default=0, help="only the first N members (smoke test)")
    ap.add_argument("--current-only", action="store_true", help="sitting members only")
    ap.add_argument("--delay", type=float, default=1.2, help="seconds between requests (politeness)")
    args = ap.parse_args()

    members = load_members()
    if args.current_only:
        members = [m for m in members if m.get("is_current")]
    # de-dup members by code (the roster can carry a member twice) and require a usable name
    seen, uniq = set(), []
    for m in members:
        c = m["unique_member_code"]
        if c in seen or not (m.get("full_name") or (m.get("first_name") and m.get("last_name"))):
            continue
        seen.add(c)
        uniq.append(m)
    if args.limit:
        uniq = uniq[: args.limit]

    print(f"Searching {len(uniq)} members · window={args.days}d · delay={args.delay}s")
    fetched_at = datetime.now(UTC)
    rows: list[dict] = []
    ok = fail = empty = 0
    for i, m in enumerate(uniq, 1):
        name = m.get("full_name") or f"{m['first_name']} {m['last_name']}"
        try:
            items = fetch_member(name, args.days)
            mrows = _rows_for_member(m, items, fetched_at)
            rows.extend(mrows)
            ok += 1
            if not mrows:
                empty += 1
        except Exception as e:  # noqa: BLE001 — one bad member must not kill the run
            fail += 1
            print(f"  FAIL {name}: {type(e).__name__}")
        if i % 25 == 0:
            print(f"  ...{i}/{len(uniq)}  rows so far={len(rows)}  (ok={ok} empty={empty} fail={fail})")
        time.sleep(args.delay)

    new = pl.DataFrame(rows) if rows else pl.DataFrame()
    merged = _accumulate(new)
    merged.write_parquet(OUT, compression="zstd", compression_level=3, statistics=True)

    new_members = new["unique_member_code"].n_unique() if new.height else 0
    tot_members = merged["unique_member_code"].n_unique() if merged.height else 0
    print(f"\nthis run: {new.height} rows / {new_members} members "
          f"(searched {len(uniq)}, {empty} had no hits, {fail} failed)")
    print(f"accumulated parquet: {merged.height} rows / {tot_members} members / "
          f"{merged['article_id'].n_unique() if merged.height else 0} articles -> {OUT}")


if __name__ == "__main__":
    main()
