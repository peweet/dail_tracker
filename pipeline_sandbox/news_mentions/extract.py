"""Sandbox extractor for 'recent media mentions'.

Fetches the curated feed registry, parses items, and matches Oireachtas members
(current AND former) by a CONSERVATIVE first+last adjacency rule (all matching
logic lives HERE, not in the UI -> logic-firewall clean). Writes one row per
(article x matched member) to a sandbox parquet.

Former members are deliberately included: the historic roster
(pipeline_sandbox/historic_members/_out) is the primary source so ex-TDs/Senators
still in the news are surfaced (each row carries `is_current`). Falls back to the
live sitting-only rosters if that roster hasn't been built yet.

Run:  python pipeline_sandbox/news_mentions/extract.py
"""
from __future__ import annotations
import sys, io, re, html, hashlib, unicodedata
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests
import polars as pl
from dateutil import parser as dateparser

sys.path.insert(0, str(Path(__file__).resolve().parent))
from feeds import FEEDS  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent / "news_mentions_sandbox.parquet"
# Historic roster (current + former members, all terms back to 2011). Primary
# source so former politicians are examined too; see historic_members sandbox.
HISTORIC_DIR = ROOT / "pipeline_sandbox/historic_members/_out"
HISTORIC_ROSTER = HISTORIC_DIR / "member_roster_wide.parquet"
HISTORIC_TERMS = HISTORIC_DIR / "member_terms.parquet"
# Fallback: live, sitting-only rosters (used if the historic roster isn't built).
MEMBER_PATHS = [
    (ROOT / "data/silver/parquet/flattened_members.parquet", "Dail"),
    (ROOT / "data/silver/parquet/flattened_seanad_members.parquet", "Seanad"),
]
# request gzip/deflate only -> avoids Brotli (brotli pkg not installed) on Reach feeds
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "application/rss+xml, application/xml, text/xml, */*;q=0.8",
    "Accept-Encoding": "gzip, deflate",
}


def strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def norm(s: str) -> str:
    s = html.unescape(s or "")
    s = strip_accents(s).lower()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


# ----------------------------- members ------------------------------------
def _house_map() -> dict[str, str]:
    """code -> house ('Dail'/'Seanad') from the most-recent term in the sidecar.

    The wide roster dedups to a member's most-recent term but drops the house tag;
    the terms sidecar carries it, so map by the latest membership_start_date.
    """
    terms = pl.read_parquet(HISTORIC_TERMS)
    latest = (terms.sort("membership_start_date")
                   .group_by("unique_member_code")
                   .agg(pl.col("house_tag").last()))
    return {c: ("Seanad" if h == "seanad" else "Dail")
            for c, h in zip(latest["unique_member_code"], latest["house_tag"])}


def load_members() -> list[dict]:
    rows = []
    if HISTORIC_ROSTER.exists():
        # Primary path: current + former members from the historic roster.
        hmap = _house_map() if HISTORIC_TERMS.exists() else {}
        df = pl.read_parquet(HISTORIC_ROSTER)
        cols = [c for c in ["unique_member_code", "first_name", "last_name", "full_name",
                            "party", "constituency_name", "is_current"] if c in df.columns]
        df = df.select(cols).unique()
        for r in df.iter_rows(named=True):
            r["house"] = hmap.get(r["unique_member_code"], "Dail")
            r["is_current"] = bool(r.get("is_current"))
            rows.append(r)
        return rows
    # Fallback: live, sitting-only rosters.
    for path, house in MEMBER_PATHS:
        df = pl.read_parquet(path)
        cols = [c for c in ["unique_member_code", "first_name", "last_name", "full_name",
                            "party", "constituency_name"] if c in df.columns]
        df = df.select(cols).unique()
        for r in df.iter_rows(named=True):
            r["house"] = house
            r["is_current"] = True
            rows.append(r)
    return rows


def member_aliases(m: dict) -> set[str]:
    al = set()
    if m.get("first_name") and m.get("last_name"):
        al.add(norm(f"{m['first_name']} {m['last_name']}"))
    if m.get("full_name"):
        al.add(norm(m["full_name"]))
    return {a for a in al if a and len(a.split()) >= 2}


# ----------------------------- feeds --------------------------------------
ITEM_RE = re.compile(r"<(item|entry)\b.*?</\1>", re.S | re.I)
TAG_RE = {t: re.compile(rf"<{t}\b[^>]*>(.*?)</{t}>", re.S | re.I) for t in ("title", "description", "link", "pubDate")}


def _clean(x: str) -> str:
    x = re.sub(r"<!\[CDATA\[(.*?)\]\]>", r"\1", x or "", flags=re.S)
    return re.sub(r"<[^>]+>", " ", x).strip()


def _parse_date(raw: str):
    if not raw:
        return None
    try:
        return parsedate_to_datetime(raw)
    except Exception:
        pass
    try:
        return dateparser.parse(raw)
    except Exception:
        return None


def fetch_items(url: str):
    r = requests.get(url, timeout=20, headers=HEADERS)
    r.raise_for_status()
    items = []
    try:
        root = ET.fromstring(r.content)
        for it in root.iter():
            if it.tag.split("}")[-1] not in ("item", "entry"):
                continue
            d = {}
            for ch in it:
                ct = ch.tag.split("}")[-1]
                if ct == "link" and not (ch.text or "").strip():
                    d["link"] = ch.attrib.get("href", "")
                elif ct in ("title", "description", "summary", "link", "pubDate", "published", "updated"):
                    d[ct] = (ch.text or "").strip()
            items.append((
                _clean(d.get("title", "")),
                _clean(d.get("description") or d.get("summary") or ""),
                d.get("link", ""),
                d.get("pubDate") or d.get("published") or d.get("updated") or "",
            ))
        if items:
            return items
    except ET.ParseError:
        pass
    # regex fallback for malformed feeds
    for m in ITEM_RE.finditer(r.text):
        block = m.group(0)
        t = TAG_RE["title"].search(block)
        de = TAG_RE["description"].search(block)
        ln = TAG_RE["link"].search(block)
        pd_ = TAG_RE["pubDate"].search(block)
        items.append((_clean(t.group(1)) if t else "", _clean(de.group(1)) if de else "",
                      _clean(ln.group(1)) if ln else "", _clean(pd_.group(1)) if pd_ else ""))
    return items


# ----------------------------- main ---------------------------------------
def main():
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    members = load_members()
    amap: dict[str, dict] = {}
    for m in members:
        for a in member_aliases(m):
            amap.setdefault(a, m)
    aliases = sorted(amap, key=lambda a: -len(a))
    print(f"Members: {len(members)} | aliases: {len(amap)}")

    fetched_at = datetime.now(timezone.utc)
    rows, live, dead = [], 0, 0
    for f in FEEDS:
        try:
            items = fetch_items(f["url"])
            if len(items) < 1:
                dead += 1
                print(f"  EMPTY {f['name']}")
                continue
            live += 1
        except Exception as e:
            dead += 1
            print(f"  FAIL  {f['name']}: {type(e).__name__}")
            continue
        for title, desc, link, raw_date in items:
            nt, nb = norm(title), norm(title + " " + desc)
            bt, bb = " " + nt + " ", " " + nb + " "
            dt = _parse_date(raw_date)
            seen = set()
            for a in aliases:
                if " " + a + " " in bb:
                    m = amap[a]
                    if m["unique_member_code"] in seen:
                        continue
                    seen.add(m["unique_member_code"])
                    rows.append({
                        "article_id": hashlib.sha1((link or title).encode("utf-8")).hexdigest()[:16],
                        "unique_member_code": m["unique_member_code"],
                        "matched_name": m.get("full_name") or a,
                        "party": m.get("party"),
                        "constituency": m.get("constituency_name"),
                        "house": m["house"],
                        "outlet": f["name"],
                        "outlet_tier": f["tier"],
                        "article_title": title,
                        "article_url": link,
                        "published_at": dt.astimezone(timezone.utc).replace(tzinfo=None) if dt else None,
                        "match_in_title": (" " + a + " ") in bt,
                        "fetched_at": fetched_at.replace(tzinfo=None),
                    })

    df = pl.DataFrame(rows)
    if df.height:
        df = df.unique(subset=["article_url", "unique_member_code"], keep="first")
    df.write_parquet(OUT, compression="zstd", compression_level=3, statistics=True)

    print(f"\nLive feeds: {live}/{len(FEEDS)}  (empty/dead: {dead})")
    print(f"Match rows: {df.height}  | distinct members: {df['matched_name'].n_unique() if df.height else 0}"
          f"  | distinct articles: {df['article_id'].n_unique() if df.height else 0}")
    print(f"Wrote -> {OUT}")
    if df.height:
        top = (df.group_by("matched_name").len().sort("len", descending=True).head(12))
        print("\nTop matched members:")
        for name, c in top.iter_rows():
            print(f"  {c:3}  {name}")


if __name__ == "__main__":
    main()
