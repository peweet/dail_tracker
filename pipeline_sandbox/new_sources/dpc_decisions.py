"""P1 — Data Protection Commission decisions (SANDBOX).

Scrapes dataprotection.ie/en/dpc-guidance/decisions (server-rendered, paginated).
Open public record. One row per decision. Part of the proposed
`regulatory_enforcement_fact` (regulator='DPC').

CAVEAT: a GDPR decision is a finding about a specific data-protection matter;
not evidence of unrelated wrongdoing. Some decisions concern public bodies.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import ROOT, fetch, now_iso, write_silver  # noqa: E402

BASE = "https://www.dataprotection.ie/en/dpc-guidance/decisions"
MAX_PAGES = 60
_MONTHS = {m: f"{i:02d}" for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}


def _iso(date_raw: str | None) -> str | None:
    if not date_raw:
        return None
    m = re.match(r"(\d{1,2})\s+([A-Za-z]{3})[a-z]*\s+(20\d\d)", date_raw)
    if not m:
        return None
    d, mon, y = m.groups()
    return f"{y}-{_MONTHS.get(mon[:3], '01')}-{int(d):02d}"


def parse_page(html: str, page: int) -> list[dict]:
    s = BeautifulSoup(html, "html.parser")
    out = []
    for box in s.find_all("div", class_="faq-section-results-box"):
        h3 = box.find("h3")
        a = h3.find("a", href=True) if h3 else None
        if not a:
            continue
        dt = box.find("span", class_="datetime")
        tags = [x.get_text(strip=True) for x in box.select(".faq-section-category-link .item-list a")]
        arts = [x.get_text(strip=True) for x in box.select(".classArticles a")]
        summ = box.find("p")
        date_raw = dt.get_text(strip=True) if dt else None
        out.append({
            "regulator": "DPC",
            "title": a.get_text(" ", strip=True),
            "decision_date": _iso(date_raw),
            "decision_date_raw": date_raw,
            "sector_tags": ";".join(tags),
            "gdpr_articles": ";".join(arts),
            "summary": summ.get_text(" ", strip=True) if summ else None,
            "source_url": "https://www.dataprotection.ie" + a["href"],
            "list_page": page,
            "fetched_at": now_iso(),
            "extraction_method": "html_scrape",
            "confidence": "high",
            "privacy_tier": "public",
            "caveat": "GDPR finding on a specific matter; not proof of unrelated wrongdoing",
        })
    return out


def run(max_pages: int = MAX_PAGES) -> None:
    rows: list[dict] = []
    for page in range(0, max_pages):
        try:
            html, _m = fetch(BASE, params={"page": page} if page else None)
        except Exception as e:  # noqa: BLE001
            print(f"  page {page}: {type(e).__name__} {e}")
            break
        batch = parse_page(html, page)
        if not batch:
            print(f"  page {page}: 0 results — stopping")
            break
        rows.extend(batch)
        print(f"  page {page}: +{len(batch)} (running {len(rows)})")

    df = pl.DataFrame(rows).unique(subset=["source_url"], keep="first")
    out = write_silver("dpc_decisions", df)
    print(f"\nSILVER: {out}  rows={df.height}")
    if df.height:
        print(f"  date range: {df['decision_date'].min()} … {df['decision_date'].max()}")
        (ROOT / "dpc_sample.txt").write_text(
            "\n".join(f"{r['decision_date']} | arts:{r['gdpr_articles']} | {r['title']}"
                      for r in df.head(15).to_dicts()), encoding="utf-8")


if __name__ == "__main__":
    run()
