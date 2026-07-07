"""P0-5 — OIC / FOI decisions (SANDBOX).

Scrapes the Office of the Information Commissioner decisions DB
(oic.ie/en/decisions/, paginated). Open public record. One row per decision.

CAVEAT baked into provenance: an FOI access dispute/outcome is a finding about
*access to records*, never proof of wrongdoing in the underlying matter.
"""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import ROOT, fetch, now_iso, write_silver  # noqa: E402

BASE = "https://www.oic.ie/en/decisions/"
MAX_PAGES = 345  # ~3,434 decisions at ~10/page; per-page try/except keeps partials


def _field(li, label: str) -> str | None:
    for p in li.find_all("p"):
        strong = p.find("strong")
        if strong and strong.get_text(strip=True).rstrip(".:").strip().lower() == label.lower():
            # text after the <strong> label
            txt = p.get_text(" ", strip=True)
            return txt.split(":", 1)[1].strip() if ":" in txt else txt
    return None


def parse_page(html: str, page: int) -> list[dict]:
    s = BeautifulSoup(html, "html.parser")
    ul = s.find("ul", attrs={"reboot-site-list": True})
    if ul is None:
        return []
    out = []
    for li in ul.find_all("li", recursive=False):
        h = li.find("h4")
        a = h.find("a", href=True) if h else None
        title = a.get_text(" ", strip=True) if a else (h.get_text(" ", strip=True) if h else None)
        href = a["href"] if a else None
        date_raw = _field(li, "Date")
        # DD-MM-YYYY -> ISO
        date_iso = None
        if date_raw and len(date_raw) == 10 and date_raw[2] == "-":
            d, m, y = date_raw.split("-")
            date_iso = f"{y}-{m}-{d}"
        sections = _field(li, "Section of the Act")
        out.append({
            "title": title,
            "case_reference": _field(li, "Case Number"),
            "summary": _field(li, "Summary"),
            "decision_date": date_iso,
            "decision_date_raw": date_raw,
            "public_body": _field(li, "Public Body"),
            "foi_sections": sections,
            "source_url": f"https://www.oic.ie{href}" if href else None,
            "list_page": page,
            "fetched_at": now_iso(),
            "extraction_method": "html_scrape",
            "confidence": "high",
            "privacy_tier": "public",
            "caveat": "FOI access finding; not proof of underlying wrongdoing",
        })
    return out


def run(max_pages: int = MAX_PAGES) -> None:
    rows: list[dict] = []
    pages_ok = 0
    for page in range(1, max_pages + 1):
        try:
            html, _meta = fetch(BASE, params={"page": page})
        except Exception as e:  # noqa: BLE001 — keep partials on a flaky page
            print(f"  page {page}: fetch error {type(e).__name__}: {e}")
            continue
        batch = parse_page(html, page)
        if not batch:
            print(f"  page {page}: 0 results — stopping (end of list)")
            break
        rows.extend(batch)
        pages_ok += 1
        if page % 25 == 0 or page == 1:
            print(f"  page {page}: +{len(batch)} (running {len(rows)})")

    df = pl.DataFrame(rows)
    # dedupe on case_reference (a case can repeat across re-paginated runs)
    if "case_reference" in df.columns:
        df = df.unique(subset=["case_reference"], keep="first")
    out = write_silver("oic_foi_decisions", df)
    print(f"\nSILVER: {out}  rows={df.height}  pages_ok={pages_ok}")
    if df.height:
        print(f"  date range: {df['decision_date'].min()} … {df['decision_date'].max()}")
        top = df.group_by("public_body").len().sort("len", descending=True).head(8)
        print("  top public bodies:")
        for r in top.to_dicts():
            print(f"    {r['len']:>4}  {r['public_body']}")
        (ROOT / "oic_sample.txt").write_text(
            "\n".join(f"{r['decision_date']} | {r['case_reference']} | {r['public_body']} | {r['title']}"
                      for r in df.head(15).to_dicts()), encoding="utf-8")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else MAX_PAGES
    run(n)
