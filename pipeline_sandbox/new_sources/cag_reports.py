"""P0-1 — C&AG audit reports index (SANDBOX).

Walks the three Comptroller & Auditor General publication categories on
audit.gov.ie, captures the report index (type, number, title, URL), then
enriches each with publication date + primary PDF link from the detail page.

Open public record. Index-level only here; deep findings/cost extraction from
the dense PDFs is a follow-up (see NEW_SOURCE_INGESTION_PLAN §6 P0-1).
Money figures are NEVER summed; any cost captured is value_safe_to_sum=False.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import ROOT, fetch, now_iso, write_silver  # noqa: E402

BASE = "https://www.audit.gov.ie"
CATEGORIES = {
    "special_report": "/en/publications/special-reports/",
    "report_on_accounts": "/en/publications/report-on-the-accounts-of-the-public-services/",
    "appropriation_accounts": "/en/publications/appropriation-accounts/",
}
MAX_PAGES = 25
MAX_DETAIL = 200  # bound the per-report detail fetches


def collect_index() -> list[dict]:
    rows: list[dict] = []
    seen: set[str] = set()
    for rtype, path in CATEGORIES.items():
        cat_slug = path.strip("/").split("/")[-1]
        for page in range(1, MAX_PAGES + 1):
            try:
                html, _m = fetch(BASE + path, params={"pageNumber": page} if page > 1 else None)
            except Exception as e:  # noqa: BLE001
                print(f"  {rtype} p{page}: {type(e).__name__} {e}")
                break
            s = BeautifulSoup(html, "html.parser")
            new = 0
            for a in s.find_all("a", href=True):
                h = a["href"]
                if f"/publications/{cat_slug}/" in h and h.rstrip("/").split("/")[-1] != cat_slug:
                    url = BASE + h if h.startswith("/") else h
                    if url in seen:
                        continue
                    seen.add(url)
                    title = a.get_text(" ", strip=True)
                    if not title or len(title) < 5:
                        continue
                    num = re.search(r"(?:Special\s+)?Report\s+(\d+)", title)
                    rows.append({
                        "report_type": rtype,
                        "report_number": int(num.group(1)) if num else None,
                        "title": title,
                        "source_url": url,
                        "list_page": page,
                    })
                    new += 1
            if new == 0:
                break
            print(f"  {rtype} p{page}: +{new} (running {len(rows)})")
    return rows


def enrich_detail(rows: list[dict]) -> None:
    for r in rows[:MAX_DETAIL]:
        try:
            html, meta = fetch(r["source_url"])
        except Exception as e:  # noqa: BLE001
            r["detail_error"] = f"{type(e).__name__}"
            continue
        s = BeautifulSoup(html, "html.parser")
        text = s.get_text(" ", strip=True)
        d = re.search(r"\b(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+20\d\d)\b", text)
        r["source_published_date"] = d.group(1) if d else None
        pdf = next((a["href"] for a in s.find_all("a", href=True) if a["href"].lower().endswith(".pdf")), None)
        r["pdf_url"] = (BASE + pdf if pdf and pdf.startswith("/") else pdf)
        r["source_last_modified"] = meta.get("source_last_modified")


def run() -> None:
    rows = collect_index()
    if not rows:
        print("No reports collected (site structure may have changed).")
        return
    print(f"\nEnriching detail for up to {MAX_DETAIL} reports…")
    enrich_detail(rows)
    fetched = now_iso()
    for r in rows:
        r.update({
            "fetched_at": fetched,
            "extraction_method": "html_scrape",
            "confidence": "high",
            "privacy_tier": "public",
            "value_safe_to_sum": False,
        })
    df = pl.DataFrame(rows)
    out = write_silver("cag_reports", df)
    print(f"\nSILVER: {out}  rows={df.height}")
    by_type = df.group_by("report_type").len().sort("len", descending=True)
    for t in by_type.to_dicts():
        print(f"  {t['len']:>4}  {t['report_type']}")
    sr = df.filter(pl.col("report_type") == "special_report").sort("report_number", descending=True)
    (ROOT / "cag_sample.txt").write_text(
        "\n".join(f"{r['report_number']} | {r.get('source_published_date')} | {r['title']}"
                  for r in sr.head(20).to_dicts()), encoding="utf-8")
    print("  latest special reports:")
    for r in sr.head(6).to_dicts():
        print(f"    SR{r['report_number']} ({r.get('source_published_date')}): {r['title'][:60]}")


if __name__ == "__main__":
    run()
