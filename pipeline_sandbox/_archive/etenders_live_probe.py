"""SANDBOX PROBE ONLY (no extractor, no parquet, no pipeline changes) — can the NEW eTenders
platform (etenders.gov.ie, Eurodyn EPPS) public tender search be read with Playwright: are the
current notices liftable (title + detail hyperlink + buyer + deadline + published date), and can
it be ordered newest-first?

Outputs to c:/tmp only: a screenshot + the extracted rows printed to stdout. Headless, one page,
generous timeout. Run: ./.venv/Scripts/python.exe pipeline_sandbox/etenders_live_probe.py
"""

from __future__ import annotations

import contextlib
import sys

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from playwright.sync_api import sync_playwright

CANDIDATES = [
    "https://www.etenders.gov.ie/epps/quickSearchAction.do",
    "https://www.etenders.gov.ie/epps/cft/searchContractNotices.do",
]
OUT_SHOT = "c:/tmp/etenders_live_probe.png"


def probe() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent="Mozilla/5.0 (dail-tracker research probe)")
        page.set_default_timeout(45_000)
        # NEW etenders.gov.ie — clean sample of REAL current opportunities (Open procedure, near-term).
        url = "https://www.etenders.gov.ie/epps/prepareCurrentOpportunities.do?currentType=cft"
        print(f"-> goto {url}")
        page.goto(url, wait_until="networkidle")
        page.wait_for_timeout(3500)
        print("   final url:", page.url)
        # structured rows: each data row's cells + the title-cell detail link
        recs = page.eval_on_selector_all(
            "table tr",
            "trs => trs.map(tr => {"
            "  const c = Array.from(tr.querySelectorAll('td')).map(x => (x.innerText||'').trim());"
            "  const a = tr.querySelector('a[href*=prepareViewCfTWS]');"
            "  return {cells: c, href: a ? a.getAttribute('href') : null};"
            "}).filter(r => r.cells.length >= 8);",
        )
        print(f"   current CfT rows on page 1: {len(recs)}")
        # columns: # Title ResourceId CA Info Published Deadline Procedure Status PDF Award Est Cycle
        import csv as _csv

        out = "c:/tmp/etenders_new_sample.csv"
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = _csv.writer(f)
            w.writerow(["title", "resource_id", "buyer", "published", "deadline", "procedure", "status", "est_value_eur", "detail_url"])
            for r in recs:
                c = r["cells"]
                w.writerow([c[1], c[2], c[3], c[5], c[6], c[7], c[8], c[11] if len(c) > 11 else "",
                            ("https://www.etenders.gov.ie" + r["href"]) if r["href"] else ""])
        print(f"   wrote sample -> {out}")
        print("\n   SAMPLE (title | buyer | published | deadline | procedure | est €):")
        for r in recs[:12]:
            c = r["cells"]
            ev = (c[11] if len(c) > 11 and c[11] else "—")
            print(f"     • {c[1][:42]:42} | {c[3][:26]:26} | {c[5][:11]} | {c[6][:11]} | {c[7][:18]:18} | {ev}")

        # structure dump: tables, and anchors that look like a CFT detail link
        n_tables = page.locator("table").count()
        cft_links = page.eval_on_selector_all(
            "a[href]",
            "els => els.map(e => ({t: (e.innerText||'').trim().slice(0,70), h: e.getAttribute('href')}))"
            ".filter(x => x.h && /cft|prepareView|resourceId|ContractNotice|viewWS/i.test(x.h)).slice(0,25)",
        )
        print(f"   tables on page: {n_tables}")
        print(f"   CFT-detail-looking links found: {len(cft_links)}")
        for x in cft_links[:12]:
            print(f"      [{x['t']!r}]  {x['h']}")

        # try to read the first results table as rows (header + first data rows)
        rows = page.eval_on_selector_all(
            "table tr",
            "trs => trs.slice(0,8).map(tr => Array.from(tr.querySelectorAll('th,td'))"
            ".map(c => (c.innerText||'').trim().slice(0,40)))",
        )
        print("\n   first table rows (header + sample):")
        for r in rows:
            if any(r):
                print("     |", " | ".join(r))

        page.screenshot(path=OUT_SHOT, full_page=False)
        print(f"\n   screenshot -> {OUT_SHOT}")
        browser.close()


if __name__ == "__main__":
    probe()
