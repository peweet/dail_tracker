"""eTenders.gov.ie (Eurodyn EPPS) -> LIVE national tender pipeline -> SILVER parquet.

The forward-looking PLANNED-tier lane the OGP quarterly open-data CSV and TED (EU-threshold only) cannot
give us: currently-open national tenders incl. SUB-EU-THRESHOLD contracts (schools, councils, water schemes).
Promoted from pipeline_sandbox/etenders_live_tenders_extract.py after the probe proved the grid is liftable.

Playwright is REQUIRED (the grid is JS-rendered; curl/RSS return nothing). Writes:
    data/silver/parquet/etenders_live_tenders.parquet  (+ a coverage JSON)

It is its OWN silver fact — it does NOT touch procurement_awards / procurement_payments_fact / the consolidate.
NOT in the standard pipeline.py sequence (needs a browser + is a point-in-time snapshot); refreshed by the
poll wrapper tools/poll_live_tenders.ps1 instead. The view sql_views/procurement/procurement_live_tenders.sql
filters this to genuinely-open tenders.

VALUE SEMANTICS — every estimated value is a BUYER ESTIMATE at the PLANNED (pre-award) lifecycle stage:
realisation_tier='PLANNED', value_kind='estimate_advertised', value_safe_to_sum=FALSE. A NEW tier EARLIER
than AWARDED — NEVER summed with eTenders/TED awards or with payments.

Feeds (tagged in column `feed`): cft = Latest CfTs (open opportunities) · notice = Latest Notices.
POLITENESS / ToU: public procurement record (aggregators like Stotles/OpenOpps scrape the same); ToU
unconfirmed — research use, low rate (delay per page), bounded pages.

Run:
    ./.venv/Scripts/python.exe extractors/etenders_live_tenders_extract.py                 # both feeds
    ./.venv/Scripts/python.exe extractors/etenders_live_tenders_extract.py --max-pages 120 --only cft
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urljoin

import polars as pl
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")
from services.logging_setup import setup_standalone_logging  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

log = logging.getLogger(__name__)

BASE = "https://www.etenders.gov.ie"
FEEDS = {
    "cft": "/epps/prepareCurrentOpportunities.do?currentType=cft",
    "notice": "/epps/prepareCurrentOpportunities.do?currentType=notice",
}
OUT_SILVER = ROOT / "data/silver/parquet/etenders_live_tenders.parquet"
OUT_COV = ROOT / "data/_meta/etenders_live_tenders_coverage.json"
UA = "Mozilla/5.0 (dail-tracker civic-research; +https://github.com/)"
PARSER_VERSION = "0.1.0"

SOURCE = {
    "dataset": "eTenders current opportunities (Calls for Tender + Notices)",
    "publisher": "Office of Government Procurement (OGP)",
    "platform": "etenders.gov.ie (European Dynamics EPPS)",
    "landing_page": "https://www.etenders.gov.ie/epps/prepareCurrentOpportunities.do?currentType=cft",
    "access": "Playwright (public tender list is JS-rendered)",
    "license": "Public procurement record; OGP open data is CC-BY 4.0. Live-portal ToU unconfirmed — research use.",
}

COLMAP = [
    ("title", re.compile(r"title|tender name|name", re.I)),
    ("resource_id", re.compile(r"resource id|^id$|system id", re.I)),
    ("buyer", re.compile(r"\bca\b|authority|contracting", re.I)),
    ("published", re.compile(r"publi", re.I)),
    ("deadline", re.compile(r"deadline|submission", re.I)),
    ("procedure", re.compile(r"procedure", re.I)),
    ("status", re.compile(r"status", re.I)),
    ("estimated_value_eur", re.compile(r"estimat|value", re.I)),
    ("award_date", re.compile(r"award", re.I)),
]
_MONTHS = "Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec".split()

# eTenders appends an internal organisation ID to the buyer name ("Cork County Council_424") and,
# for schools, a roll number ("Scoil Ailbhe - (18030I)"). Both are identifiers, NOT part of the
# display name. We lift the org ID into its own column (a stable per-buyer join key) and strip both
# off the name. A parenthetical that does NOT start with a digit ("…Authority (HIQA)", "School
# (Navan)") is a real acronym/place-name and is preserved — the digit-leading test is what tells an
# ID apart from a name.
_BUYER_ORG_ID_RX = r"_(\d+)$"  # trailing _<digits> = the eTenders org id
_BUYER_ROLL_RX = r"\s*[-–]\s*\(\d[0-9A-Za-z]*\)$"  # trailing " - (<roll-number>)"


def _clean_buyer(df: pl.DataFrame) -> pl.DataFrame:
    """Split the eTenders org-id suffix off the buyer name into ``buyer_org_id`` and strip
    identifier debris (org id + school roll number) from the display ``buyer``. Idempotent —
    safe to re-run on an already-cleaned snapshot (the patterns simply no longer match)."""
    if "buyer" not in df.columns:
        return df
    return df.with_columns(
        pl.col("buyer").str.extract(_BUYER_ORG_ID_RX, 1).alias("buyer_org_id"),
    ).with_columns(
        pl.col("buyer")
        .str.replace(_BUYER_ORG_ID_RX, "")
        .str.replace(_BUYER_ROLL_RX, "")
        .str.strip_chars()
        .alias("buyer"),
    )


def _parse_dt(raw: str | None) -> str | None:
    if not raw:
        return None
    m = re.search(r"([A-Z][a-z]{2}) (\d{1,2}) .*?(\d{4})", raw)
    if not m:
        return None
    with contextlib.suppress(Exception):
        return f"{int(m.group(3)):04d}-{_MONTHS.index(m.group(1)) + 1:02d}-{int(m.group(2)):02d}"
    return None


def _header_index(page) -> dict[str, int]:
    hdr = page.eval_on_selector_all(
        "table tr:first-child th, table tr:first-child td",
        "els => els.map(e => (e.innerText||'').trim())",
    )
    idx: dict[str, int] = {}
    for i, h in enumerate(hdr):
        for name, rx in COLMAP:
            if name not in idx and rx.search(h):
                idx[name] = i
    return idx


def _rows(page) -> list[dict]:
    return page.eval_on_selector_all(
        "table tr",
        "trs => trs.map(tr => {"
        "  const c = Array.from(tr.querySelectorAll('td')).map(x => (x.innerText||'').trim());"
        "  const a = tr.querySelector('a[href*=prepareViewCfTWS], a[href*=prepareViewNotice], a[href*=resourceId]');"
        "  return {cells: c, href: a ? a.getAttribute('href') : null};"
        "}).filter(r => r.cells.length >= 6);",
    )


def _scrape_feed(page, feed: str, max_pages: int, delay_ms: int) -> list[dict]:
    page.goto(BASE + FEEDS[feed], wait_until="networkidle")
    page.wait_for_timeout(2500)
    idx = _header_index(page)
    out: list[dict] = []
    p = 1
    while p <= max_pages:
        got = 0
        for r in _rows(page):
            c = r["cells"]

            def cell(key: str, _c=c) -> str | None:
                i = idx.get(key)
                return _c[i] if i is not None and i < len(_c) else None

            title = cell("title")
            if not title:
                continue
            evf = None
            ev = cell("estimated_value_eur")
            if ev:
                mm = re.search(r"[\d,]+\.?\d*", ev.replace(",", ""))
                if mm:
                    with contextlib.suppress(ValueError):
                        evf = float(mm.group())
            out.append(
                {
                    "feed": feed,
                    "resource_id": cell("resource_id"),
                    "title": title,
                    "buyer": cell("buyer"),
                    "published_raw": cell("published"),
                    "published_date": _parse_dt(cell("published")),
                    "deadline_raw": cell("deadline"),
                    "deadline_date": _parse_dt(cell("deadline")),
                    "procedure": cell("procedure"),
                    "status": cell("status"),
                    "estimated_value_eur": evf,
                    "detail_url": urljoin(page.url, r["href"]) if r["href"] else None,
                }
            )
            got += 1
        log.info("[%s] page %d: %d rows (cumulative %d)", feed, p, got, len(out))
        nxt = page.query_selector(f"a[href*='-p={p + 1}&']")
        if not nxt or got == 0:
            break
        page.goto(urljoin(page.url, nxt.get_attribute("href")), wait_until="networkidle")
        page.wait_for_timeout(delay_ms)
        p += 1
    return out


def main() -> None:
    setup_standalone_logging("etenders_live_tenders_extract")
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=120, help="page cap per feed (politeness)")
    ap.add_argument("--only", default="", help="cft | notice (default both)")
    ap.add_argument("--delay-ms", type=int, default=1000)
    args = ap.parse_args()
    feeds = [args.only] if args.only in FEEDS else list(FEEDS)

    rows: list[dict] = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        page = browser.new_page(user_agent=UA)
        page.set_default_timeout(45_000)
        for feed in feeds:
            log.info("feed: %s", feed)
            rows.extend(_scrape_feed(page, feed, args.max_pages, args.delay_ms))
        browser.close()

    if not rows:
        log.warning("no rows scraped — leaving existing silver untouched")
        return
    now = datetime.now(UTC).strftime("%Y-%m-%d")
    df = pl.DataFrame(rows, infer_schema_length=None).with_columns(
        pl.lit("etenders.gov.ie current opportunities").alias("source"),
        pl.lit(now).alias("retrieved_utc"),
        pl.lit(PARSER_VERSION).alias("parser_version"),
        pl.lit("PLANNED").alias("realisation_tier"),
        pl.lit("estimate_advertised").alias("value_kind"),
        pl.lit(False).alias("value_safe_to_sum"),
    )
    df = _clean_buyer(df)
    df = df.unique(subset=["feed", "resource_id", "title"], keep="first", maintain_order=True)

    OUT_SILVER.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, OUT_SILVER)
    log.info("SILVER WRITTEN: %d live tenders -> %s", df.height, OUT_SILVER)
    print(f"SILVER WRITTEN: {df.height:,} live tenders -> {OUT_SILVER}")

    cov = {
        "generated_utc": datetime.now(UTC).isoformat(),
        "layer": "silver",
        "source": SOURCE,
        "n_rows": df.height,
        "by_feed": {r["feed"]: r["len"] for r in df.group_by("feed").len().iter_rows(named=True)},
        "rows_with_estimated_value": int(df["estimated_value_eur"].is_not_null().sum()),
        "value_note": "estimated_value_eur is a BUYER ESTIMATE at PLANNED stage (value_kind=estimate_advertised, "
        "realisation_tier=PLANNED, value_safe_to_sum=FALSE) — a NEW lifecycle tier before AWARDED; NEVER summed.",
        "caveat": "Live national pipeline (incl. sub-EU-threshold). The cft feed also lists already-closed + "
        "DPS/Qualification-System records back to 2023; the consuming view filters to genuinely-open. Snapshot at "
        "retrieved_utc — refresh via tools/poll_live_tenders.ps1. ToU unconfirmed — research use.",
        "parser_version": PARSER_VERSION,
    }
    OUT_COV.write_text(json.dumps(cov, indent=2), encoding="utf-8")
    print(f"coverage -> {OUT_COV}")


if __name__ == "__main__":
    main()
