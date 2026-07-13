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
from shared import buyer_clean  # noqa: E402

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
_MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# CPV division labels — leading 2 digits of the 8-digit CPV code -> a market label. COPIED from
# extractors/ted_ireland_extract.py (CPV_DIV) and kept in sync, so the national open-tender sector
# facet uses the SAME taxonomy as the TED tender pipeline (one vocabulary across both registers).
CPV_DIV = {
    "45": "Construction",
    "71": "Architecture/Engineering",
    "79": "Business/Consulting",
    "72": "IT services",
    "85": "Health/Social",
    "80": "Education",
    "90": "Environment/Waste",
    "50": "Repair/Maintenance",
    "48": "Software",
    "33": "Medical equipment",
    "34": "Transport equipment",
    "09": "Energy/Fuel",
    "73": "R&D",
    "55": "Hotel/Catering",
    "60": "Transport services",
    "92": "Recreation/Culture",
    "30": "Office/IT equipment",
    "98": "Other services",
    "70": "Real estate",
    "66": "Financial/Insurance",
}
# The detail page prints "CPV Codes: 45000000 - Construction work" — grab the first 8-digit code
# within a short window of the CPV label (avoids picking up an unrelated long number elsewhere).
_CPV_RE = re.compile(r"CPV[^\d]{0,60}(\d{8})", re.I)


def _cpv_from_text(text: str | None) -> tuple[str | None, str | None]:
    """Parse the first CPV code + its division label from a detail page's text. Returns
    (cpv_code, cpv_division) — (None, None) when no CPV is present (some notices carry none)."""
    if not text:
        return None, None
    m = _CPV_RE.search(text)
    if not m:
        return None, None
    code = m.group(1)
    return code, CPV_DIV.get(code[:2], "Other/Unknown")


def _detail_cpv(page, url: str, delay_ms: int) -> tuple[str | None, str | None]:
    """Open one tender's detail page and read its CPV. Defensive — a slow/failed page yields
    (None, None) rather than aborting the run (the row simply carries no sector)."""
    try:
        page.goto(url, wait_until="domcontentloaded")
        page.wait_for_timeout(min(delay_ms, 600))
        return _cpv_from_text(page.inner_text("body"))
    except Exception as e:  # noqa: BLE001 — one bad detail page must not sink the snapshot
        log.warning("cpv detail fetch failed for %s: %s", url, type(e).__name__)
        return None, None


def _clean_buyer(df: pl.DataFrame) -> pl.DataFrame:
    """Split the eTenders org-id suffix off the buyer name into ``buyer_org_id`` and strip
    identifier debris (org id + school roll number) from the display ``buyer``. Idempotent.
    The id/roll rules live in shared.buyer_clean — one source of truth, shared with the TED lane."""
    if "buyer" not in df.columns:
        return df
    return df.with_columns(
        buyer_clean.org_id_expr("buyer").alias("buyer_org_id"),
    ).with_columns(
        buyer_clean.clean_name_expr("buyer").alias("buyer"),
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


# Text is all we ever read (grid cells + detail-page inner_text) — keep Chromium's footprint
# minimal so a long pass survives on a memory-pressured host (see ERR_INSUFFICIENT_RESOURCES note).
_BROWSER_ARGS = [
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--disable-extensions",
    "--blink-settings=imagesEnabled=false",
]


def _launch(pw):
    browser = pw.chromium.launch(headless=True, args=_BROWSER_ARGS)
    page = browser.new_page(user_agent=UA)
    page.set_default_timeout(45_000)
    return browser, page


def _grid_goto(page, url: str, settle_ms: int) -> None:
    """Navigate to a grid page robustly. The portal holds long-lived connections open, so
    ``networkidle`` can hang indefinitely; wait for DOM + the rendered table instead, with a
    short settle for the JS grid to populate. One retry after a backoff — transient
    resource pressure (observed: net::ERR_INSUFFICIENT_RESOURCES on a low-RAM host) must
    not sink a long scrape."""
    try:
        page.goto(url, wait_until="domcontentloaded")
    except Exception:
        page.wait_for_timeout(8_000)
        page.goto(url, wait_until="domcontentloaded")
    with contextlib.suppress(Exception):
        page.wait_for_selector("table tr", state="attached", timeout=20_000)
    page.wait_for_timeout(settle_ms)


def _scrape_feed(page, feed: str, max_pages: int, delay_ms: int) -> list[dict]:
    _grid_goto(page, BASE + FEEDS[feed], 2500)
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
                    # Filled by the optional detail-page CPV pass (cft only); None otherwise so the
                    # parquet always carries the columns and the view can reference them safely.
                    "cpv_code": None,
                    "cpv_division": None,
                }
            )
            got += 1
        log.info("[%s] page %d: %d rows (cumulative %d)", feed, p, got, len(out))
        nxt = page.query_selector(f"a[href*='-p={p + 1}&']")
        if not nxt or got == 0:
            break
        _grid_goto(page, urljoin(page.url, nxt.get_attribute("href")), max(delay_ms, 1200))
        p += 1
    return out


def _snapshot_age_hours() -> float | None:
    """Hours since the last successful snapshot, read from the coverage JSON's generated_utc.
    None if no prior snapshot exists (or its timestamp is unreadable)."""
    if not OUT_COV.exists():
        return None
    try:
        gen = json.loads(OUT_COV.read_text(encoding="utf-8")).get("generated_utc")
        return (datetime.now(UTC) - datetime.fromisoformat(gen)).total_seconds() / 3600.0
    except Exception:  # noqa: BLE001 — a malformed/missing timestamp must not block a refresh
        return None


def main() -> None:
    setup_standalone_logging("etenders_live_tenders_extract")
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=120, help="page cap per feed (politeness)")
    ap.add_argument(
        "--min-age-hours",
        type=float,
        default=20.0,
        help="refuse to re-scrape if the existing snapshot is younger than this (guards against a "
        "redundant same-day pull of data we already have). Override with --force.",
    )
    ap.add_argument("--force", action="store_true", help="scrape even if a fresh snapshot already exists")
    ap.add_argument("--only", default="", help="cft | notice (default both)")
    ap.add_argument("--delay-ms", type=int, default=1000)
    ap.add_argument(
        "--no-cpv-details",
        action="store_true",
        help="skip the per-notice CPV detail pass (faster; leaves the sector facet empty)",
    )
    ap.add_argument(
        "--max-details",
        type=int,
        default=4000,
        help="cap on CPV detail-page fetches (cft only; bounds the extra request load)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="scrape + log a CPV sample but do NOT write silver (safe for a small smoke test that "
        "must not clobber the full production snapshot)",
    )
    args = ap.parse_args()
    feeds = [args.only] if args.only in FEEDS else list(FEEDS)

    # Freshness guard: this is a tracked silver fact refreshed daily — don't re-scrape (and don't
    # build a one-off scraper) when a recent snapshot already exists. --dry-run is exempt (it never
    # writes), as is --force.
    if not args.dry_run and not args.force:
        age = _snapshot_age_hours()
        if age is not None and age < args.min_age_hours:
            log.info(
                "Snapshot is %.1fh old (< --min-age-hours=%.0f) at %s — already have this; skipping. "
                "Use --force to scrape anyway.",
                age,
                args.min_age_hours,
                OUT_SILVER,
            )
            print(
                f"SKIP: existing snapshot is {age:.1f}h old (< {args.min_age_hours:.0f}h). "
                f"Already have it at {OUT_SILVER}. Re-run with --force to override."
            )
            return

    rows: list[dict] = []
    with sync_playwright() as pw:
        browser, page = _launch(pw)
        for feed in feeds:
            log.info("feed: %s", feed)
            rows.extend(_scrape_feed(page, feed, args.max_pages, args.delay_ms))
        # CPV enrichment (cft only): the grid carries no CPV, so the sector facet needs one read
        # of each open notice's detail page. Bounded by --max-details + the politeness delay; a
        # failed page just leaves that row without a sector. Skipped entirely with --no-cpv-details.
        if not args.no_cpv_details:
            todo = [r for r in rows if r["feed"] == "cft" and r.get("detail_url")][: args.max_details]
            log.info("CPV detail pass: %d cft notices", len(todo))
            for i, r in enumerate(todo, start=1):
                # A long CPV pass can outlive the browser (observed: TargetClosedError ~35min in).
                # _detail_cpv swallows the death, so every later fetch would silently miss — and
                # losing the session must not lose the already-scraped rows. Relaunch and go on.
                if not browser.is_connected():
                    log.warning("browser session lost at cpv %d/%d — relaunching", i, len(todo))
                    browser, page = _launch(pw)
                elif page.is_closed():
                    page = browser.new_page(user_agent=UA)
                    page.set_default_timeout(45_000)
                r["cpv_code"], r["cpv_division"] = _detail_cpv(page, r["detail_url"], args.delay_ms)
                if i % 25 == 0 or i == len(todo):
                    log.info("  cpv %d/%d (last=%s)", i, len(todo), r["cpv_division"])
                with contextlib.suppress(Exception):
                    page.wait_for_timeout(args.delay_ms)
        with contextlib.suppress(Exception):
            browser.close()

    if not rows:
        log.warning("no rows scraped — leaving existing silver untouched")
        return
    if args.dry_run:
        sample = [(r.get("cpv_division"), r.get("title", "")[:50]) for r in rows if r["feed"] == "cft"][:15]
        n_cpv = sum(1 for r in rows if r["feed"] == "cft" and r.get("cpv_division"))
        n_cft = sum(1 for r in rows if r["feed"] == "cft")
        log.info("DRY RUN: %d rows scraped; CPV found on %d/%d cft rows", len(rows), n_cpv, n_cft)
        for div, title in sample:
            log.info("  cpv=%-26s %s", div, title)
        print(f"DRY RUN: {len(rows)} rows; CPV on {n_cpv}/{n_cft} cft — NOT written")
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
        "rows_with_cpv": int(df["cpv_division"].is_not_null().sum()) if "cpv_division" in df.columns else 0,
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
