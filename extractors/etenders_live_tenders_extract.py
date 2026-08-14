"""eTenders.gov.ie (Eurodyn EPPS) -> LIVE national tender pipeline -> SILVER parquet.

The forward-looking PLANNED-tier lane the OGP quarterly open-data CSV and TED (EU-threshold only) cannot
give us: currently-open national tenders incl. SUB-EU-THRESHOLD contracts (schools, councils, water schemes).
Promoted from pipeline_sandbox/etenders_live_tenders_extract.py after the probe proved the grid is liftable.

Playwright is REQUIRED (the grid is JS-rendered; curl/RSS return nothing). Writes:
    data/silver/parquet/etenders_live_tenders.parquet  (+ a coverage JSON)
    data/bronze/parquet/etenders_live_tender_observations.parquet
    data/silver/parquet/etenders_live_tender_events.parquet

It is its OWN silver fact — it does NOT touch procurement_awards / procurement_payments_fact / the consolidate.
NOT in the standard pipeline.py sequence (needs a browser + is a point-in-time snapshot); refreshed by the
poll wrapper tools/poll_live_tenders.ps1 instead. The view sql_views/procurement/procurement_live_tenders.sql
filters this to genuinely-open tenders.

VALUE SEMANTICS — every estimated value is a BUYER ESTIMATE at the PLANNED (pre-award) lifecycle stage:
realisation_tier='PLANNED', value_kind='estimate_advertised', value_safe_to_sum=FALSE. A NEW tier EARLIER
than AWARDED — NEVER summed with eTenders/TED awards or with payments.

Feeds (tagged in column `feed`): cft = Latest CfTs (open opportunities) · notice = Latest Notices.
SOURCE AUTHORITY: the live portal's terms restrict automated retrieval and onward publication. Network
access fails closed unless written permission is represented by the two ETENDERS_RETRIEVAL_AUTHORITY
environment variables described by ``_retrieval_authority_receipt``.

Run:
    ./.venv/Scripts/python.exe extractors/etenders_live_tenders_extract.py  # complete grid snapshot
    ./.venv/Scripts/python.exe extractors/etenders_live_tenders_extract.py --details-only
    ./.venv/Scripts/python.exe extractors/etenders_live_tenders_extract.py --max-pages 2 --only cft --dry-run
"""

from __future__ import annotations

# isort: off
# Apply native thread caps before Polars/NumPy loads. Ordering is the contract.
import services.runtime_env  # noqa: F401
# isort: on

import argparse
import contextlib
import hashlib
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urljoin

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    reconfigure_stdout = getattr(sys.stdout, "reconfigure", None)
    if callable(reconfigure_stdout):
        reconfigure_stdout(encoding="utf-8")
from services.coverage_io import save_coverage  # noqa: E402
from services.extract_runner import run_extractor  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402
from shared import buyer_clean  # noqa: E402

log = logging.getLogger(__name__)

BASE = "https://www.etenders.gov.ie"
FEEDS = {
    "cft": "/epps/prepareCurrentOpportunities.do?currentType=cft",
    "notice": "/epps/prepareCurrentOpportunities.do?currentType=notice",
}
OUT_SILVER = ROOT / "data/silver/parquet/etenders_live_tenders.parquet"
OUT_OBSERVATIONS = ROOT / "data/bronze/parquet/etenders_live_tender_observations.parquet"
OUT_EVENTS = ROOT / "data/silver/parquet/etenders_live_tender_events.parquet"
OUT_COV = ROOT / "data/_meta/etenders_live_tenders_coverage.json"
UA = "Mozilla/5.0 (dail-tracker civic-research; +https://github.com/)"
PARSER_VERSION = "0.2.0"


@dataclass(frozen=True)
class ScrapeResult:
    """One feed's bounded pagination result and its publication safety receipt."""

    rows: list[dict]
    pages_visited: int
    terminal_reached: bool
    termination_reason: str


SOURCE = {
    "dataset": "eTenders current opportunities (Calls for Tender + Notices)",
    "publisher": "Office of Government Procurement (OGP)",
    "platform": "etenders.gov.ie (European Dynamics EPPS)",
    "landing_page": "https://www.etenders.gov.ie/epps/prepareCurrentOpportunities.do?currentType=cft",
    "access": "Playwright (public tender list is JS-rendered)",
    "license": "No general live-portal reuse licence established; automated retrieval requires documented authority.",
}


def _retrieval_authority_receipt() -> dict[str, str]:
    """Return a non-secret receipt summary or fail closed before any live portal request."""
    basis = os.environ.get("ETENDERS_RETRIEVAL_AUTHORITY", "").strip().lower()
    receipt = os.environ.get("ETENDERS_RETRIEVAL_AUTHORITY_RECEIPT", "").strip()
    if basis != "written_permission" or len(receipt) < 8:
        raise RuntimeError(
            "Live eTenders retrieval is blocked pending written source authority. Set "
            "ETENDERS_RETRIEVAL_AUTHORITY=written_permission and a non-public "
            "ETENDERS_RETRIEVAL_AUTHORITY_RECEIPT only after permission is recorded."
        )
    return {"basis": basis, "receipt_sha256": hashlib.sha256(receipt.encode("utf-8")).hexdigest()}


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


def _scrape_feed(page, feed: str, max_pages: int, delay_ms: int) -> ScrapeResult:
    """Capture one complete grid feed or return an explicit incomplete receipt.

    Reaching ``max_pages`` while the portal still exposes a next link is not a
    successful snapshot. The caller may inspect it in ``--dry-run`` mode, but a
    publishing run must leave the previous canonical parquet untouched.
    """
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
        if got == 0:
            return ScrapeResult(out, p, True, "empty_page")
        if not nxt:
            return ScrapeResult(out, p, True, "no_next_page")
        if p == max_pages:
            return ScrapeResult(out, p, False, "page_cap_reached")
        _grid_goto(page, urljoin(page.url, nxt.get_attribute("href")), max(delay_ms, 1200))
        p += 1
    return ScrapeResult(out, p - 1, False, "page_cap_reached")


_OBSERVATION_FIELDS = (
    "feed",
    "resource_id",
    "title",
    "buyer",
    "published_raw",
    "deadline_raw",
    "procedure",
    "status",
    "estimated_value_eur",
    "detail_url",
)
_CHANGE_FIELDS = ("deadline_raw", "status", "procedure", "estimated_value_eur", "title", "buyer", "detail_url")
_EVENT_SCHEMA = {
    "event_id": pl.String,
    "observed_at": pl.String,
    "event_type": pl.String,
    "field_name": pl.String,
    "feed": pl.String,
    "resource_id": pl.String,
    "title": pl.String,
    "detail_url": pl.String,
    "previous_value": pl.String,
    "current_value": pl.String,
}


def _json_value(value) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _sha256(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _build_observations(df: pl.DataFrame, observed_at: str) -> pl.DataFrame:
    """Bind each source-grid observation to a timestamp and deterministic content hash."""
    rows: list[dict] = []
    for row in df.iter_rows(named=True):
        content = {field: row.get(field) for field in _OBSERVATION_FIELDS}
        content_hash = _sha256(json.dumps(content, ensure_ascii=False, sort_keys=True, default=str))
        identity = {
            "feed": row.get("feed"),
            "resource_id": row.get("resource_id"),
            "title": row.get("title"),
            "detail_url": row.get("detail_url"),
            "observed_at": observed_at,
            "content_hash": content_hash,
        }
        rows.append(
            {
                **row,
                "observed_at": observed_at,
                "content_hash": content_hash,
                "observation_id": _sha256(json.dumps(identity, ensure_ascii=False, sort_keys=True, default=str)),
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()


def _detect_events(previous: pl.DataFrame, current: pl.DataFrame, observed_at: str) -> pl.DataFrame:
    """Create source-stated new/change events; absence never implies closure or withdrawal."""

    def keyed_rows(frame: pl.DataFrame) -> dict[tuple[str, str], dict]:
        keyed: dict[tuple[str, str], dict] = {}
        for row in frame.iter_rows(named=True):
            feed = row.get("feed")
            resource_id = row.get("resource_id")
            if feed is not None and resource_id is not None:
                keyed[(str(feed), str(resource_id))] = row
        return keyed

    old_rows = keyed_rows(previous)
    new_rows = keyed_rows(current)
    events: list[dict] = []

    def add_event(row: dict, event_type: str, field: str | None, old_value, new_value) -> None:
        payload = {
            "observed_at": observed_at,
            "event_type": event_type,
            "field_name": field,
            "feed": row.get("feed"),
            "resource_id": row.get("resource_id"),
            "previous_value": _json_value(old_value),
            "current_value": _json_value(new_value),
        }
        events.append(
            {
                "event_id": _sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True)),
                **payload,
                "title": row.get("title"),
                "detail_url": row.get("detail_url"),
            }
        )

    for key, row in new_rows.items():
        prior = old_rows.get(key)
        if prior is None:
            add_event(row, "new_opportunity", None, None, row.get("title"))
            continue
        for field in _CHANGE_FIELDS:
            if prior.get(field) != row.get(field):
                add_event(row, f"{field}_changed", field, prior.get(field), row.get(field))

    if not events:
        return pl.DataFrame(schema=_EVENT_SCHEMA)
    return pl.DataFrame(events, schema=_EVENT_SCHEMA)


def _merge_prior_cpv(current: pl.DataFrame, previous: pl.DataFrame | None) -> pl.DataFrame:
    """Carry prior detail enrichment into a fresh grid snapshot by stable portal id."""
    required = {"feed", "resource_id", "cpv_code", "cpv_division"}
    if previous is None or not required.issubset(previous.columns):
        return current
    prior = (
        previous.select("feed", "resource_id", "cpv_code", "cpv_division")
        .filter(pl.col("resource_id").is_not_null())
        .unique(subset=["feed", "resource_id"], keep="first")
        .rename({"cpv_code": "_prior_cpv_code", "cpv_division": "_prior_cpv_division"})
    )
    return (
        current.join(prior, on=["feed", "resource_id"], how="left")
        .with_columns(
            pl.coalesce("cpv_code", "_prior_cpv_code").alias("cpv_code"),
            pl.coalesce("cpv_division", "_prior_cpv_division").alias("cpv_division"),
        )
        .drop("_prior_cpv_code", "_prior_cpv_division")
    )


def _append_history(new_rows: pl.DataFrame, path: Path, id_column: str) -> None:
    if new_rows.is_empty():
        return
    previous = pl.read_parquet(path) if path.exists() else None
    combined = (pl.concat([previous, new_rows], how="diagonal_relaxed") if previous is not None else new_rows).unique(
        subset=[id_column], keep="last", maintain_order=True
    )
    save_parquet(combined, path, min_rows=previous.height if previous is not None else None)


def _enrich_cpv_rows(pw, df: pl.DataFrame, max_details: int, delay_ms: int) -> tuple[pl.DataFrame, int, int]:
    """Enrich only missing CFT CPVs in an existing snapshot, in a separate browser lifetime."""
    rows = df.to_dicts()
    todo = [row for row in rows if row.get("feed") == "cft" and not row.get("cpv_code") and row.get("detail_url")][
        :max_details
    ]
    if not todo:
        return df, 0, 0
    browser, page = _launch(pw)
    found = 0
    try:
        for i, row in enumerate(todo, start=1):
            if not browser.is_connected():
                log.warning("browser session lost at cpv %d/%d — relaunching", i, len(todo))
                browser, page = _launch(pw)
            elif page.is_closed():
                page = browser.new_page(user_agent=UA)
                page.set_default_timeout(45_000)
            row["cpv_code"], row["cpv_division"] = _detail_cpv(page, row["detail_url"], delay_ms)
            found += int(bool(row["cpv_code"]))
            if i % 25 == 0 or i == len(todo):
                log.info("  cpv %d/%d (found=%d)", i, len(todo), found)
            with contextlib.suppress(Exception):
                page.wait_for_timeout(delay_ms)
    finally:
        with contextlib.suppress(Exception):
            browser.close()
    return pl.DataFrame(rows, schema=df.schema), len(todo), found


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
    # logging + UTF-8 + exit-code handling live in run_extractor (__main__ below)
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=400, help="safety cap per feed; publication fails if reached")
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
        help="deprecated compatibility flag; grid capture never fetches detail pages",
    )
    ap.add_argument(
        "--details-only",
        action="store_true",
        help="enrich missing CFT CPVs in the existing snapshot without scraping or replacing the grid",
    )
    ap.add_argument(
        "--max-details",
        type=int,
        default=300,
        help="cap on CPV detail-page fetches in --details-only mode",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="perform the requested browser work but do NOT write any canonical artifact",
    )
    args = ap.parse_args()
    if args.max_pages < 1 or args.max_details < 1 or args.delay_ms < 0:
        ap.error("--max-pages and --max-details must be positive; --delay-ms must be non-negative")
    if args.only and not args.dry_run:
        ap.error("--only is limited to --dry-run so a partial feed cannot replace the complete snapshot")
    retrieval_authority = _retrieval_authority_receipt()
    # Keep the browser dependency lazy so parser/unit-test imports work in the base pipeline
    # environment, and check authority before importing or starting browser machinery.
    from playwright.sync_api import sync_playwright

    feeds = [args.only] if args.only in FEEDS else list(FEEDS)

    if args.details_only:
        if not OUT_SILVER.exists():
            raise FileNotFoundError(f"No grid snapshot to enrich: {OUT_SILVER}")
        existing = pl.read_parquet(OUT_SILVER)
        with sync_playwright() as pw:
            enriched, attempted, found = _enrich_cpv_rows(pw, existing, args.max_details, args.delay_ms)
        if args.dry_run:
            print(f"DRY RUN: CPV found on {found}/{attempted} attempted detail pages — NOT written")
            return
        if attempted:
            save_parquet(enriched, OUT_SILVER, min_rows=existing.height)
        remaining = enriched.filter(
            (pl.col("feed") == "cft") & pl.col("cpv_code").is_null() & pl.col("detail_url").is_not_null()
        ).height
        cov = json.loads(OUT_COV.read_text(encoding="utf-8")) if OUT_COV.exists() else {}
        cov["detail_enrichment"] = {
            "generated_utc": datetime.now(UTC).isoformat(),
            "attempted": attempted,
            "found": found,
            "remaining": remaining,
            "max_details": args.max_details,
            "complete": remaining == 0,
        }
        cov["rows_with_cpv"] = (
            int(enriched["cpv_division"].is_not_null().sum()) if "cpv_division" in enriched.columns else 0
        )
        save_coverage(cov, OUT_COV)
        print(f"CPV ENRICHMENT: found {found:,}/{attempted:,}; grid snapshot retained at {OUT_SILVER}")
        return

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
    receipts: dict[str, ScrapeResult] = {}
    with sync_playwright() as pw:
        browser, page = _launch(pw)
        for feed in feeds:
            log.info("feed: %s", feed)
            receipt = _scrape_feed(page, feed, args.max_pages, args.delay_ms)
            receipts[feed] = receipt
            rows.extend(receipt.rows)
        with contextlib.suppress(Exception):
            browser.close()

    incomplete = {feed: result for feed, result in receipts.items() if not result.terminal_reached}
    if incomplete and not args.dry_run:
        summary = ", ".join(
            f"{feed}: {result.termination_reason} after {result.pages_visited} pages"
            for feed, result in incomplete.items()
        )
        raise RuntimeError(f"Incomplete eTenders pagination ({summary}); previous silver snapshot left untouched")
    if not rows:
        log.warning("no rows scraped — leaving existing silver untouched")
        return
    if args.dry_run:
        receipt_text = ", ".join(
            f"{feed}={result.pages_visited} pages/{result.termination_reason}" for feed, result in receipts.items()
        )
        print(f"DRY RUN: {len(rows)} rows; {receipt_text} — NOT written")
        return
    observed_at = datetime.now(UTC).isoformat()
    df = pl.DataFrame(rows, infer_schema_length=None).with_columns(
        pl.lit("etenders.gov.ie current opportunities").alias("source"),
        pl.lit(observed_at).alias("retrieved_utc"),
        pl.lit(PARSER_VERSION).alias("parser_version"),
        pl.lit("PLANNED").alias("realisation_tier"),
        pl.lit("estimate_advertised").alias("value_kind"),
        pl.lit(False).alias("value_safe_to_sum"),
    )
    df = _clean_buyer(df)
    df = df.unique(subset=["feed", "resource_id", "title"], keep="first", maintain_order=True)
    previous = pl.read_parquet(OUT_SILVER) if OUT_SILVER.exists() else None
    df = _merge_prior_cpv(df, previous)

    observations = _build_observations(df, observed_at)
    events = _detect_events(previous, df, observed_at) if previous is not None else pl.DataFrame(schema=_EVENT_SCHEMA)
    _append_history(observations, OUT_OBSERVATIONS, "observation_id")
    _append_history(events, OUT_EVENTS, "event_id")

    OUT_SILVER.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, OUT_SILVER)
    log.info("SILVER WRITTEN: %d live tenders -> %s", df.height, OUT_SILVER)
    print(f"SILVER WRITTEN: {df.height:,} live tenders -> {OUT_SILVER}")

    cov = {
        "generated_utc": observed_at,
        "layer": "silver",
        "source": SOURCE,
        "retrieval_authority": retrieval_authority,
        "n_rows": df.height,
        "by_feed": {r["feed"]: r["len"] for r in df.group_by("feed").len().iter_rows(named=True)},
        "rows_with_estimated_value": int(df["estimated_value_eur"].is_not_null().sum()),
        "rows_with_cpv": int(df["cpv_division"].is_not_null().sum()) if "cpv_division" in df.columns else 0,
        "pagination_complete": all(result.terminal_reached for result in receipts.values()),
        "feed_receipts": {
            feed: {
                "pages_visited": result.pages_visited,
                "rows_observed": len(result.rows),
                "terminal_reached": result.terminal_reached,
                "termination_reason": result.termination_reason,
            }
            for feed, result in receipts.items()
        },
        "history": {
            "observations_added": observations.height,
            "events_added": events.height,
            "absence_policy": "Absence from a later snapshot is not treated as closure or withdrawal.",
        },
        "value_note": "estimated_value_eur is a BUYER ESTIMATE at PLANNED stage (value_kind=estimate_advertised, "
        "realisation_tier=PLANNED, value_safe_to_sum=FALSE) — a NEW lifecycle tier before AWARDED; NEVER summed.",
        "caveat": "Live national pipeline (incl. sub-EU-threshold). Grid pagination completed before publication. "
        "CPV detail enrichment is a separate bounded job and may be incomplete. The cft feed also lists already-closed + "
        "DPS/Qualification-System records back to 2023; the consuming view filters to genuinely-open. Snapshot at "
        "retrieved_utc — refresh via tools/poll_live_tenders.ps1 only while documented source authority remains valid.",
        "parser_version": PARSER_VERSION,
    }
    save_coverage(cov, OUT_COV)
    print(f"coverage -> {OUT_COV}")


if __name__ == "__main__":
    run_extractor(main)
