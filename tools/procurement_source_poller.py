"""tools/procurement_source_poller.py — watch the *orphan* procurement publishers
for newly-published files we have not yet ingested.

The problem this solves
-----------------------
Most money-flow sources self-refresh: their extractors are wired into ``pipeline.py``
and re-fetch on every daily run (eTenders, TED, the public-body crawler, LA payments).
But a handful of publishers are ingested by **bespoke parsers that are NOT in the
pipeline** — NTA, NPHDB, SEAI and the three reading-order departments. The
consolidate step folds their *frozen* silver facts to gold every run
(``procurement_payments_consolidate.SOURCE_FACTS``), so when one of those bodies
publishes a new quarter, nobody notices and gold silently stays stale.

This poller is the missing watch. For each orphan source it:
  1. fetches the publisher's listing page(s) with a browser User-Agent + a curl
     fallback (the same approach the public-body crawler uses to get past the
     gov.ie / SEAI WAFs that 403 a naive client),
  2. extracts every PO/payment PDF link and the period it covers,
  3. compares the newest upstream period against ``held_through`` — what our last
     ingest actually captured (seeded from each source's coverage JSON),
  4. reports FRESH (upstream has a newer period than we hold), CURRENT,
     UNREACHABLE or NO_PERIODS.

It writes a machine-readable report to ``data/_meta/procurement_source_poll.json``
and records a ``procurement_poller`` heartbeat so ``tools/freshness_status.py``
can flag the *poller itself* going silent.

Why a poller and not just "trust quarterly": Irish disclosure files disappear.
The HSE €20k PO export was deleted in the 2026 site rebuild and was never archived
(see the HSE notes in ``procurement_hse_tusla_materialize``). The cheapest insurance
is to notice — and pull — the moment a file lands, not on a quarterly assumption.

Recommended cadence: WEEKLY. The publishers release roughly quarterly, but a weekly
check catches a new quarter within days of it landing for near-zero cost.

Deliberately stdlib-only (urllib + a curl fallback): it must run inside the minimal
cloud Action env and a PowerShell scheduled task without importing polars/config —
same constraint as the other freshness tools.

Usage:
    python tools/procurement_source_poller.py            # print the table + write report
    python tools/procurement_source_poller.py --json     # machine-readable rollup
    python tools/procurement_source_poller.py --strict   # exit 1 if any source is FRESH/UNREACHABLE
    python tools/procurement_source_poller.py --no-beat   # don't write the heartbeat (dry probe)
"""

from __future__ import annotations

import argparse
import json
import re
import ssl
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
REPORT_PATH = PROJECT_ROOT / "data" / "_meta" / "procurement_source_poll.json"
UA = "Mozilla/5.0 (dail-tracker research probe)"

# How often we expect this poller to run. Quarterly publishing, weekly watch.
POLLER_CADENCE_HOURS = 168.0


# ── source registry ─────────────────────────────────────────────────────────
# Each entry is an orphan procurement publisher (bespoke parser, not in pipeline.py).
#   grain        — "quarterly" | "annual": how a period is keyed and compared.
#   held_through — the newest period our LAST ingest captured, seeded from the
#                  source's coverage JSON. Bump it when you re-ingest. For quarterly
#                  it is [year, quarter]; for annual it is [year].
#   check        — "auto": fetch the listing and diff; "manual": listed for awareness
#                  but not auto-fetched (its files aren't reachable by a plain link scan).
#   must_match   — a candidate PDF link must contain this (case-insensitive) to count,
#                  so navigation/brochure PDFs don't masquerade as PO listings.
SOURCES: list[dict] = [
    {
        "id": "nphdb",
        "name": "National Paediatric Hospital Development Board",
        "grain": "quarterly",
        "check": "auto",
        "listing_urls": ["https://newchildrenshospital.ie/freedom-of-information/procurement/"],
        "must_match": r"\bpo|purchase|listing|quarter|q[1-4]",
        "held_through": [2025, 2],
        "coverage": "data/_meta/nphdb_payments_coverage.json",
        "parser": "extractors/procurement_nphdb_parser.py",
    },
    {
        "id": "nta",
        "name": "National Transport Authority",
        "grain": "quarterly",
        "check": "auto",
        "listing_urls": [
            "https://www.nationaltransport.ie/publications/2025-purchase-orders-e20000-and-over/",
            "https://www.nationaltransport.ie/publications/2026-purchase-orders-e20000-and-over/",
        ],
        "must_match": r"purchase|\bpo|20k|20[,]?000|quarter|q[1-4]",
        "held_through": [2026, 1],
        "coverage": "data/_meta/nta_payments_coverage.json",
        "parser": "extractors/procurement_nta_parser.py",
    },
    # ── manual-watch entries — files aren't reachable by a plain link scan ──────
    # SEAI /publications and the Tusla FOI page each return a *link-less stub*
    # (~1.5–5 KB, zero anchors) to a static client: both are JS-rendered behind a
    # WAF, so the PDFs only appear after client-side rendering. Watching them needs
    # the Playwright/direct-URL tail (same class as the gov.ie depts), so they are
    # manual rather than auto — verified by the poller's own fetch 2026-06-20.
    {
        "id": "seai",
        "name": "Sustainable Energy Authority of Ireland",
        "grain": "quarterly",
        "check": "manual",
        "listing_urls": ["https://www.seai.ie/publications"],
        "held_through": [2025, 2],
        "coverage": "data/_meta/seai_payments_coverage.json",
        "parser": "extractors/procurement_seai_parser.py",
        "manual_reason": "/publications is a JS-rendered/WAF stub (link-less to a static fetch) — needs a browser or pinned PDF URLs.",
    },
    {
        "id": "tusla",
        "name": "Tusla – Child and Family Agency",
        "grain": "annual",
        "check": "manual",
        "listing_urls": ["https://www.tusla.ie/about-us/freedom-of-information/"],
        "held_through": [2025],
        "coverage": "data/_meta/hse_tusla_payments_coverage.json",
        "parser": "extractors/procurement_hse_tusla_materialize.py",
        "manual_reason": "FOI page is a JS-rendered/WAF stub (link-less to a static fetch) — needs a browser or pinned PDF URLs.",
    },
    # gov.ie collections link to landing PAGES, not direct PDFs, and need the full
    # public-body crawler; HSE deep-links rotted in the 2026 rebuild. Listed so the
    # report keeps them in view (and records what we hold), but not auto-diffed.
    {
        "id": "hse",
        "name": "Health Service Executive",
        "grain": "quarterly",
        "check": "manual",
        "listing_urls": ["https://www.hse.ie/eng/about/who/finance/"],
        "held_through": [2025, 3],
        "coverage": "data/_meta/hse_tusla_payments_coverage.json",
        "parser": "extractors/procurement_hse_tusla_materialize.py",
        "manual_reason": "single cumulative FOI export; deep-links rotted in the 2026 site rebuild — verify landing page by hand.",
    },
    {
        "id": "dept_justice",
        "name": "Department of Justice, Home Affairs and Migration",
        "grain": "quarterly",
        "check": "manual",
        "listing_urls": [
            "https://www.gov.ie/en/department-of-justice-home-affairs-and-migration/collections/department-of-justice-purchase-orders-issued-over-20000-in-value/"
        ],
        "held_through": [2026, 1],
        "coverage": "data/_meta/dept_readingorder_payments_coverage.json",
        "parser": "extractors/procurement_dept_readingorder_parser.py",
        "manual_reason": "gov.ie collection links to landing pages, not direct PDFs — needs the public-body crawler.",
    },
    {
        "id": "dept_foreign_affairs",
        "name": "Department of Foreign Affairs and Trade",
        "grain": "quarterly",
        "check": "manual",
        "listing_urls": [
            "https://www.gov.ie/en/department-of-foreign-affairs/organisation-information/payments-over-20000/"
        ],
        "held_through": [2026, 1],
        "coverage": "data/_meta/dept_readingorder_payments_coverage.json",
        "parser": "extractors/procurement_dept_readingorder_parser.py",
        "manual_reason": "gov.ie collection links to landing pages, not direct PDFs — needs the public-body crawler.",
    },
    {
        "id": "dept_transport",
        "name": "Department of Transport",
        "grain": "quarterly",
        "check": "manual",
        "listing_urls": [
            "https://www.gov.ie/en/department-of-transport/organisation-information/departmental-purchase-orders-greater-than-20000/"
        ],
        "held_through": [2026, 1],
        "coverage": "data/_meta/dept_readingorder_payments_coverage.json",
        "parser": "extractors/procurement_dept_readingorder_parser.py",
        "manual_reason": "gov.ie collection links to landing pages, not direct PDFs — needs the public-body crawler.",
    },
]


# ── fetch (browser UA + curl fallback, like the public-body crawler) ─────────
def _fetch(url: str) -> tuple[str | None, str | None]:
    """Return (html, error). Tries urllib with a browser UA, then curl -k (which
    gets past the gov.ie / SEAI WAFs that 403 a default client)."""
    req = urllib.request.Request(
        url, headers={"User-Agent": UA, "Accept": "text/html,application/xhtml+xml,*/*"}
    )
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    try:
        with urllib.request.urlopen(req, timeout=60, context=ctx) as r:  # noqa: S310 (trusted gov hosts)
            return r.read().decode("utf-8", "ignore"), None
    except (urllib.error.URLError, TimeoutError, ValueError) as e:
        first_err = f"{type(e).__name__}: {e}"
        try:
            p = subprocess.run(
                ["curl", "-sS", "-k", "-L", "--max-time", "60", "-A", UA, url],
                capture_output=True,
                timeout=80,
            )
            if p.returncode == 0 and p.stdout:
                return p.stdout.decode("utf-8", "ignore"), None
            return None, f"{first_err}; curl rc={p.returncode}"
        except (OSError, subprocess.SubprocessError) as e2:
            return None, f"{first_err}; curl {type(e2).__name__}: {e2}"


# ── period extraction ────────────────────────────────────────────────────────
_A_RE = re.compile(r'<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', re.I | re.S)
_TAG_RE = re.compile(r"<[^>]+>")
_PAT_QY = re.compile(r"Q\s*([1-4])[\s_/.-]*((?:19|20)\d{2})", re.I)
_PAT_YQ = re.compile(r"((?:19|20)\d{2})[\s_/.-]*Q\s*([1-4])", re.I)
_PAT_QUARTER = re.compile(r"quarter[\s_/.-]*([1-4])[\s_/.-]*((?:19|20)\d{2})", re.I)
_PAT_YEAR = re.compile(r"(20\d{2})")


def _pdf_links(html: str, must_match: str | None) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for href, inner in _A_RE.findall(html):
        if ".pdf" not in href.lower():
            continue
        text = _TAG_RE.sub(" ", inner).strip()
        blob = f"{href} {text}"
        if must_match and not re.search(must_match, blob, re.I):
            continue
        out.append((href, text))
    return out


def _quarters(text: str) -> set[tuple[int, int]]:
    out: set[tuple[int, int]] = set()
    out.update((int(m.group(2)), int(m.group(1))) for m in _PAT_QY.finditer(text))
    out.update((int(m.group(1)), int(m.group(2))) for m in _PAT_YQ.finditer(text))
    out.update((int(m.group(2)), int(m.group(1))) for m in _PAT_QUARTER.finditer(text))
    return out


def _years(text: str) -> set[int]:
    return {int(y) for y in _PAT_YEAR.findall(text)}


def _fmt_q(p) -> str:
    if p is None:
        return "--"
    if isinstance(p, (list, tuple)) and len(p) == 2:
        return f"{p[0]}-Q{p[1]}"
    if isinstance(p, (list, tuple)) and len(p) == 1:
        return f"{p[0]}"
    return str(p)


# ── evaluation ───────────────────────────────────────────────────────────────
def _evaluate_source(src: dict) -> dict:
    held = tuple(src["held_through"])
    row: dict = {
        "id": src["id"],
        "name": src["name"],
        "grain": src["grain"],
        "check": src["check"],
        "held_through": list(held),
        "parser": src.get("parser"),
        "upstream_newest": None,
        "new_periods": [],
        "error": None,
        "status": None,
    }

    if src["check"] == "manual":
        row["status"] = "MANUAL"
        row["note"] = src.get("manual_reason")
        return row

    # Fetch every listing URL; tolerate a partial failure (one of N pages down).
    html_parts: list[str] = []
    errors: list[str] = []
    for url in src["listing_urls"]:
        html, err = _fetch(url)
        if html:
            html_parts.append(html)
        else:
            errors.append(f"{url} -> {err}")
    if not html_parts:
        row["status"] = "UNREACHABLE"
        row["error"] = "; ".join(errors)
        return row

    # A page that fetched but carries no anchors at all is a JS-rendered/WAF stub,
    # not an empty listing — calling it NO_PERIODS (or CURRENT) would be a false
    # negative that silently hides a stale source. Treat it as UNREACHABLE.
    if not any(_A_RE.search(html) for html in html_parts):
        row["status"] = "UNREACHABLE"
        row["error"] = "listing returned a link-less stub (JS-rendered/WAF); " + "; ".join(errors)
        return row

    links = []
    for html in html_parts:
        links.extend(_pdf_links(html, src.get("must_match")))
    row["pdf_links_seen"] = len(links)

    if src["grain"] == "quarterly":
        found: set[tuple[int, int]] = set()
        for href, text in links:
            found |= _quarters(f"{href} {text}")
        if not found:
            row["status"] = "NO_PERIODS"
            if errors:
                row["error"] = "; ".join(errors)
            return row
        newest = max(found)
        row["upstream_newest"] = list(newest)
        new = sorted(p for p in found if p > held)
        row["new_periods"] = [list(p) for p in new]
        row["status"] = "FRESH" if new else "CURRENT"
    else:  # annual
        found_y: set[int] = set()
        for href, text in links:
            found_y |= _years(f"{href} {text}")
        if not found_y:
            row["status"] = "NO_PERIODS"
            if errors:
                row["error"] = "; ".join(errors)
            return row
        newest_y = max(found_y)
        held_y = held[0]
        row["upstream_newest"] = [newest_y]
        new_y = sorted(y for y in found_y if y > held_y)
        row["new_periods"] = [[y] for y in new_y]
        row["status"] = "FRESH" if new_y else "CURRENT"

    if errors:
        row["error"] = "; ".join(errors)
    return row


# Statuses that mean "a human should act".
_PROBLEM = {"FRESH", "UNREACHABLE", "NO_PERIODS"}


def poll() -> dict:
    rows = [_evaluate_source(s) for s in SOURCES]
    problems = [r["id"] for r in rows if r["status"] in _PROBLEM]
    fresh = [r["id"] for r in rows if r["status"] == "FRESH"]
    return {
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "fresh_sources": fresh,
        "problem_sources": problems,
        "sources": rows,
    }


def _print_table(rollup: dict) -> None:
    print(f"procurement source poll @ {rollup['generated_at']}")
    print(f"  {'source':<22} {'status':<11} {'held':<10} {'upstream':<10} new / note")
    for r in rollup["sources"]:
        new = ", ".join(_fmt_q(p) for p in r["new_periods"])
        tail = new or r.get("note") or r.get("error") or ""
        print(
            f"  {r['id']:<22} {r['status']:<11} {_fmt_q(r['held_through']):<10} "
            f"{_fmt_q(r['upstream_newest']):<10} {tail}"
        )
    fresh = rollup["fresh_sources"]
    if fresh:
        print(f"\n[FRESH DATA UPSTREAM] re-ingest: {', '.join(fresh)}")
    else:
        print("\nNo new upstream periods beyond what we hold (auto-checked sources).")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Poll orphan procurement publishers for fresh files.")
    parser.add_argument("--strict", action="store_true", help="exit 1 if any source is FRESH/UNREACHABLE/NO_PERIODS")
    parser.add_argument("--json", action="store_true", help="emit the machine-readable rollup")
    parser.add_argument("--no-beat", action="store_true", help="do not write the heartbeat (dry probe)")
    args = parser.parse_args(argv)

    rollup = poll()

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(json.dumps(rollup, indent=2) + "\n", encoding="utf-8")

    if args.json:
        sys.stdout.write(json.dumps(rollup, indent=2) + "\n")
    else:
        _print_table(rollup)
        print(f"\nreport -> {REPORT_PATH.relative_to(PROJECT_ROOT).as_posix()}")

    if not args.no_beat:
        try:
            sys.path.insert(0, str(PROJECT_ROOT / "tools"))
            import freshness_heartbeat  # stdlib-only sibling

            n_fresh = len(rollup["fresh_sources"])
            freshness_heartbeat.record(
                "procurement_poller",
                runner="local",
                cadence_hours=POLLER_CADENCE_HOURS,
                note=f"{len(rollup['sources'])} sources, {n_fresh} with fresh data",
            )
        except Exception as e:  # never let a beat failure mask the poll result
            sys.stderr.write(f"poller: heartbeat skipped ({type(e).__name__}: {e})\n")

    if args.strict and rollup["problem_sources"]:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
