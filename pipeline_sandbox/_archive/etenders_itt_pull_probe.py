"""SANDBOX PROBE ONLY (no extractor, no parquet, no pipeline changes) — drive YOUR OWN
authenticated eTenders session with Playwright to download the ITT/tender-document packs for
an EXPLICIT allowlist of RFT IDs, then locate the price-scoring / award-criteria section in
each pack. The marking algorithm lives in login-gated PDFs attached to each notice and is NOT
in the open notice metadata, so this is the only route to it short of manual download.

THIS IS NOT A CRAWLER AND NOT A BULK HARVEST
  - Works from an explicit --rfts allowlist (or a small _meta file). It never follows links to
    discover new notices.
  - Drives a single, real, logged-in session at human pace (serial, with delays). One tab.
  - Each pack you download may register your supplier identity against that competition (the
    "express interest" step). Pull ONLY competitions you have a genuine interest in.
  - Read the eTenders terms of use re: automated access before running. The account holder
    (you) owns that decision; this script does not bypass any gate — it logs in as you.

CREDENTIALS (never hardcoded)
  Set ETENDERS_USER / ETENDERS_PASS in the environment. The script reads them at runtime and
  refuses to run if either is unset.

DISCOVER-LIVE PLACEHOLDERS
  The login form selectors, the "express interest" control, and the document-download links are
  marked `# DISCOVER-LIVE` below. They depend on the authenticated DOM, which is not visible
  without an account. On the first run use --pause to drop into Playwright's inspector, read the
  real selectors off the page, and fill them in. Everything else (allowlist, pacing, manifest,
  formula-section extraction) is ready.

Outputs (gitignored — c:/tmp, nothing touches data/ gold or silver):
  c:/tmp/etenders_itt/<RFT_ID>/<original_filename>      downloaded packs
  c:/tmp/etenders_itt/<RFT_ID>/_formula.txt             extracted scoring-section text (best effort)
  c:/tmp/etenders_itt/manifest.csv                      one row per RFT: pulled / skipped / why

Run:
  $env:ETENDERS_USER="..."; $env:ETENDERS_PASS="..."
  ./.venv/Scripts/python.exe pipeline_sandbox/etenders_itt_pull_probe.py --rfts 123456,123457 --pause
  ./.venv/Scripts/python.exe pipeline_sandbox/etenders_itt_pull_probe.py --rfts-file c:/tmp/my_rfts.txt
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import logging
import os
import re
import sys
import time
from pathlib import Path

from playwright.sync_api import TimeoutError as PWTimeout
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from services.logging_setup import setup_standalone_logging  # noqa: E402

logger = logging.getLogger(__name__)

BASE = "https://www.etenders.gov.ie"
LOGIN_URL = f"{BASE}/epps/cft/listCfts.do"  # DISCOVER-LIVE: confirm the real login landing page
OUT_DIR = Path("c:/tmp/etenders_itt")
MANIFEST = OUT_DIR / "manifest.csv"

PAGE_TIMEOUT = 45_000
POLITE_DELAY = 4.0  # seconds between RFTs — human pace, gentle on the server. Do not lower.

# award-criteria / marking-scheme cues used to locate the scoring section inside a pack
FORMULA_CUES = re.compile(
    r"award criteria|most economically advantageous|MEAT\b|marking scheme|scoring|"
    r"price\s*(?:score|formula|criterion)|weighting|sub[- ]?criteri|points? awarded|"
    r"lowest price|tender price",
    re.I,
)


def _credentials() -> tuple[str, str]:
    user, pw = os.environ.get("ETENDERS_USER"), os.environ.get("ETENDERS_PASS")
    if not user or not pw:
        logger.error("Set ETENDERS_USER and ETENDERS_PASS in the environment first. Refusing to run.")
        raise SystemExit(2)
    return user, pw


def _read_rfts(args) -> list[str]:
    if args.rfts_file:
        ids = [ln.strip() for ln in Path(args.rfts_file).read_text(encoding="utf-8").splitlines()]
    else:
        ids = (args.rfts or "").split(",")
    ids = [i.strip() for i in ids if i.strip()]
    if not ids:
        logger.error("No RFT IDs given. Use --rfts 123,456 or --rfts-file <path>.")
        raise SystemExit(2)
    if len(ids) > args.max_rfts:
        logger.error(
            "%d RFTs exceeds --max-rfts=%d. This probe is for a TARGETED pull, not a corpus "
            "harvest. Raise the cap deliberately if you really mean it.",
            len(ids),
            args.max_rfts,
        )
        raise SystemExit(2)
    return ids


def _login(page, user: str, pw: str) -> None:
    logger.info("Logging in as %s ...", user)
    page.goto(LOGIN_URL, wait_until="domcontentloaded")  # domcontentloaded, not networkidle (memory: EPPS trap)
    page.wait_for_timeout(2000)
    # DISCOVER-LIVE: replace these three selectors with the real login form fields/button.
    page.fill("input[name='username']", user)
    page.fill("input[name='password']", pw)
    page.click("button[type='submit']")
    page.wait_for_timeout(3000)
    logger.info("   post-login url: %s", page.url)


def _detail_url(rft: str) -> str:
    # confirmed live 2026-06-18 against public current-opportunities listing
    return f"{BASE}/epps/cft/prepareViewCfTWS.do?resourceId={rft}"


def _dry_one(page, rft: str) -> dict:
    """READ-ONLY triage: visit the public detail page WITHOUT login, express-interest, or any
    download. Reports whether the notice is reachable, whether it bounced to a login wall, its
    title, and how many document links are visible pre-auth. Lets you cull a list before
    committing your supplier identity to anything."""
    rec: dict = {"rft_id": rft, "status": "dry", "reachable": False, "login_wall": False,
                 "docs_visible": 0, "title": "", "note": ""}
    try:
        page.goto(_detail_url(rft), wait_until="domcontentloaded")
        page.wait_for_timeout(2000)
    except PWTimeout:
        rec["note"] = "timed out"
        return rec
    rec["reachable"] = True
    url_l = page.url.lower()
    rec["login_wall"] = any(k in url_l for k in ("login", "signin", "j_security", "sso"))
    with contextlib.suppress(Exception):
        rec["title"] = (page.title() or "").strip()[:80]
    rec["docs_visible"] = page.eval_on_selector_all(
        "a[href]",
        "els => els.filter(e => /\\.(pdf|docx?|zip)(\\?|$)/i.test(e.getAttribute('href')||'')).length",
    )
    body = (page.inner_text("body")[:4000] if rec["reachable"] else "").lower()
    if "express interest" in body or "register interest" in body:
        rec["note"] = "express-interest gate present"
    elif rec["login_wall"]:
        rec["note"] = "redirected to login"
    return rec


def _pull_one(page, rft: str) -> dict:
    """Open one RFT detail page, (express interest if required,) download its document pack,
    and best-effort extract the scoring section. Returns a manifest row."""
    rec: dict = {"rft_id": rft, "status": "skipped", "n_docs": 0, "formula_found": False, "note": ""}
    dest = OUT_DIR / rft
    dest.mkdir(parents=True, exist_ok=True)
    detail_url = _detail_url(rft)
    try:
        page.goto(detail_url, wait_until="domcontentloaded")
        page.wait_for_timeout(2500)
    except PWTimeout:
        rec["note"] = "detail page timed out"
        return rec

    # DISCOVER-LIVE: many competitions need an "Express Interest" / "Register Interest" click
    # before the document pack is downloadable. Confirm the control text, then enable this.
    # NOTE: clicking it registers YOUR supplier identity against this competition.
    # ei = page.get_by_role("button", name=re.compile("express interest", re.I))
    # if ei.count():
    #     ei.first.click(); page.wait_for_timeout(2500)

    # DISCOVER-LIVE: the document table / download links. Collect anchors that look like file
    # downloads (pdf/doc/zip) within the documents area.
    links = page.eval_on_selector_all(
        "a[href]",
        "els => els.map(e => ({t:(e.innerText||'').trim(), h:e.getAttribute('href')}))"
        ".filter(x => x.h && /\\.(pdf|docx?|zip)(\\?|$)/i.test(x.h))",
    )
    if not links:
        rec["note"] = "no downloadable docs found (login/express-interest needed, or awarded & withdrawn)"
        return rec

    for ln in links:
        href = ln["h"]
        url = href if href.startswith("http") else f"{BASE}{href if href.startswith('/') else '/' + href}"
        fname = re.sub(r"[^\w.\-]", "_", (url.split("/")[-1].split("?")[0] or "document"))[:120]
        try:
            with page.expect_download(timeout=PAGE_TIMEOUT) as dl_info:
                page.goto(url)  # navigating to a file URL triggers a download in chromium
            dl_info.value.save_as(str(dest / fname))
            rec["n_docs"] += 1
            time.sleep(1.5)
        except (PWTimeout, Exception) as exc:  # noqa: BLE001 — a probe skips a bad doc, never aborts
            logger.warning("   %s: download failed for %s (%s)", rft, fname, exc)

    rec["formula_found"] = _extract_formula(dest)
    rec["status"] = "pulled" if rec["n_docs"] else "skipped"
    return rec


def _extract_formula(dest: Path) -> bool:
    """Best-effort: scan downloaded PDFs/DOCX for the award-criteria / scoring section and dump
    the surrounding text to _formula.txt. This is the SAMPLED-FINDING extraction, not a parser —
    hand-verify each one. Returns True if any scoring cue was hit."""
    try:
        import fitz  # PyMuPDF — already a project dep
    except ImportError:
        logger.warning("   PyMuPDF (fitz) not importable; skipping formula extraction.")
        return False
    hits: list[str] = []
    for f in sorted(dest.glob("*")):
        if f.suffix.lower() != ".pdf":
            continue
        try:
            with fitz.open(f) as doc:
                for pno, pg in enumerate(doc):
                    txt = pg.get_text()
                    if FORMULA_CUES.search(txt):
                        hits.append(f"=== {f.name} p.{pno + 1} ===\n{txt.strip()}\n")
        except Exception as exc:  # noqa: BLE001
            logger.warning("   could not read %s (%s)", f.name, exc)
    if hits:
        (dest / "_formula.txt").write_text("\n".join(hits), encoding="utf-8")
    return bool(hits)


def run(args) -> int:
    # dry-run is READ-ONLY on public pages — no credentials, no login, no downloads.
    user = pw = None
    if not args.dry_run:
        user, pw = _credentials()
    rfts = _read_rfts(args)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    mode = "DRY-RUN triage (no login, no download)" if args.dry_run else "AUTHENTICATED ITT pull"
    logger.info("%s: %d RFT(s), serial @ %.1fs spacing. NOT a crawl.", mode, len(rfts), POLITE_DELAY)

    rows: list[dict] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not args.pause)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (dail-tracker research; mailto:p.glynn18@gmail.com)",
            accept_downloads=True,
        )
        page = ctx.new_page()
        page.set_default_timeout(PAGE_TIMEOUT)
        if not args.dry_run:
            if args.pause:
                # first-run selector discovery: inspector opens, read real DOM, then resume
                page.goto(LOGIN_URL, wait_until="domcontentloaded")
                page.pause()
            _login(page, user, pw)

        for i, rft in enumerate(rfts, 1):
            logger.info("[%d/%d] RFT %s", i, len(rfts), rft)
            rows.append(_dry_one(page, rft) if args.dry_run else _pull_one(page, rft))
            time.sleep(POLITE_DELAY)
        browser.close()

    if args.dry_run:
        fields = ["rft_id", "status", "reachable", "login_wall", "docs_visible", "title", "note"]
        out = OUT_DIR / "dry_run_manifest.csv"
    else:
        fields = ["rft_id", "status", "n_docs", "formula_found", "note"]
        out = MANIFEST
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    if args.dry_run:
        reach = sum(r["reachable"] for r in rows)
        walled = sum(r["login_wall"] for r in rows)
        with_docs = sum(r["docs_visible"] > 0 for r in rows)
        logger.info(
            "DRY-RUN DONE: %d/%d reachable, %d behind login wall, %d expose docs pre-auth. Manifest -> %s.",
            reach, len(rows), walled, with_docs, out,
        )
        for r in rows:
            logger.info(
                "   %s reachable=%s login_wall=%s docs=%d | %s | %s",
                r["rft_id"], r["reachable"], r["login_wall"], r["docs_visible"], r["title"], r["note"],
            )
        return 0

    pulled = sum(r["status"] == "pulled" for r in rows)
    with_formula = sum(r["formula_found"] for r in rows)
    logger.info(
        "DONE: %d/%d pulled, %d had a scoring section. Manifest -> %s (out dir %s).",
        pulled, len(rows), with_formula, out, OUT_DIR,
    )
    for r in rows:
        if r["status"] != "pulled":
            logger.info("   skipped %s: %s", r["rft_id"], r["note"])
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Targeted, authenticated eTenders ITT pull (sandbox probe).")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--rfts", help="comma-separated RFT/resource ids (the allowlist)")
    g.add_argument("--rfts-file", help="file with one RFT id per line")
    ap.add_argument("--max-rfts", type=int, default=25, help="hard cap — guard against accidental harvest")
    ap.add_argument("--dry-run", action="store_true",
                    help="READ-ONLY triage: no login/download/express-interest; just report reachability per RFT")
    ap.add_argument("--pause", action="store_true", help="headed + Playwright inspector for first-run selector discovery")
    args = ap.parse_args()
    setup_standalone_logging("etenders_itt_pull_probe")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
