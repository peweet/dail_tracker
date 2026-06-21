"""legal_diary_openview_poller.py — archive the Courts Service Legal Diary OpenView
jurisdictions (the party-level history the downloadable .docx omits).

COMPANION to pdf_infra/legal_diary_poller.py. That poller grabs the single /download
.docx (Four Courts current day only). This one walks the Domino *OpenView* index per
jurisdiction and archives one HTML detail document per sitting, giving the FULL history
for the Circuit Court (entirely absent from the .docx) and the higher courts.

SOURCE (discovered 2026-06-16, see doc/archive/LEGAL_DIARY_OPENVIEW_BUILD_PLAN.md)
  Index:  /legaldiary.nsf/<slug>?OpenView&Jurisdiction=<slug>&... -> rows
          <tr class="clickable-row" data-url="/legaldiary.nsf/<slug>/<UNID>?OpenDocument">
          cells include a Date and an "Updated" date.
  Detail: /legaldiary.nsf/<slug>/<UNID>?OpenDocument  (HTML, <div class="ld-content">)

INCREMENTAL: each archived detail is keyed by its UNID; the per-jurisdiction manifest
records the index "Updated" stamp we last archived for that UNID. A row is (re)fetched
only when its UNID is new OR its Updated stamp changed — so a daily run touches only the
handful of sittings that actually changed, and a forward-scheduled sitting is refreshed
as its list firms up. The HIGH COURT is NOT polled here (OpenView 500s; the .docx covers
it) and the in-camera "Circuit Court – Family" jurisdiction is never polled.

PRIVACY: the archived HTML is the RAW source — it names private parties AND solicitors in
full. It is git-ignored (see .gitignore "Legal Diary OpenView raw HTML archive"). Only the
anonymised gold parquet from extractors/legal_diary_openview_extract.py may leave this box.

Exit codes (poller convention):
    0  ok — archived new/changed sittings, or already current
    1  transient failure (network / IO) — safe to retry
    2  source drift / human needed — index unparseable for every jurisdiction

CLI:
    python -m pdf_infra.legal_diary_openview_poller                  # incremental, all courts
    python -m pdf_infra.legal_diary_openview_poller --limit 20       # cap new fetches per court (smoke)
    python -m pdf_infra.legal_diary_openview_poller --jurisdictions circuit-court
    python -m pdf_infra.legal_diary_openview_poller --full           # ignore manifest, refetch all
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from config import BRONZE_DIR, DATA_DIR  # noqa: E402
from services.logging_setup import setup_standalone_logging  # noqa: E402

logger = logging.getLogger(__name__)

BASE = "https://legaldiary.courts.ie"
USER_AGENT = "dail-tracker-bot/0.1 (+https://github.com/peweet/dail_tracker; mailto:p.glynn18@gmail.com)"
ARCHIVE_DIR = BRONZE_DIR / "legal_diary_openview"
MANIFEST_PATH = DATA_DIR / "_meta" / "legal_diary_openview_manifest.json"
TIMEOUT = 60
POLITE_DELAY = 0.3  # seconds between detail fetches — gentle on the flaky Domino server
MIN_BYTES = 2_000

# OpenView jurisdictions carrying party-level case lists. high-court 500s (covered by the
# .docx); district-court has no case view (schedule only); Circuit Court – Family is
# in-camera and excluded. "circuit-court" is the Civil & Criminal jurisdiction.
JURISDICTIONS = ["supreme-court", "court-of-appeal", "central-criminal-court", "circuit-court"]

_ROW_RE = re.compile(r'<tr class="clickable-row" data-url="([^"]+)">(.*?)</tr>', re.S)
_CELL_RE = re.compile(r"<td[^>]*data-text=\"([^\"]*)\"[^>]*>(.*?)</td>", re.S)


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _get(sess: requests.Session, url: str, attempts: int = 4) -> requests.Response | None:
    """GET with retry. The Domino server intermittently drops a keep-alive mid-session
    (RemoteDisconnected); a fresh attempt clears it. Returns None if all attempts fail."""
    last: Exception | None = None
    for i in range(attempts):
        try:
            r = sess.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            last = exc
            time.sleep(1.0 * (i + 1))
    logger.warning("GET failed after %d attempts: %s (%s)", attempts, url, last)
    return None


def _index_url(slug: str) -> str:
    return (
        f"{BASE}/legaldiary.nsf/{slug}?OpenView&Jurisdiction={slug}&area=&type=&dateType=Date&dateFrom=&dateTo=&text="
    )


def _unid(data_url: str) -> str | None:
    m = re.search(r"/([0-9A-F]{16,})\?OpenDocument", data_url, re.I)
    return m.group(1) if m else None


def parse_index_rows(html: str) -> list[dict]:
    """One dict per sitting: {data_url, unid, updated}. 'updated' is the LAST data-text
    cell (a sortable YYYYMMDD) — the change-key the incremental fetch compares against."""
    rows: list[dict] = []
    for data_url, body in _ROW_RE.findall(html):
        unid = _unid(data_url)
        if not unid:
            continue
        dts = [dt for dt, _txt in _CELL_RE.findall(body)]
        rows.append({"data_url": data_url, "unid": unid, "updated": dts[-1] if dts else ""})
    return rows


def _load_manifest() -> dict:
    if MANIFEST_PATH.exists():
        with contextlib.suppress(Exception):
            return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    return {}


def _save_manifest(m: dict) -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_text(json.dumps(m, indent=2, ensure_ascii=False), encoding="utf-8")


def poll(args) -> int:
    slugs = [s.strip() for s in (args.jurisdictions or ",".join(JURISDICTIONS)).split(",") if s.strip()]
    bad = [s for s in slugs if s not in JURISDICTIONS]
    if bad:
        logger.error("Unknown jurisdiction(s): %s. Known: %s", bad, JURISDICTIONS)
        return 2

    sess = _session()
    manifest = _load_manifest()
    any_index_ok = False
    total_new = total_skip = total_fail = 0

    for slug in slugs:
        iv = _get(sess, _index_url(slug))
        if iv is None:
            logger.error("Index unreachable for %s — skipping.", slug)
            continue
        rows = parse_index_rows(iv.text)
        if not rows:
            logger.warning("Index for %s parsed 0 rows — structure may have drifted.", slug)
            continue
        any_index_ok = True
        held = manifest.setdefault(slug, {})
        # newest first so a capped smoke run archives the most current sittings
        rows.sort(key=lambda r: r["updated"], reverse=True)
        out_dir = ARCHIVE_DIR / slug
        out_dir.mkdir(parents=True, exist_ok=True)

        stale = [r for r in rows if args.full or held.get(r["unid"]) != r["updated"]]
        capped = stale[: args.limit] if args.limit else stale
        logger.info(
            "%s: %d sittings, %d new/changed%s.",
            slug,
            len(rows),
            len(stale),
            f" (capping to {len(capped)})" if args.limit and len(stale) > len(capped) else "",
        )

        n_new = n_fail = 0
        for r in capped:
            dr = _get(sess, urljoin(BASE, r["data_url"]))
            time.sleep(POLITE_DELAY)
            if dr is None or len(dr.content) < MIN_BYTES:
                n_fail += 1
                continue
            (out_dir / f"{r['unid']}.html").write_bytes(dr.content)
            held[r["unid"]] = r["updated"]
            n_new += 1
        total_new += n_new
        total_fail += n_fail
        total_skip += len(rows) - len(stale)
        logger.info("%s: archived %d, failed %d.", slug, n_new, n_fail)
        _save_manifest(manifest)  # persist after each jurisdiction so a mid-run drop keeps progress

    if not any_index_ok:
        logger.error("No jurisdiction index parsed — source drift or total network failure.")
        return 2
    logger.info(
        "OpenView poll done: %d archived, %d unchanged, %d failed. Manifest -> %s",
        total_new,
        total_skip,
        total_fail,
        MANIFEST_PATH,
    )
    logger.info("Next: ./.venv/Scripts/python.exe extractors/legal_diary_openview_extract.py")
    return 1 if (total_new == 0 and total_fail > 0) else 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Archive the Legal Diary OpenView jurisdictions.")
    ap.add_argument("--limit", type=int, default=0, help="cap new fetches per jurisdiction (0 = no cap)")
    ap.add_argument("--jurisdictions", help=f"comma list; default all of {JURISDICTIONS}")
    ap.add_argument("--full", action="store_true", help="ignore manifest; refetch every sitting")
    args = ap.parse_args()
    setup_standalone_logging("legal_diary_openview_poller")
    return poll(args)


if __name__ == "__main__":
    raise SystemExit(main())
