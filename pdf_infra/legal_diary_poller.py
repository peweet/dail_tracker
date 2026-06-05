"""legal_diary_poller.py — capture the Courts Service daily Legal Diary, repeatably.

The Legal Diary (https://legaldiary.courts.ie/) publishes a fresh MS-Word .docx
every court day and the download page exposes ONLY the current day — there is no
historical URL. So the judiciary legal-diary fact is forward-accumulating: this
poller archives one .docx per diary date, and legal_diary_extract.py rebuilds gold
from the full archive. Run it daily (cron / pipeline step) to build history.

This poller:
  1. fetches the diary landing page and resolves the current .docx download link
     (the page links the live file; the slug/path can rotate, so we discover it);
  2. downloads the .docx and reads its date header ("THURSDAY THE 4TH DAY OF ...");
  3. is idempotent — if we already hold that diary date with the same sha256 it
     skips; a changed file for a known date is archived as <date>.rNN.docx so a
     same-day republish never silently overwrites the first capture;
  4. writes data/bronze/legal_diary/<YYYY-MM-DD>.docx (git-ignored — the raw file
     names private parties) and updates data/_meta/legal_diary_archive_index.json.

PRIVACY: the archived .docx is the RAW source (full party names). It is git-ignored
(see .gitignore "Legal Diary raw daily DOCX archive"). Only the anonymised gold
parquet produced by the extractor may leave this machine.

Exit codes (poller convention, cf. cro_poller / oireachtas_pdf_poller):
    0  ok — archived a new day, or already current
    1  transient failure (network / IO) — safe to retry
    2  source drift / human needed — no .docx link found, or date header unparseable

CLI:
    python -m pdf_infra.legal_diary_poller            # poll + archive if new
    python -m pdf_infra.legal_diary_poller --force    # archive even if already held
    python -m pdf_infra.legal_diary_poller --check-only
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import logging
import re
import sys
import time
import zipfile
from pathlib import Path
from urllib.parse import urljoin

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from config import BRONZE_DIR, DATA_DIR  # noqa: E402
from extractors.legal_diary_extract import diary_date_from_lines  # noqa: E402
from services.logging_setup import setup_standalone_logging  # noqa: E402

logger = logging.getLogger(__name__)

LANDING_URL = "https://legaldiary.courts.ie/"
USER_AGENT = "dail-tracker-bot/0.1 (+https://github.com/peweet/dail_tracker; mailto:p.glynn18@gmail.com)"
ARCHIVE_DIR = BRONZE_DIR / "legal_diary"
INDEX_PATH = DATA_DIR / "_meta" / "legal_diary_archive_index.json"
MIN_BYTES = 10_000
TIMEOUT = 60


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _get(sess: requests.Session, url: str, attempts: int = 3) -> requests.Response:
    """GET with a small retry. The legaldiary Domino/.nsf server intermittently
    drops a keep-alive connection mid-session (RemoteDisconnected); a retry on a
    fresh connection clears it. Raises the last error if all attempts fail."""
    last: Exception | None = None
    for i in range(attempts):
        try:
            r = sess.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            last = exc
            logger.warning("GET %s failed (attempt %d/%d): %s", url, i + 1, attempts, exc)
            time.sleep(1.5 * (i + 1))
    raise last  # type: ignore[misc]


def _find_docx(html: str, base: str) -> str | None:
    """A direct .docx link in this page, if any (e.g. the $File attachment)."""
    hrefs = re.findall(r'href=["\']([^"\']+\.docx[^"\']*)["\']', html, re.I)
    return urljoin(base, hrefs[0]) if hrefs else None


def _find_chooser(html: str, base: str) -> str | None:
    """The /download chooser page link (which itself lists the .docx + .pdf)."""
    hrefs = re.findall(r'href=["\']([^"\']*download[^"\']*)["\']', html, re.I)
    for h in hrefs:
        if h.rstrip("/").lower().endswith("download"):
            return urljoin(base, h)
    return urljoin(base, hrefs[0]) if hrefs else None


def resolve_docx_url(sess: requests.Session, landing_html: str, base: str) -> str | None:
    """Resolve the current diary .docx. The landing page links a /download chooser
    page; the chooser lists the dated .docx ($File attachment). Walk both hops."""
    direct = _find_docx(landing_html, base)
    if direct:
        return direct
    chooser = _find_chooser(landing_html, base)
    if not chooser:
        return None
    try:
        cr = _get(sess, chooser)
    except requests.RequestException as exc:
        logger.error("Download chooser unreachable (%s): %s", chooser, exc)
        return None
    return _find_docx(cr.text, chooser)


def _load_index() -> dict:
    if INDEX_PATH.exists():
        with contextlib.suppress(Exception):
            return json.loads(INDEX_PATH.read_text(encoding="utf-8"))
    return {}


def _save_index(idx: dict) -> None:
    INDEX_PATH.parent.mkdir(parents=True, exist_ok=True)
    INDEX_PATH.write_text(json.dumps(idx, indent=2, ensure_ascii=False), encoding="utf-8")


def _archive_path(diary_date: str, sha: str, idx: dict) -> Path | None:
    """Path to write, or None if this exact (date, sha) is already held."""
    held = idx.get(diary_date)
    if held:
        if held.get("sha256") == sha:
            return None  # identical file already archived — skip
        # known date, changed content -> keep both as a revision
        n = held.get("revisions", 1)
        return ARCHIVE_DIR / f"{diary_date}.r{n + 1:02d}.docx"
    return ARCHIVE_DIR / f"{diary_date}.docx"


def poll(force: bool = False, check_only: bool = False) -> int:
    ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    sess = _session()
    try:
        r = _get(sess, LANDING_URL)
    except requests.RequestException as exc:
        logger.error("Landing page unreachable: %s", exc)
        return 1

    docx_url = resolve_docx_url(sess, r.text, LANDING_URL)
    if not docx_url:
        logger.error("No .docx link found via %s (or its /download chooser) — page structure "
                     "may have drifted.", LANDING_URL)
        return 2
    logger.info("Resolved diary docx: %s", docx_url)

    try:
        d = _get(sess, docx_url)
    except requests.RequestException as exc:
        logger.error("Download failed: %s", exc)
        return 1

    blob = d.content
    if len(blob) < MIN_BYTES:
        logger.error("Downloaded file too small (%d bytes) — not a valid diary.", len(blob))
        return 1
    try:
        lines = read_docx_lines_from_bytes(blob)
    except Exception as exc:  # noqa: BLE001
        logger.error("Downloaded file is not a readable .docx: %s", exc)
        return 2

    diary_date = diary_date_from_lines(lines)
    if not diary_date:
        logger.error("Could not parse the diary date header — source drift.")
        return 2

    sha = hashlib.sha256(blob).hexdigest()
    idx = _load_index()

    if check_only:
        held = idx.get(diary_date)
        status = "current" if held and held.get("sha256") == sha else "update available"
        logger.info("Diary date %s: %s.", diary_date, status)
        return 0

    target = _archive_path(diary_date, sha, idx)
    if target is None and not force:
        logger.info("Diary %s already archived (sha %s) — nothing to do.", diary_date, sha[:16])
        return 0
    if target is None:  # force re-archive of identical file
        target = ARCHIVE_DIR / f"{diary_date}.docx"

    target.write_bytes(blob)
    held = idx.get(diary_date, {})
    idx[diary_date] = {
        "sha256": sha,
        "filename": target.name,
        "bytes": len(blob),
        "source_url": docx_url,
        "revisions": held.get("revisions", 1) + (1 if held and held.get("sha256") != sha else 0),
    }
    _save_index(idx)
    logger.info("Archived diary %s -> %s (%d bytes, sha %s).",
                diary_date, target.name, len(blob), sha[:16])
    logger.info("Next: ./.venv/Scripts/python.exe extractors/legal_diary_extract.py")
    return 0


def read_docx_lines_from_bytes(blob: bytes) -> list[str]:
    import html as _html
    import io

    xml = zipfile.ZipFile(io.BytesIO(blob)).read("word/document.xml").decode("utf-8", "ignore")
    out = []
    for para in re.split(r"</w:p>", xml):
        txt = "".join(re.findall(r"<w:t[^>]*>([^<]*)</w:t>", para))
        txt = _html.unescape(txt).replace("’", "'").strip()
        if txt:
            out.append(txt)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Archive the Courts Service daily Legal Diary.")
    ap.add_argument("--force", action="store_true", help="archive even if already held")
    ap.add_argument("--check-only", action="store_true", help="report update availability; no download archive")
    args = ap.parse_args()
    setup_standalone_logging("legal_diary_poller")
    return poll(force=args.force, check_only=args.check_only)


if __name__ == "__main__":
    raise SystemExit(main())
