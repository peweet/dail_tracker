"""
oireachtas_pdf_poller.py — discover and download new PDFs from the
oireachtas.ie publications index.

Each source is described by a PollSource entry in SOURCES. The poller
itself doesn't care what 'payments' or 'attendance' is — it runs the
same fetch → parse → filter → download loop with whichever config it's
handed. Adding a new source = one dict entry.

Pipeline usage:
    Added as a STEP in pipeline.py. Polls every source defined in SOURCES.
    Anything new lands in the source's target_dir, where the existing ETL
    glob picks it up on the same run.

Standalone usage:
    python oireachtas_pdf_poller.py            # poll every source
    python run_payments_poll.py                # poll one source (click-and-run)

Exit codes:
    0 — clean (nothing new or one+ files downloaded successfully)
    1 — infra failure (index unreachable, any download failed)
    2 — HTML drift (index returned 0 cards — selectors may have shifted)
"""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from config import ATTENDANCE_PDF_DIR, INTERESTS_PDF_DIR, PAYMENTS_PDF_DIR

logger = logging.getLogger(__name__)

USER_AGENT = "dail-tracker-bot/0.1 (+https://github.com/peweet/dail_tracker; mailto:p.glynn18@gmail.com)"
INDEX_BASE = "https://www.oireachtas.ie/en/publications/?topic%5B%5D={slug}&resultsPerPage=50"

# Selectors confirmed against all three topic pages — same CMS template
# across parliamentary-allowances, record-of-attendance, and
# register-of-members-interests.
_CARD_SEL = "div.c-publications-list__item"
_LINK_SEL = "p.c-publications-list__view a[href]"
_DATE_SEL = "p.c-publications-list__date"
_TITLE_SEL = "p.c-publications-list__title"


@dataclass(frozen=True)
class PollSource:
    """The five things that vary across publications topics."""

    name: str
    topic_slug: str
    target_dir: Path
    filename_hint: str
    allowed_file_types: frozenset = field(default_factory=lambda: frozenset({"pdf"}))
    min_file_bytes: int = 10_000

    @property
    def index_url(self) -> str:
        return INDEX_BASE.format(slug=self.topic_slug)


SOURCES: dict[str, PollSource] = {
    "payments": PollSource(
        name="payments",
        topic_slug="parliamentary-allowances",
        target_dir=PAYMENTS_PDF_DIR,
        filename_hint="parliamentary-standard-allowance-payments-to-deputies",
    ),
    "attendance": PollSource(
        name="attendance",
        topic_slug="record-of-attendance",
        target_dir=ATTENDANCE_PDF_DIR,
        filename_hint="deputies-verification-of-attendance",
    ),
    # Hint deliberately stops at 'register-of-member' so it catches both
    # pre-2022 ('register-of-members-interests-...') and 2022+
    # ('register-of-member-s-interests-...') filename forms, plus supplements.
    "interests": PollSource(
        name="interests",
        topic_slug="register-of-members-interests",
        target_dir=INTERESTS_PDF_DIR,
        filename_hint="register-of-member",
    ),
}


@dataclass
class IndexEntry:
    url: str
    file_type: str
    title: str
    pub_date_raw: str
    filename: str


# ── Phase 1: fetch ──────────────────────────────────────────────────────────


def fetch_index_html(source: PollSource) -> str:
    resp = requests.get(
        source.index_url,
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    resp.raise_for_status()
    # Force utf-8: the site sends utf-8 but meta-tag detection sometimes
    # mis-fires, which would turn 'Dáil Éireann' into 'D�il �ireann'.
    resp.encoding = "utf-8"
    return resp.text


# ── Phase 2: parse ──────────────────────────────────────────────────────────


def parse_index(source: PollSource, html: str) -> list[IndexEntry]:
    """Walk publication cards and yield matching entries.

    Anything filtered out is logged at INFO with the reason — that way a
    site change (different hint, new file type, redirect) is visible
    rather than silent.
    """
    soup = BeautifulSoup(html, "html.parser")
    accepted: list[IndexEntry] = []
    skipped: list[tuple[str, list[str]]] = []
    for card in soup.select(_CARD_SEL):
        link = card.select_one(_LINK_SEL)
        if not link:
            continue
        href = link["href"]
        ftype = link.get("data-file-type", "")
        reasons: list[str] = []
        if "data.oireachtas.ie" not in href:
            reasons.append("not on data.oireachtas.ie")
        if source.filename_hint not in href:
            reasons.append(f"hint {source.filename_hint!r} missing")
        if ftype not in source.allowed_file_types:
            reasons.append(f"file_type={ftype!r} not in {sorted(source.allowed_file_types)}")
        if reasons:
            skipped.append((href, reasons))
            continue
        title_el = card.select_one(_TITLE_SEL)
        date_el = card.select_one(_DATE_SEL)
        accepted.append(
            IndexEntry(
                url=href,
                file_type=ftype,
                title=title_el.get_text(" ", strip=True) if title_el else "",
                pub_date_raw=date_el.get_text(" ", strip=True) if date_el else "",
                filename=href.rsplit("/", 1)[-1],
            )
        )
    if skipped:
        logger.info(
            "[%s] filtered %d index entries (sample: %s)",
            source.name,
            len(skipped),
            skipped[:3],
        )
    return accepted


# ── Phase 3: filter against on-disk ─────────────────────────────────────────


def filter_new(source: PollSource, entries: list[IndexEntry]) -> list[IndexEntry]:
    """Drop entries whose filename is already in target_dir.

    Also sweeps any stale .tmp files from a previously-interrupted run so
    they can't collide with a new download stream sharing the same name.
    """
    source.target_dir.mkdir(parents=True, exist_ok=True)
    for stale in source.target_dir.glob("*.tmp"):
        try:
            stale.unlink()
            logger.warning("[%s] removed stale tmp: %s", source.name, stale.name)
        except OSError:
            pass
    existing = {p.name for p in source.target_dir.glob("*") if p.is_file()}
    return [e for e in entries if e.filename not in existing]


# ── Phase 4: download ───────────────────────────────────────────────────────


def download(source: PollSource, entry: IndexEntry, session: requests.Session) -> Path:
    """Stream to <name>.tmp, sanity-check, then atomic rename.

    On any error the .tmp is unlinked so a partial file never becomes
    visible to downstream ETL globs.
    """
    final = source.target_dir / entry.filename
    tmp = final.with_suffix(final.suffix + ".tmp")
    try:
        with session.get(entry.url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        size = tmp.stat().st_size
        if size < source.min_file_bytes:
            raise ValueError(f"suspiciously small download ({size} bytes < {source.min_file_bytes}): {entry.url}")
        # First five bytes of a real PDF are b'%PDF-'. Cheap protection
        # against HTML error pages mis-served with a PDF content-type.
        if entry.file_type == "pdf":
            with open(tmp, "rb") as f:
                head = f.read(5)
            if head != b"%PDF-":
                raise ValueError(f"not a PDF (first 5 bytes = {head!r}): {entry.url}")
        tmp.rename(final)
        return final
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


# ── Orchestrators ───────────────────────────────────────────────────────────


def run_one(source: PollSource) -> dict:
    """Poll one source. Always returns a summary dict (never raises)."""
    logger.info(
        "[%s] poll start — index=%s target=%s",
        source.name,
        source.index_url,
        source.target_dir,
    )

    try:
        html = fetch_index_html(source)
    except Exception as exc:
        logger.error("[%s] index fetch failed: %s", source.name, exc)
        return {
            "source": source.name,
            "status": "fetch_failed",
            "error": str(exc),
            "scanned": 0,
            "already_on_disk": 0,
            "downloaded": 0,
            "downloads_failed": 0,
            "new": [],
        }

    try:
        entries = parse_index(source, html)
    except Exception as exc:
        logger.exception("[%s] parse failed", source.name)
        return {
            "source": source.name,
            "status": "parse_failed",
            "error": str(exc),
            "scanned": 0,
            "already_on_disk": 0,
            "downloaded": 0,
            "downloads_failed": 0,
            "new": [],
        }

    if not entries:
        logger.warning(
            "[%s] 0 matching entries on the index — selectors or filename hint may have drifted. Check %s",
            source.name,
            source.index_url,
        )
        return {
            "source": source.name,
            "status": "no_entries",
            "scanned": 0,
            "already_on_disk": 0,
            "downloaded": 0,
            "downloads_failed": 0,
            "new": [],
        }

    new_entries = filter_new(source, entries)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    downloaded: list[dict] = []
    failed = 0
    for entry in new_entries:
        try:
            path = download(source, entry, session)
            downloaded.append({**asdict(entry), "saved_to": str(path)})
            logger.info("[%s] downloaded: %s", source.name, path.name)
        except Exception as exc:
            failed += 1
            logger.error(
                "[%s] download failed for %s: %s",
                source.name,
                entry.url,
                exc,
            )

    logger.info(
        "[%s] poll done — scanned=%d already_on_disk=%d downloaded=%d failed=%d",
        source.name,
        len(entries),
        len(entries) - len(new_entries),
        len(downloaded),
        failed,
    )
    return {
        "source": source.name,
        "status": "ok",
        "scanned": len(entries),
        "already_on_disk": len(entries) - len(new_entries),
        "downloaded": len(downloaded),
        "downloads_failed": failed,
        "new": downloaded,
    }


def run_all() -> dict[str, dict]:
    return {name: run_one(src) for name, src in SOURCES.items()}


def _exit_code(results: dict[str, dict]) -> int:
    """0 = clean · 1 = infra failure · 2 = HTML drift (needs human)."""
    statuses = {r["status"] for r in results.values()}
    if "no_entries" in statuses:
        return 2
    if (
        "fetch_failed" in statuses
        or "parse_failed" in statuses
        or any(r.get("downloads_failed", 0) > 0 for r in results.values())
    ):
        return 1
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    results = run_all()
    print(json.dumps(results, indent=2))
    sys.exit(_exit_code(results))
