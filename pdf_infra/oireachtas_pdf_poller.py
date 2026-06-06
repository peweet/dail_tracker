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
    python -m pdf_infra.oireachtas_pdf_poller  # poll every source

Exit codes:
    0 — clean (nothing new or one+ files downloaded successfully)
    1 — infra failure (index unreachable, any download failed)
    2 — HTML drift (index returned 0 cards — selectors may have shifted)
"""

from __future__ import annotations

import contextlib
import json
import logging
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from config import (
    ATTENDANCE_PDF_DIR,
    ATTENDANCE_PDF_DIR_SEANAD,
    DATA_DIR,
    INTERESTS_PDF_DIR,
    PAYMENTS_PDF_DIR,
    PAYMENTS_PDF_DIR_SEANAD,
)
from pdf_infra.pdf_fingerprint import (
    SUPERSEDED,
    compare,
    load_index,
    save_index,
    sha256_file,
)

logger = logging.getLogger(__name__)

# Per-source content-fingerprint store (lives inside the source's own bronze dir,
# which is gitignored operational state and auto-isolated under tmp_path in tests).
# Detects DAIL-162: a same-filename file whose bytes changed at source.
FINGERPRINT_FILENAME = ".pdf_fingerprints.json"
# Where main() aggregates the cross-source supersession signal (data/_meta IS
# committed, so the flag is visible to a human / source-health, unlike the bronze dir).
SUPERSESSION_LOG_PATH = DATA_DIR / "_meta" / "oireachtas_pdf_supersessions.json"

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
    # Seanad equivalents — same CMS topic pages, Senator filename hints. Land in
    # sibling dirs so the deputies-format ETL never sees them (the Senator chain
    # globs these). The hints are clean: "payments-to-senators" excludes the
    # combined "payments-to-deputies-senators-and-ministers" PDF.
    "payments_seanad": PollSource(
        name="payments_seanad",
        topic_slug="parliamentary-allowances",
        target_dir=PAYMENTS_PDF_DIR_SEANAD,
        filename_hint="parliamentary-standard-allowance-payments-to-senators",
    ),
    "attendance_seanad": PollSource(
        name="attendance_seanad",
        topic_slug="record-of-attendance",
        target_dir=ATTENDANCE_PDF_DIR_SEANAD,
        filename_hint="senators-verification-of-attendance",
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


# ── Phase 3.5: supersession check (DAIL-162) ────────────────────────────────


def _remote_content_length(session: requests.Session, url: str) -> int | None:
    """Cheap size probe via HEAD. Returns None on any error or a missing/garbled
    Content-Length — the conservative input that maps to an UNKNOWN verdict (no
    false supersession). Never raises: a transport hiccup must not fail a poll."""
    try:
        r = session.head(url, allow_redirects=True, timeout=30)
        if r.status_code >= 400:
            return None
        cl = r.headers.get("Content-Length")
        return int(cl) if cl and cl.strip().isdigit() else None
    except Exception:  # noqa: BLE001 — a failed probe is a silent UNKNOWN, by design
        return None


def check_supersessions(
    source: PollSource,
    on_disk_entries: list[IndexEntry],
    session: requests.Session,
    index: dict,
) -> list[dict]:
    """For files we already hold, baseline never-seen ones (no network) and FLAG any
    whose size changed at source. Mutates ``index`` in place; returns one dict per
    suspected supersession. Flag-only by design — we never auto-download the
    replacement into this ETL-globbed dir (it would double-count the period)."""
    superseded: list[dict] = []
    for e in on_disk_entries:
        stored = index.get(e.filename)
        local_path = source.target_dir / e.filename
        if stored is None:
            # First fingerprinting of an on-disk file: record a baseline, no network,
            # no comparison. Comparison begins on the NEXT run, once a baseline exists.
            if local_path.exists():
                index[e.filename] = {
                    "sha256": sha256_file(local_path),
                    "bytes": local_path.stat().st_size,
                    "source_url": e.url,
                }
            continue
        remote = _remote_content_length(session, e.url)
        verdict = compare(stored.get("bytes"), remote)
        if verdict == SUPERSEDED:
            superseded.append(
                {
                    "source": source.name,
                    "filename": e.filename,
                    "url": e.url,
                    "held_bytes": stored.get("bytes"),
                    "remote_bytes": remote,
                }
            )
            logger.warning(
                "[%s] SUPERSEDED at source: %s (held %s B, remote %s B). The held copy "
                "may be stale — re-download with overwrite to refresh gold.",
                source.name,
                e.filename,
                stored.get("bytes"),
                remote,
            )
    return superseded


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
            "superseded": [],
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
            "superseded": [],
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
            "superseded": [],
        }

    new_entries = filter_new(source, entries)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    # DAIL-162 supersession check on the files we already hold: baseline the new ones,
    # flag any whose size changed at source. Defensive — never raises, never blocks
    # the download phase below.
    fp_path = source.target_dir / FINGERPRINT_FILENAME
    index = load_index(fp_path)
    new_filenames = {e.filename for e in new_entries}
    on_disk_entries = [e for e in entries if e.filename not in new_filenames]
    try:
        superseded = check_supersessions(source, on_disk_entries, session, index)
    except Exception:  # noqa: BLE001 — fingerprinting must never break a poll
        logger.exception("[%s] supersession check failed (continuing)", source.name)
        superseded = []

    downloaded: list[dict] = []
    failed = 0
    for entry in new_entries:
        try:
            path = download(source, entry, session)
            downloaded.append({**asdict(entry), "saved_to": str(path)})
            # Record the fresh download's fingerprint so a later in-place replacement
            # is detectable on the next run.
            index[entry.filename] = {
                "sha256": sha256_file(path),
                "bytes": path.stat().st_size,
                "source_url": entry.url,
            }
            logger.info("[%s] downloaded: %s", source.name, path.name)
        except Exception as exc:
            failed += 1
            logger.error(
                "[%s] download failed for %s: %s",
                source.name,
                entry.url,
                exc,
            )

    with contextlib.suppress(Exception):  # persisting fingerprints is best-effort
        save_index(fp_path, index)

    logger.info(
        "[%s] poll done — scanned=%d already_on_disk=%d downloaded=%d failed=%d superseded=%d",
        source.name,
        len(entries),
        len(entries) - len(new_entries),
        len(downloaded),
        failed,
        len(superseded),
    )
    return {
        "source": source.name,
        "status": "ok",
        "scanned": len(entries),
        "already_on_disk": len(entries) - len(new_entries),
        "downloaded": len(downloaded),
        "downloads_failed": failed,
        "new": downloaded,
        "superseded": superseded,
    }


def run_all() -> dict[str, dict]:
    return {name: run_one(src) for name, src in SOURCES.items()}


def write_supersession_log(results: dict[str, dict], path: Path = SUPERSESSION_LOG_PATH) -> int:
    """Aggregate every source's suspected supersessions into one committed signal
    under data/_meta (visible to a human / source-health). Returns the count.
    Writes the file only when there is something to report, so a clean run leaves
    no noise. Called by main() — never by run_one — so unit tests stay off data/_meta."""
    all_superseded = [s for r in results.values() for s in r.get("superseded", [])]
    if not all_superseded:
        return 0
    payload = {"count": len(all_superseded), "detected": all_superseded}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    return len(all_superseded)


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
    from services.logging_setup import setup_standalone_logging

    setup_standalone_logging("oireachtas_pdf_poller")
    results = run_all()
    print(json.dumps(results, indent=2))
    n_superseded = write_supersession_log(results)
    if n_superseded:
        print(
            f"\n[SUPERSEDED] {n_superseded} source file(s) appear superseded (same filename, "
            f"changed size) - held copies may be stale. See {SUPERSESSION_LOG_PATH}"
        )
    sys.exit(_exit_code(results))
