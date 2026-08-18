"""ACP EIAR-NIS document TEXT layer — a bounded, polite, privacy-gated fetch + extract pass.

    uv run --locked --extra pipeline --extra ocr python -m pipeline_sandbox.new_sources.abp_doc_text_extract --dry-run
    uv run --locked --extra pipeline --extra ocr python -m pipeline_sandbox.new_sources.abp_doc_text_extract --limit 5
    (--extra ocr is REQUIRED: `uv run --locked` syncs the venv to lockfile + requested extras, so
     the canonical dev invocation UNINSTALLS winocr mid-session. It also installs ocrmypdf, which
     is absent today, so that form needs a network install. To keep the venv exactly as it is and
     stay offline for the install step: uv run --no-sync python -m pipeline_sandbox.new_sources.abp_doc_text_extract
     — and verify the engine immediately before every batch:
        uv run --no-sync python -c "import winocr, fitz; from PIL import Image; print('ok')")

WHY: the index (abp_case_documents*.parquet) holds URLs, not text. The measured value sits in the
scoping documents — the census counts 85 scoping docs corpus-wide — where the applicant records
what the authority asked to be assessed. This script turns a BOUNDED slice of that index into
text, and nothing more.

EVERY DESIGN CHOICE HERE IS A BRAKE, because the corpus is ~32k documents at an 18 MB mean
(≈580 GB if mirrored) and the source is a live public site:
  * --limit caps REQUESTS, not merely documents, and defaults to 10. One document costs one GET
    plus up to RETRY_MAX_ATTEMPTS-1 retries, so a per-document cap would have permitted 3x the
    traffic the flag advertises; the budget is spent by attempt and clamped per document, and the
    same number also caps new documents. There is no unbounded mode.
  * One request at a time, --delay seconds apart. The sleep sits INSIDE the retry loop, so a
    retry is spaced like any other request. services/http_engine.py applies NO rate limit of its
    own (verified: its only sleep is retry backoff), so this delay is the only spacing that
    exists anywhere in the stack. A negative delay is refused outright; 0 is refused unless
    --no-delay is passed explicitly, and that prints a warning.
  * A hard byte cap enforced three ways — Content-Length pre-flight, an in-flight counter, and a
    wall-clock deadline — because a scalar requests timeout under stream=True is per socket read,
    not per download, and Content-Length can be absent or wrong. A size refusal is OUR policy,
    not a publisher fault: it never trips the circuit breaker and is never reported as a
    pleanala.ie failure.
  * OCR is SEQUENTIAL, page-capped AND raster-capped. Never parallel, never multiprocess: this
    box OOMs on concurrent OCR, and PaddleOCR is GPU-only and crashes here. winocr only. The dpi
    is clamped per page so no single sheet exceeds OCR_MAX_PIXELS, because this corpus carries A0
    drawing sheets that a flat 200 dpi turns into a ~62 Mpx (~186 MB) pixmap; free RAM is
    re-checked per page rather than once per run. tools/hooks/guard_memory.py does NOT gate this
    command — its OCR pattern is `paddleocr|ocr_run|run_ocr`, which does not match this module —
    so the RAM floor is enforced in-process here or not at all.
  * Only the applicant /publicaccess/EIAR-NIS/ tree is eligible, and only file_ext == 'pdf'
    within it. That ALLOW-list is the one filter doing real work.

WHY ORDERS, DIRECTIONS, REPORTS AND BMRs ARE OUT — and it is not a filter. Those documents (the
ones verified to name private individuals with home addresses) are absent from this index
STRUCTURALLY: they live in abp_case_decision_docs.parquet, a separate table this script never
reads. The `_DENY_URL` filter and the `privacy_tier == 'public'` filter in eligible() are cheap
invariants kept for a future producer change, NOT controls that fire: measured 2026-08-18, the
index carries zero order/direction/report/BMR URLs and privacy_tier == 'public' on 5,261 of
5,261 rows, so both remove nothing. Cite the allow-list and the table split as the exclusion —
never those two filters.

PRIVACY POSTURE: detect-and-quarantine, NOT redact-and-publish. The repo has no general-purpose
prose name redactor (extractors/legal_diary_extract.py:609 handles court case titles only;
services/logging_cloud.py:273 handles secrets and emails only), so any row carrying body text is
written with privacy_tier='review_personal_data' and public_display=False, and a residual-token
scan counts probable personal-data hits before the write. A PrivacyInvariantError — never an
assert, because -O strips asserts — refuses the whole write if a text-bearing row ever leaves
here marked 'public'.

ISOLATION: sandbox only. Writes under data/_sandbox/dail_new_sources/ plus the shared
data/_meta/fetch_failures.json failure report. Nothing promoted. Polars only.

Licence: doc/source_licensing.md does not exist and no repo doc records pleanala re-use terms.
Re-use of the APPLICANT's documents (third-party copyright, not ACP's own output) is UNRESOLVED —
that is why this stays a bounded sandbox extract and nothing here is promotable.
"""
from __future__ import annotations

# isort: off
# MUST stay first: caps the BLAS thread count before polars/numpy load. Ordering is the
# contract — once numpy is imported the cap is a no-op. See services/runtime_env.py.
import services.runtime_env  # noqa: F401
# isort: on

import argparse
import hashlib
import io
import json
import math
import re
import time
from datetime import UTC, datetime
from pathlib import Path

import fitz  # PyMuPDF — born-digital text first; OCR only where text is genuinely absent
import polars as pl
import requests

from pipeline_sandbox.new_sources import _common
from pipeline_sandbox.new_sources.abp_inspector_reports import scan_flags
from services.fetch_report import Breaker, FetchReport, classify_body, classify_exception
from services.http_engine import (
    RETRY_BACKOFF_BASE,
    RETRY_MAX_ATTEMPTS,
    RETRY_STATUS_FORCELIST,
    polite_headers,
    session,
)
from services.parquet_io import save_parquet

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SANDBOX = PROJECT_ROOT / "data" / "_sandbox" / "dail_new_sources"
# _common.py:20 points at c:/tmp/dail_new_sources, which holds no bronze and no silver; importing
# it silently re-creates those empty dirs. Repoint before anything reads a path.
_common.ROOT = SANDBOX
_common.BRONZE = SANDBOX / "bronze"
_common.SILVER = SANDBOX / "silver"
BRONZE = _common.BRONZE
SILVER = _common.SILVER

SOURCE = "abp_doc_text"
INDEX_EXPANDED = SILVER / "abp_case_documents_expanded.parquet"
INDEX_BASE = SILVER / "abp_case_documents.parquet"
TEXT_CACHE = SILVER / "abp_doc_text_cache"
PDF_CACHE = BRONZE / SOURCE
MISSES = SILVER / "abp_doc_text_misses.tsv"
PUBLISHER_ID = "pleanala_ie"
PUBLISHER_NAME = "An Coimisiún Pleanála"

# --- brakes -----------------------------------------------------------------------------------
DELAY_S = 1.0  # double abp_case_documents.py:49's 0.5s — a PDF GET is ~35x heavier than a page GET
MIN_DELAY_S = 0.25  # below this it is not a brake; --force-large is the deliberate override
DEFAULT_LIMIT = 10
DEFAULT_OCR_LIMIT = 25  # rasterising is the RAM cost here, and cache hits do not spend --limit
MAX_LIMIT = 200  # requests, not documents. A bigger run is a deliberate decision (--force-large)
MAX_DOC_MB = 80  # 4.4x the 18 MB mean; above this it is a drawing/photomontage volume, not prose
DOWNLOAD_DEADLINE_S = 600  # wall clock per document; a scalar timeout does not bound this
TIMEOUTS = (10, 120)  # (connect, read) — a tuple, so a stalled read cannot hang on the connect budget
MAX_PDF_PAGES = 400  # extract the first N pages; beyond that the row is flagged truncated
OCR_MAX_PAGES = 12  # the ABP-shaped precedent (eplanning_pull_ocr.py:38); rasterising is the RAM cost
OCR_DPI = 200
# The precedent's page cap was sized on A4 decision orders ("decision orders are short",
# eplanning_pull_ocr.py:33). This corpus is EIAR/NIS volumes with A0/A1 drawing and photomontage
# sheets: A0 at 200 dpi is 6,620 x 9,360 px = 62 Mpx ≈ 186 MB as an RGB pixmap, and the PNG encode
# buffer plus PIL's decoded copy inside winocr hold two more. Cap the RASTER, not just the pages.
OCR_MAX_PIXELS = 12_000_000  # A4 at 200 dpi is 3.9 Mpx, so an ordinary page is never downsampled
OCR_MIN_DPI = 96  # below this winocr reads nothing useful even on large-format sheets
RAM_FLOOR_MB = 1500  # mirrors tools/hooks/guard_memory.py:58 — refuse to START rather than die mid-batch

# --- eligibility ------------------------------------------------------------------------------
# ALLOW-list, not a deny-list: the applicant EIAR-NIS tree only. The (?i) is INLINE rather than
# re.I because the same strings are handed to polars' Rust regex engine, which never sees a
# Python flag argument — and the served href case does vary (the parser lowercases before
# comparing at abp_case_documents.py:222).
_ALLOW_URL = r"(?i)/publicaccess/EIAR-NIS/"
# Hard refusal. Same path shape as _DECISION_HREF (abp_case_documents.py:70-72), which is what
# populates abp_case_decision_docs.doc_kind — so the discriminator is the one already measured.
_DENY_URL = r"(?i)media/abp/cases/(reports|orders|directions|bmr)/"
# "EIAR-category" is not a controlled vocabulary — doc_category is applicant free text (project
# names, 234 nulls). Matched loosely across category/rel_path/filename and REPORTED as a count.
_EIAR_CAT = r"(?i)eiar|e\.i\.a\.r|environmental\s*impact|\bnis\b|natura"

# --- privacy ----------------------------------------------------------------------------------
# Values must come from services/data_contracts.py:85 PRIVACY_STATUS — do not invent a new literal.
TIER_INDEX_ONLY = "public"
TIER_WITH_TEXT = "review_personal_data"
# The observed leak shape is "Appeal by David Tobin of 3 Bay View, Possess Point, County Sligo"
# (order_308717). These detect; they do not redact.
_PERSONAL_PATTERNS = (
    re.compile(r"\bappeal(?:ed)? by\s+[A-Z][a-z]+\s+[A-Z][a-z]+"),
    re.compile(r"\bof\s+\d{1,4}\s+[A-Z][a-z]+"),
    re.compile(r"\b[AC-FHKNPRTV-Y]\d{2}\s?[0-9AC-FHKNPRTV-Y]{4}\b"),  # Eircode shape
    re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"),  # services/logging_cloud.py:224-227
)

OUT_COLUMNS = [
    "abp_case",
    "filename",
    "full_url",
    "doc_category",
    "is_scoping",
    "n_pages",
    "n_chars",
    "extraction_method",  # fitz_text | winocr | failed
    "confidence",
    "privacy_tier",
    "source_document_hash",
    "fetched_at",
    "text",
    # audit trail — the OCR provenance stays on the row so a later reader can see WHY
    "file_bytes",
    "image_only_pages",
    "is_scanned",
    "needs_ocr",
    "ocr_pages",
    "pages_truncated",
    "personal_data_hits",
    "public_display",
    "error_class",
    "http_status",
    "notes",
]


class PrivacyInvariantError(RuntimeError):
    """A text-bearing row was about to be written with a public tier. Hard gate, never an assert."""


# ---------------------------------------------------------------- helpers


def _now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _doc_key(full_url: str) -> str:
    """Stable cache key derived from the URL alone, so a cache hit costs no request."""
    return hashlib.sha256(full_url.encode("utf-8")).hexdigest()[:16]


def _free_mb() -> int | None:
    try:
        import psutil
    except ImportError:
        return None
    return int(psutil.virtual_memory().available / (1024 * 1024))


def personal_data_hits(text: str) -> int:
    """Count probable personal-data spans. Detection only — there is no redactor in this repo."""
    return sum(len(p.findall(text)) for p in _PERSONAL_PATTERNS)


def _compose_notes(truncated: bool, *extra: str | None) -> str | None:
    """One place that builds `notes`, so the fitz cap and the OCR cap cannot disagree about it."""
    parts = (["truncated_pages"] if truncated else []) + [e for e in extra if e]
    return ";".join(parts) or None


def assert_privacy_invariant(df: pl.DataFrame) -> None:
    """Refuse the whole write if any text-bearing row is tiered public, or any review-tier row is
    flagged for display. A module-level function, not an inline expression in main(), so the gate
    can be EXERCISED: a gate nobody can make fail is indistinguishable from a gate that cannot.

    Both the derived count AND the stored string are tested, so a future edit that lets n_chars
    drift away from text still trips this rather than sliding through on whichever column happens
    to agree. Raises PrivacyInvariantError — never an assert, because -O strips asserts.
    """
    leaked = df.filter(
        ((pl.col("n_chars") > 0) | (pl.col("text").fill_null("").str.strip_chars().str.len_chars() > 0))
        & (pl.col("privacy_tier") == TIER_INDEX_ONLY)
    ).height
    unsafe = df.filter(pl.col("public_display") & (pl.col("privacy_tier") == TIER_WITH_TEXT)).height
    if leaked or unsafe:
        raise PrivacyInvariantError(
            f"refusing to write: {leaked} text-bearing rows tiered '{TIER_INDEX_ONLY}', {unsafe} rows public_display with review tier"
        )


# ---------------------------------------------------------------- fetch (streamed, capped)


def download_pdf(
    url: str,
    dest: Path,
    *,
    max_bytes: int,
    delay: float,
    max_attempts: int = RETRY_MAX_ATTEMPTS,
    refresh: bool = False,
) -> dict:
    """Stream one PDF to `dest` atomically, under a byte cap and a wall-clock deadline.

    Deliberately NOT services/http_engine.download_file: that helper streams correctly but has no
    byte cap, and its curl fallback reads the whole body into memory, which re-introduces exactly
    the RAM risk the streaming leg avoids. This keeps the engine's session, headers and retry
    constants and owns only the loop. Two transports in one ingest is a real cost — do not "tidy"
    this back onto _common.fetch, which has no retry and calls r.content.

    `delay` is slept before EVERY attempt, retries included: the retry backoff is a fault-recovery
    interval, not a politeness interval, and the caller-side sleep this replaced spaced only the
    first GET of each document. `max_attempts` lets the caller clamp a document to the request
    budget it has left, so --limit bounds requests rather than documents.

    An already-downloaded `dest` is served from disk with NO request unless `refresh` — that is
    what makes an OCR back-fill of a retained PDF genuinely offline. `fetched_at` is then the
    file's mtime, not now, because the bytes were fetched then and not now.

    Returns a dict: ok, error_class, http_status, sha256, bytes, attempts, from_cache, fetched_at,
    source_last_modified.
    """
    out = {
        "ok": False,
        "error_class": None,
        "http_status": None,
        "sha256": None,
        "bytes": 0,
        "attempts": 0,
        "from_cache": False,
        "fetched_at": _now_iso(),
        "source_last_modified": None,
    }
    headers = polite_headers(extra={"Accept": "application/pdf,*/*;q=0.8", "Accept-Language": "en-IE,en;q=0.9"})
    tmp = dest.with_name(dest.name + ".part")
    dest.parent.mkdir(parents=True, exist_ok=True)

    if dest.exists() and not refresh:
        # Hashed in chunks, never read whole: an 80 MB body held as bytes is the RAM risk this
        # whole function is shaped to avoid.
        digest = hashlib.sha256()
        total = 0
        with dest.open("rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 16), b""):
                digest.update(chunk)
                total += len(chunk)
        out.update(
            ok=True,
            sha256=digest.hexdigest(),
            bytes=total,
            from_cache=True,
            fetched_at=datetime.fromtimestamp(dest.stat().st_mtime, UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        return out

    for attempt in range(1, max_attempts + 1):
        if delay > 0:
            time.sleep(delay)  # per REQUEST, retries included — see the docstring
        out["attempts"] = attempt
        started = time.monotonic()
        try:
            with session.get(url, headers=headers, timeout=TIMEOUTS, stream=True, allow_redirects=True) as r:
                out["http_status"] = r.status_code
                if r.status_code in RETRY_STATUS_FORCELIST and attempt < max_attempts:
                    # The fault-recovery backoff is a LOCAL. Rebinding `delay` here replaced the
                    # operator's politeness spacing with a server-chosen one for the rest of the
                    # document: `Retry-After: 0` is a digit string, so it disabled the only brake
                    # in the stack outright, and with no header `--delay 5` collapsed to 0.5s —
                    # speeding up exactly when the server signalled distress. The top-of-loop sleep
                    # already spaces this retry by `delay`, so top up to the longer of the two and
                    # never sleep both.
                    retry_after = r.headers.get("retry-after", "")
                    backoff = float(retry_after) if retry_after.isdigit() else RETRY_BACKOFF_BASE * 2 ** (attempt - 1)
                    if backoff > delay:
                        time.sleep(backoff - delay)
                    continue
                r.raise_for_status()
                out["source_last_modified"] = r.headers.get("last-modified")
                declared = r.headers.get("content-length")
                if declared and declared.isdigit() and int(declared) > max_bytes:
                    # Pre-flight refusal: the connection closes without consuming the body.
                    out["error_class"] = "oversize"
                    out["bytes"] = int(declared)
                    return out
                digest = hashlib.sha256()
                total = 0
                head = b""
                with tmp.open("wb") as fh:
                    for chunk in r.iter_content(1 << 16):
                        if not chunk:
                            continue
                        total += len(chunk)
                        if total > max_bytes:  # Content-Length can be absent or lie
                            out["error_class"] = "oversize"
                            out["bytes"] = total
                            tmp.unlink(missing_ok=True)
                            return out
                        if time.monotonic() - started > DOWNLOAD_DEADLINE_S:
                            out["error_class"] = "download_deadline"
                            tmp.unlink(missing_ok=True)
                            return out
                        if len(head) < 2048:
                            head += chunk[: 2048 - len(head)]
                        digest.update(chunk)
                        fh.write(chunk)
                # A 200 is not a PDF: a WAF interstitial arrives with the same status code.
                body_class = classify_body(head, expected_magic=b"%PDF")
                if body_class is not None:
                    out["error_class"] = body_class
                    tmp.unlink(missing_ok=True)
                    return out
                tmp.replace(dest)
                out.update(ok=True, sha256=digest.hexdigest(), bytes=total)
                return out
        except requests.RequestException as exc:
            error_class, status = classify_exception(exc)
            out["error_class"] = error_class
            out["http_status"] = status if status is not None else out["http_status"]
            tmp.unlink(missing_ok=True)
            # Permanent 4xx (403 and 404 stay DISTINCT in the report) never retries.
            if status is not None and 400 <= status < 500:
                return out
            if attempt < max_attempts:
                time.sleep(RETRY_BACKOFF_BASE * 2 ** (attempt - 1))
                continue
            return out
    # Retry budget spent on 429/5xx: name the status rather than reporting a bare failure.
    if out["error_class"] is None:
        out["error_class"] = f"http_{out['http_status']}" if out["http_status"] else "unknown"
    return out


# ---------------------------------------------------------------- extract


def extract_text(path: Path, max_pages: int = MAX_PDF_PAGES) -> tuple[str, int, int, int, bool]:
    """→ (text, page_count, pages_read, image_only_pages, truncated).

    Same rule as abp_inspector_reports.extract_pdf:231-246 — pages joined with a form feed, a page
    counts as image-only when it carries an image but under 50 characters — with two deliberate
    differences for this corpus: the file is opened BY PATH (an 80 MB body held as bytes alongside
    fitz's page cache is the RAM risk here, and that helper takes bytes), and the page walk is
    capped so a 2,000-page photomontage volume cannot run the box out of memory.

    `pages_read` is returned SEPARATELY from `page_count` because scan_flags divides img_only by
    the page count it is given: feeding it the untruncated 900 while img_only could only ever
    reach 400 records is_scanned=False on a fully-scanned volume. The caller passes pages_read to
    scan_flags and keeps page_count for the row's disclosure column.
    """
    with fitz.open(path) as doc:
        n_pages = doc.page_count
        take = min(n_pages, max_pages)
        texts, img_only = [], 0
        for i in range(take):
            page = doc[i]
            t = page.get_text()
            if len(t.strip()) < 50 and page.get_images():
                img_only += 1
            texts.append(t)
        return "\f".join(texts), n_pages, take, img_only, take < n_pages


def _winocr_run():
    """winocr closure — the public repo has no shared helper, only this same 8-line shape in four
    files (council_minutes_ocr_recover.py:41-50 and siblings). Duplicated here on purpose: the one
    shared implementation lives in the PRIVATE planning/product tree and must not be imported into
    public code. recognize_pil_sync takes a PIL Image (not bytes, not a path) and returns a plain
    dict carrying both ['text'] and ['lines']; the non-sync variant returns a WinRT
    IAsyncOperation, not a coroutine, so it can never be awaited.
    """
    import winocr
    from PIL import Image

    def run(png: bytes) -> str:
        img = Image.open(io.BytesIO(png))
        res = winocr.recognize_pil_sync(img, "en")
        text = res.get("text") if isinstance(res, dict) else getattr(res, "text", "")
        if not text and isinstance(res, dict):
            text = "\n".join(ln["text"] for ln in res.get("lines", []))
        return text or ""

    return run


def _page_dpi(rect) -> int:
    """Clamp the dpi so one page's raster stays under OCR_MAX_PIXELS. fitz rects are in points."""
    w_in = max(float(rect.width), 1.0) / 72.0
    h_in = max(float(rect.height), 1.0) / 72.0
    px = w_in * h_in * OCR_DPI * OCR_DPI
    if px <= OCR_MAX_PIXELS:
        return OCR_DPI
    return max(OCR_MIN_DPI, int(OCR_DPI * math.sqrt(OCR_MAX_PIXELS / px)))


def ocr_pdf(path: Path, ocr, max_pages: int = OCR_MAX_PAGES) -> tuple[str, int, str | None]:
    """→ (text, pages_ocred, note). Rasterise then OCR, SEQUENTIALLY. Never parallel and never
    multiprocess — concurrent OCR is what OOMs this box. `del pix, png` each iteration bounds the
    peak to ONE page, and _page_dpi bounds the size of that page: the page cap alone does not,
    because a single A0 sheet at a flat 200 dpi is a ~186 MB pixmap plus its encode and decode
    copies. Free RAM is re-checked per page, so a run that started above the floor still aborts
    the document rather than paging when something else on the box takes the memory."""
    chunks: list[str] = []
    note: str | None = None
    done = 0
    with fitz.open(path) as doc:
        take = min(doc.page_count, max_pages)
        for i in range(take):
            free = _free_mb()
            if free is not None and free < RAM_FLOOR_MB:
                note = "ocr_ram_abort"
                print(f"[ocr] ABORT {path.name} at page {i + 1}/{take}: {free} MB free < {RAM_FLOOR_MB} MB floor")
                break
            page = doc[i]
            pix = page.get_pixmap(dpi=_page_dpi(page.rect))
            png = pix.tobytes("png")
            chunks.append(ocr(png))
            del pix, png
            done = i + 1
    return "\n".join(chunks), done, note


# ---------------------------------------------------------------- selection


def load_index() -> pl.DataFrame:
    """The expanded index if it exists, else the original 52-case extract."""
    path = INDEX_EXPANDED if INDEX_EXPANDED.exists() else INDEX_BASE
    if not path.exists():
        raise FileNotFoundError(f"no document index found at {INDEX_EXPANDED} or {INDEX_BASE}")
    print(f"[index] {path.name}")
    return pl.read_parquet(path)


def eligible(df: pl.DataFrame, kind: str) -> pl.DataFrame:
    """Allow-list first, belt-and-braces second, then the --kind slice. Counts printed at each step
    so a filter that silently empties the queue is visible rather than looking like 'no documents'.

    ONLY the allow-list and the file_ext filter do real work. The deny-list and the privacy_tier
    filter are kept as cheap invariants against a future producer change and are advertised as
    nothing more: measured 2026-08-18, this index carries ZERO order/direction/report/BMR URLs
    (they live in abp_case_decision_docs.parquet, a table this script never opens) and
    privacy_tier is a hardcoded 'public' literal (abp_case_documents.py:539) on 5,261 of 5,261
    rows. Both therefore exclude nothing today, and their printed deltas must never be read as a
    privacy control that fired.
    """
    n0 = df.height
    df = df.filter(pl.col("full_url").str.contains(_ALLOW_URL))
    n_allow = df.height
    df = df.filter(~pl.col("full_url").str.contains(_DENY_URL))
    n_deny = df.height
    df = df.filter(pl.col("file_ext") == "pdf")  # .msg/.zip/.dwg/.shp are refused with no exception
    n2 = df.height
    df = df.filter(pl.col("privacy_tier") == "public")
    n3 = df.height
    if kind == "scoping":
        df = df.filter(pl.col("is_scoping"))
    else:
        pat = _EIAR_CAT
        df = df.filter(
            pl.col("doc_category").fill_null("").str.contains(pat)
            | pl.col("rel_path").fill_null("").str.contains(pat)
            | pl.col("filename").fill_null("").str.contains(pat)
        )
    print(f"[filter] {n0} index rows -> EIAR-NIS allow-list {n_allow} -> pdf {n2} -> --kind {kind} {df.height}")
    print(
        f"[filter] belt-and-braces, expected to remove 0: decision-path deny-list -{n_allow - n_deny}, "
        f"privacy_tier!=public -{n2 - n3}. Decision documents are excluded STRUCTURALLY (they are not "
        "in this index; they live in abp_case_decision_docs.parquet, never read here) — these two "
        "deltas are not the control and must not be cited as one."
    )
    return df.unique(subset=["full_url"], keep="first").sort(["abp_case", "filename"])


def load_misses() -> dict[str, tuple[str, str, int | None]]:
    """full_url -> (abp_case, error_class, http_status). A confirmed http_404 and a parse_* failure
    are permanent; a 403, a bot_challenge or a timeout is retried, because a WAF 403 says nothing
    about whether the document exists — the same permanence logic as abp_inspector_reports.py:332-343,
    plus parse_*: a PDF fitz could not open will not open next run either, and re-queuing it burns
    the whole request budget on the same corrupt bytes every run, forever. Entries are REMOVED on a
    later success (see the main loop), so this file is current state, not cumulative history."""
    if not MISSES.exists():
        return {}
    out: dict[str, tuple[str, str, int | None]] = {}
    for line in MISSES.read_text(encoding="utf-8").splitlines()[1:]:
        parts = line.split("\t")
        if len(parts) >= 3:
            status = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else None
            out[parts[1]] = (parts[0], parts[2], status)
    return out


def save_misses(rows: dict[str, tuple[str, str, int | None]]) -> None:
    """TSV, not the newline-joined case-id list the sibling extractors use: this grain is the
    document, and 403 must stay distinguishable from 404, which a bare id list cannot carry."""
    lines = ["abp_case\tfull_url\terror_class\thttp_status\tts_utc"]
    for url, (case, error_class, status) in sorted(rows.items()):
        lines.append(f"{case}\t{url}\t{error_class}\t{status if status is not None else ''}\t{_now_iso()}")
    MISSES.write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------- main


def main() -> None:
    ap = argparse.ArgumentParser(description="Bounded EIAR-NIS document text extract (fitz first, winocr only where needed)")
    ap.add_argument("--kind", choices=("scoping", "eiar"), default="scoping", help="which slice of the index (default scoping)")
    ap.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"cap on REQUESTS this run, and on new documents (default {DEFAULT_LIMIT}); one document "
        f"costs 1 GET plus up to {RETRY_MAX_ATTEMPTS - 1} retries, all of which spend this budget. "
        "Cached documents and PDFs already on disk are free",
    )
    ap.add_argument("--force-large", action="store_true", help=f"permit --limit above {MAX_LIMIT}, or --delay below {MIN_DELAY_S}s")
    ap.add_argument("--max-mb", type=int, default=MAX_DOC_MB, help=f"per-document byte cap in MB (default {MAX_DOC_MB})")
    ap.add_argument("--delay", type=float, default=DELAY_S, help=f"seconds before EVERY request, retries included (default {DELAY_S}) — the http engine adds none")
    ap.add_argument("--no-delay", action="store_true", help="required to actually run with --delay 0; prints a warning and removes the only rate limit in the stack")
    ap.add_argument(
        "--ocr-limit",
        type=int,
        default=DEFAULT_OCR_LIMIT,
        help=f"cap on documents OCRed this run (default {DEFAULT_OCR_LIMIT}). Separate from --limit: "
        "cache hits and on-disk PDFs cost the publisher nothing but rasterising costs RAM here, so "
        "the back-fill needs its own ceiling",
    )
    ap.add_argument("--no-ocr", action="store_true", help="skip the OCR pass; needs_ocr rows are recorded, not extracted")
    ap.add_argument("--ocr-only", action="store_true", help="back-fill OCR from RETAINED PDFs only; never enters the fetch branch, so the run is offline")
    ap.add_argument("--refresh", action="store_true", help="ignore the text cache and re-fetch/re-extract")
    ap.add_argument("--dry-run", action="store_true", help="report the queue and write nothing; no request is made")
    args = ap.parse_args()

    if args.limit < 1:
        ap.error("--limit must be at least 1: this script has no unbounded mode")
    if args.limit > MAX_LIMIT and not args.force_large:
        ap.error(f"--limit {args.limit} exceeds {MAX_LIMIT}; pass --force-large if that is deliberate")
    # --delay is the ONLY spacing in the stack (http_engine mounts Retry(total=0)), so it gets the
    # same validation --limit gets. A negative value used to reach time.sleep() mid-loop and raise
    # AFTER documents had been fetched but BEFORE save_misses ran, losing the run's miss ledger.
    if args.delay < 0:
        ap.error(f"--delay cannot be negative (got {args.delay}); it is the only rate limit in this stack")
    if args.delay == 0 and not args.no_delay:
        ap.error("--delay 0 removes the only rate limit against a live public site; pass --no-delay as well if that is deliberate")
    if 0 < args.delay < MIN_DELAY_S and not args.force_large:
        ap.error(f"--delay {args.delay} is below the {MIN_DELAY_S}s floor; pass --force-large if that is deliberate")
    if args.delay == 0:
        print(f"[delay] WARNING: --no-delay — requests will be issued back to back against {PUBLISHER_NAME}. There is no other rate limit.")
    if args.ocr_only and args.refresh:
        ap.error("--ocr-only and --refresh contradict: --refresh forces a re-fetch, --ocr-only forbids any request")
    if args.ocr_only and args.no_ocr:
        ap.error("--ocr-only and --no-ocr contradict: the run would do nothing")
    max_bytes = args.max_mb * 1024 * 1024

    queue = eligible(load_index(), args.kind)
    misses = load_misses()
    # parse_* joins http_404 as permanent: fitz will not open next run a file it cannot open now,
    # and the PDF was unlinked, so a retryable parse miss re-downloads the same corrupt bytes and
    # spends the entire request budget on it every run without making progress.
    permanent = {
        u
        for u, (_case, error_class, _status) in misses.items()
        if error_class == "http_404" or error_class.startswith("parse_")
    }
    print(f"[queue] {queue.height} eligible documents  cached_misses={len(misses)} (permanent 404/parse failures skipped: {len(permanent)})")
    if args.dry_run:
        print(f"[dry-run] would spend at most {args.limit} requests on at most {args.limit} new documents; nothing written, no request made")
        return

    TEXT_CACHE.mkdir(parents=True, exist_ok=True)
    ocr = None
    if not args.no_ocr:
        free = _free_mb()
        if free is not None and free < RAM_FLOOR_MB:
            print(f"[ocr] DISABLED: {free} MB free < {RAM_FLOOR_MB} MB floor — refusing to start rather than dying mid-batch")
        else:
            try:
                ocr = _winocr_run()
                print(f"[ocr] winocr ready (sequential, {OCR_DPI} dpi, <={OCR_MAX_PAGES} pages/doc, free={free} MB)")
            except Exception as exc:  # noqa: BLE001 — a pruned venv is the expected cause, not a bug
                print(f"[ocr] DISABLED: winocr unavailable ({type(exc).__name__}) — did `uv run --locked` prune the ocr extra?")

    report = FetchReport("abp_doc_text_extract")
    breaker = Breaker()
    rows: list[dict] = []
    counts = {
        "cached": 0,
        "fetched": 0,
        "requests": 0,
        "from_disk": 0,
        "oversize": 0,
        "failed": 0,
        "ocr": 0,
        "ocr_backfilled": 0,
        "ocr_attempts": 0,
        "ocr_lost": 0,
        "skipped_permanent": 0,
        "privacy_hits": 0,
    }

    for rec in queue.iter_rows(named=True):
        url = rec["full_url"]
        if url in permanent:
            counts["skipped_permanent"] += 1
            continue
        key = _doc_key(url)
        txt_path = TEXT_CACHE / f"{rec['abp_case']}_{key}.txt"
        meta_path = TEXT_CACHE / f"{rec['abp_case']}_{key}.json"
        pdf_path = PDF_CACHE / f"{rec['abp_case']}_{key}.pdf"

        # -- cache hit: no request, no fitz. This is what makes a re-run free — and, because the
        #    PDF is RETAINED for exactly the needs_ocr rows, what makes an OCR back-fill offline:
        #    a row first written with notes='ocr_skipped' is OCRed here from the file already on
        #    disk. Without this the only escape was --refresh, which re-downloaded an 18 MB mean.
        if txt_path.exists() and meta_path.exists() and not args.refresh:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            text = txt_path.read_text(encoding="utf-8", errors="ignore")
            counts["cached"] += 1
            misses.pop(url, None)  # held text is not a miss, whatever an older run recorded
            # The back-fill is BUDGETED. Rasterising is the RAM cost on this box, and this branch
            # is reached by cache hits — which --limit deliberately does not charge, because they
            # cost the publisher nothing. Without its own cap, one --ocr-only run would OCR every
            # queued document (--kind eiar alone selects 1,858 of 5,261 base rows) at up to
            # OCR_MAX_PAGES rasterised pages each. The per-page RAM check aborts one document, not
            # a runaway batch.
            if (
                meta.get("needs_ocr")
                and meta.get("extraction_method") != "winocr"
                and not meta.get("ocr_attempted")
                and ocr is not None
                and pdf_path.exists()
                and counts["ocr_attempts"] < args.ocr_limit
            ):
                counts["ocr_attempts"] += 1
                ocr_text, ocr_pages, ocr_note = ocr_pdf(pdf_path, ocr)
                if len(ocr_text.strip()) > len(text.strip()):
                    text = ocr_text
                    truncated = bool(meta.get("pages_truncated")) or ocr_pages < (meta.get("n_pages") or 0)
                    meta.update(
                        extraction_method="winocr",
                        ocr_pages=ocr_pages,
                        pages_truncated=truncated,
                        notes=_compose_notes(truncated, ocr_note),
                    )
                    txt_path.write_text(text, encoding="utf-8")
                    meta_path.write_text(json.dumps(meta), encoding="utf-8")
                    counts["ocr"] += 1
                    counts["ocr_backfilled"] += 1
                    pdf_path.unlink(missing_ok=True)  # OCR has landed; the bytes are not needed again
                else:
                    # OCR ran and did not beat the fitz text. Record that TERMINALLY: without a
                    # marker the row still says needs_ocr, so every future run re-rasterises the
                    # same document forever and never progresses. The PDF is kept — a later engine
                    # may do better — but this run's verdict is stored.
                    meta["ocr_attempted"] = True
                    meta_path.write_text(json.dumps(meta), encoding="utf-8")
                    counts["ocr_lost"] += 1
        elif args.ocr_only and not pdf_path.exists():
            continue  # --ocr-only issues no request, so with no retained PDF there is nothing to do
        else:
            # With --ocr-only the PDF below is always already on disk and --refresh is refused at
            # parse time, so download_pdf serves it from disk and the run stays offline.
            # --limit binds BOTH new documents and requests, and a document is clamped to the
            # requests still left, so the advertised cap is the real ceiling on GETs.
            if counts["fetched"] >= args.limit or counts["requests"] >= args.limit:
                continue
            if breaker.tripped:
                continue
            budget = min(RETRY_MAX_ATTEMPTS, args.limit - counts["requests"])
            res = download_pdf(
                url,
                pdf_path,
                max_bytes=max_bytes,
                delay=args.delay,  # slept per request INSIDE download_pdf, retries included
                max_attempts=budget,
                refresh=args.refresh,
            )
            counts["requests"] += res["attempts"]
            # A PDF served from disk cost the publisher nothing, so it spends neither budget —
            # otherwise "PDFs already on disk are free" in --limit's help would be false.
            if res["from_cache"]:
                counts["from_disk"] += 1
            else:
                counts["fetched"] += 1
            # An oversize refusal is OUR size policy, not a publisher fault. Feeding it to the
            # breaker let three ordinary large PDFs abort the run and write a false
            # 'pleanala.ie is down' record into the shared data/_meta/fetch_failures.json.
            policy_skip = res["error_class"] == "oversize"
            breaker.record(res["ok"] or policy_skip)
            if not res["ok"]:
                counts["oversize" if policy_skip else "failed"] += 1
                misses[url] = (rec["abp_case"], res["error_class"] or "unknown", res["http_status"])
                if not policy_skip:
                    report.record_failure(
                        publisher_id=PUBLISHER_ID,
                        publisher_name=PUBLISHER_NAME,
                        url=url,
                        error_class=res["error_class"] or "unknown",
                        http_status=res["http_status"],
                        attempts=res["attempts"],
                    )
                rows.append(
                    {
                        **{c: None for c in OUT_COLUMNS},
                        "abp_case": rec["abp_case"],
                        "filename": rec["filename"],
                        "full_url": url,
                        "doc_category": rec.get("doc_category"),
                        "is_scoping": rec["is_scoping"],
                        "n_pages": 0,
                        "n_chars": 0,
                        "extraction_method": "failed",
                        "confidence": "none",
                        "privacy_tier": TIER_INDEX_ONLY,  # no body text stored, so no review needed
                        "fetched_at": res["fetched_at"],
                        "text": None,
                        "file_bytes": res["bytes"],
                        "personal_data_hits": 0,
                        "public_display": False,
                        "error_class": res["error_class"],
                        "http_status": res["http_status"],
                        "notes": "oversize" if res["error_class"] == "oversize" else "fetch_failed",
                    }
                )
                if breaker.tripped:
                    print("[breaker] 3 consecutive failures on www.pleanala.ie — stopping the fetch loop")
                    report.record_breaker_trip(publisher_id=PUBLISHER_ID, publisher_name=PUBLISHER_NAME, files_skipped=0)
                continue

            # -- fitz first
            try:
                text, n_pages, pages_read, img_only, truncated = extract_text(pdf_path)
            except Exception as exc:  # noqa: BLE001 — a corrupt PDF is a coverage stat, not an abort
                counts["failed"] += 1
                misses[url] = (rec["abp_case"], f"parse_{type(exc).__name__}", None)
                rows.append(
                    {
                        **{c: None for c in OUT_COLUMNS},
                        "abp_case": rec["abp_case"],
                        "filename": rec["filename"],
                        "full_url": url,
                        "doc_category": rec.get("doc_category"),
                        "is_scoping": rec["is_scoping"],
                        "n_pages": 0,
                        "n_chars": 0,
                        "extraction_method": "failed",
                        "confidence": "none",
                        "privacy_tier": TIER_INDEX_ONLY,
                        "source_document_hash": res["sha256"],
                        "fetched_at": res["fetched_at"],
                        "text": None,
                        "file_bytes": res["bytes"],
                        "personal_data_hits": 0,
                        "public_display": False,
                        "error_class": f"parse_{type(exc).__name__}",
                        "notes": "fitz_parse_failed",
                    }
                )
                pdf_path.unlink(missing_ok=True)
                continue

            # The heuristic is imported verbatim, and so is its INPUT CONTRACT: img_only and text
            # cover pages_read, so pages_read is the denominator. Passing the untruncated n_pages
            # capped the numerator at 400 while the denominator ran free, recording is_scanned=
            # False on a fully-scanned 900-page volume.
            flags = scan_flags(text, pages_read, img_only)
            method = "fitz_text"
            ocr_pages = 0
            ocr_note = None
            if flags["needs_ocr"] and ocr is not None:
                ocr_text, ocr_pages, ocr_note = ocr_pdf(pdf_path, ocr)
                if len(ocr_text.strip()) > len(text.strip()):
                    # Never overwrite good fitz text with OCR — only take OCR when it wins.
                    text, method = ocr_text, "winocr"
                    counts["ocr"] += 1
                    # The OCR pass has its OWN, far smaller page cap (12 vs 400) and replaces the
                    # text wholesale, so its truncation is the row's truncation. Without this a
                    # 12-of-300-page winocr row asserted pages_truncated=False — a disclosure
                    # column actively denying the truncation it exists to disclose.
                    truncated = truncated or ocr_pages < n_pages
            elif flags["needs_ocr"]:
                ocr_note = "ocr_skipped"
            notes = _compose_notes(truncated, ocr_note)

            meta = {
                "source_document_hash": res["sha256"],
                "fetched_at": res["fetched_at"],
                "file_bytes": res["bytes"],
                "n_pages": n_pages,
                # Recorded, not assumed: a PDF served from disk has no status, and the row must
                # not claim a 200 for a request that was never made.
                "http_status": res["http_status"],
                "image_only_pages": img_only,
                "is_scanned": flags["is_scanned"],
                "needs_ocr": flags["needs_ocr"],
                "extraction_method": method,
                "ocr_pages": ocr_pages,
                "pages_truncated": truncated,
                "notes": notes,
            }
            txt_path.write_text(text, encoding="utf-8")
            meta_path.write_text(json.dumps(meta), encoding="utf-8")
            misses.pop(url, None)  # the ledger is current state, not cumulative history
            # Text-only by default (abp_inspector_reports.py:383-388): the PDF stays ONLY where it
            # is needed again, and at an 18 MB mean that ratio matters far more than it did there.
            # "Needed again" means an OCR back-fill that has not happened yet — once winocr has
            # won, the bytes have served their purpose.
            if not flags["needs_ocr"] or method == "winocr":
                pdf_path.unlink(missing_ok=True)

        # ONE derivation of the body, used by every downstream field. n_chars, the tier, the
        # confidence and the personal-data scan previously disagreed about whitespace: a 3-page
        # image-only PDF extracts as "\f\f", which is len 2 but strips to '', so the row was
        # tiered index-only AND counted as text-bearing by the write gate — raising
        # PrivacyInvariantError and discarding every good row in the run, permanently, because
        # that empty text was cached and re-read on every later run.
        body = text.strip()
        hits = personal_data_hits(body)
        counts["privacy_hits"] += bool(hits)
        rows.append(
            {
                "abp_case": rec["abp_case"],
                "filename": rec["filename"],
                "full_url": url,
                "doc_category": rec.get("doc_category"),
                "is_scoping": rec["is_scoping"],
                "n_pages": meta.get("n_pages"),
                "n_chars": len(body),
                "extraction_method": meta.get("extraction_method"),
                "confidence": "low" if meta.get("extraction_method") == "winocr" else ("high" if len(body) > 2000 else "low"),
                # Body text is never written as 'public' — detect-and-quarantine, no redactor exists.
                "privacy_tier": TIER_WITH_TEXT if body else TIER_INDEX_ONLY,
                "source_document_hash": meta.get("source_document_hash"),
                "fetched_at": meta.get("fetched_at"),
                "text": body or None,
                "file_bytes": meta.get("file_bytes"),
                "image_only_pages": meta.get("image_only_pages"),
                "is_scanned": meta.get("is_scanned"),
                "needs_ocr": meta.get("needs_ocr"),
                "ocr_pages": meta.get("ocr_pages"),
                "pages_truncated": meta.get("pages_truncated"),
                "personal_data_hits": hits,
                "public_display": False,
                "error_class": None,
                "http_status": meta.get("http_status"),  # None where the bytes came off disk
                "notes": meta.get("notes"),
            }
        )

    save_misses(misses)
    if report.failures or report.breaker_trips:
        report.write()

    print(
        f"[run] cached={counts['cached']} fetched={counts['fetched']} requests={counts['requests']}/{args.limit} "
        f"from_disk={counts['from_disk']} ocr={counts['ocr']} (back-filled offline: {counts['ocr_backfilled']}) "
        f"oversize={counts['oversize']} failed={counts['failed']} skipped_permanent={counts['skipped_permanent']}"
    )
    print(f"[privacy] rows with >=1 personal-data pattern hit: {counts['privacy_hits']} of {len(rows)} (quarantined for review, not redacted)")
    if not rows:
        print("[write] no rows this run")
        return

    df = pl.DataFrame(rows, infer_schema_length=None).select(OUT_COLUMNS)
    assert_privacy_invariant(df)  # hard gate, before any write

    dest = SILVER / f"{SOURCE}.parquet"
    floor = None
    if dest.exists():
        existing = pl.read_parquet(dest)
        before = existing.height
        df = pl.concat([existing, df], how="diagonal_relaxed").unique(subset=["full_url"], keep="last")
        floor = before  # a merged frame that SHRANK is a truncated run, not a harvest
    p = save_parquet(df, dest, min_rows=floor)
    print(f"[silver] {df.height} document-text rows -> {p} (row floor={floor})")


if __name__ == "__main__":
    main()
