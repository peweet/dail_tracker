"""ITERATION-mode paginator for the TED v3 Search API (api.ted.europa.eu/v3/notices/search).

WHY THIS EXISTS — the bespoke `pull()` loops in extractors/ted_ireland_*.py used
`paginationMode=PAGE_NUMBER`, which the TED API **hard-caps at 15,000 notices**
(docs.ted.europa.eu/ODS/latest/reuse/search-api.html), and they capped themselves even
lower (PAGE_CAP=40 → 10k) with **no assertion against the declared total** — so a pull that
exceeded the cap silently truncated (this bit the 2016+ backfill: 16k expected, 10k landed).

THE FIX — TED's own recommended pattern for "download everything": `paginationMode=ITERATION`,
which returns an `iterationNextToken`; pass it back to scroll the next page, with **no limit**
on total notices. We model the project's Oireachtas paginator (services/member_paginated.py):
read the server's declared total (`totalNoticeCount`) and **assert we got all of it** — silent
truncation is the bug-class this prevents. Retry/backoff mirrors services/http_engine.py
(429 + 5xx + connection/timeout, exponential backoff), which the API needs under load and
which neither the bespoke loop nor the community tap-eu-ted Singer tap had.

Refs: TED ODS reuse/search-api.html (ITERATION + iterationNextToken; PAGE_NUMBER 15k cap);
OP-TED/tedapi-docs; pattern parity with services/member_paginated.py + services/http_engine.py.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

URL = "https://api.ted.europa.eu/v3/notices/search"
MAX_LIMIT = 250  # API max notices per page
DEFAULT_HEADERS = {
    "User-Agent": "dail-tracker research probe",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

# Retry policy — identical spirit to services/http_engine.py. TED returns 429 under load
# (observed) and intermittent 5xx; without backoff a single blip aborts a whole scroll or
# (worse, in the old loop) breaks out silently mid-pagination.
RETRY_MAX_ATTEMPTS = 4
RETRY_BACKOFF_BASE = 0.8  # seconds; sleep before retry N = BASE * 2 ** (N - 1)
RETRY_STATUS_FORCELIST = frozenset({429, 500, 502, 503, 504})
_RETRYABLE_EXC = (requests.exceptions.ConnectionError, requests.exceptions.Timeout)

_session = requests.Session()
_adapter = HTTPAdapter(pool_connections=4, pool_maxsize=4)
_session.mount("https://", _adapter)


def _post_with_retry(body: dict, timeout: tuple[int, int]) -> dict:
    """POST one search page, retrying transient faults with exponential backoff."""
    last_exc: Exception | None = None
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            r = _session.post(URL, json=body, headers=DEFAULT_HEADERS, timeout=timeout)
            if r.status_code in RETRY_STATUS_FORCELIST and attempt < RETRY_MAX_ATTEMPTS:
                logger.warning("TED search -> HTTP %s (attempt %d/%d), backing off", r.status_code, attempt, RETRY_MAX_ATTEMPTS)
                time.sleep(RETRY_BACKOFF_BASE * (2 ** (attempt - 1)))
                continue
            r.raise_for_status()
            return r.json()
        except _RETRYABLE_EXC as exc:
            last_exc = exc
            if attempt < RETRY_MAX_ATTEMPTS:
                logger.warning("TED search -> %s (attempt %d/%d), backing off", type(exc).__name__, attempt, RETRY_MAX_ATTEMPTS)
                time.sleep(RETRY_BACKOFF_BASE * (2 ** (attempt - 1)))
                continue
            raise
    raise last_exc if last_exc is not None else RuntimeError("TED search: retry loop exited unexpectedly")


def fetch_ted_search(
    query: str,
    fields: list[str],
    *,
    label: str = "ted-search",
    limit: int = MAX_LIMIT,
    max_pages: int | None = None,
    timeout: tuple[int, int] = (10, 120),
    assert_complete: bool = True,
) -> list[dict]:
    """Scroll the full TED v3 search result set via ITERATION pagination.

    Args:
        query:   TED expert-search expression.
        fields:  fields to return per notice.
        label:   for logging.
        limit:   notices per page (clamped to MAX_LIMIT=250).
        max_pages: smoke-test bound (None = all pages). When set, the completeness
                   assertion is skipped (a deliberate partial pull).
        assert_complete: when True (and not bounded by max_pages), assert that the number
                   of notices collected >= the API's declared totalNoticeCount. This is the
                   anti-silent-truncation guard, mirroring member_paginated.py.

    Returns:
        list of notice dicts (all pages concatenated).
    """
    limit = min(limit, MAX_LIMIT)
    notices: list[dict] = []
    token: str | None = None
    expected: int | None = None
    page = 0

    while True:
        page += 1
        body: dict = {"query": query, "fields": fields, "limit": limit, "paginationMode": "ITERATION"}
        if token:
            body["iterationNextToken"] = token
        payload = _post_with_retry(body, timeout)

        if expected is None:
            expected = int(payload.get("totalNoticeCount", 0))
            logger.info("%s: totalNoticeCount=%s", label, expected)

        batch = payload.get("notices", []) or []
        notices.extend(batch)
        token = payload.get("iterationNextToken")
        logger.info("%s: page %d +%d (total %d/%s)", label, page, len(batch), len(notices), expected)

        # Stop: server exhausted (no token / empty page), reached declared total, or hit the
        # smoke-test page bound. An empty page with a token still present means done.
        if not batch or not token or (expected and len(notices) >= expected):
            break
        if max_pages is not None and page >= max_pages:
            logger.warning("%s: stopped at max_pages=%d (PARTIAL pull, assertion skipped)", label, max_pages)
            return notices

    if assert_complete and expected:
        # The bug-class this module exists to prevent: server says N, we must have >= N.
        assert len(notices) >= expected, (
            f"{label} pagination drift: got {len(notices)} notices, totalNoticeCount={expected} "
            f"(silent truncation — do NOT write this to silver)"
        )
    return notices


# ── per-notice XML lane (legacy winner backfill; see doc/TED_ENRICHMENT.md §6) ──────────────
NOTICE_XML_URL = "https://ted.europa.eu/{lang}/notice/{pn}/xml"


def fetch_notice_xml(pn: str, dest: Path, *, lang: str = "en", timeout: tuple[int, int] = (10, 60)) -> bool:
    """Fetch one notice's full XML to dest/{pn}.xml. Resumable + retrying.

    Returns True if the file is present after the call (already cached OR freshly written),
    False on permanent failure. The pre-2024 API JSON drops the winner; the per-notice XML
    (legacy TED_EXPORT envelope) carries the full AWARDED_CONTRACT/CONTRACTOR roster.
    """
    dest = Path(dest)
    out = dest / f"{pn}.xml"
    if out.exists() and out.stat().st_size > 0:
        return True  # resumable: never re-fetch a cached notice
    url = NOTICE_XML_URL.format(lang=lang, pn=pn)
    last_exc: Exception | None = None
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            r = _session.get(url, headers={"User-Agent": DEFAULT_HEADERS["User-Agent"]}, timeout=timeout)
            if r.status_code in RETRY_STATUS_FORCELIST and attempt < RETRY_MAX_ATTEMPTS:
                time.sleep(RETRY_BACKOFF_BASE * (2 ** (attempt - 1)))
                continue
            if r.status_code == 404:
                logger.warning("notice %s -> 404 (skipping)", pn)
                return False
            r.raise_for_status()
            dest.mkdir(parents=True, exist_ok=True)
            out.write_bytes(r.content)
            return True
        except _RETRYABLE_EXC as exc:
            last_exc = exc
            if attempt < RETRY_MAX_ATTEMPTS:
                time.sleep(RETRY_BACKOFF_BASE * (2 ** (attempt - 1)))
                continue
            logger.error("notice %s -> %s (gave up)", pn, type(exc).__name__)
            return False
    logger.error("notice %s -> exhausted retries (%s)", pn, last_exc)
    return False
