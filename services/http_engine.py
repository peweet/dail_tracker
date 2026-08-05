import concurrent.futures
import contextlib
import logging
import os
import secrets
import shutil
import subprocess
import time
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from services.schema_validation import validate_if_known

logger = logging.getLogger(__name__)

# Transient-fault retry policy. The Oireachtas API (and the gov endpoints sharing
# this session) intermittently drop connections, time out mid-read on large pages,
# or return 5xx/429 under load. Without retry, a single blip aborts a whole
# pagination scenario or silently drops a member from bronze (see
# member_paginated.py) — exactly what a marginal connection produced in practice.
# We retry those with exponential backoff; permanent 4xx still raise immediately.
RETRY_MAX_ATTEMPTS = 3  # total attempts (1 initial + 2 retries)
RETRY_BACKOFF_BASE = 0.5  # seconds; sleep before retry N = BASE * 2 ** (N - 1)
RETRY_STATUS_FORCELIST = frozenset({429, 500, 502, 503, 504})
_RETRYABLE_EXC = (requests.exceptions.ConnectionError, requests.exceptions.Timeout)

type QueryScalar = str | bytes | int | float
type QueryValue = QueryScalar | Iterable[QueryScalar] | None
type QueryParams = Mapping[str, QueryValue] | Iterable[tuple[str, QueryValue]] | str | bytes | None


def new_session(
    *,
    headers: dict[str, str] | None = None,
    retries: int = 2,
    retry_statuses: bool = True,
    pool_size: int = 20,
) -> requests.Session:
    """Create a bounded, retrying session for genuinely stateful HTTP flows.

    Most callers should use :func:`fetch_json`, :func:`fetch_text` or
    :func:`fetch_bytes`. This factory is for exchanges that must retain cookies,
    inspect response status/headers, or stream a response across several related
    requests. GET/HEAD connection and read failures are retried by urllib3;
    retryable HTTP statuses use the same policy as the stateless helpers unless
    ``retry_statuses`` is false for a caller that must observe the first status.
    POST is deliberately excluded because replaying a state-changing form is not
    generally safe.
    """
    if retries < 0:
        raise ValueError("retries cannot be negative")
    if pool_size < 1:
        raise ValueError("pool_size must be at least 1")
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries if retry_statuses else 0,
        allowed_methods=frozenset({"GET", "HEAD"}),
        status_forcelist=RETRY_STATUS_FORCELIST,
        backoff_factor=RETRY_BACKOFF_BASE,
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=pool_size, pool_maxsize=pool_size)
    created = requests.Session()
    created.mount("http://", adapter)
    created.mount("https://", adapter)
    if headers:
        created.headers.update(headers)
    return created


# Stateless helpers own their explicit retry loop, so their shared session only
# needs pooling. Stateful callers opt into the factory's transport retries.
session = new_session(retries=0)


def _sleep_backoff(attempt: int, response: requests.Response | None = None) -> None:
    """Sleep for exponential backoff, preferring an integer Retry-After hint."""
    delay = RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
    if response is not None:
        with contextlib.suppress(TypeError, ValueError):
            delay = max(0.0, float(response.headers.get("Retry-After", delay)))
    time.sleep(delay)


def fetch_json(
    url: str,
    timeout: tuple[int, int] | int = (10, 60),
    *,
    headers: dict[str, str] | None = None,
    params: QueryParams = None,
    attempts: int = RETRY_MAX_ATTEMPTS,
) -> tuple[dict, int]:
    """Fetch one URL using the shared session, retrying transient faults.

    Retries (exponential backoff) on connection errors, timeouts, and retryable
    status codes (429 + 5xx). Permanent 4xx responses raise on the first attempt.
    Once RETRY_MAX_ATTEMPTS is exhausted the final error propagates, preserving
    the "raises on failure" contract fetch_all relies on to count failures.
    """
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = session.get(url, timeout=timeout, headers=headers, params=params)
            if response.status_code in RETRY_STATUS_FORCELIST and attempt < attempts:
                logger.warning(
                    "fetch_json %s -> HTTP %s (attempt %d/%d), retrying",
                    url,
                    response.status_code,
                    attempt,
                    attempts,
                )
                _sleep_backoff(attempt, response)
                continue
            response.raise_for_status()
            raw_bytes = len(response.content)
            payload = response.json()
            # Validate-at-fetch: assert the Oireachtas API envelope still matches
            # its registered schema before any flattener reads it (DAIL-019/020).
            # No-op for unrecognised hosts/endpoints. Raises SchemaValidationError
            # on drift; the caller lets it propagate so a break fails loudly.
            validate_if_known(url, payload)
            return payload, raw_bytes
        except _RETRYABLE_EXC as exc:
            last_exc = exc
            if attempt < attempts:
                logger.warning(
                    "fetch_json %s -> %s (attempt %d/%d), retrying",
                    url,
                    type(exc).__name__,
                    attempt,
                    attempts,
                )
                _sleep_backoff(attempt)
                continue
            raise
    # Unreachable in practice: the final attempt either returns, raises
    # raise_for_status (forcelist status), or re-raises the transient exception.
    raise last_exc if last_exc is not None else RuntimeError("fetch_json: retry loop exited unexpectedly")


def post_json(
    url: str,
    payload: dict,
    timeout: tuple[int, int] | int = (10, 60),
    *,
    headers: dict[str, str] | None = None,
    attempts: int = RETRY_MAX_ATTEMPTS,
) -> tuple[dict, int]:
    """POST a JSON document through the shared retrying session.

    This is the write-method sibling of :func:`fetch_json`: transient connection,
    timeout, 429 and 5xx failures are retried; permanent 4xx responses and an
    exhausted retry budget raise. The response JSON and raw byte count mirror the
    GET helper's contract, which keeps API pagination callers deterministic.
    """
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = session.post(url, json=payload, timeout=timeout, headers=headers)
            if response.status_code in RETRY_STATUS_FORCELIST and attempt < attempts:
                logger.warning(
                    "post_json %s -> HTTP %s (attempt %d/%d), retrying",
                    url,
                    response.status_code,
                    attempt,
                    attempts,
                )
                _sleep_backoff(attempt, response)
                continue
            response.raise_for_status()
            raw_bytes = len(response.content)
            data = response.json()
            validate_if_known(url, data)
            return data, raw_bytes
        except _RETRYABLE_EXC as exc:
            last_exc = exc
            if attempt < attempts:
                logger.warning(
                    "post_json %s -> %s (attempt %d/%d), retrying",
                    url,
                    type(exc).__name__,
                    attempt,
                    attempts,
                )
                _sleep_backoff(attempt)
                continue
            raise
    raise last_exc if last_exc is not None else RuntimeError("post_json: retry loop exited unexpectedly")


def fetch_text(
    url: str,
    timeout: tuple[int, int] | int = (10, 60),
    *,
    headers: dict[str, str] | None = None,
    params: QueryParams = None,
    encoding: str | None = None,
    attempts: int = RETRY_MAX_ATTEMPTS,
) -> tuple[str, int]:
    """Fetch one URL as raw text, retrying transient faults.

    Sibling of fetch_json for non-JSON payloads (e.g. AKN debate XML). Same
    shared session, same retry policy and "raises on failure" contract — only
    the decode differs (response.text, no response.json()/schema validation).
    Permanent 4xx still raise immediately: a 403 from the Oireachtas S3/AKN
    bucket means the object key does not exist, so failing fast (rather than
    retrying) is correct — the caller counts it as a miss.
    """
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = session.get(url, timeout=timeout, headers=headers, params=params)
            if response.status_code in RETRY_STATUS_FORCELIST and attempt < attempts:
                logger.warning(
                    "fetch_text %s -> HTTP %s (attempt %d/%d), retrying",
                    url,
                    response.status_code,
                    attempt,
                    attempts,
                )
                _sleep_backoff(attempt, response)
                continue
            response.raise_for_status()
            content = response.content
            raw_bytes = len(content)
            # Decode deterministically rather than via response.text: when the server sends no
            # charset, requests' .text sniffs one with chardet.detect(), and a broken/namespace
            # chardet install (seen 2026-07-07 shadowing charset_normalizer -> AttributeError:
            # module 'chardet' has no attribute 'detect') makes that raise. Use the server-declared
            # charset if present, else UTF-8 — correct for the Oireachtas AKN debate XML this fetches.
            text = content.decode(encoding or response.encoding or "utf-8", errors="replace")
            return text, raw_bytes
        except _RETRYABLE_EXC as exc:
            last_exc = exc
            if attempt < attempts:
                logger.warning(
                    "fetch_text %s -> %s (attempt %d/%d), retrying",
                    url,
                    type(exc).__name__,
                    attempt,
                    attempts,
                )
                _sleep_backoff(attempt)
                continue
            raise
    raise last_exc if last_exc is not None else RuntimeError("fetch_text: retry loop exited unexpectedly")


def fetch_all_text(urls: list[str], max_workers: int = 5) -> tuple[list[tuple[str, str]], int, int]:
    """Fetch many URLs concurrently as text.

    Mirrors fetch_all but yields (url, text) pairs so a caller can name an
    output file per source URL (the AKN harvest writes one XML file per
    sitting-day main.xml). A failed URL is omitted from results and counted.

    Returns:
        results [(url, text), ...], total_downloaded_bytes, failure_count
    """
    results: list[tuple[str, str]] = []
    total_bytes = 0
    failures = 0

    if not urls:
        logger.warning("No URLs provided to fetch_all_text().")
        return results, total_bytes, failures

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(fetch_text, url): url for url in urls}

        for i, future in enumerate(concurrent.futures.as_completed(future_to_url), start=1):
            url = future_to_url[future]
            try:
                text, raw_bytes = future.result()
                results.append((url, text))
                total_bytes += raw_bytes
                if i % 10 == 0 or i == len(urls):
                    logger.info(
                        f"Fetched {i}/{len(urls)} text URLs | successes={len(results)} | "
                        f"failures={failures} | total_downloaded={total_bytes:,} bytes"
                    )
            except Exception as exc:
                failures += 1
                logger.error(f"Text fetch failed for {url}: {exc}")

    logger.info(
        f"Finished fetch_all_text | results={len(results)} | failures={failures} | downloaded={total_bytes:,} bytes"
    )
    return results, total_bytes, failures


def fetch_all(urls: list[str], max_workers: int = 5) -> tuple[list[dict], int, int]:
    """Fetch many URLs concurrently.

    Returns:
        results, total_downloaded_bytes, failure_count
    """
    results: list[dict] = []
    total_bytes = 0
    failures = 0

    if not urls:
        logger.warning("No URLs provided to fetch_all().")
        return results, total_bytes, failures

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(fetch_json, url): url for url in urls}

        for i, future in enumerate(concurrent.futures.as_completed(future_to_url), start=1):
            url = future_to_url[future]
            try:
                data, raw_bytes = future.result()
                results.append(data)
                total_bytes += raw_bytes

                if i % 10 == 0 or i == len(urls):
                    logger.info(
                        f"Fetched {i}/{len(urls)} URLs | "
                        f"successes={len(results)} | failures={failures} | "
                        f"last_payload={raw_bytes:,} bytes | "
                        f"total_downloaded={total_bytes:,} bytes"
                    )
            except Exception as exc:
                failures += 1
                logger.error(f"API call failed for {url}: {exc}")

    logger.info(f"Finished fetch_all | results={len(results)} | failures={failures} | downloaded={total_bytes:,} bytes")
    return results, total_bytes, failures


# ---------------------------------------------------------------------------
# Scraper-grade download path (2026-07 utility consolidation)
#
# fetch_json/fetch_text above serve the Oireachtas JSON/AKN case: trusted
# hosts, raise-on-failure. The file-downloading extractors (council PDFs,
# gov.ie CSVs, revenue.ie tables) need a different contract — polite headers,
# best-effort bytes-or-None, and a curl fallback, because several gov CDNs/
# WAFs reject python-requests' TLS fingerprint or its UA while serving curl
# or a browser UA fine (gov.ie GOVIE_HEADERS precedent; Meath/Sligo council
# TLS quirks; revenue.ie on some networks). Before this existed ~11 extractors
# hand-rolled the same requests→curl ladder with ~28 divergent UA literals.
# New scrapers should call fetch_bytes() instead of rolling their own.
# ---------------------------------------------------------------------------

# Honest-contact research UA (the default): most publishers serve it fine and
# it identifies the project. browser=True is the escape hatch for WAFs that
# block anything non-browser (Sligo) — prefer the honest UA where it works.
RESEARCH_UA = "Mozilla/5.0 (dail-tracker research; contact: p.glynn18@gmail.com)"
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36"
)

# some publishers emit hrefs with raw spaces — requests/curl reject them as malformed;
# '%' stays in the safe set so already-encoded hrefs don't double-encode.
_URL_QUOTE_SAFE = "!#$%&'()*+,/:;=?@[]~"


def polite_headers(*, browser: bool = False, extra: dict[str, str] | None = None) -> dict[str, str]:
    """Canonical request headers for scrapers — one UA instead of 28 literals.

    browser=True swaps in a real-browser UA for hosts whose WAF blocks the
    research UA. `extra` merges additional headers (Accept, Accept-Encoding…).
    """
    headers = {"User-Agent": BROWSER_UA if browser else RESEARCH_UA}
    if extra:
        headers.update(extra)
    return headers


def post_bytes(
    url: str,
    data: dict[str, str],
    *,
    headers: dict[str, str] | None = None,
    timeout: tuple[int, int] | int = 90,
    validate: Callable[[bytes], bool] | None = None,
    attempts: int = RETRY_MAX_ATTEMPTS,
) -> bytes | None:
    """Best-effort form POST: bytes or None, never raises (fetch_bytes' sibling contract).

    For APIs that reject long GET query strings or non-browser GETs but accept a form POST —
    Overpass is the canonical case (its main instance 406s a bare GET yet serves the same
    query as `data=` POST). Same polite-headers default and retry ladder as fetch_bytes; no
    curl leg (a POST body through subprocess quoting is where encodings go to die — callers
    needing that resilience should batch/retry at their own level).
    """
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    headers = headers if headers is not None else polite_headers()
    body: bytes | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = session.post(url, data=data, headers=headers, timeout=timeout, allow_redirects=True)
            if response.status_code in RETRY_STATUS_FORCELIST and attempt < attempts:
                _sleep_backoff(attempt, response)
                continue
            response.raise_for_status()
            body = response.content
            break
        except _RETRYABLE_EXC:
            if attempt < attempts:
                _sleep_backoff(attempt)
                continue
            break
        except Exception:
            break
    if body and (validate is None or validate(body)):
        return body
    return None


def _curl_candidates() -> tuple[str, ...]:
    """Return absolute curl executables in capability-first order.

    ``DAIL_CURL_BIN`` is the explicit deployment override. On Windows, Git's
    bundled curl is considered before the system copy because it commonly has
    Brotli support needed by Irish public-sector sites. All discovery happens
    through ``PATH`` and the resolved Git installation; no workstation-specific
    path is embedded in the code.
    """
    candidates: list[str] = []
    configured = os.environ.get("DAIL_CURL_BIN")
    if configured:
        resolved = shutil.which(configured)
        if resolved is None:
            configured_path = Path(configured).expanduser()
            if configured_path.is_file():
                resolved = str(configured_path.resolve())
        if resolved:
            candidates.append(str(Path(resolved).resolve()))

    git_executable = shutil.which("git")
    if git_executable:
        git_root = Path(git_executable).resolve().parent.parent
        embedded = git_root / "mingw64" / "bin" / "curl.exe"
        if embedded.is_file():
            candidates.append(str(embedded.resolve()))

    system_curl = shutil.which("curl")
    if system_curl:
        candidates.append(str(Path(system_curl).resolve()))

    return tuple(dict.fromkeys(candidates))


def _curl_bytes(url: str, user_agent: str, timeout: int) -> bytes | None:
    """Last-resort fetch via curl with normal certificate verification.

    ``--compressed`` lets curl negotiate only encodings it can decode (avoids
    the gov.ie brotli trap seen on the diary refresh). A certificate failure is
    a failed source fetch, never a reason to accept unverified bytes.
    """
    for executable in _curl_candidates():
        try:
            process = subprocess.run(
                [
                    executable,
                    "-sS",
                    "-L",
                    "--compressed",
                    "--max-time",
                    str(timeout),
                    "-A",
                    user_agent,
                    url,
                ],
                capture_output=True,
                timeout=timeout + 30,
                check=False,
            )
        except (OSError, subprocess.SubprocessError):
            continue
        if process.returncode == 0 and process.stdout:
            return process.stdout
    return None


def fetch_bytes(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: QueryParams = None,
    timeout: int = 90,
    curl_fallback: bool = True,
    validate: Callable[[bytes], bool] | None = None,
    on_failure: Callable[[Exception | None], None] | None = None,
    attempts: int = RETRY_MAX_ATTEMPTS,
) -> bytes | None:
    """Best-effort file download: requests (shared session + retry) → curl.

    Contract differs from fetch_json/fetch_text deliberately: returns bytes or
    None, NEVER raises — harvest loops over hundreds of publisher files treat a
    miss as a coverage stat, not an abort.

    `validate` (bytes -> bool, e.g. ``lambda b: b[:4] == b"%PDF"``) guards
    against WAF interstitials that return HTTP 200 HTML instead of the asked-for
    asset: a response failing it is treated as a miss, which also triggers the
    curl fallback. Transient faults (429/5xx/timeouts) retry with backoff on the
    requests leg; permanent 4xx (WAF 403 is the canonical case) fall straight
    through to curl. ``on_failure`` is called once, only after every enabled leg
    fails, with the final requests exception when one exists; harvesters use it
    to retain their structured failure reports without owning the network path.
    """
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    headers = headers if headers is not None else polite_headers()
    prepared_url = requests.Request("GET", url, params=params).prepare().url
    url = quote(prepared_url or url, safe=_URL_QUOTE_SAFE)

    body: bytes | None = None
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = session.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            if response.status_code in RETRY_STATUS_FORCELIST and attempt < attempts:
                _sleep_backoff(attempt, response)
                continue
            response.raise_for_status()
            body = response.content
            break
        except _RETRYABLE_EXC as exc:
            last_exc = exc
            if attempt < attempts:
                _sleep_backoff(attempt)
                continue
            break  # transient budget exhausted → curl leg
        except Exception as exc:
            last_exc = exc
            break  # permanent failure (4xx, TLS handshake…) → curl leg

    if body and (validate is None or validate(body)):
        return body

    if not curl_fallback:
        if on_failure is not None:
            on_failure(last_exc)
        return None
    logger.warning("fetch_bytes %s: requests leg failed%s, trying curl", url, " validation" if body else "")
    curl_timeout = max(timeout) if isinstance(timeout, tuple) else timeout
    body = _curl_bytes(url, headers.get("User-Agent", RESEARCH_UA), curl_timeout)
    if body and (validate is None or validate(body)):
        return body
    if on_failure is not None:
        on_failure(last_exc)
    return None


def download_file(
    url: str,
    destination: Path,
    *,
    headers: dict[str, str] | None = None,
    timeout: int = 180,
    validate: Callable[[Path], bool] | None = None,
    on_failure: Callable[[Exception | None], None] | None = None,
) -> bool:
    """Stream a download to an atomic destination, preserving any old file.

    The normal requests leg streams bounded chunks instead of holding large CSVs
    or PDFs in memory. A failed or invalid transfer is removed; ``destination``
    is replaced only after a complete validated download. ``on_failure`` follows
    :func:`fetch_bytes`: it runs only after requests and curl both fail.
    """
    headers = headers if headers is not None else polite_headers()
    url = quote(url, safe=_URL_QUOTE_SAFE)
    destination = destination.resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{secrets.token_hex(6)}.part")
    last_exc: Exception | None = None
    try:
        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            try:
                with session.get(
                    url,
                    headers=headers,
                    timeout=timeout,
                    allow_redirects=True,
                    stream=True,
                ) as response:
                    if response.status_code in RETRY_STATUS_FORCELIST and attempt < RETRY_MAX_ATTEMPTS:
                        _sleep_backoff(attempt, response)
                        continue
                    response.raise_for_status()
                    with temporary.open("wb") as output:
                        for chunk in response.iter_content(1 << 16):
                            if chunk:
                                output.write(chunk)
                if validate is None or validate(temporary):
                    os.replace(temporary, destination)
                    return True
                break
            except _RETRYABLE_EXC as exc:
                last_exc = exc
                if attempt < RETRY_MAX_ATTEMPTS:
                    _sleep_backoff(attempt)
                    continue
                break
            except Exception as exc:
                last_exc = exc
                break

        body = _curl_bytes(url, headers.get("User-Agent", RESEARCH_UA), timeout)
        if body:
            temporary.write_bytes(body)
            if validate is None or validate(temporary):
                os.replace(temporary, destination)
                return True
        if on_failure is not None:
            on_failure(last_exc)
        return False
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()


if __name__ == "__main__":
    # Example usage
    test_urls = [
        "https://jsonplaceholder.typicode.com/posts/1",
        "https://jsonplaceholder.typicode.com/posts/2",
        "https://jsonplaceholder.typicode.com/posts/3",
    ]
    fetch_all(test_urls, max_workers=3)
