"""Bounded, atomic PDF acquisition shared by public extractors and private pilots."""

from __future__ import annotations

import services.runtime_env  # noqa: F401  # isort: skip

import hashlib
import os
import tempfile
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import pymupdf as fitz
import requests

from services.fetch_report import classify_body, classify_exception
from services.http_engine import (
    RETRY_BACKOFF_BASE,
    RETRY_MAX_ATTEMPTS,
    RETRY_STATUS_FORCELIST,
    polite_headers,
)
from services.http_engine import (
    session as default_session,
)

Request = Callable[..., Any]
Admission = Callable[[str], None]


class RequestBudgetExceeded(RuntimeError):
    """A caller's request ledger refused another HTTP attempt."""


def _now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _hash_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    total = 0
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 16), b""):
            digest.update(chunk)
            total += len(chunk)
    return digest.hexdigest(), total


def _valid_pdf(path: Path) -> bool:
    """Reject stale, truncated, HTML, and malformed cached files."""
    try:
        with path.open("rb") as fh:
            if fh.read(5) != b"%PDF-":
                return False
        with fitz.open(path) as doc:
            return doc.page_count > 0 and not bool(getattr(doc, "is_repaired", False))
    except (OSError, RuntimeError, ValueError):
        return False


def extract_text(path: Path, max_pages: int = 400) -> tuple[str, int, int, int, bool]:
    """Return bounded native PDF text and page/image accounting."""
    with fitz.open(path) as doc:
        n_pages = doc.page_count
        take = min(n_pages, max_pages)
        texts: list[str] = []
        image_only = 0
        for i in range(take):
            page = doc[i]
            text = cast(str, page.get_text("text"))
            if len(text.strip()) < 50 and page.get_images():
                image_only += 1
            texts.append(text)
        return "\f".join(texts), n_pages, take, image_only, take < n_pages


def download_pdf(
    url: str,
    dest: Path,
    *,
    max_bytes: int,
    delay: float,
    max_attempts: int = RETRY_MAX_ATTEMPTS,
    refresh: bool = False,
    session: Any = default_session,
    headers: Mapping[str, str] | None = None,
    request: Request | None = None,
    request_admission: Admission | None = None,
    timeouts: tuple[float, float] = (10, 120),
    deadline_s: float = 600,
) -> dict[str, Any]:
    """Stream one PDF under byte/deadline/attempt bounds and publish atomically.

    ``request`` is an explicit transport hook called as ``request(url, **request_options)``.
    When omitted, ``session.get`` is used. Both routes receive headers, timeout, streaming,
    and redirect options. ``request_admission`` runs before every request, including retries.
    """
    out: dict[str, Any] = {
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
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and not refresh and dest.stat().st_size <= max_bytes and _valid_pdf(dest):
        digest, total = _hash_file(dest)
        out.update(
            ok=True,
            sha256=digest,
            bytes=total,
            from_cache=True,
            fetched_at=datetime.fromtimestamp(dest.stat().st_mtime, UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        return out

    if headers is None:
        headers = polite_headers(extra={"Accept": "application/pdf,*/*;q=0.8", "Accept-Language": "en-IE,en;q=0.9"})
    getter = request or session.get
    fd, temp_name = tempfile.mkstemp(prefix=f".{dest.name}.", suffix=".part", dir=dest.parent)
    os.close(fd)
    temp = Path(temp_name)
    deadline = time.monotonic() + deadline_s

    def pause(seconds: float) -> bool:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or seconds >= remaining:
            out["error_class"] = "download_deadline"
            return False
        if seconds > 0:
            time.sleep(seconds)
        return True

    try:
        for attempt in range(1, max_attempts + 1):
            if not pause(delay):
                return out
            out["attempts"] = attempt
            try:
                if request_admission is not None:
                    request_admission(url)
                kwargs = {
                    "headers": headers,
                    "timeout": tuple(min(value, max(0.001, (deadline - time.monotonic()) / 2)) for value in timeouts),
                    "stream": True,
                    "allow_redirects": True,
                }
                response = getter(url, **kwargs)
                with response as r:
                    out["http_status"] = r.status_code
                    if r.status_code in RETRY_STATUS_FORCELIST and attempt < max_attempts:
                        retry_after = r.headers.get("retry-after", "")
                        backoff = (
                            float(retry_after) if retry_after.isdigit() else RETRY_BACKOFF_BASE * 2 ** (attempt - 1)
                        )
                        if not pause(max(0, backoff - delay)):
                            return out
                        continue
                    r.raise_for_status()
                    out["source_last_modified"] = r.headers.get("last-modified")
                    declared = r.headers.get("content-length")
                    if declared and declared.isdigit() and int(declared) > max_bytes:
                        out.update(error_class="oversize", bytes=int(declared))
                        return out
                    digest = hashlib.sha256()
                    total = 0
                    head = b""
                    with temp.open("wb") as fh:
                        for chunk in r.iter_content(1 << 16):
                            if not chunk:
                                continue
                            total += len(chunk)
                            if total > max_bytes:
                                out.update(error_class="oversize", bytes=total)
                                return out
                            if time.monotonic() >= deadline:
                                out.update(error_class="download_deadline", bytes=total)
                                return out
                            if len(head) < 2048:
                                head += chunk[: 2048 - len(head)]
                            digest.update(chunk)
                            fh.write(chunk)
                        fh.flush()
                        os.fsync(fh.fileno())
                    if declared and declared.isdigit() and int(declared) != total:
                        out.update(error_class="truncated_transfer", bytes=total)
                        return out
                    body_class = classify_body(head, expected_magic=b"%PDF")
                    if body_class is not None:
                        out["error_class"] = body_class
                        return out
                    # Validate before replacing a last-good destination; fitz also catches a
                    # truncated transfer that happened to begin with the PDF magic bytes.
                    if not _valid_pdf(temp):
                        out["error_class"] = "invalid_pdf"
                        return out
                    temp.replace(dest)
                    out.update(ok=True, error_class=None, sha256=digest.hexdigest(), bytes=total)
                    return out
            except RequestBudgetExceeded:
                out["error_class"] = "request_budget"
                return out
            except requests.RequestException as exc:
                error_class, status = classify_exception(exc)
                out["error_class"] = error_class
                out["http_status"] = status if status is not None else out["http_status"]
                if status is not None and 400 <= status < 500:
                    return out
                if attempt < max_attempts:
                    if not pause(RETRY_BACKOFF_BASE * 2 ** (attempt - 1)):
                        return out
                    continue
                return out
        if out["error_class"] is None:
            out["error_class"] = f"http_{out['http_status']}" if out["http_status"] else "unknown"
        return out
    finally:
        # Close happens before this cleanup, which is required for Windows file handles.
        temp.unlink(missing_ok=True)
