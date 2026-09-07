"""Focused contracts for the shared bounded PDF acquisition seam."""

from __future__ import annotations

from pathlib import Path

import fitz
import pytest
import requests

from services import pdf_acquisition as acquisition
from services.pdf_acquisition import download_pdf


class _Response:
    status_code = 200
    headers = {"content-type": "text/html"}

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def raise_for_status(self):
        return None

    def iter_content(self, _chunk_size):
        yield b"<html>challenge</html>"


class _Session:
    def __init__(self):
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return _Response()


def test_injected_transport_rejects_html_without_replacing_destination(tmp_path: Path):
    destination = tmp_path / "document.pdf"
    destination.write_bytes(b"%PDF-last-good")
    session = _Session()

    result = download_pdf(
        "https://example.test/document",
        destination,
        max_bytes=1024,
        delay=0,
        max_attempts=1,
        refresh=True,
        session=session,
        headers={"X-Test": "yes"},
    )

    assert result["ok"] is False
    assert result["error_class"]
    assert destination.read_bytes() == b"%PDF-last-good"
    assert session.calls[0][1]["headers"] == {"X-Test": "yes"}


def _pdf():
    with fitz.open() as doc:
        doc.new_page().insert_text((30, 50), "Planning decision test document")
        return doc.tobytes()


class PDFResponse(_Response):
    def __init__(self, payload, status=200, headers=None):
        self.payload = payload
        self.status_code = status
        self.headers = headers or {}

    def iter_content(self, size):
        yield self.payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)


def test_retry_success_has_clean_error_and_admission_for_every_attempt(tmp_path, monkeypatch):
    attempts = []
    admitted = []
    monkeypatch.setattr(acquisition.time, "sleep", lambda _: None)

    def request(url, **_kwargs):
        attempts.append(url)
        if len(attempts) == 1:
            raise requests.ConnectionError("temporary")
        return PDFResponse(_pdf())

    result = download_pdf(
        "https://example.test/pdf",
        tmp_path / "test.pdf",
        max_bytes=10000,
        delay=0,
        request=request,
        request_admission=admitted.append,
    )
    assert result["ok"] and result["error_class"] is None
    assert result["attempts"] == len(attempts) == len(admitted) == 2


@pytest.mark.parametrize("failure", ["oversize", "truncated", "repaired"])
def test_rejected_transfer_keeps_previous_pdf_and_cleans_temp(tmp_path, failure):
    payload = _pdf()
    original = payload
    dest = tmp_path / "test.pdf"
    dest.write_bytes(payload)
    limit = len(payload) - 1 if failure == "oversize" else 10000
    headers = {"content-length": str(len(payload) + 10)} if failure == "truncated" else {}
    if failure == "repaired":
        payload = payload[: payload.index(b"xref")]
    result = download_pdf(
        "https://example.test/pdf",
        dest,
        max_bytes=limit,
        delay=0,
        refresh=True,
        request=lambda *_a, **_k: PDFResponse(payload, headers=headers),
    )
    assert not result["ok"]
    assert dest.read_bytes() == original
    assert len(list(tmp_path.iterdir())) == 1


def test_abp_wrapper_retains_receipt_and_native_text_contract(tmp_path, monkeypatch):
    from pipeline_sandbox.new_sources import abp_doc_text_extract as abp

    payload = _pdf()
    calls = []

    class Session:
        def get(self, url, **kwargs):
            calls.append(kwargs)
            return PDFResponse(payload)

    monkeypatch.setattr(abp, "session", Session())
    dest = tmp_path / "abp.pdf"
    result = abp.download_pdf("https://example.test/pdf", dest, max_bytes=10000, delay=0)
    assert result["ok"] and result["attempts"] == 1 and not result["from_cache"]
    assert set(result) == {
        "ok",
        "error_class",
        "http_status",
        "sha256",
        "bytes",
        "attempts",
        "from_cache",
        "fetched_at",
        "source_last_modified",
    }
    assert calls[0]["timeout"] == abp.TIMEOUTS
    cached = abp.download_pdf("https://example.test/pdf", dest, max_bytes=10000, delay=0)
    assert cached["from_cache"] and len(calls) == 1
    text, total, read, scans, truncated = abp.extract_text(dest)
    assert "Planning decision" in text
    assert (total, read, scans, truncated) == (1, 1, 0, False)


def test_server_retry_after_cannot_exceed_document_deadline(tmp_path, monkeypatch):
    sleeps = []
    monkeypatch.setattr(acquisition.time, "sleep", sleeps.append)
    result = download_pdf(
        "https://example.test/pdf",
        tmp_path / "deadline.pdf",
        max_bytes=10000,
        delay=0,
        deadline_s=1,
        request=lambda *_a, **_k: PDFResponse(b"", status=503, headers={"retry-after": "999999999"}),
    )
    assert result["error_class"] == "download_deadline"
    assert sum(sleeps) <= 1
