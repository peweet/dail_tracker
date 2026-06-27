"""Tests for pdf_infra/legal_diary_poller — the Courts Service daily Legal Diary poller.

Covers the pull/idempotency brain that previously had 0% coverage: resolving the .docx
link (direct or via the /download chooser hop), the archive-or-skip decision (same sha →
skip, changed → revision), reading text from a .docx, and the poll() exit-code branches.
`responses` intercepts HTTP; sleeps are neutralised so retry paths don't stall the test.
"""

from __future__ import annotations

import io
import json
import sys
import zipfile
from pathlib import Path

import pytest
import responses

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pdf_infra.legal_diary_poller as ldp  # noqa: E402


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(ldp.time, "sleep", lambda *_a, **_k: None)


# ── pure link resolution ──────────────────────────────────────────────────────
def test_find_docx_returns_absolute_url():
    html = '<a href="/legaldiary.nsf/x/file.docx">diary</a>'
    assert ldp._find_docx(html, ldp.LANDING_URL) == "https://legaldiary.courts.ie/legaldiary.nsf/x/file.docx"


def test_find_docx_none_when_absent():
    assert ldp._find_docx('<a href="/foo.pdf">', ldp.LANDING_URL) is None


def test_find_chooser_prefers_download_link():
    html = '<a href="/legaldiary.nsf/abc/download">download</a>'
    assert ldp._find_chooser(html, ldp.LANDING_URL).rstrip("/").endswith("download")


def test_resolve_docx_url_direct():
    out = ldp.resolve_docx_url(ldp._session(), '<a href="/x/file.docx">', ldp.LANDING_URL)
    assert out.endswith("file.docx")


@responses.activate
def test_resolve_docx_url_walks_chooser_hop():
    chooser = "https://legaldiary.courts.ie/legaldiary.nsf/abc/download"
    responses.add(responses.GET, chooser, body='<a href="/legaldiary.nsf/abc/2026.docx">', status=200)
    landing = '<a href="/legaldiary.nsf/abc/download">download</a>'
    out = ldp.resolve_docx_url(ldp._session(), landing, ldp.LANDING_URL)
    assert out.endswith("2026.docx")


# ── archive-or-skip idempotency ───────────────────────────────────────────────
def test_archive_path_skips_identical_sha():
    idx = {"2026-06-04": {"sha256": "abc", "revisions": 1}}
    assert ldp._archive_path("2026-06-04", "abc", idx) is None


def test_archive_path_revisions_changed_content():
    idx = {"2026-06-04": {"sha256": "old", "revisions": 1}}
    assert ldp._archive_path("2026-06-04", "new", idx).name == "2026-06-04.r02.docx"


def test_archive_path_new_date():
    assert ldp._archive_path("2026-06-05", "x", {}).name == "2026-06-05.docx"


# ── docx text extraction ──────────────────────────────────────────────────────
def test_read_docx_lines_from_bytes():
    doc = (
        "<w:document><w:body>"
        "<w:p><w:r><w:t>HELLO</w:t></w:r></w:p>"
        "<w:p><w:r><w:t>WORLD</w:t></w:r></w:p>"
        "</w:body></w:document>"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("word/document.xml", doc)
    assert ldp.read_docx_lines_from_bytes(buf.getvalue()) == ["HELLO", "WORLD"]


# ── poll() exit-code branches ─────────────────────────────────────────────────
@responses.activate
def test_poll_landing_unreachable_returns_1(tmp_path, monkeypatch):
    monkeypatch.setattr(ldp, "ARCHIVE_DIR", tmp_path)
    responses.add(responses.GET, ldp.LANDING_URL, status=500)
    assert ldp.poll() == 1  # transient — safe to retry


@responses.activate
def test_poll_no_docx_link_returns_2(tmp_path, monkeypatch):
    monkeypatch.setattr(ldp, "ARCHIVE_DIR", tmp_path)
    responses.add(responses.GET, ldp.LANDING_URL, body="<html>no diary link here</html>", status=200)
    assert ldp.poll() == 2  # source drift — human needed


@responses.activate
def test_poll_file_too_small_returns_1(tmp_path, monkeypatch):
    monkeypatch.setattr(ldp, "ARCHIVE_DIR", tmp_path)
    responses.add(responses.GET, ldp.LANDING_URL, body='<a href="/x/today.docx">', status=200)
    responses.add(responses.GET, "https://legaldiary.courts.ie/x/today.docx", body=b"tiny", status=200)
    assert ldp.poll() == 1  # below MIN_BYTES — not a valid diary


def _docx_with_masthead(masthead: str, repeat: int = 3) -> bytes:
    doc = (
        "<w:document><w:body>"
        + "".join(f"<w:p><w:r><w:t>{masthead}</w:t></w:r></w:p>" for _ in range(repeat))
        + "</w:body></w:document>"
    )
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("word/document.xml", doc)
    return buf.getvalue()


@responses.activate
def test_poll_archives_new_diary_and_updates_index(tmp_path, monkeypatch):
    """The happy path: resolve → download → parse date header → archive .docx + index."""
    monkeypatch.setattr(ldp, "ARCHIVE_DIR", tmp_path / "arch")
    monkeypatch.setattr(ldp, "INDEX_PATH", tmp_path / "index.json")
    monkeypatch.setattr(ldp, "MIN_BYTES", 10)
    blob = _docx_with_masthead("MONDAY THE 8TH DAY OF JUNE 2026")
    responses.add(responses.GET, ldp.LANDING_URL, body='<a href="/x/today.docx">', status=200)
    responses.add(responses.GET, "https://legaldiary.courts.ie/x/today.docx", body=blob, status=200)

    rc = ldp.poll()

    assert rc == 0
    assert (tmp_path / "arch" / "2026-06-08.docx").read_bytes() == blob
    idx = json.loads((tmp_path / "index.json").read_text(encoding="utf-8"))
    assert "2026-06-08" in idx and idx["2026-06-08"]["filename"] == "2026-06-08.docx"


@responses.activate
def test_poll_already_archived_is_noop(tmp_path, monkeypatch):
    """Same (date, sha) already held → skip, exit 0, no re-write."""
    monkeypatch.setattr(ldp, "ARCHIVE_DIR", tmp_path / "arch")
    monkeypatch.setattr(ldp, "INDEX_PATH", tmp_path / "index.json")
    monkeypatch.setattr(ldp, "MIN_BYTES", 10)
    blob = _docx_with_masthead("MONDAY THE 8TH DAY OF JUNE 2026")
    import hashlib

    (tmp_path / "index.json").write_text(
        json.dumps({"2026-06-08": {"sha256": hashlib.sha256(blob).hexdigest(), "revisions": 1}}), encoding="utf-8"
    )
    responses.add(responses.GET, ldp.LANDING_URL, body='<a href="/x/today.docx">', status=200)
    responses.add(responses.GET, "https://legaldiary.courts.ie/x/today.docx", body=blob, status=200)

    assert ldp.poll() == 0
    assert not (tmp_path / "arch" / "2026-06-08.docx").exists()  # not re-written
