"""Tests for pdf_infra/legal_diary_openview_poller — the OpenView jurisdiction poller
(0% before). Covers the index parser, UNID extraction, the unknown-jurisdiction guard,
and an end-to-end incremental poll (archive new, skip unchanged via the manifest).
"""

from __future__ import annotations

import json
import sys
import types
from pathlib import Path

import pytest
import responses

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pdf_infra.legal_diary_openview_poller as ov  # noqa: E402

SAMPLE_INDEX = """
<table>
<tr class="clickable-row" data-url="/legaldiary.nsf/slug/ABCDEF0123456789?OpenDocument">
  <td data-text="2026-06-04">4 June</td>
  <td data-text="20260604">Updated</td>
</tr>
</table>
"""


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr(ov.time, "sleep", lambda *_a, **_k: None)


def _args(**kw):
    base = {"jurisdictions": None, "full": False, "limit": 0}
    base.update(kw)
    return types.SimpleNamespace(**base)


# ── pure parsers ──────────────────────────────────────────────────────────────
def test_unid_extraction():
    assert ov._unid("/legaldiary.nsf/slug/ABCDEF0123456789?OpenDocument") == "ABCDEF0123456789"
    assert ov._unid("/no/unid/here") is None


def test_index_url_carries_slug():
    u = ov._index_url("circuit-court")
    assert u.startswith(ov.BASE) and "Jurisdiction=circuit-court" in u


def test_parse_index_rows():
    rows = ov.parse_index_rows(SAMPLE_INDEX)
    assert len(rows) == 1
    assert rows[0]["unid"] == "ABCDEF0123456789"
    assert rows[0]["updated"] == "20260604"  # the change-key (last data-text cell)


def test_parse_index_rows_skips_row_without_unid():
    html = '<tr class="clickable-row" data-url="/bad/url"><td data-text="x">y</td></tr>'
    assert ov.parse_index_rows(html) == []


# ── poll() ────────────────────────────────────────────────────────────────────
def test_poll_unknown_jurisdiction_returns_2():
    assert ov.poll(_args(jurisdictions="mars-court")) == 2


@responses.activate
def test_poll_archives_new_sitting_and_updates_manifest(tmp_path, monkeypatch):
    monkeypatch.setattr(ov, "ARCHIVE_DIR", tmp_path / "arch")
    monkeypatch.setattr(ov, "MANIFEST_PATH", tmp_path / "manifest.json")
    monkeypatch.setattr(ov, "MIN_BYTES", 5)
    slug = "circuit-court"
    responses.add(responses.GET, ov._index_url(slug), body=SAMPLE_INDEX, status=200)
    detail = ov.BASE + "/legaldiary.nsf/slug/ABCDEF0123456789?OpenDocument"
    responses.add(responses.GET, detail, body=b"<html>" + b"x" * 50 + b"</html>", status=200)

    rc = ov.poll(_args(jurisdictions=slug))

    assert rc == 0
    assert (tmp_path / "arch" / slug / "ABCDEF0123456789.html").exists()
    manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert manifest[slug]["ABCDEF0123456789"] == "20260604"


@responses.activate
def test_poll_incremental_skips_unchanged_sitting(tmp_path, monkeypatch):
    monkeypatch.setattr(ov, "ARCHIVE_DIR", tmp_path / "arch")
    mpath = tmp_path / "manifest.json"
    monkeypatch.setattr(ov, "MANIFEST_PATH", mpath)
    slug = "circuit-court"
    # manifest already holds this UNID at the same 'updated' stamp → must NOT re-fetch.
    mpath.write_text(json.dumps({slug: {"ABCDEF0123456789": "20260604"}}), encoding="utf-8")
    responses.add(responses.GET, ov._index_url(slug), body=SAMPLE_INDEX, status=200)
    # NB: no detail response registered — a fetch would raise, proving the skip.

    rc = ov.poll(_args(jurisdictions=slug))

    assert rc == 0  # nothing new, nothing failed
