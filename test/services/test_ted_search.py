"""Mocked tests for services/ted_search.py — the TED v3 ITERATION paginator.

The module exists to prevent ONE bug: silent truncation of a TED scroll (the old
PAGE_NUMBER loop capped at 10–15k with no check against the declared total). So the
load-bearing tests are: the iteration token is followed across pages, and the
completeness assertion fires when the server's totalNoticeCount isn't reached.
`responses` intercepts the POST/GET — no real network.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
import requests
import responses

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import services.ted_search as ts  # noqa: E402

URL = ts.URL


@pytest.fixture(autouse=True)
def _no_backoff(monkeypatch):
    monkeypatch.setattr(ts, "RETRY_BACKOFF_BASE", 0.0)  # no real sleeps in tests


@responses.activate
def test_single_page_returns_all_notices():
    responses.add(
        responses.POST,
        URL,
        json={"totalNoticeCount": 2, "notices": [{"pn": "1"}, {"pn": "2"}], "iterationNextToken": None},
        status=200,
    )
    out = ts.fetch_ted_search("q", ["pn"])
    assert [n["pn"] for n in out] == ["1", "2"]


@responses.activate
def test_paginates_via_iteration_token():
    responses.add(
        responses.POST, URL,
        json={"totalNoticeCount": 4, "notices": [{"pn": "1"}, {"pn": "2"}], "iterationNextToken": "tok1"}, status=200,
    )
    responses.add(
        responses.POST, URL,
        json={"totalNoticeCount": 4, "notices": [{"pn": "3"}, {"pn": "4"}], "iterationNextToken": "tok2"}, status=200,
    )
    out = ts.fetch_ted_search("q", ["pn"])
    assert len(out) == 4
    assert len(responses.calls) == 2
    # the 2nd request must carry the token the 1st returned — the scroll mechanism.
    assert json.loads(responses.calls[1].request.body)["iterationNextToken"] == "tok1"


@responses.activate
def test_completeness_assertion_catches_silent_truncation():
    # server declares 10 but returns 2 then stops (no token) → must NOT be written silently.
    responses.add(
        responses.POST, URL,
        json={"totalNoticeCount": 10, "notices": [{"pn": "1"}, {"pn": "2"}], "iterationNextToken": None}, status=200,
    )
    with pytest.raises(AssertionError):
        ts.fetch_ted_search("q", ["pn"])


@responses.activate
def test_max_pages_partial_pull_skips_assertion():
    responses.add(
        responses.POST, URL,
        json={"totalNoticeCount": 10, "notices": [{"pn": "1"}, {"pn": "2"}], "iterationNextToken": "tok1"}, status=200,
    )
    out = ts.fetch_ted_search("q", ["pn"], max_pages=1)  # deliberate partial, no assertion
    assert len(out) == 2


@responses.activate
def test_post_retries_503_then_succeeds():
    responses.add(responses.POST, URL, status=503)  # attempt 1
    responses.add(
        responses.POST, URL,
        json={"totalNoticeCount": 1, "notices": [{"pn": "1"}], "iterationNextToken": None}, status=200,
    )
    out = ts.fetch_ted_search("q", ["pn"])
    assert len(out) == 1
    assert len(responses.calls) == 2


@responses.activate
def test_post_4xx_raises_immediately():
    responses.add(responses.POST, URL, status=400)
    with pytest.raises(requests.HTTPError):
        ts.fetch_ted_search("q", ["pn"])


@responses.activate
def test_post_retries_connection_error_then_succeeds():
    responses.add(responses.POST, URL, body=requests.exceptions.ConnectionError("boom"))  # attempt 1
    responses.add(
        responses.POST, URL,
        json={"totalNoticeCount": 1, "notices": [{"pn": "1"}], "iterationNextToken": None}, status=200,
    )
    out = ts.fetch_ted_search("q", ["pn"])
    assert len(out) == 1
    assert len(responses.calls) == 2


@responses.activate
def test_post_exhausts_connection_error_then_raises():
    for _ in range(ts.RETRY_MAX_ATTEMPTS):
        responses.add(responses.POST, URL, body=requests.exceptions.ConnectionError("boom"))
    with pytest.raises(requests.exceptions.ConnectionError):
        ts.fetch_ted_search("q", ["pn"])
    assert len(responses.calls) == ts.RETRY_MAX_ATTEMPTS


# ── per-notice XML lane ───────────────────────────────────────────────────────
@responses.activate
def test_fetch_notice_xml_writes_file(tmp_path):
    pn = "123-2024"
    responses.add(responses.GET, ts.NOTICE_XML_URL.format(lang="en", pn=pn), body=b"<TED_EXPORT/>", status=200)
    assert ts.fetch_notice_xml(pn, tmp_path) is True
    assert (tmp_path / f"{pn}.xml").read_bytes() == b"<TED_EXPORT/>"


def test_fetch_notice_xml_skips_cached(tmp_path):
    pn = "999"
    (tmp_path / f"{pn}.xml").write_bytes(b"cached")
    # No responses registered: a fetch would fail. A cached file must short-circuit to True.
    assert ts.fetch_notice_xml(pn, tmp_path) is True
    assert (tmp_path / f"{pn}.xml").read_bytes() == b"cached"


@responses.activate
def test_fetch_notice_xml_404_returns_false(tmp_path):
    pn = "404x"
    responses.add(responses.GET, ts.NOTICE_XML_URL.format(lang="en", pn=pn), status=404)
    assert ts.fetch_notice_xml(pn, tmp_path) is False
    assert not (tmp_path / f"{pn}.xml").exists()
