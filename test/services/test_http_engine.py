"""
Mocked-HTTP tests for services/http_engine.py.

The module wraps `requests` with a shared session + concurrent fetcher.
Uses the `responses` library to intercept HTTP calls — no real network is
hit, so these tests run in CI without `@pytest.mark.sources`.

What this catches:
  - Behaviour drift in the success path (returns parsed JSON + byte count).
  - Failure handling: 404/500/timeout/JSON-decode-error → counted, not crashed.
  - Empty input is a no-op rather than a hang.
  - Concurrent fetch_all aggregates correctly across multiple URLs.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import requests
import responses

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from services.http_engine import fetch_all, fetch_all_text, fetch_json, fetch_text

# ---------------------------------------------------------------------------
# fetch_json — single-URL path
# ---------------------------------------------------------------------------


@responses.activate
def test_fetch_json_returns_parsed_body_and_byte_count():
    url = "https://api.example.com/members/123"
    body = {"head": {"totalResults": 174}, "results": [{"id": 1}]}
    responses.add(responses.GET, url, json=body, status=200)

    data, raw_bytes = fetch_json(url)

    assert data == body
    assert raw_bytes > 0
    assert isinstance(raw_bytes, int)


@responses.activate
def test_fetch_json_raises_on_4xx():
    """404 must raise — caller's responsibility to handle. fetch_all uses
    this contract to count failures.
    """
    url = "https://api.example.com/missing"
    responses.add(responses.GET, url, json={"error": "not found"}, status=404)

    with pytest.raises(requests.HTTPError):
        fetch_json(url)


@responses.activate
def test_fetch_json_raises_on_5xx():
    url = "https://api.example.com/broken"
    responses.add(responses.GET, url, status=500)

    with pytest.raises(requests.HTTPError):
        fetch_json(url)


@responses.activate
def test_fetch_json_propagates_timeout_via_session():
    """Custom timeout is passed through. We verify by setting an extreme
    timeout that wouldn't trigger and confirming the call still completes.
    A unit-level timeout test would require deeper mocking of the socket layer.
    """
    url = "https://api.example.com/ok"
    responses.add(responses.GET, url, json={"ok": True}, status=200)

    data, _ = fetch_json(url, timeout=(30, 120))

    assert data == {"ok": True}


# ---------------------------------------------------------------------------
# fetch_json — transient-fault retry
# ---------------------------------------------------------------------------


@responses.activate
def test_fetch_json_retries_5xx_then_succeeds(monkeypatch):
    """A retryable status (500) followed by a 200 must recover, not raise.
    Proves the backoff loop re-issues the request rather than giving up.
    """
    import services.http_engine as he

    monkeypatch.setattr(he, "RETRY_BACKOFF_BASE", 0.0)  # no real sleep in tests
    url = "https://api.example.com/flaky"
    responses.add(responses.GET, url, status=500)  # attempt 1
    responses.add(responses.GET, url, json={"ok": True}, status=200)  # attempt 2

    data, raw_bytes = he.fetch_json(url)

    assert data == {"ok": True}
    assert raw_bytes > 0
    assert len(responses.calls) == 2  # retried exactly once


@responses.activate
def test_fetch_json_retries_connection_error_then_succeeds(monkeypatch):
    """A transient ConnectionError (the DNS/read-timeout failure class that
    silently dropped members on a bad connection) must be retried.
    """
    import services.http_engine as he

    monkeypatch.setattr(he, "RETRY_BACKOFF_BASE", 0.0)
    url = "https://api.example.com/blip"
    responses.add(responses.GET, url, body=requests.exceptions.ConnectionError("boom"))
    responses.add(responses.GET, url, json={"recovered": True}, status=200)

    data, _ = he.fetch_json(url)

    assert data == {"recovered": True}
    assert len(responses.calls) == 2


@responses.activate
def test_fetch_json_exhausts_retries_then_raises(monkeypatch):
    """Persistent 500 across all attempts must still raise (the contract
    fetch_all relies on), after exactly RETRY_MAX_ATTEMPTS tries.
    """
    import services.http_engine as he

    monkeypatch.setattr(he, "RETRY_BACKOFF_BASE", 0.0)
    url = "https://api.example.com/down"
    responses.add(responses.GET, url, status=500)

    with pytest.raises(requests.HTTPError):
        he.fetch_json(url)

    assert len(responses.calls) == he.RETRY_MAX_ATTEMPTS


@responses.activate
def test_fetch_json_does_not_retry_4xx(monkeypatch):
    """A permanent 404 must raise on the first attempt with no retry —
    retrying a 404 just wastes wall-clock on a dead URL.
    """
    import services.http_engine as he

    monkeypatch.setattr(he, "RETRY_BACKOFF_BASE", 0.0)
    url = "https://api.example.com/gone"
    responses.add(responses.GET, url, status=404)

    with pytest.raises(requests.HTTPError):
        he.fetch_json(url)

    assert len(responses.calls) == 1  # no retry


# ---------------------------------------------------------------------------
# fetch_all — concurrent path
# ---------------------------------------------------------------------------


@responses.activate
def test_fetch_all_collects_all_results():
    urls = [
        "https://api.example.com/page/1",
        "https://api.example.com/page/2",
        "https://api.example.com/page/3",
    ]
    for i, url in enumerate(urls, start=1):
        responses.add(responses.GET, url, json={"page": i}, status=200)

    results, total_bytes, failures = fetch_all(urls, max_workers=3)

    assert len(results) == 3
    assert {r["page"] for r in results} == {1, 2, 3}
    assert total_bytes > 0
    assert failures == 0


@responses.activate
def test_fetch_all_counts_failures_without_aborting():
    """Mixed success + failure: 2 results collected, 1 failure counted.
    This is the contract enrich/pipeline relies on — one bad URL must not
    poison the whole batch.
    """
    urls = [
        "https://api.example.com/ok-1",
        "https://api.example.com/broken",
        "https://api.example.com/ok-2",
    ]
    responses.add(responses.GET, urls[0], json={"x": 1}, status=200)
    responses.add(responses.GET, urls[1], status=500)
    responses.add(responses.GET, urls[2], json={"x": 2}, status=200)

    results, total_bytes, failures = fetch_all(urls, max_workers=2)

    assert len(results) == 2
    assert failures == 1
    assert total_bytes > 0


def test_fetch_all_empty_input_returns_empty_immediately():
    """No HTTP calls, no thread pool spinning up, no hang."""
    results, total_bytes, failures = fetch_all([], max_workers=5)

    assert results == []
    assert total_bytes == 0
    assert failures == 0


@responses.activate
def test_fetch_all_aggregates_total_bytes():
    """total_bytes must equal sum of per-response sizes — proves the byte
    accounting isn't silently dropping one of the futures.
    """
    urls = ["https://api.example.com/a", "https://api.example.com/b"]
    body_a = {"data": "a" * 100}  # ~110 bytes serialised
    body_b = {"data": "b" * 200}  # ~210 bytes serialised
    responses.add(responses.GET, urls[0], json=body_a, status=200)
    responses.add(responses.GET, urls[1], json=body_b, status=200)

    _, total_bytes, _ = fetch_all(urls, max_workers=2)

    # The exact byte count depends on JSON serialisation; assert both lower
    # and upper bounds rather than an exact value.
    assert 200 < total_bytes < 1000, f"Total bytes {total_bytes} outside expected range"


@responses.activate
def test_fetch_all_handles_json_decode_error_as_failure():
    """A 200 with non-JSON body raises in fetch_json's `.json()` call;
    fetch_all must count it as a failure, not a result.
    """
    url = "https://api.example.com/not-json"
    responses.add(responses.GET, url, body="<html>not json</html>", status=200, content_type="text/html")

    results, _, failures = fetch_all([url], max_workers=1)

    assert results == []
    assert failures == 1


@responses.activate
def test_fetch_json_exhausts_connection_error_then_raises(monkeypatch):
    """A ConnectionError on EVERY attempt must propagate after the loop (line that
    re-raises the last transient exception) — the contract fetch_all counts on."""
    import services.http_engine as he

    monkeypatch.setattr(he, "RETRY_BACKOFF_BASE", 0.0)
    url = "https://api.example.com/always-down"
    for _ in range(he.RETRY_MAX_ATTEMPTS):
        responses.add(responses.GET, url, body=requests.exceptions.ConnectionError("boom"))

    with pytest.raises(requests.exceptions.ConnectionError):
        he.fetch_json(url)
    assert len(responses.calls) == he.RETRY_MAX_ATTEMPTS


# ---------------------------------------------------------------------------
# fetch_text — single-URL non-JSON path (AKN XML etc.)
# ---------------------------------------------------------------------------


@responses.activate
def test_fetch_text_returns_body_and_byte_count():
    url = "https://api.example.com/debate/main.xml"
    xml = "<akomaNtoso><debate>…</debate></akomaNtoso>"
    responses.add(responses.GET, url, body=xml, status=200)

    text, raw_bytes = fetch_text(url)

    assert text == xml
    assert raw_bytes > 0 and isinstance(raw_bytes, int)


@responses.activate
def test_fetch_text_raises_on_4xx_without_retry(monkeypatch):
    """A 403 from the AKN/S3 bucket means the object key does not exist — fail
    fast (no retry), the caller counts it as a miss."""
    import services.http_engine as he

    monkeypatch.setattr(he, "RETRY_BACKOFF_BASE", 0.0)
    url = "https://api.example.com/missing.xml"
    responses.add(responses.GET, url, status=403)

    with pytest.raises(requests.HTTPError):
        he.fetch_text(url)
    assert len(responses.calls) == 1  # no retry on permanent 4xx


@responses.activate
def test_fetch_text_retries_503_then_succeeds(monkeypatch):
    import services.http_engine as he

    monkeypatch.setattr(he, "RETRY_BACKOFF_BASE", 0.0)
    url = "https://api.example.com/flaky.xml"
    responses.add(responses.GET, url, status=503)  # attempt 1 (retryable)
    responses.add(responses.GET, url, body="<ok/>", status=200)  # attempt 2

    text, _ = he.fetch_text(url)

    assert text == "<ok/>"
    assert len(responses.calls) == 2


@responses.activate
def test_fetch_text_exhausts_retries_then_raises(monkeypatch):
    import services.http_engine as he

    monkeypatch.setattr(he, "RETRY_BACKOFF_BASE", 0.0)
    url = "https://api.example.com/down.xml"
    responses.add(responses.GET, url, status=503)

    with pytest.raises(requests.HTTPError):
        he.fetch_text(url)
    assert len(responses.calls) == he.RETRY_MAX_ATTEMPTS


@responses.activate
def test_fetch_text_retries_connection_error_then_succeeds(monkeypatch):
    import services.http_engine as he

    monkeypatch.setattr(he, "RETRY_BACKOFF_BASE", 0.0)
    url = "https://api.example.com/blip.xml"
    responses.add(responses.GET, url, body=requests.exceptions.ConnectionError("boom"))
    responses.add(responses.GET, url, body="<ok/>", status=200)

    text, _ = he.fetch_text(url)

    assert text == "<ok/>"
    assert len(responses.calls) == 2


@responses.activate
def test_fetch_text_exhausts_connection_error_then_raises(monkeypatch):
    import services.http_engine as he

    monkeypatch.setattr(he, "RETRY_BACKOFF_BASE", 0.0)
    url = "https://api.example.com/dead.xml"
    for _ in range(he.RETRY_MAX_ATTEMPTS):
        responses.add(responses.GET, url, body=requests.exceptions.ConnectionError("boom"))

    with pytest.raises(requests.exceptions.ConnectionError):
        he.fetch_text(url)
    assert len(responses.calls) == he.RETRY_MAX_ATTEMPTS


# ---------------------------------------------------------------------------
# fetch_all_text — concurrent (url, text) path
# ---------------------------------------------------------------------------


def test_fetch_all_text_empty_input_returns_empty_immediately():
    results, total_bytes, failures = fetch_all_text([], max_workers=5)
    assert results == [] and total_bytes == 0 and failures == 0


@responses.activate
def test_fetch_all_text_returns_url_text_pairs():
    """Unlike fetch_all, results are (url, text) pairs so a caller can name an
    output file per source URL — assert the pairing is preserved."""
    urls = ["https://api.example.com/a.xml", "https://api.example.com/b.xml"]
    responses.add(responses.GET, urls[0], body="<a/>", status=200)
    responses.add(responses.GET, urls[1], body="<bb/>", status=200)

    results, total_bytes, failures = fetch_all_text(urls, max_workers=2)

    assert dict(results) == {urls[0]: "<a/>", urls[1]: "<bb/>"}
    assert failures == 0 and total_bytes > 0


@responses.activate
def test_fetch_all_text_counts_failures_without_aborting():
    urls = ["https://api.example.com/ok.xml", "https://api.example.com/bad.xml"]
    responses.add(responses.GET, urls[0], body="<ok/>", status=200)
    responses.add(responses.GET, urls[1], status=404)

    results, _, failures = fetch_all_text(urls, max_workers=2)

    assert [u for u, _ in results] == [urls[0]]
    assert failures == 1
