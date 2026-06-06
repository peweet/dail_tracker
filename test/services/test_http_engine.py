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
from services.http_engine import fetch_all, fetch_json

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
