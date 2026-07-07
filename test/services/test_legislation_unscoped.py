"""Tests for services/legislation_unscoped.fetch_all_bills — the unscoped-bills
skip/limit paginator. Like ted_search, its reason to exist is anti-silent-truncation:
it must page until the cumulative count reaches head.counts.resultCount and assert it.

fetch_json is stubbed (its own network path is covered in test_http_engine), so these
isolate the pagination/assertion logic.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import services.legislation_unscoped as lu  # noqa: E402


def _page(result_count, n, source="Government"):
    body = {
        "head": {"counts": {"resultCount": result_count}},
        "results": [{"bill": {"source": source}} for _ in range(n)],
    }
    return body, 100  # (payload, raw_bytes)


def test_single_page_returns_synthetic_payload(monkeypatch):
    monkeypatch.setattr(lu, "fetch_json", lambda url: _page(2, 2))
    payload, total = lu.fetch_all_bills()
    assert len(payload) == 1
    assert len(payload[0]["results"]) == 2
    assert payload[0]["head"]["counts"]["resultCount"] == 2
    assert total == 100


def test_paginates_until_total_reached(monkeypatch):
    calls = {"n": 0}

    def fake(url):
        calls["n"] += 1
        return _page(1500, 1000) if calls["n"] == 1 else _page(1500, 500)  # 1000 + 500 = 1500

    monkeypatch.setattr(lu, "fetch_json", fake)
    payload, _ = lu.fetch_all_bills()
    assert len(payload[0]["results"]) == 1500
    assert calls["n"] == 2  # exactly two pages


def test_truncation_drift_raises(monkeypatch):
    # server says 10 but a short page (<PAGE_SIZE) returns 2 → cumulative < expected → assert.
    monkeypatch.setattr(lu, "fetch_json", lambda url: _page(10, 2))
    with pytest.raises(AssertionError):
        lu.fetch_all_bills()


def test_malformed_result_count_raises(monkeypatch):
    monkeypatch.setattr(
        lu, "fetch_json", lambda url: ({"head": {"counts": {"resultCount": "lots"}}, "results": []}, 10)
    )
    with pytest.raises(ValueError):
        lu.fetch_all_bills()
