"""Unit tests for the orphan-procurement source poller.

Covers the pure parsing/classification logic of tools/procurement_source_poller.py
without touching the network: ``_fetch`` is monkeypatched to return canned HTML, so
FRESH/CURRENT/UNREACHABLE/NO_PERIODS/MANUAL are all exercised deterministically.
Pure stdlib, no marker, default CI lane.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import tools.procurement_source_poller as poll  # noqa: E402


# ── period / link extraction ─────────────────────────────────────────────────
def test_quarters_parses_both_orderings_and_ranges():
    # "Q2 2025", "2025 Q2", and a range that should yield both endpoints.
    assert (2025, 2) in poll._quarters("Quarterly PO Listing Q2 2025")
    assert (2025, 2) in poll._quarters("PO-Report-2025-Q2.pdf")
    span = poll._quarters("PO Listing Q1 2024 to Quarter 2 2025")
    assert (2024, 1) in span and (2025, 2) in span
    assert max(span) == (2025, 2)


def test_years_parses_annual():
    assert poll._years("2024_Tusla_POs_over_20K_Final.pdf") == {2024}


def test_pdf_links_filters_by_must_match():
    html = """
    <a href="/files/Quarterly-PO-Listing-Q3-2025.pdf">PO Listing Q3 2025</a>
    <a href="/files/annual-report-2025.pdf">Annual Report</a>
    <a href="/contact">Contact</a>
    """
    links = poll._pdf_links(html, must_match=r"\bpo|purchase|listing|q[1-4]")
    hrefs = [h for h, _ in links]
    assert "/files/Quarterly-PO-Listing-Q3-2025.pdf" in hrefs
    assert "/files/annual-report-2025.pdf" not in hrefs  # filtered out
    assert "/contact" not in hrefs  # not a pdf


# ── classification ───────────────────────────────────────────────────────────
def _patch_fetch(monkeypatch, mapping):
    """mapping: url -> html (or None to simulate an unreachable fetch)."""
    def fake(url):
        html = mapping.get(url)
        return (html, None) if html else (None, "patched: down")
    monkeypatch.setattr(poll, "_fetch", fake)


def test_fresh_when_upstream_has_newer_quarter(monkeypatch):
    url = "https://example.test/po/"
    _patch_fetch(monkeypatch, {url: (
        '<a href="po-q2-2025.pdf">PO Q2 2025</a>'
        '<a href="po-q3-2025.pdf">PO Q3 2025</a>'
        '<a href="po-q4-2025.pdf">PO Q4 2025</a>'
    )})
    src = {
        "id": "x", "name": "X", "grain": "quarterly", "check": "auto",
        "listing_urls": [url], "must_match": r"po|q[1-4]", "held_through": [2025, 2],
    }
    row = poll._evaluate_source(src)
    assert row["status"] == "FRESH"
    assert row["upstream_newest"] == [2025, 4]
    assert row["new_periods"] == [[2025, 3], [2025, 4]]


def test_current_when_held_is_newest(monkeypatch):
    url = "https://example.test/po/"
    _patch_fetch(monkeypatch, {url: '<a href="po-q1-2026.pdf">PO Q1 2026</a>'})
    src = {
        "id": "x", "name": "X", "grain": "quarterly", "check": "auto",
        "listing_urls": [url], "must_match": r"po|q[1-4]", "held_through": [2026, 1],
    }
    assert poll._evaluate_source(src)["status"] == "CURRENT"


def test_linkless_stub_is_unreachable_not_no_periods(monkeypatch):
    # A WAF/JS stub fetches OK but carries no anchors — must not read as CURRENT/NO_PERIODS.
    url = "https://example.test/po/"
    _patch_fetch(monkeypatch, {url: "<html><body>Loading…</body></html>"})
    src = {
        "id": "x", "name": "X", "grain": "quarterly", "check": "auto",
        "listing_urls": [url], "must_match": r"po", "held_through": [2025, 2],
    }
    assert poll._evaluate_source(src)["status"] == "UNREACHABLE"


def test_unreachable_when_fetch_fails(monkeypatch):
    url = "https://example.test/po/"
    _patch_fetch(monkeypatch, {})  # nothing maps -> fetch returns None
    src = {
        "id": "x", "name": "X", "grain": "quarterly", "check": "auto",
        "listing_urls": [url], "must_match": r"po", "held_through": [2025, 2],
    }
    assert poll._evaluate_source(src)["status"] == "UNREACHABLE"


def test_no_periods_when_links_lack_dates(monkeypatch):
    url = "https://example.test/po/"
    _patch_fetch(monkeypatch, {url: '<a href="purchase-orders.pdf">Purchase Orders</a>'})
    src = {
        "id": "x", "name": "X", "grain": "quarterly", "check": "auto",
        "listing_urls": [url], "must_match": r"purchase", "held_through": [2025, 2],
    }
    assert poll._evaluate_source(src)["status"] == "NO_PERIODS"


def test_annual_fresh(monkeypatch):
    url = "https://example.test/po/"
    _patch_fetch(monkeypatch, {url: (
        '<a href="2024_POs_over_20k.pdf">2024 POs</a>'
        '<a href="2025_POs_over_20k.pdf">2025 POs</a>'
    )})
    src = {
        "id": "x", "name": "X", "grain": "annual", "check": "auto",
        "listing_urls": [url], "must_match": r"po", "held_through": [2024],
    }
    row = poll._evaluate_source(src)
    assert row["status"] == "FRESH"
    assert row["new_periods"] == [[2025]]


def test_manual_source_is_not_fetched(monkeypatch):
    def boom(url):  # would raise if called
        raise AssertionError("manual source must not fetch")
    monkeypatch.setattr(poll, "_fetch", boom)
    src = {
        "id": "x", "name": "X", "grain": "quarterly", "check": "manual",
        "listing_urls": ["https://example.test"], "held_through": [2025, 2],
        "manual_reason": "needs a browser",
    }
    row = poll._evaluate_source(src)
    assert row["status"] == "MANUAL"
    assert row["note"] == "needs a browser"


def test_live_registry_is_well_formed():
    # Every registered source has the fields the evaluator reads.
    for src in poll.SOURCES:
        assert src["grain"] in ("quarterly", "annual")
        assert src["check"] in ("auto", "manual")
        assert isinstance(src["held_through"], list) and src["held_through"]
        if src["check"] == "auto":
            assert src.get("must_match")
        else:
            assert src.get("manual_reason")
