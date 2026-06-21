"""WAF circuit-breaker for the ministerial-diary downloader.

The EDUCATION backfill (160 born-digital PDFs) used to grind every file through its full
retry budget once the gov.ie WAF window shut — ~45 min of futile 405s landing nothing. The
breaker trips after WAF_CIRCUIT_BREAK consecutive files exhaust their retries on a WAF status,
so the download loops stop hammering and defer the rest to a fresh-window run. A real fetch
resets it. These pin that increment/reset behaviour and the threshold flip.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import requests

import extractors.ministerial_diaries_extract as m


@pytest.fixture(autouse=True)
def _isolate(monkeypatch, tmp_path):
    """Reset the breaker, silence pacing, and point the PDF cache at an empty tmp dir."""
    m.reset_waf_circuit()
    monkeypatch.setattr(m, "_pace", lambda: None)
    monkeypatch.setattr(m, "PDF_CACHE", tmp_path)
    yield
    m.reset_waf_circuit()


def _raise_waf(*_a, **_k):
    raise requests.exceptions.HTTPError(response=SimpleNamespace(status_code=405))


def test_waf_exhaustion_increments_then_trips(monkeypatch):
    monkeypatch.setattr(m._SESSION, "get", _raise_waf)
    assert not m.waf_window_shut()
    for i in range(1, m.WAF_CIRCUIT_BREAK + 1):
        assert m.download("https://x/y.pdf", f"f{i}.pdf", retries=1) is None
    # exactly WAF_CIRCUIT_BREAK consecutive blocks → window reads as shut
    assert m.waf_window_shut()


def test_successful_fetch_resets_breaker(monkeypatch):
    m._consecutive_waf_blocks = m.WAF_CIRCUIT_BREAK  # pretend the window had been shut
    assert m.waf_window_shut()

    def _ok(*_a, **_k):
        return SimpleNamespace(status_code=200, content=b"%PDF-1.4 ok", raise_for_status=lambda: None)

    monkeypatch.setattr(m._SESSION, "get", _ok)
    assert m.download("https://x/ok.pdf", "ok.pdf", retries=1) is not None
    assert not m.waf_window_shut()


def test_non_waf_failure_does_not_trip(monkeypatch):
    """A genuine 404 is not a WAF block — it must not advance the breaker."""

    def _raise_404(*_a, **_k):
        raise requests.exceptions.HTTPError(response=SimpleNamespace(status_code=404))

    monkeypatch.setattr(m._SESSION, "get", _raise_404)
    for i in range(m.WAF_CIRCUIT_BREAK + 2):
        m.download("https://x/missing.pdf", f"m{i}.pdf", retries=1)
    assert not m.waf_window_shut()


# --------------------------------------------------------------------------- brotli guard
def test_accept_encoding_excludes_brotli() -> None:
    """gov.ie's CDN serves brotli intermittently; `requests` can't decode it without the
    (uninstalled) brotli package, so r.text becomes mojibake and BeautifulSoup finds 0 PDFs —
    listing discovery silently returns nothing. The header must not advertise 'br'. If this
    fails because someone re-added 'br', they MUST also `pip install brotli`."""
    enc = m.HEADERS.get("Accept-Encoding", "")
    assert "br" not in [tok.strip() for tok in enc.split(",")], f"Accept-Encoding advertises brotli: {enc!r}"
