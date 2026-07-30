"""Support page + app-level footer.

Deliberately asserts INVARIANTS, never the live figures. A test pinning
"2,606,405 records" fails on the next ETL refresh and trains everyone to
ignore it; a test asserting officials == members + judges keeps working and
still catches a broken derivation.
"""

from __future__ import annotations

import re
import sys
from html.parser import HTMLParser
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "utility"))

import ui.components as C  # noqa: E402
from data_access.support_data import SupportStats, load_support_stats  # noqa: E402

_VOID = {"br", "hr", "img", "meta", "input", "link"}


class _Balance(HTMLParser):
    """Minimal well-formedness check: every open tag closes, in order."""

    def __init__(self) -> None:
        super().__init__()
        self.stack: list[str] = []
        self.mismatched: list[str] = []

    def handle_starttag(self, tag, attrs):
        if tag not in _VOID:
            self.stack.append(tag)

    def handle_endtag(self, tag):
        if not self.stack or self.stack[-1] != tag:
            self.mismatched.append(tag)
        else:
            self.stack.pop()


def _assert_balanced(fragment: str) -> None:
    p = _Balance()
    p.feed(fragment)
    assert not p.mismatched, f"mismatched close tags: {p.mismatched}"
    assert not p.stack, f"unclosed tags: {p.stack}"


@pytest.fixture
def stats() -> SupportStats:
    s = load_support_stats()
    if s is None:
        pytest.skip("metadata/parquet not available in this environment")
    return s


# ── The figures ───────────────────────────────────────────────────────────────
def test_stats_are_positive(stats: SupportStats) -> None:
    assert stats.records > 0
    assert stats.record_datasets > 0
    assert stats.sources > 0
    assert stats.publishers > 0
    assert stats.oireachtas_members > 0
    assert stats.judges > 0


def test_officials_is_the_sum_of_its_disclosed_parts(stats: SupportStats) -> None:
    """The page prints the split beside the total so a reader can check it.
    If they ever disagree, the page is lying to them."""
    assert stats.officials == stats.oireachtas_members + stats.judges


def test_publishers_never_exceed_feeds(stats: SupportStats) -> None:
    """One publisher can file several feeds, never the other way round."""
    assert stats.publishers <= stats.sources


@pytest.mark.parametrize(
    ("records", "expected"),
    [(2_606_405, "2.6m"), (1_000_000, "1.0m"), (45_900, "45k"), (999, "999")],
)
def test_records_display_rounds_down(records: int, expected: str) -> None:
    """Never round a claim upward — 2,699,999 is "2.6m", not "2.7m"."""
    s = SupportStats(
        records=records,
        record_datasets=1,
        sources=1,
        publishers=1,
        oireachtas_members=1,
        judges=1,
    )
    assert s.records_display == expected


def test_display_never_overstates(stats: SupportStats) -> None:
    m = re.fullmatch(r"([\d.]+)([mk]?)", stats.records_display)
    assert m, stats.records_display
    scale = {"m": 1_000_000, "k": 1_000, "": 1}[m.group(2)]
    assert float(m.group(1)) * scale <= stats.records


# ── Markup ────────────────────────────────────────────────────────────────────
def test_page_html_is_balanced_and_script_free(stats: SupportStats) -> None:
    body = C.support_page_html(stats)
    _assert_balanced(body)
    assert "<script" not in body.lower()


def test_footer_is_balanced_and_script_free() -> None:
    footer = C.site_footer_html(last_refresh="26 July 2026")
    _assert_balanced(footer)
    assert "<script" not in footer.lower()


def test_page_renders_without_stats() -> None:
    """On Cloud a parquet can genuinely be absent. Dashes, never a guess."""
    body = C.support_page_html(None)
    _assert_balanced(body)
    assert "figure unavailable" in body


def test_exact_counts_are_shown_beside_rounded_ones(stats: SupportStats) -> None:
    body = C.support_page_html(stats)
    assert f"{stats.records:,}" in body
    # "&" is escaped to "&amp;" on the way out, which is the point of escaping —
    # assert the disclosed parts rather than the punctuation between them.
    assert f"{stats.oireachtas_members} TDs" in body
    assert f"{stats.judges} judges" in body


# ── Env gates: both OFF by default, so a fresh checkout publishes neither ─────
def test_no_coffee_link_when_url_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DT_COFFEE_URL", raising=False)
    assert C.support_ask_html() == ""
    assert "buymeacoffee" not in C.support_page_html(None).lower()
    assert "from=footer" not in C.site_footer_html()


def test_coffee_link_appears_when_url_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DT_COFFEE_URL", "https://buymeacoffee.com/example")
    ask = C.support_ask_html()
    assert "https://buymeacoffee.com/example" in ask
    assert 'target="_blank"' in ask and "noopener" in ask
    _assert_balanced(ask)
    # The footer routes INWARD to /support so the cookieless page-view log can
    # count interest; it must never link straight out to the payment page.
    footer = C.site_footer_html()
    assert "/support?from=footer" in footer
    assert "buymeacoffee" not in footer.lower()


def test_no_email_published_when_alias_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    """The most important gate here: a fresh checkout must not publish an
    address that nobody has decided to expose."""
    monkeypatch.delenv("DT_CONTACT_EMAIL", raising=False)
    assert "mailto:" not in C.support_help_html()


def test_email_appears_when_alias_set(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DT_CONTACT_EMAIL", "alias@example.org")
    help_html = C.support_help_html()
    assert "mailto:alias@example.org" in help_html
    _assert_balanced(help_html)


def test_issue_links_are_prefilled_and_public() -> None:
    help_html = C.support_help_html()
    assert "github.com/peweet/dail_tracker/issues/new" in help_html
    assert "labels=data-correction" in help_html
    assert "labels=enhancement" in help_html
    # The correction template must keep asking for a source: an unsourced
    # "this number is wrong" cannot be acted on.
    assert "Source" in C._CORRECTION_BODY
