"""Entity and source links in procurement rows remain separate, valid anchors."""

from __future__ import annotations

import sys
from html.parser import HTMLParser
from pathlib import Path
from types import SimpleNamespace

import pytest

_ROOT = Path(__file__).resolve().parents[2]
for _path in (_ROOT, _ROOT / "utility", _ROOT / "utility" / "pages_code"):
    value = str(_path)
    if value not in sys.path:
        sys.path.insert(0, value)

from procurement import _shared, profiles, ted  # noqa: E402
from ui.entity_links import (  # noqa: E402
    body_link_html,
    procurement_register_url,
    procurement_ted_winner_url,
)


class _AnchorAudit(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.depth = 0
        self.count = 0
        self.nested = False

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag == "a":
            self.nested |= self.depth > 0
            self.depth += 1
            self.count += 1

    def handle_endtag(self, tag: str) -> None:
        if tag == "a":
            self.depth -= 1


def _assert_separate_links(fragment: str, expected: int = 2) -> None:
    audit = _AnchorAudit()
    audit.feed(fragment)
    assert audit.count == expected
    assert not audit.nested
    assert audit.depth == 0


def test_procurement_register_builders_declare_and_retain_register_state():
    assert procurement_register_url("ted") == "/rankings-procurement?tab=wins&reg=ted"
    assert procurement_ted_winner_url("ACME & CO", relative=True) == (
        "?tab=wins&reg=ted&ted_winner=ACME%20%26%20CO"
    )
    with pytest.raises(ValueError, match="unknown procurement register"):
        procurement_register_url("unknown")


def test_unknown_buyer_degrades_to_escaped_text():
    assert body_link_html(None, "Unknown & Body") == "Unknown &amp; Body"


def test_supplier_award_keeps_authority_and_source_as_sibling_links():
    row = SimpleNamespace(
        contracting_authority="Department & Office",
        etenders_notice_url="https://example.test/notice/1",
    )
    fragment = profiles._award_row_html(row)

    _assert_separate_links(fragment)
    assert "?authority=Department%20%26%20Office" in fragment
    assert "Source notice" in fragment


def test_authority_award_keeps_company_and_source_as_sibling_links():
    row = SimpleNamespace(
        supplier="Acme & Co",
        supplier_norm="ACME CO",
        supplier_class="company",
        etenders_notice_url="https://example.test/notice/2",
    )
    fragment = profiles._award_row_by_supplier(row)

    _assert_separate_links(fragment)
    assert "/company?supplier=ACME%20CO" in fragment
    assert "Source notice" in fragment


def test_ted_notice_keeps_buyer_and_source_as_sibling_links(monkeypatch):
    monkeypatch.setattr(
        ted,
        "_buyer_link",
        lambda name: f'<a href="/body?buyer=known" target="_self">{name}</a>',
    )
    row = SimpleNamespace(
        notice_url="https://example.test/ted/1",
        dispatch_date="2026-08-01",
        buyer_name="Known Body",
        value_kind="contract_award",
    )
    fragment = ted._ted_notice_li(row, show_name=False)

    _assert_separate_links(fragment)
    assert "Source notice" in fragment


def test_ted_back_action_retains_register(monkeypatch):
    query_params: dict[str, str] = {"ted_winner": "ACME", "reg": "ted"}
    reruns: list[bool] = []
    monkeypatch.setattr(
        _shared,
        "st",
        SimpleNamespace(query_params=query_params, rerun=lambda: reruns.append(True)),
    )

    _shared._return_to_browse("wins", register="ted")

    assert query_params == {"tab": "wins", "reg": "ted"}
    assert reruns == [True]
