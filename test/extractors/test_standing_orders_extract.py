"""Tests for extractors/standing_orders_extract.py (graduated from the sandbox 2026-08-01).

Pins the verbatim-clause extraction logic — the no-inference rule means this extractor's
whole value is in returning the source's OWN words unmangled, so the parsing helpers are
the load-bearing surface, not the network crawl (mocking fitz/requests for that buys little).

  1. clause() — first verbatim sentence around a keyword, cleaned of whitespace, capped
     at `window` chars. Case-insensitive; returns "" when the keyword is absent (never
     invents a clause).
  2. order_of_business() — numbered/lettered agenda-template items under the "Order of
     Business" heading, deduplicated, capped at 10.
  3. records_named_votes regression — the roll-call/recorded-vote detector inside parse_so()
     is the STRUCTURAL reason named voting records exist for some councils and not others
     (council_votes_extract.py's Carlow/Cork/Galway adapters only fire where this is true);
     a false negative here would silently explain away a real named-vote council as
     "no roll call", so the phrasing variants it must catch are pinned directly.
"""

from __future__ import annotations

import re

from extractors.standing_orders_extract import SO_RX, clause, order_of_business

# The exact detector inside parse_so() — duplicated here (not imported) because it lives
# inline in that function; kept byte-identical so this test fails the moment it drifts.
_NAMED_VOTES_RX = re.compile(
    r"roll[\s-]?call|recorded vote|names?.{0,30}recorded|voting.{0,20}by name", re.I
)


def test_clause_returns_verbatim_sentence_around_keyword() -> None:
    text = "Some preamble. The quorum shall be six members of the council. Trailing text."
    assert clause(text, "quorum") == "The quorum shall be six members of the council."


def test_clause_is_case_insensitive() -> None:
    text = "A recorded VOTE may be demanded by any three members present."
    assert "recorded VOTE" in clause(text, "recorded vote")


def test_clause_returns_empty_when_keyword_absent() -> None:
    assert clause("No governance content here at all.", "quorum") == ""


def test_clause_tries_keywords_in_order_first_match_wins() -> None:
    text = "A vote may be taken by a show of hands unless a division is demanded."
    got = clause(text, "roll call", "show of hands", window=200)
    assert "show of hands" in got


def test_clause_caps_at_window() -> None:
    long_tail = "quorum " + ("x" * 500) + "."
    got = clause(long_tail, "quorum", window=50)
    assert len(got) <= 50


def test_order_of_business_extracts_numbered_items() -> None:
    text = """Order of Business
    1. Minutes of the previous meeting
    2. Correspondence
    3. Notices of Motion
    Next section starts here.
    """
    items = order_of_business(text)
    assert items[:3] == ["Minutes of the previous meeting", "Correspondence", "Notices of Motion"]


def test_order_of_business_dedupes_and_caps_at_ten() -> None:
    lines = "\n".join(f"{i}. Item {i % 3}" for i in range(1, 15))
    text = f"Order of Business\n{lines}\n"
    items = order_of_business(text)
    assert len(items) <= 10
    assert len(items) == len(set(items))


def test_order_of_business_empty_when_heading_absent() -> None:
    assert order_of_business("No agenda template section in this document.") == []


def test_so_rx_matches_hyphenated_and_plain_forms() -> None:
    assert SO_RX.search("Standing Orders 2024")
    assert SO_RX.search("standing-order.pdf")


def test_named_votes_regex_catches_roll_call_hyphen_variants() -> None:
    for phrasing in (
        "a roll call vote may be demanded",
        "a roll-call shall be taken",
        "the names of members present shall be recorded",
        "voting shall be by name on request",
        "a recorded vote is required",
    ):
        assert _NAMED_VOTES_RX.search(phrasing), phrasing


def test_named_votes_regex_does_not_fire_on_ordinary_voting_text() -> None:
    assert not _NAMED_VOTES_RX.search("motions are decided by a simple show of hands")
