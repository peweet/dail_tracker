"""Unit cover for the HSA COMAH extractor's pure functions (no network, no store).

The two defects the first build actually hit are pinned here: undecoded HTML entities
("ESB&nbsp;Moneypoint" shipped with a literal &nbsp;), and hardcoded region slugs silently
losing 35 lower-tier rows because the two tiers group counties differently and the curl
fallback turned 404 pages into zero parsed rows without an error.
"""

from __future__ import annotations

from extractors.hsa_comah_extract import _REGION_LINK, _norm, _parse_table, _tokens

_PAGE = """
<html><body>
<table>
<tr><th>Establishment Name</th><th>Establishment Address</th></tr>
<tr><td>Eli Lilly Kinsale Limited</td><td>Dunderrow, Kinsale, Co. Cork</td></tr>
<tr><td>ESB&nbsp;Moneypoint</td><td>Kilrush,   Co. Clare</td></tr>
<tr><td></td><td>blank-name row must be dropped</td></tr>
</table>
</body></html>
"""


def test_parse_table_drops_header_and_decodes_entities():
    rows = _parse_table(_PAGE)
    assert rows == [
        ("Eli Lilly Kinsale Limited", "Dunderrow, Kinsale, Co. Cork"),
        ("ESB Moneypoint", "Kilrush, Co. Clare"),  # &nbsp; decoded, whitespace collapsed
    ]


def test_parse_table_returns_empty_on_pages_without_tables():
    """A 404 body has no establishment table; the caller treats [] as a dead link to record,
    never as a region with zero establishments."""
    assert _parse_table("<html><body><h1>Page not found</h1></body></html>") == []


def test_norm_strips_legal_forms_and_accents():
    assert _norm("Calor Teoranta") == "calor"
    assert _norm("Hovione Ltd.") == "hovione"
    assert _norm("Sí Éire Chemicals Ltd") == "si eire chemicals"


def test_tokens_ignore_single_letters():
    assert _tokens("A B Chemical Co.") == {"chemical"}


def test_region_link_regex_matches_both_tiers():
    html = (
        '<a href="/x/upper_tier_comah_establishments_by_region/upper_tier_establishments_in_cork_kerry/">u</a>'
        '<a href="/x/lower_tier_establishments_by_region/lower_tier_establishments_in_dublin_louth/">l</a>'
        '<a href="/x/unrelated_page/">n</a>'
    )
    links = _REGION_LINK.findall(html)
    assert len(links) == 2
    assert all("_tier_establishments_in_" in l for l in links)
