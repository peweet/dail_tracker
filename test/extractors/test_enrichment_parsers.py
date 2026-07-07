"""Unit tests for the pure parsing functions of the three enrichment scrapers.

These lock the parse CONTRACTS (amount/currency/scale, party-name extraction,
title-vs-truncated-text cell logic) without any network — the scrapers themselves
live in pipeline_sandbox/ and are run by hand to refresh the sandbox parquet that
extractors/enrichment_promote_to_gold.py then promotes.

Run:  pytest test/extractors/test_enrichment_parsers.py -v
"""

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "pipeline_sandbox"))

pytest.importorskip("polars")
pytest.importorskip("bs4")
isif = pytest.importorskip("isif_portfolio_extract")
cbi = pytest.importorskip("cbi_enforcement_extract")
tam = pytest.importorskip("eu_tam_ireland_extract")

from bs4 import BeautifulSoup  # noqa: E402

# ----------------------------- ISIF: parse_lead_amount -----------------------------


@pytest.mark.parametrize(
    "desc, amount, currency, up_to",
    [
        ("€140m commitment to Acme Energy", 140_000_000.0, "EUR", False),
        ("$20m commitment alongside partners", 20_000_000.0, "USD", False),
        ("£5.5m investment in housing", 5_500_000.0, "GBP", False),
        ("€1.2bn fund anchored by ISIF", 1_200_000_000.0, "EUR", False),
        ("up to €5m committed to the venture", 5_000_000.0, "EUR", True),
        ("A strategic partnership with no figure", None, None, False),
    ],
)
def test_isif_parse_lead_amount(desc, amount, currency, up_to):
    assert isif.parse_lead_amount(desc) == (amount, currency, up_to)


def test_isif_takes_first_money_mention_only():
    # later figures (fund target sizes) must not override ISIF's own lead commitment
    amount, currency, _ = isif.parse_lead_amount("€30m commitment to a €500m fund")
    assert (amount, currency) == (30_000_000.0, "EUR")


# ----------------------------- CBI: _euro_to_float + parse_app_data -----------------------------


@pytest.mark.parametrize(
    "num, scale, expected",
    [
        ("83,300,000", None, 83_300_000.0),
        ("192,500", None, 192_500.0),
        ("83.3", "million", 83_300_000.0),
        ("1.4", "billion", 1_400_000_000.0),
    ],
)
def test_cbi_euro_to_float(num, scale, expected):
    assert cbi._euro_to_float(num, scale) == expected


def test_cbi_parse_app_data_extracts_party_and_url():
    html = (
        "<script>var appData = [\n"
        '{ "type": "Settlement", "date": "01/02/2024", '
        '"documentName": decodeTitle("Settlement Agreement between the Central Bank of Ireland and Acme Bank DAC"), '
        '"url": decodeTitle("/docs/acme.pdf") }\n'
        "];</script>"
    )
    rows = cbi.parse_app_data(html)
    assert len(rows) == 1
    row = rows[0]
    assert row["party_name"] == "Acme Bank DAC"
    assert row["notice_date"] == "01/02/2024"
    assert row["pdf_url"].endswith("/docs/acme.pdf")
    assert row["pdf_url"].startswith("https://www.centralbank.ie")


def test_cbi_parse_app_data_handles_enforcement_against_phrasing():
    html = (
        "<script>var appData = [\n"
        '{ "type": "Notice", "date": "15/06/2023", '
        '"documentName": decodeTitle("Enforcement Action against Beta Insurance Limited"), '
        '"url": decodeTitle("/x.pdf") }\n'
        "];</script>"
    )
    assert cbi.parse_app_data(html)[0]["party_name"] == "Beta Insurance Limited"


def test_cbi_parse_app_data_raises_on_markup_drift():
    with pytest.raises(RuntimeError, match="appData"):
        cbi.parse_app_data("<html>no app data array here</html>")


# ----------------------------- EU-TAM: cell_text -----------------------------


def _td(html: str):
    return BeautifulSoup(html, "html.parser").find("td")


def test_tam_cell_text_prefers_title_when_display_truncated():
    td = _td(
        '<td title="Enterprise Ireland Research Development and Innovation Programme">Enterprise Ireland Rese...</td>'
    )
    assert tam.cell_text(td) == "Enterprise Ireland Research Development and Innovation Programme"


def test_tam_cell_text_keeps_text_for_national_id_tooltip():
    # the National-ID cell abuses title for a tooltip ("Six digit Company Registration")
    td = _td('<td title="Six digit Company Registration number">123456</td>')
    assert tam.cell_text(td) == "123456"


def test_tam_cell_text_plain_cell():
    td = _td("<td>Galway</td>")
    assert tam.cell_text(td) == "Galway"
