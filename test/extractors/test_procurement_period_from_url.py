"""Tests for the two procurement period_from_url parsers (LA payments + public body).

These turn a file URL into (period, year, quarter). Mis-parsing silently FILES PAYMENTS
UNDER THE WRONG YEAR/QUARTER — a money-grain defect. Pure regex, so pin the key cases,
including the LA parser's documented %20→"2020" trap.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from extractors.procurement_la_payments_extract import period_from_url as la_period  # noqa: E402
from extractors.procurement_public_body_extract import period_from_url as pb_period  # noqa: E402


# ── LA payments ───────────────────────────────────────────────────────────────
def test_la_parses_quarter_and_year():
    assert la_period("https://council.ie/files/Mayo Q2 2025.xlsx") == ("2025-Q2", 2025, 2)


def test_la_unquotes_so_pct20_is_not_misread_as_2020():
    # raw "...file%202024..." contains the substring "2020"; a naive year regex grabs it.
    # period_from_url unquotes first, so the real year (2024) wins.
    assert la_period("https://council.ie/files/file%202024.xlsx") == ("2024", 2024, None)


def test_la_none_when_url_is_dateless():
    assert la_period("https://mayococo.ie/getattachment/abc-guid/attachment.aspx") == (None, None, None)


# ── public body ───────────────────────────────────────────────────────────────
def test_pb_parses_quarter_and_year():
    assert pb_period("https://gov.ie/files/POs-q1-2024.csv") == ("2024-Q1", 2024, 1)


def test_pb_year_only_when_no_quarter():
    assert pb_period("https://gov.ie/files/spend-2023.pdf") == ("2023", 2023, None)


def test_pb_none_when_dateless():
    assert pb_period("https://gov.ie/files/purchase-orders.pdf") == (None, None, None)
