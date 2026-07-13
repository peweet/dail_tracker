"""Guards for the LGAS statutory-audit-reports fact (extractors/lgas_audit_reports_extract.py).

Pure-function units always run; parquet invariants skip cleanly when the fact is absent.
The load-bearing rule: this lane is VERBATIM-only (opinion text + literal-heading booleans) —
no derived classification may creep in."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "extractors"))

from lgas_audit_reports_extract import CANON_31, council_from_slug  # noqa: E402

PARQUET = ROOT / "data" / "silver" / "parquet" / "la_lgas_audit_reports.parquet"


def test_council_from_slug_handles_url_forms():
    assert council_from_slug("carlow-county-council-statutory-audit-report-2023") == "Carlow"
    assert council_from_slug("cavan-county-council-audit-report-2023") == "Cavan"
    assert council_from_slug("cork-city-council-statutory-audit-report-2023") == "Cork City"
    assert council_from_slug("cork-county-council-statutory-audit-report-2023") == "Cork County"
    # the URL-encoded, hyphen-collapsed DLR form seen in the real sitemap
    assert council_from_slug("d%C3%BAn-laoghairerathdown-county-council-audit-report-2023") == "Dun Laoghaire-Rathdown"
    assert council_from_slug("south-dublin-county-council-audit-report-2022") == "South Dublin"
    assert council_from_slug("galway-county-council-audit-report-2020") == "Galway County"
    assert council_from_slug("galway-city-council-audit-report-2020") == "Galway City"


def test_council_from_slug_rejects_non_councils():
    assert council_from_slug("overview-of-the-work-of-the-lgas-2023") is None
    assert council_from_slug("value-for-money-report-27") is None


pl = pytest.importorskip("polars")
needs_fact = pytest.mark.skipif(not PARQUET.exists(), reason="LGAS fact parquet not present")


@pytest.fixture(scope="module")
def fact():
    return pl.read_parquet(PARQUET)


@needs_fact
def test_shape_and_keys(fact):
    assert set(fact["council"].unique()) <= CANON_31
    assert fact["council"].n_unique() >= 28  # near-complete council coverage
    assert fact.group_by("council", "year").len().filter(pl.col("len") > 1).height == 0
    assert fact["year"].min() >= 2012


@needs_fact
def test_verbatim_only_schema(fact):
    # the fact must never grow a score/verdict column — verbatim text + literal-heading flags only
    allowed = {
        "council", "year", "report_page_url", "pages", "audit_opinion_text",
        "has_emphasis_of_matter", "has_ce_response", "section_headings",
    }
    assert set(fact.columns) <= allowed


@needs_fact
def test_opinion_extraction_rate(fact):
    with_opinion = fact.filter(pl.col("audit_opinion_text").str.len_chars() > 50).height
    assert with_opinion / fact.height > 0.7  # templated reports; most must yield the opinion verbatim
