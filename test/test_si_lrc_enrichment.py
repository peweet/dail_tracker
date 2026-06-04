"""Tests for the LRC Classified List enrichment (sandbox spike, PR1 scope).

Two layers:
  1. Pure parse contract — SI_CITE / ELI_SI / parse_category against a small
     hand-built HTML fixture mirroring the real classlist DOM (nested
     <section id="title-N-N"> + <li id="siN"> entries). No network.
  2. Legal-safety invariants on the built summary parquet (skipped if absent):
     the dangerous edge cases the brief calls out — unmatched must NEVER read as
     "in force", status vocabulary must avoid forbidden assertion words, matched
     rows must carry a caveat, the key must be one-row-per-SI.

Run:  pytest test/test_si_lrc_enrichment.py -v
"""

import sys
from pathlib import Path

import polars as pl
import pytest

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "pipeline_sandbox"))

from si_lrc_classlist_extract import ELI_SI, SI_CITE, parse_category  # noqa: E402

# Promoted to gold (si_lrc_enrichment_build.py now writes here; read by
# v_si_lrc_enrichment). Tests skip gracefully if the pipeline hasn't run.
SUMMARY = _ROOT / "data/gold/parquet/si_lrc_enrichment_summary.parquet"

# A miniature of the real page: subject h2, two nested subheadings, three SI
# entries (one via ELI href only, one via citation text only, one under a deeper
# subheading), plus a catch-all "General" leaf alongside a specific one.
FIXTURE_HTML = """
<html><body>
<h2>Classified List</h2>
<div class="cell medium-9">
  <h2>9. Communications and Energy</h2>
  <p id="updated"><span>Updated to 1 June 2026</span></p>
  <section class="level-1" id="title-9-1"><h2><span>9.1. Communications</span></h2>
    <section class="level-2" id="title-9-1-1"><h2><span>9.1.1. Postal Services</span></h2>
      <ul>
        <li id="si101"><a href="http://www.irishstatutebook.ie/eli/2018/si/200/made/en/print">Postal Order Regulations 2018, S.I. No. 200 of 2018</a></li>
        <li id="si102"><a href="http://www.irishstatutebook.ie/somewhere">Broadcasting (No. 9) Order 2019, S.I. No. 305 of 2019</a></li>
      </ul>
    </section>
    <section class="level-2" id="title-9-1-2"><h2><span>9.1.2. General</span></h2>
      <ul>
        <li id="si103"><a href="http://www.irishstatutebook.ie/eli/2020/si/55/made/en/print">Random Commencement Order 2020, S.I. No. 55 of 2020</a></li>
      </ul>
    </section>
  </section>
</div>
</body></html>
"""


# ----------------------------------------------------------------- parse contract
def test_si_cite_regex():
    m = SI_CITE.search("Foo Regulations 2018, S.I. No. 200 of 2018")
    assert m and (int(m.group(1)), int(m.group(2))) == (200, 2018)


def test_eli_href_regex():
    m = ELI_SI.search("http://www.irishstatutebook.ie/eli/2018/si/200/made/en/print")
    assert m and (int(m.group(1)), int(m.group(2))) == (2018, 200)


def test_parse_category_extracts_subject_and_entries():
    rows, stats = parse_category(9, FIXTURE_HTML)
    assert stats["subject"] == "Communications and Energy"  # not the page-header h2
    assert stats["updated_to"] == "1 June 2026"
    assert len(rows) == 3


def test_parse_prefers_eli_href_then_falls_back_to_citation():
    rows, _ = parse_category(9, FIXTURE_HTML)
    by_id = {r["lrc_entry_dom_id"]: r for r in rows}
    # si101: number/year from the ELI href
    assert (by_id["si101"]["si_number"], by_id["si101"]["si_year"]) == (200, 2018)
    # si102: no ELI si href -> falls back to the citation text
    assert (by_id["si102"]["si_number"], by_id["si102"]["si_year"]) == (305, 2019)


def test_parse_builds_subheading_path_from_section_ids():
    rows, _ = parse_category(9, FIXTURE_HTML)
    r = next(r for r in rows if r["lrc_entry_dom_id"] == "si101")
    assert r["lrc_subheading_path_num"] == "9.1.1"
    assert r["lrc_subheading_leaf"] == "Postal Services"
    assert "Communications" in r["lrc_subheading_path_name"]


# ----------------------------------------------------------------- legal-safety invariants
ALLOWED_STATUS = {"matched_classified_list", "not_matched"}
FORBIDDEN_TOKENS = {
    "in_force",
    "valid",
    "invalid",
    "official_status",
    "legally_current",
    "legally_effective",
    "proved_in_force",
}


@pytest.fixture(scope="module")
def summary() -> pl.DataFrame:
    if not SUMMARY.exists():
        pytest.skip(f"summary not built: run si_lrc_enrichment_build.py ({SUMMARY})")
    return pl.read_parquet(SUMMARY)


def test_one_row_per_si(summary):
    assert summary.height == summary.select("si_year", "si_number").n_unique()


def test_status_vocabulary_is_safe(summary):
    vals = set(summary["lrc_enrichment_status"].unique().to_list())
    assert vals <= ALLOWED_STATUS, f"unexpected status values: {vals - ALLOWED_STATUS}"


def test_no_forbidden_status_tokens_anywhere(summary):
    # the dangerous failure: an assertion word leaking into any status/method col
    for col in ("lrc_enrichment_status", "match_method"):
        joined = " ".join(v for v in summary[col].drop_nulls().unique().to_list())
        for bad in FORBIDDEN_TOKENS:
            assert bad not in joined, f"forbidden token {bad!r} in {col}"


def test_unmatched_never_reads_as_in_force(summary):
    unmatched = summary.filter(~pl.col("has_lrc_classified_list_match"))
    assert (unmatched["lrc_enrichment_status"] == "not_matched").all()
    # unmatched rows carry no subject, no confidence, no method
    assert unmatched["lrc_primary_subject"].null_count() == unmatched.height
    assert unmatched["match_confidence"].null_count() == unmatched.height


def test_matched_rows_carry_caveat_and_exact_method(summary):
    matched = summary.filter(pl.col("has_lrc_classified_list_match"))
    assert matched.height > 0
    assert matched["lrc_caveat"].null_count() == 0
    assert (matched["match_method"] == "exact_number_year").all()
    assert (matched["match_confidence"] == 1.0).all()


def test_gap_fill_flag_only_when_domain_was_null(summary):
    fills = summary.filter(pl.col("lrc_fills_empty_domain"))
    assert fills.height > 0  # the concrete win exists
    assert fills["si_policy_domain"].null_count() == fills.height
    assert fills["has_lrc_classified_list_match"].all()
