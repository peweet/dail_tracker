"""Unit tests for the Companies-Act corporate-notice recovery rule in enrich_records.

Background: the strict ``has_companies_act`` flag only matched "COMPANIES ACT 2014"
/ "COMPANIES ACTS 2014" / "THE COMPANIES ACTS", so corporate notices citing the
comma form ("Companies Act, 2014"), the Assurance Companies Acts, "Section 509",
or simply opening "IN THE MATTER OF <X> LIMITED" fell through to ``other`` and were
quarantined. The recovery rule (gated on still-``other``) reclassifies them to
``corporate_notice`` so they flow to the existing Corporate page.

These guards are the load-bearing safety properties and are asserted here:
  - it must NOT steal SIs (an SI citing the Companies Act stays a statutory_instrument);
  - it must NOT surface limited partnerships (privacy: they can name individuals);
  - it must NOT promote bare "COMPANIES ACTS, 2014" page-shards (no body).
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from iris.iris_oifigiuil_etl_polars import enrich_records


def _classify(raw: str) -> tuple[str, str]:
    # enrich_records passes through the bronze record metadata; supply a minimal
    # single-record frame (classification depends only on raw_text).
    rec = pl.DataFrame(
        {
            "source_file": ["t.pdf"],
            "issue_date": ["2024-01-01"],
            "issue_number": [1],
            "start_page": [1],
            "end_page": [1],
            "start_block_id": [0],
            "start_line_id": [0],
            "end_block_id": [0],
            "end_line_id": [0],
            "bbox_union": ["[0, 0, 0, 0]"],
            "raw_text": [raw],
            "split_reason": ["eof"],
            "line_count": [raw.count("\n") + 1],
        }
    )
    out = enrich_records(rec)
    return out["notice_category"][0], out["notice_subtype"][0]


# ── recovered → corporate_notice ──────────────────────────────────────────────


def test_section_509_comma_variant_recovered():
    cat, sub = _classify(
        "IN THE MATTER OF I SUPPLY LIMITED // AND IN THE MATTER OF // SECTION 509 OF // THE COMPANIES ACT, 2014"
    )
    assert cat == "corporate_notice"
    assert sub == "companies_act_notice"


def test_assurance_companies_act_recovered():
    cat, _ = _classify(
        "IN THE MATTER OF CNP EUROPE LIFE DESIGNATED ACTIVITY COMPANY // AND IN THE MATTER OF // "
        "THE ASSURANCE COMPANIES ACT 1909"
    )
    assert cat == "corporate_notice"


def test_matter_of_company_recovered():
    cat, _ = _classify("IN THE MATTER OF // BESTWELL LIMITED")
    assert cat == "corporate_notice"


# ── guards (must NOT recover) ─────────────────────────────────────────────────


def test_companies_act_si_not_stolen():
    # An SI that cites the Companies Act must stay an SI — the rule is gated on
    # notice_category == "other", so the SI classification (set earlier) wins.
    cat, _ = _classify(
        "S.I. No. 142 of 2024.\nCOMPANIES (FORMS) REGULATIONS 2024\n"
        "The Minister for Enterprise, Trade and Employment, in exercise of the powers "
        "conferred by the Companies Act, 2014, hereby makes the following regulations."
    )
    assert cat == "statutory_instrument"


def test_limited_partnership_not_recovered_privacy():
    # Privacy: limited partnerships can name individuals (e.g. pension trusts) and
    # have no personal-insolvency backstop in the corporate enrichment.
    cat, _ = _classify(
        "IN THE MATTER OF // THE LIMITED PARTNERSHIPS ACT 1907 // AND IN THE MATTER OF // "
        "HOTEL INVESTMENT FUND LIMITED PARTNERSHIP"
    )
    assert cat != "corporate_notice"


def test_bare_companies_act_fragment_not_recovered():
    # A <60-char page-shard with no matter-opener must stay 'other' (split debris).
    cat, _ = _classify("COMPANIES ACTS, 2014")
    assert cat != "corporate_notice"


def test_insolvency_verb_left_to_insolvency_rules():
    # A winding-up notice citing the Companies Act must classify as insolvency, not
    # the generic corporate_notice recovery bucket.
    cat, _ = _classify(
        "IN THE MATTER OF ACME LIMITED // AND IN THE MATTER OF THE COMPANIES ACT, 2014 // "
        "By order of the High Court the company be wound up and a liquidator appointed."
    )
    assert cat == "corporate_insolvency"
