"""Regression tests for the CRO ↔ corporate-notice xref.

The specific issue this locks (promoted from probe_cro_corporate_join.py):
  - the notice join key MUST use cro_normalise.name_norm_expr (the same rule that
    built the CRO `name_norm` column) — re-implementing it is how drift rots the
    join. Test by relying on legal-suffix stripping ("FOO LIMITED" → "FOO").
  - junk boilerplate / empty / sub-4-char names are excluded BEFORE the join.
  - a CRO name that resolves to >1 company (e.g. "ULSTER BANK") is ambiguous and
    gets NO badge — the notice must not fan out.
  - a no-match notice produces no row.
  - every emitted row carries a non-null company_num and the fixed column set.

Pure-function tests on tiny synthetic frames — no parquet, no IO, CI-safe.

Run:  pytest test/test_cro_corporate_xref.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

_ROOT = Path(__file__).resolve().parent.parent
# Root for `cro_normalise` + root `config`; extractors/ for the enrichment.
for _p in (str(_ROOT), str(_ROOT / "extractors")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from cro_corporate_xref_enrichment import _OUTPUT_COLS, build_cro_xref  # noqa: E402 — sys.path set above


def _notices(rows: list[dict]) -> pl.DataFrame:
    """Build a notices frame with the v_corporate_notices columns the xref reads."""
    base = {
        "notice_ref": None,
        "entity_name": None,
        "issue_date": "2023-01-01",
        "notice_category": "corporate_insolvency",
        "notice_subtype": "members_voluntary_liquidation",
    }
    return pl.DataFrame([{**base, **r} for r in rows])


def _cro(rows: list[dict]) -> pl.DataFrame:
    """Build a CRO silver frame with name_norm + the badge columns."""
    base = {
        "name_norm": None,
        "company_num": None,
        "company_status": "Dissolved",
        "company_reg_date": None,
        "comp_dissolved_date": None,
        "status_pill_value": "dead",
    }
    return pl.DataFrame([{**base, **r} for r in rows])


def test_clean_one_to_one_match_is_kept_with_cro_fields():
    notices = _notices([{"entity_name": "ACME WIDGETS LIMITED", "notice_ref": "N1"}])
    cro = _cro([{"name_norm": "ACME WIDGETS", "company_num": 111, "company_status": "Normal", "status_pill_value": "active"}])
    out = build_cro_xref(notices, cro)
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["company_num"] == 111
    assert row["status_pill_value"] == "active"
    assert row["entity_norm"] == "ACME WIDGETS"  # legal suffix stripped by CRO rule


def test_join_key_uses_cro_normalisation_rule_not_a_reimplementation():
    # "LIMITED"/"DAC" and punctuation are dropped only by cro_normalise's rule.
    # If the enrichment re-implemented normalisation differently this would miss.
    notices = _notices([{"entity_name": "O'Brien Holdings Designated Activity Company"}])
    cro = _cro([{"name_norm": "O BRIEN", "company_num": 222}])
    out = build_cro_xref(notices, cro)
    assert out.height == 1
    assert out.row(0, named=True)["company_num"] == 222


def test_junk_boilerplate_name_excluded():
    notices = _notices([{"entity_name": "IN THE MATTER OF THE COMPANIES ACT 2014"}])
    cro = _cro([{"name_norm": "IN THE MATTER OF THE 2014", "company_num": 333}])
    assert build_cro_xref(notices, cro).height == 0


def test_empty_name_excluded():
    notices = _notices([{"entity_name": ""}])
    cro = _cro([{"name_norm": "", "company_num": 444}])
    assert build_cro_xref(notices, cro).height == 0


def test_sub_four_char_normalised_name_excluded():
    # "ABC LTD" -> "ABC" (3 chars) is below the noise floor.
    notices = _notices([{"entity_name": "ABC LTD"}])
    cro = _cro([{"name_norm": "ABC", "company_num": 555}])
    assert build_cro_xref(notices, cro).height == 0


def test_ambiguous_cro_name_gets_no_badge():
    # Two companies share the normalised name -> notice must NOT fan out.
    notices = _notices([{"entity_name": "ULSTER BANK LIMITED"}])
    cro = _cro(
        [
            {"name_norm": "ULSTER BANK", "company_num": 1001},
            {"name_norm": "ULSTER BANK", "company_num": 1002},
        ]
    )
    assert build_cro_xref(notices, cro).height == 0


def test_no_match_notice_produces_no_row():
    notices = _notices([{"entity_name": "ENTITY NOT IN CRO LIMITED"}])
    cro = _cro([{"name_norm": "SOME OTHER COMPANY", "company_num": 777}])
    assert build_cro_xref(notices, cro).height == 0


def test_output_schema_is_fixed_and_company_num_non_null():
    notices = _notices(
        [
            {"entity_name": "ACME WIDGETS LIMITED", "notice_ref": "N1"},
            {"entity_name": "ENTITY NOT IN CRO LIMITED"},
            {"entity_name": "IN THE MATTER OF THE COMPANIES ACT"},
        ]
    )
    cro = _cro([{"name_norm": "ACME WIDGETS", "company_num": 111}])
    out = build_cro_xref(notices, cro)
    assert out.columns == _OUTPUT_COLS
    assert out.height == 1
    assert out["company_num"].null_count() == 0
