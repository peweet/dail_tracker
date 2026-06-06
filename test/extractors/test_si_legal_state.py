"""Unit tests locking the SI legal-state parsing contract.

These pin derive_state() + affecting_sis() in
extractors/si_legislation_directory_extract.py — the extractor that writes
data/gold/parquet/si_current_state.parquet (read by v_si_current_state and
LEFT-joined into v_statutory_instruments). The eISB "How Affected" wording is the
sole signal for an SI's legal state, so a silent drift in these rules would
mislabel revocations across the whole SI page. Cases are verbatim shapes drawn
from the real directory (2016-2026).

Key invariant: derive_state sees ONLY the 'How Affected' column. The 'Affecting
Provision' column (the *revoking/amending* SI's own provisions, e.g.
"S.I. No. 332 of 2025 , reg. 16") must never scope THIS SI's state down to
partial — that distinction is what the end-anchored, dot-required
PROVISION_MARKER protects.

Run:  pytest test/test_si_legal_state.py -v
"""

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "extractors"))

from si_legislation_directory_extract import (  # noqa: E402
    affecting_sis,
    confidence_for,
    derive_state,
)

_ENUM = {
    "in_force_as_made",
    "amended",
    "partially_revoked",
    "amended_and_partially_revoked",
    "revoked",
    "other_affected",
}


@pytest.mark.parametrize(
    "how,expected",
    [
        # in force as made
        ("", "in_force_as_made"),
        ("Not affected", "in_force_as_made"),
        # WHOLE revocation — bare, dated, and the "Whole S.I." prefix forms.
        # A trailing date ("…2026") or an "S.I."/"SI" token must NOT scope these.
        ("Revoked", "revoked"),
        ("Revoked on 16 July 2025", "revoked"),
        ("Revoked with transitional provisions", "revoked"),
        ("Whole S.I. revoked", "revoked"),
        ("Whole SI revoked on 1 January 2025", "revoked"),
        ("Whole S.I. revoked on 1 June 2023 amended", "revoked"),  # revoked is most severe
        # PARTIAL revocation — a provision marker (with its dot) leads the verb.
        ("Reg. 2(3) revoked", "partially_revoked"),
        ("Reg. 3(a) revoked on 2 September 2022", "partially_revoked"),
        ("s. 5 revoked", "partially_revoked"),
        # "Whole S.I. other than reg. 10 revoked" — reg. 10 survives, so partial.
        ("Whole S.I. other than reg. 10 revoked on 27 April 2022", "partially_revoked"),
        # AMENDMENT (provision-level amend is still just "amended")
        ("Amended", "amended"),
        ("Reg. 2(1) amended", "amended"),
        ("Art. 2(1) amended", "amended"),
        # OTHER affected — substitutions/transfers/obsolescence are not amend/revoke
        ("Sch. 2 substituted", "other_affected"),
        ("Continued in force", "other_affected"),
        ("Rendered obsolete by repeal of enabling provision", "other_affected"),
    ],
)
def test_derive_state(how, expected):
    assert derive_state(how) == expected


def test_derive_state_always_in_enum():
    for how in ["", "garbage text with no verb", "Reg. 2 revoked and amended"]:
        assert derive_state(how) in _ENUM


def test_affecting_column_provision_does_not_scope_state():
    """The crux: a whole 'Revoked' must stay whole even though the affecting
    column carries the revoking SI's own provision. derive_state only ever sees
    the 'How Affected' half, so this is a regression guard on that wiring."""
    how_affected = "Revoked on 16 July 2025"  # column 2 only
    assert derive_state(how_affected) == "revoked"  # NOT partially_revoked


def test_amended_and_partially_revoked_combination():
    # Both a provision-scoped revoke and an amend, no whole revoke -> combined.
    assert derive_state("Reg. 2 revoked and Reg. 3 amended") == "amended_and_partially_revoked"


def test_affecting_sis_parsing_and_dedup():
    # Pulls "S.I. No. N of YYYY" cites from both how + affecting columns, dedups,
    # and normalises to "number/year".
    refs = affecting_sis("Revoked on 7 August 2024", "S.I. No. 394 of 2024 , reg. 5")
    assert refs == ["394/2024"]
    multi = affecting_sis("", "S.I. No. 5 of 2020 , S.I. No. 12 of 2019")
    assert multi == ["12/2019", "5/2020"]  # sorted by (year, number)
    assert affecting_sis("Not affected") == []


def test_confidence_bounds_and_severity():
    assert confidence_for("in_force_as_made", "") == 0.95
    assert confidence_for("revoked", "Revoked") == 0.92  # bare cell = high
    assert confidence_for("revoked", "Revoked on 1 May 2024") < 0.92  # dated = slightly lower
    assert confidence_for("other_affected", "Sch. 2 substituted") == 0.4
