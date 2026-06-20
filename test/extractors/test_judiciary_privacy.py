"""Golden privacy tests for the judiciary Legal Diary Tier C (anonymised cases).

Locks ``extractors/legal_diary_extract.py`` ``anonymise()`` + ``residual_name_tokens()`` —
the sole barrier between the raw Courts Service diary and the COMMITTED, Streamlit-Cloud-
shipped gold parquet ``data/gold/parquet/judicial_legal_diary_cases.parquet``.

Two real leak paths were found 2026-06-05 (doc/JUDICIARY_LANE_REVIEW.md):
  (A) the splitter used ``maxsplit=1`` and never anonymised a 2nd ``v`` clause;
  (B) ``_is_org()`` tested the WHOLE side, so individuals riding alongside a public
      body (``... and X County Council``) were kept verbatim.
A natural-person name surviving into gold is a Critical privacy incident, so these
tests are the regression wall. All names below are invented — no real party data lives
in this file.

Run:  pytest test/test_judiciary_privacy.py -v
"""

import re
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "extractors"))

from legal_diary_extract import (  # noqa: E402
    PrivacyInvariantError,
    anonymise,
    parties,
    plaintiff_kind,
    protected_reason,
    residual_name_tokens,
    strip_refs,
)

# a properly-anonymised non-org chunk is pure initials: 'J.', 'J.S.', 'A.B.C.D.'
_INITIALS = re.compile(r"^[A-Z](?:\.[A-Z])*\.?$")

GOLD_CASES = _ROOT / "data" / "gold" / "parquet" / "judicial_legal_diary_cases.parquet"
CONTRACT_COLS = {
    "diary_date",
    "court",
    "judge",
    "list_type",
    "status",
    "category",
    "case_anonymised",
    "plaintiff",
    "defendant",
    "plaintiff_kind",
    "source",
    "source_url",
    "source_sha256",
}
FORBIDDEN_COLS = {"raw_case", "party", "parties", "solicitor", "solicitors"}


# ----------------------------------------------------------------- no-residual cases
@pytest.mark.parametrize(
    "raw",
    [
        # plain person v person — both sides must initialise
        "JOHN SMITHFIELD -v- MARY MOORLAND",
        # bug A: a second 'v' clause must STILL be anonymised
        "ANNE BRACKENRIDGE -v- ACME PLC -v- DAVID NORTHGATE",
        "WILLIAM OAKHAM v PETER QUILLFORD v ROBERT STANWICK",
        # bug B: individuals riding alongside a public body must NOT be kept in clear
        "JOHN SMITHFIELD AND MARY MOORLAND AND CORK COUNTY COUNCIL -v- THE MINISTER FOR HEALTH",
        "BARRY ELMWOOD AND THE HEALTH SERVICE EXECUTIVE -v- DUBLIN CITY COUNCIL",
        # criminal prosecutor side kept, accused anonymised
        "THE PEOPLE AT THE SUIT OF THE DPP -v- JOHN DOEFIELD",
        # tail forms must not leak the named lead party
        "PATRICK MULBERRY & ORS -v- ELECTRICITY SUPPLY BOARD",
        "SARAH VANBROOK AND ANOTHER -v- THE REVENUE COMMISSIONERS",
        # in-the-matter single-side person
        "IN THE MATTER OF GEORGE HOLLOWFIELD A BANKRUPT",
    ],
)
def test_anonymise_leaves_no_residual_name(raw):
    out = anonymise(raw)
    leaked = residual_name_tokens(out)
    assert leaked == [], f"anonymise({raw!r}) = {out!r} leaked {leaked}"


# ----------------------------------------------------------------- orgs kept in clear
@pytest.mark.parametrize(
    "raw,expect_substr",
    [
        ("ACME LIMITED -v- BETA HOLDINGS DAC", "Acme Limited"),
        ("THE MINISTER FOR HEALTH -v- GAMMA INSURANCE PLC", "Minister For Health"),
    ],
)
def test_org_sides_survive_in_clear(raw, expect_substr):
    out = anonymise(raw)
    assert expect_substr.lower() in out.lower(), f"org name dropped: {raw!r} -> {out!r}"
    assert residual_name_tokens(out) == []


# ----------------------------------------------------------------- detector itself
def test_residual_detector_flags_a_real_leak():
    # the pre-fix bug-B output shape: a named individual kept beside an org
    assert "Smithfield" in residual_name_tokens("John Smithfield and Cork County Council v M.M.")


def test_residual_detector_passes_pure_initials_and_orgs():
    assert residual_name_tokens("J.S. and M.M. and Cork County Council v Minister For Health") == []


def test_initials_shape_on_person_sides():
    out = anonymise("JOHN SMITHFIELD -v- MARY MOORLAND")
    a, b = out.split(" v ")
    assert _INITIALS.match(a.strip()), out
    assert _INITIALS.match(b.strip()), out


def test_ors_tail_preserved_without_leaking_lead():
    out = anonymise("PATRICK MULBERRY & ORS -v- ELECTRICITY SUPPLY BOARD")
    assert out.endswith("& Ors") or "& Ors v" in out, out
    assert residual_name_tokens(out) == []


def test_empty_and_refs_are_safe():
    assert anonymise("") == ""
    assert residual_name_tokens("") == []
    # a pure case-reference line strips to nothing publishable
    assert residual_name_tokens(anonymise("RECORD NO. 2022 3507 P")) == []


# ----------------------------------------------------------------- in-camera drop keys
@pytest.mark.parametrize(
    "list_type,case,expect_key",
    [
        # ampersand variants must hit the same in-camera keys as the spelled-out forms —
        # "The Child & Family Agency" bypassed the drop until 2026-06-11 (3 gold rows)
        ("", "14. 2026 159 The Child & Family Agency -v- D", "child and family"),
        ("", "The Child and Family Agency -v- T", "child and family"),
        ("", "K v TUSLA CHILD AND FAMILY AGENCY", "child and family"),  # list-order precedence
        ("Wards Of Court List", "In the matter of J.M.", "wards of court"),
        ("", "ACME LIMITED -v- BETA HOLDINGS DAC", None),  # ordinary commercial: kept
    ],
)
def test_protected_reason_catches_ampersand_variants(list_type, case, expect_key):
    assert protected_reason(list_type, case) == expect_key


# ----------------------------------------------------------------- live gold parquet
@pytest.mark.integration
def test_gold_cases_contract_and_zero_residual_names():
    """The committed/Cloud-shipped gold parquet must (a) match the 10-col contract,
    (b) carry no raw-name column, and (c) contain ZERO residual natural-person names.
    Regenerate via `python extractors/legal_diary_extract.py --all-archived` if stale."""
    pl = pytest.importorskip("polars")
    if not GOLD_CASES.exists():
        pytest.skip(f"{GOLD_CASES} not built; run the legal_diary extractor first")
    df = pl.read_parquet(GOLD_CASES)
    cols = set(df.columns)
    assert cols == CONTRACT_COLS, f"gold cases columns drifted: {cols ^ CONTRACT_COLS}"
    assert not (FORBIDDEN_COLS & cols), f"raw-name column in gold: {FORBIDDEN_COLS & cols}"
    # zero residual names across every published free-text column (title + party split)
    offenders = [
        (col, c, residual_name_tokens(c))
        for col in ("case_anonymised", "plaintiff", "defendant")
        for c in df.get_column(col).to_list()
        if residual_name_tokens(c)
    ]
    assert not offenders, f"{len(offenders)} gold cells leak names, e.g. {offenders[:5]}"


@pytest.mark.integration
def test_writer_privacy_gate_is_importable_and_runtime():
    # the gate must be a real exception class (survives `python -O`), not an assert
    assert issubclass(PrivacyInvariantError, Exception)


# ----------------------------------------------------------------- plaintiff split (v1.1)
@pytest.mark.parametrize(
    "raw,kind",
    [
        ("THE PEOPLE AT THE SUIT OF THE DPP -v- JOHN DOEFIELD", "state-prosecutor"),
        ("ACME MORTGAGE FINANCE DAC -v- MARY MOORLAND", "organisation"),
        ("BANK OF IRELAND MORTGAGE BANK -v- PETER QUILLFORD", "organisation"),  # not State on "ireland"
        ("THE MINISTER FOR JUSTICE -v- JOHN SMITHFIELD", "state-body"),
        ("JOHN SMITHFIELD -v- THE MINISTER FOR JUSTICE", "individual"),
    ],
)
def test_plaintiff_kind_classifies_first_party(raw, kind):
    assert plaintiff_kind(raw.split(" -v- ")[0]) == kind
    assert parties(raw)["plaintiff_kind"] == kind


def test_parties_split_is_consistent_with_case_anonymised():
    p = parties("ACME LIMITED -v- MARY MOORLAND -v- BETA HOLDINGS DAC")
    # plaintiff is the first segment, defendant the rest, joined back == the title
    assert p["case_anonymised"] == anonymise("ACME LIMITED -v- MARY MOORLAND -v- BETA HOLDINGS DAC")
    assert p["plaintiff"] == "ACME LIMITED"  # org kept verbatim in its source case
    assert p["case_anonymised"] == f"{p['plaintiff']} v {p['defendant']}"
    # the anonymised person in the middle must not leak through plaintiff/defendant
    assert residual_name_tokens(p["plaintiff"]) == []
    assert residual_name_tokens(p["defendant"]) == []


def test_strip_refs_clears_court_record_codes():
    # generalised High Court ref family + glued trailing seq + leading list index
    assert "COS" not in strip_refs("H.COS.2025.0000177SOLFRENO LTD -V- COMPANIES ACT 2014")
    assert strip_refs("2026 82 Filbeck Limited -v- Kirwan").lower().startswith("filbeck")
    assert strip_refs("20. 2026 134 Webster & anor -v- X Ltd").lower().startswith("webster")
