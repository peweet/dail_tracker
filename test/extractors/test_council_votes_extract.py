"""Tests for extractors/council_votes_extract.py (graduated from the sandbox 2026-08-01).

Pins the four load-bearing behaviours:
  1. RECONCILE GATE — a division is emitted only when each side's parsed names count
     exactly to the minutes' printed tally (Cork "(N)" per side; Galway tally line with
     OCR letter-O zeros). Off-by-one → the whole division drops.
  2. _vote_year fallback — the 2018 cutoff's year lookup. Both regressions from the
     first cutoff run are pinned: slugged "%20"→"_20" years ("april_202026") and
     underscore-suffixed years ("February 2026_0") defeat word-boundary matching and
     deleted the Carlow + Laois records wholesale before the fallback was fixed.
  3. MIN_VOTE_YEAR — the serving cutoff exists and is 2018+ (user decision 2026-08-01).
  4. fold_initial_forms — merges a printed initial-form member name ('O. O'Leary') onto
     the full form observed elsewhere in the same council's corpus ('Orla O'Leary'), the
     Cork name-form-split fix (VOTE_VERIFICATION.md, 2026-08-01).
"""

from __future__ import annotations

from extractors.council_votes_extract import (
    MIN_VOTE_YEAR,
    Coverage,
    RosterResolver,
    _vote_year,
    fold_initial_forms,
    parse_cork_prose,
    parse_galway_prose,
)

CORK_OK = """
A vote was called for on the approval of the motion, where there appeared as follows:-
FOR: Comhairleoirí J. Maher, M. McDonnell. (2)
AGAINST: Comhairleoirí T. Shannon. (1)
ABSTAIN: (0)
The motion was carried.
"""

CORK_BAD_TALLY = CORK_OK.replace("(2)", "(3)")  # names no longer count to the tally

GALWAY_OK = """
Proposed by: Cllr. D. Lyons Seconded by: Cllr. F. Fahy
'To adopt the Scheme per Report circulated'.
In favour: Cllr. A. Aaa, Cllr. B. Bbb & Cllr. C. Ccc
Against: Cllr. D. Ddd
In Favour: 3 Against: 1 Abstain: O Present: 4
The Scheme was adopted.
"""

GALWAY_BAD = GALWAY_OK.replace("In Favour: 3", "In Favour: 4")


def _run(parser, la: str, text: str) -> list[dict]:
    return parser(la, "minutes_test_2024.pdf", text, Coverage(), RosterResolver(la))


def test_cork_gate_passes_matching_tally() -> None:
    rows = _run(parse_cork_prose, "Cork City", CORK_OK)
    assert len(rows) == 3  # 2 for + 1 against
    assert {r["vote"] for r in rows} == {"for", "against"}


def test_cork_gate_drops_mismatched_tally() -> None:
    assert _run(parse_cork_prose, "Cork City", CORK_BAD_TALLY) == []


def test_galway_gate_passes_and_reads_ocr_zero() -> None:
    rows = _run(parse_galway_prose, "Galway City", GALWAY_OK)
    assert len(rows) == 4  # 3 for + 1 against; Abstain "O" read as 0
    assert all(r["source_status"] == "ocr_winocr" for r in rows)


def test_galway_gate_drops_mismatched_tally() -> None:
    assert _run(parse_galway_prose, "Galway City", GALWAY_BAD) == []


def test_vote_year_prefers_meeting_date() -> None:
    assert _vote_year({"meeting_date": "2020-11-30", "meeting": "x_1999.pdf"}) == 2020


def test_vote_year_filename_fallback_slugged() -> None:
    # "%20" mangled to "_20": the year hides in "april_202026" (deleted Laois once)
    assert _vote_year({"meeting_date": "", "meeting": "minutes_20council_20april_202026.pdf"}) == 2026


def test_vote_year_filename_fallback_underscore_suffix() -> None:
    # "February%202026_0.pdf": '_' is a word char, \b never matches (deleted 18 Carlow rows)
    assert _vote_year({"meeting_date": "", "meeting": "Minutes%20Carlow%20February%202026_0.pdf"}) == 2026


def test_vote_year_none_when_truly_undated() -> None:
    assert _vote_year({"meeting_date": "", "meeting": "louth-co-council-mb-copy-volume-7.pdf"}) is None


def test_cutoff_constant_is_2018() -> None:
    assert MIN_VOTE_YEAR == 2018


def _vrow(la: str, member: str) -> dict:
    return {"local_authority": la, "meeting": "m.pdf", "meeting_date": "2024-01-01",
            "motion": "x", "member": member, "vote": "for"}


def test_fold_merges_initial_form_onto_observed_full_name() -> None:
    rows = [_vrow("Cork City", "O. O’Leary"), _vrow("Cork City", "Orla O’Leary")]
    out = fold_initial_forms(rows)
    assert {r["member"] for r in out} == {"Orla O’Leary"}


def test_fold_leaves_unmatched_initial_form_unchanged() -> None:
    # no full form for 'Finn' anywhere in the corpus -> kept as printed, no misattribution
    rows = [_vrow("Cork City", "C. Finn"), _vrow("Cork City", "Orla O’Leary")]
    out = fold_initial_forms(rows)
    assert {r["member"] for r in out} == {"C. Finn", "Orla O’Leary"}


def test_fold_does_not_merge_ambiguous_initial_surname_pair() -> None:
    # two distinct full names share the same (initial, surname) -> stays printed
    rows = [_vrow("Cork City", "J. Maher"), _vrow("Cork City", "John Maher"),
            _vrow("Cork City", "Jane Maher")]
    out = fold_initial_forms(rows)
    assert "J. Maher" in {r["member"] for r in out}


def test_fold_skips_carlow_already_normalised() -> None:
    rows = [_vrow("Carlow", "O. O’Leary"), _vrow("Carlow", "Orla O’Leary")]
    out = fold_initial_forms(rows)
    # Carlow rows pass through untouched (normalise_members runs earlier in refresh_carlow)
    assert {r["member"] for r in out} == {"O. O’Leary", "Orla O’Leary"}
