"""Guards on the sandbox -> gold promote (extractors/councillors_promote_to_gold.py).

The promote is the boundary where sandbox artifacts become the CSVs the app renders as fact,
so the things tested here are the ones that would put a wrong claim in front of a user:
provenance columns must survive the crossing, coverage counts must describe the data we
actually hold, and a re-run must not change what it produced.

Runs against a TEMP output directory (module-level META is monkeypatched) — a test must never
rewrite the git-tracked gold CSVs.
"""

import csv
from pathlib import Path

import pytest

import extractors.councillors_promote_to_gold as promote

ROOT = Path(__file__).parents[2]
SBX = ROOT / "pipeline_sandbox" / "council_minutes"

pytestmark = pytest.mark.skipif(
    not (SBX / "member_votes.jsonl").exists() or not (ROOT / "data" / "_meta" / "la_councillors.csv").exists(),
    reason="council-minutes sandbox or gold roster absent",
)


def _read(d: Path, name: str) -> list[dict]:
    with open(d / name, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


@pytest.fixture(scope="module")
def out(
    tmp_path_factory,
) -> Path:
    d = tmp_path_factory.mktemp("gold")
    original = promote.META
    promote.META = d
    try:
        assert promote.main() == 0
    finally:
        promote.META = original
    return d


def test_vote_rows_all_carry_provenance(out):
    rows = _read(out, "la_councillor_votes.csv")
    assert rows
    assert {"source_status", "join_status"} <= set(rows[0])
    assert {r["join_status"] for r in rows} == {"resolved", "printed_form"}
    assert {r["source_status"] for r in rows} <= {"text", "ocr_winocr", "html"}
    # a blank band is the dangerous case: the UI would have nothing to badge with
    assert not [r for r in rows if not r["source_status"].strip()]


def test_ocr_rows_are_marked_as_ocr(out):
    """Galway City's minutes are winocr output end to end. If a row of theirs ever promotes as
    'text' the UI would render an OCR-derived vote as born-digital fact."""
    rows = [r for r in _read(out, "la_councillor_votes.csv") if r["local_authority"] == "Galway City"]
    assert rows
    assert all(r["source_status"] == "ocr_winocr" for r in rows)


def test_join_status_matches_the_extractors_own_roster_fold(out):
    """join_status is computed with the vote extractor's fold, not a second matcher. A separate
    implementation could label a row 'resolved' that the extractor had kept as a printed form."""
    from extractors.council_votes_extract import _fold, _load_gold_roster

    rows = _read(out, "la_councillor_votes.csv")
    folds: dict[str, set[str]] = {}
    for r in rows:
        la = r["local_authority"]
        if la not in folds:
            folds[la] = {_fold(n) for n in _load_gold_roster(la)}
        expected = "resolved" if _fold(r["member"]) in folds[la] else "printed_form"
        assert r["join_status"] == expected, f"{la} / {r['member']}"


def test_no_bare_galway_in_the_join_key(out):
    """'Galway' alone is ambiguous between the city and county authorities and matches neither
    spelling used by the CE roster, payments or AFS — it would orphan the rows from every join."""
    for name in (
        "la_councillors.csv",
        "la_council_meeting_coverage.csv",
        "la_councillor_votes.csv",
        "la_meeting_agendas.csv",
        "la_council_decisions.csv",
    ):
        las = {r["local_authority"] for r in _read(out, name)}
        assert "Galway" not in las, name
        assert "Dún Laoghaire-Rathdown" not in las, name  # accented form is the sandbox spelling


def test_coverage_is_recounted_not_copied(out):
    """council_coverage.csv still carries the counts from a ~150-document corpus. Promoting them
    unchanged is what made the page claim Galway City had no minutes while we held 104."""
    cov = {r["local_authority"]: r for r in _read(out, "la_council_meeting_coverage.csv")}
    clean: dict[str, int] = {}
    for (local_authority, _meeting), doc_type in promote._published_document_types().items():
        if doc_type.endswith("_minutes"):
            clean[local_authority] = clean.get(local_authority, 0) + 1
    for la, row in cov.items():
        assert int(row["clean_minutes"]) == clean.get(la, 0), la
    assert sum(int(r["clean_minutes"]) for r in cov.values()) == sum(clean.values())
    # and the stale zeros are gone where we hold documents
    assert int(cov["Galway City"]["clean_minutes"]) > 0
    assert cov["Galway City"]["has_votes"] == "True"


def test_tier_follows_the_data(out):
    cov = _read(out, "la_council_meeting_coverage.csv")
    assert {r["tier"] for r in cov} <= {"roll_call", "proposer_seconder", "scanned_pending", "cmis_pending", "unseeded"}
    votes_by_la: dict[str, int] = {}
    for r in _read(out, "la_councillor_votes.csv"):
        votes_by_la[r["local_authority"]] = votes_by_la.get(r["local_authority"], 0) + 1
    for r in cov:
        if votes_by_la.get(r["local_authority"]):
            assert r["tier"] == "roll_call", r["local_authority"]
        if int(r["clean_minutes"]) == 0:
            assert r["tier"] == "unseeded", r["local_authority"]


def test_roster_count_keeps_a_councillor_named_after_her_county(out):
    """The placeholder filter drops rows where the Wikipedia parse left the LEA name behind.
    Testing the name prefix alone would also drop 'Clare Colleran Molloy' from Clare's count."""
    cov = {r["local_authority"]: r for r in _read(out, "la_council_meeting_coverage.csv")}
    roster = _read(out, "la_councillors.csv")
    clare = [r for r in roster if r["local_authority"] == "Clare"]
    assert any(r["name"] == "Clare Colleran Molloy" for r in clare)
    assert int(cov["Clare"]["roster_councillors"]) == len([r for r in clare if " " in r["name"]])


def test_decisions_do_not_ship_python_repr_strings(out):
    """The sandbox writes str(None) / str(False) into these fields. Promoted unchanged, gold
    carries the literal text 'None' in a count column and DuckDB types it VARCHAR."""
    rows = _read(out, "la_council_decisions.csv")
    assert len(rows) > 6000
    for col in ("tally_for", "tally_against", "tally_abstain"):
        vals = {r[col] for r in rows}
        assert "None" not in vals, col
        assert all(v == "" or v.isdigit() for v in vals), col
    assert {r["rollcall"] for r in rows} <= {"True", "False"}


def test_decisions_carry_no_duplicate_motion_events(out):
    """The sandbox emits the same motion twice where parse windows overlap (50 rows found
    2026-08-01). Promoted unchanged, every decision count the page shows is inflated."""
    rows = _read(out, "la_council_decisions.csv")
    key = ("local_authority", "meeting_date", "item_context", "motion_snippet", "proposer", "seconder")
    seen = [tuple(r[k] for k in key) for r in rows]
    assert len(seen) == len(set(seen)), f"{len(seen) - len(set(seen))} duplicate motion events"


def test_power_split_contains_plenary_council_minutes_only(out):
    """Reserved/executive powers are a full-council legal grain, not a committee
    or municipal-district metric and never an agenda-text metric."""
    rows = _read(out, "la_council_power_events.csv")
    assert rows
    assert {row["doc_type"] for row in rows} == {"plenary_minutes"}


def test_promote_is_idempotent(out, tmp_path):
    """A second run over unchanged inputs must produce byte-identical CSVs — otherwise every
    re-promote shows as a diff and real changes get lost in the noise."""
    second = tmp_path / "again"
    second.mkdir()
    original = promote.META
    promote.META = second
    try:
        assert promote.main() == 0
    finally:
        promote.META = original
    for f in sorted(out.glob("*.csv")):
        assert f.read_bytes() == (second / f.name).read_bytes(), f.name
