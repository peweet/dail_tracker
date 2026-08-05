"""Tripwire for the Your-Councillors gold views (v_la_councillors + 4 siblings).

Each reads a git-tracked data/_meta CSV (committed, like la_chief_executives.csv), so this runs
in CI (no skip). Guards: roster covers all 31 councils, the coverage tier set is valid, Carlow is
the roll_call council with named votes, agendas are non-empty, and Standing Orders carry the
records_named_votes flag.
"""

from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).parents[2]
SQL = ROOT / "sql_views" / "constituency"
META = ROOT / "data" / "_meta"

VIEWS = {
    "constituency_la_councillors.sql": "la_councillors.csv",
    "constituency_la_council_meeting_coverage.sql": "la_council_meeting_coverage.csv",
    "constituency_la_councillor_votes.sql": "la_councillor_votes.csv",
    "constituency_la_meeting_agendas.sql": "la_meeting_agendas.csv",
    "constituency_la_standing_orders.sql": "la_standing_orders.csv",
    "constituency_la_council_decisions.sql": "la_council_decisions.csv",
}

pytestmark = pytest.mark.skipif(not (META / "la_councillors.csv").exists(), reason="councillor gold CSVs absent")


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    for fname, csv in VIEWS.items():
        sql = (SQL / fname).read_text(encoding="utf-8")
        sql = sql.replace(f"data/_meta/{csv}", str(META / csv).replace("\\", "/"))
        c.execute(sql)
    return c


def test_roster_covers_31_councils(con):
    n = con.execute("SELECT COUNT(DISTINCT local_authority) FROM v_la_councillors").fetchone()[0]
    assert n == 31
    total = con.execute("SELECT COUNT(*) FROM v_la_councillors").fetchone()[0]
    assert total > 850  # ~916, ~96% of the ~949 elected


def test_coverage_tiers_valid(con):
    tiers = {r[0] for r in con.execute("SELECT DISTINCT tier FROM v_la_council_meeting_coverage").fetchall()}
    assert tiers <= {"roll_call", "proposer_seconder", "scanned_pending", "cmis_pending", "unseeded"}
    carlow = con.execute("SELECT tier FROM v_la_council_meeting_coverage WHERE local_authority='Carlow'").fetchone()[0]
    assert carlow == "roll_call"


def test_carlow_has_named_votes(con):
    n = con.execute("SELECT COUNT(*) FROM v_la_councillor_votes WHERE local_authority='Carlow'").fetchone()[0]
    assert n > 50
    bad = con.execute(
        "SELECT COUNT(*) FROM v_la_councillor_votes WHERE vote NOT IN ('for','against','abstain','absent')"
    ).fetchone()[0]
    assert bad == 0


def test_agendas_present(con):
    n = con.execute("SELECT COUNT(*) FROM v_la_meeting_agendas WHERE agenda <> ''").fetchone()[0]
    assert n > 100  # ~212 meetings


def test_standing_orders_named_vote_flag(con):
    rows = con.execute("SELECT local_authority, records_named_votes FROM v_la_standing_orders").fetchall()
    assert len(rows) >= 5
    gc = con.execute(
        "SELECT records_named_votes FROM v_la_standing_orders WHERE local_authority='Galway County'"
    ).fetchone()
    assert gc is not None and bool(gc[0]) is True


# ── vote provenance (added 2026-08-01 with join_status/source_status) ──────────────────────


def test_vote_provenance_columns_are_closed_sets(con):
    """A value outside these sets means an extractor started emitting a band the UI has no
    rule for — which is how an OCR-derived vote ends up rendered as plain fact."""
    js = {r[0] for r in con.execute("SELECT DISTINCT join_status FROM v_la_councillor_votes").fetchall()}
    ss = {r[0] for r in con.execute("SELECT DISTINCT source_status FROM v_la_councillor_votes").fetchall()}
    assert js == {"resolved", "printed_form"}
    assert ss <= {"text", "ocr_winocr", "html"}
    nulls = con.execute(
        "SELECT COUNT(*) FROM v_la_councillor_votes WHERE join_status IS NULL OR source_status IS NULL "
        "OR source_status = ''"
    ).fetchone()[0]
    assert nulls == 0


def test_printed_form_rows_are_never_dropped(con):
    """The reconcile gate proved each division's names count to its printed tally, so removing
    the unresolvable names would break that arithmetic. Every row stays; the page filters."""
    total, resolved, printed = con.execute(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE join_status='resolved'), "
        "COUNT(*) FILTER (WHERE join_status='printed_form') FROM v_la_councillor_votes"
    ).fetchone()
    assert resolved + printed == total
    assert printed > 0  # they exist and are visible — silence here would mean silent omission


def test_provenance_view_agrees_with_the_base_view(con):
    """The Trust rail reads the provenance view; if it could disagree with the base view the
    page would report a count it does not show."""
    for la, rows, res, printed in con.execute(
        "SELECT local_authority, vote_rows, resolved_rows, printed_form_rows FROM v_la_councillor_vote_provenance"
    ).fetchall():
        base = con.execute(
            "SELECT COUNT(*), COUNT(*) FILTER (WHERE join_status='resolved'), "
            "COUNT(*) FILTER (WHERE join_status='printed_form') FROM v_la_councillor_votes "
            "WHERE local_authority = ?",
            [la],
        ).fetchone()
        assert (rows, res, printed) == base


def test_galway_city_votes_are_ocr_and_predate_the_current_council(con):
    """Two facts that set Galway City's band and explain its printed-form share: every row is
    OCR-derived, and every division is from the 2019-2024 council (elections 2024-06-07), so a
    roster of sitting members structurally cannot resolve those names."""
    non_ocr = con.execute(
        "SELECT COUNT(*) FROM v_la_councillor_votes WHERE local_authority='Galway City' "
        "AND source_status <> 'ocr_winocr'"
    ).fetchone()[0]
    assert non_ocr == 0
    last = con.execute(
        "SELECT last_meeting FROM v_la_councillor_vote_provenance WHERE local_authority='Galway City'"
    ).fetchone()[0]
    assert last is not None and last.year <= 2024


# ── coverage tiers are RECOUNTED, not copied (2026-08-01) ─────────────────────────────────


def test_coverage_counts_match_the_data_they_describe(con):
    """The stale CSV claimed Galway City had 0 clean minutes and no votes while we held 104
    documents and 508 votes. A page that says 'not yet harvested' over data we hold is a false
    statement about our own coverage, so the tier and the counts must follow the artifacts."""
    for la, tier, has_votes in con.execute(
        "SELECT local_authority, tier, has_votes FROM v_la_council_meeting_coverage"
    ).fetchall():
        n = con.execute("SELECT COUNT(*) FROM v_la_councillor_votes WHERE local_authority = ?", [la]).fetchone()[0]
        assert bool(has_votes) == (n > 0), f"{la}: has_votes={has_votes} but {n} vote rows"
        if n > 0:
            assert tier == "roll_call", f"{la} has named votes but tier={tier}"


def test_unseeded_means_nothing_harvested(con):
    """'unseeded' must mean we hold no minutes — never 'this council does not publish'."""
    rows = con.execute(
        "SELECT local_authority, clean_minutes FROM v_la_council_meeting_coverage WHERE tier='unseeded'"
    ).fetchall()
    assert all(n == 0 for _, n in rows)
    seeded_zero = con.execute(
        "SELECT COUNT(*) FROM v_la_council_meeting_coverage WHERE clean_minutes = 0 AND tier <> 'unseeded'"
    ).fetchone()[0]
    assert seeded_zero == 0


# ── decisions (Extracted band) ────────────────────────────────────────────────────────────


def test_decisions_tally_columns_stay_numeric(con):
    """Only 5 of ~6,500 rows carry a tally, so AUTO_DETECT types an all-empty column VARCHAR
    and its siblings BIGINT — the schema would change shape with the data. The view casts."""
    types = {r[0]: r[1] for r in con.execute("DESCRIBE v_la_council_decisions").fetchall() if r[0].startswith("tally_")}
    assert set(types.values()) == {"BIGINT"}, types
    assert len(types) == 3


def test_decisions_keep_rows_with_no_recorded_outcome(con):
    """~90% of rows have no outcome word because the minutes record the proposer but not the
    resolution. Dropping them would turn a parsing limit into a claim that nothing happened."""
    total, with_outcome = con.execute(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE outcome <> '') FROM v_la_council_decisions"
    ).fetchone()
    assert total > 6000
    assert with_outcome < total  # the empty ones are present, not filtered out
    # read_csv yields NULL for an empty field; the view coalesces so that "not recorded" is
    # ONE value a page can filter on. Without this, `WHERE outcome = ''` matches nothing.
    assert con.execute("SELECT COUNT(*) FROM v_la_council_decisions WHERE outcome IS NULL").fetchone()[0] == 0
    assert (
        con.execute("SELECT COUNT(*) FROM v_la_council_decisions WHERE outcome = ''").fetchone()[0]
        == total - with_outcome
    )


def test_decision_coverage_agrees_with_the_base_view(con):
    for la, rows, with_outcome in con.execute(
        "SELECT local_authority, decision_rows, with_outcome FROM v_la_council_decision_coverage"
    ).fetchall():
        base = con.execute(
            "SELECT COUNT(*), COUNT(*) FILTER (WHERE outcome <> '') FROM v_la_council_decisions "
            "WHERE local_authority = ?",
            [la],
        ).fetchone()
        assert (rows, with_outcome) == base
