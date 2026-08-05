"""
SQL view contract tests — committee assignments/office-holders/party-seats
and committee evidence/meeting-history views.

Split out of the former monolithic test_sql_views.py (REFACTORING_CANDIDATES C6).
Shared fixtures/helpers live in _view_test_helpers.py.
"""

import duckdb
import pytest

from ._view_test_helpers import (
    GOLD_PARQUET_DIR,
    _assert_cols,
    _con,
    _load,
    _result,
    _skip_missing,
    _src,
    _view_path,
)

# ---------------------------------------------------------------------------
# COMMITTEES VIEWS
# ---------------------------------------------------------------------------
#
# committees_data registers with swallow_errors=False, so a break fails the app
# loudly — but no test pinned the columns. v_committee_member_detail and
# v_committee_party_seats both read v_committee_assignments, so that file must be
# created first on the same connection.


@pytest.mark.sql
def test_v_committee_assignments_executes():
    _skip_missing(
        *_src("data/silver/committees/committee_assignments.parquet", "data/silver/parquet/flattened_members.parquet")
    )
    con = _con()
    # v_committee_assignments LEFT JOINs v_lobbying_base_member_codes for
    # unique_member_code — must load first.
    con.execute(_load("lobbying_base_member_codes.sql"))
    con.execute(_load("committees_assignments.sql"))
    result = _result(con, "v_committee_assignments")
    _assert_cols(
        result, "chamber", "name", "party", "committee", "role", "is_chair", "start", "end", "unique_member_code"
    )
    assert len(result) > 0


@pytest.mark.sql
def test_v_committee_office_holders_executes():
    _skip_missing(
        *_src("data/silver/committees/office_holders.parquet", "data/silver/parquet/flattened_members.parquet")
    )
    con = _con()
    # v_committee_office_holders LEFT JOINs v_lobbying_base_member_codes for
    # unique_member_code — must load first.
    con.execute(_load("lobbying_base_member_codes.sql"))
    con.execute(_load("committees_offices.sql"))
    result = _result(con, "v_committee_office_holders")
    _assert_cols(result, "chamber", "name", "party", "office", "start", "end", "unique_member_code")
    assert len(result) > 0


@pytest.mark.sql
def test_v_committee_member_detail_executes():
    """Reads v_committee_assignments — load assignments first. party_seats_json
    is the column the composition stacked-bar card parses."""
    _skip_missing(
        *_src("data/silver/committees/committee_assignments.parquet", "data/silver/parquet/flattened_members.parquet")
    )
    con = _con()
    # v_committee_assignments and v_committee_member_detail both LEFT JOIN
    # v_lobbying_base_member_codes — must load first.
    con.execute(_load("lobbying_base_member_codes.sql"))
    con.execute(_load("committees_assignments.sql"))
    con.execute(_load("committees_zz_member_detail.sql"))
    result = _result(con, "v_committee_member_detail")
    _assert_cols(
        result, "chamber", "committee", "members", "parties", "chair_name", "party_seats_json", "unique_member_code"
    )
    assert len(result) > 0


@pytest.mark.sql
def test_v_committee_party_seats_executes():
    _skip_missing(
        *_src("data/silver/committees/committee_assignments.parquet", "data/silver/parquet/flattened_members.parquet")
    )
    con = _con()
    # v_committee_assignments LEFT JOINs v_lobbying_base_member_codes for
    # unique_member_code — must load first.
    con.execute(_load("lobbying_base_member_codes.sql"))
    con.execute(_load("committees_assignments.sql"))
    con.execute(_load("committees_zz_party_seats.sql"))
    result = _result(con, "v_committee_party_seats")
    _assert_cols(result, "chamber", "committee", "party", "seats")
    assert len(result) > 0


# ---------------------------------------------------------------------------
# COMMITTEE EVIDENCE / MEETING HISTORY
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_v_committee_meetings_executes():
    """Committee meeting-history spine — one row per (committee, date) with the
    session topics, witness orgs/people, and the transcript link. Locks the
    column contract the Committees page meeting-history section reads, the
    casefold crosswalk key the page filters on, and the LEFT-JOIN no-inflation
    invariant (orgs/persons aggregated to lists, never fanning out the spine)."""
    _skip_missing(
        GOLD_PARQUET_DIR / "committee_meetings.parquet",
        GOLD_PARQUET_DIR / "committee_witnesses.parquet",
        GOLD_PARQUET_DIR / "committee_witness_persons.parquet",
    )
    con = _con()
    con.execute(_load("committee_evidence_meetings.sql"))
    result = _result(con, "v_committee_meetings")
    for col in (
        "committee_code",
        "committee_name",
        "committee_key",
        "date",
        "source_xml",
        "transcript_url",
        "topics",
        "n_topics",
        "n_orgs",
        "n_persons",
        "witness_orgs",
        "witness_persons",
    ):
        assert col in result.columns, f"Expected column {col!r} in v_committee_meetings"
    assert len(result) > 0

    # transcript_url must be re-homed onto the citizen-facing debates site, never
    # left pointing at the raw AKN XML.
    bad_url = con.execute(
        "SELECT count(*) FROM v_committee_meetings"
        " WHERE transcript_url NOT LIKE 'https://www.oireachtas.ie/en/debates/debate/%'"
    ).fetchone()[0]
    assert bad_url == 0, "transcript_url must point at oireachtas.ie/en/debates/debate/"

    # crosswalk key is the committee name normalized to a formation stem for the page
    # filter: curly apostrophes folded to ASCII, whitespace collapsed, lowercased, and
    # the leading Joint/Select formation prefix stripped (so a meeting attaches to the
    # register entry regardless of apostrophe glyph, case, or Joint↔Select formation).
    bad_key = con.execute(
        "SELECT count(*) FROM v_committee_meetings WHERE committee_key <> "
        " regexp_replace(lower(trim(regexp_replace(replace(replace(committee_name, chr(8217), ''''),"
        " chr(8216), ''''), '\\s+', ' ', 'g'))),"
        " '^(seanad |dail )?(joint |select )?(committee (on|of) |comhchoiste |roghchoiste )', '')"
    ).fetchone()[0]
    assert bad_key == 0, "committee_key must equal the normalized formation stem"

    # the typographic-apostrophe committee must carry an ASCII-folded key (the apostrophe
    # half of the crosswalk) — "Committee on Members' Interests of Dáil Éireann".
    apos_key = con.execute(
        "SELECT count(*) FROM v_committee_meetings WHERE committee_name LIKE '%Members%Interests%'"
        " AND committee_key NOT LIKE '%members'' interests%'"
    ).fetchone()[0]
    assert apos_key == 0, "Members' Interests key must fold the curly apostrophe to ASCII"

    # the formation half: every meeting key must be free of a leading Joint/Select
    # prefix, so the Select page and Joint page of one committee share a key.
    formation_key = con.execute(
        "SELECT count(*) FROM v_committee_meetings WHERE committee_key ~  '^(joint|select|comhchoiste|roghchoiste)\\b'"
    ).fetchone()[0]
    assert formation_key == 0, "committee_key must strip the Joint/Select formation prefix"

    # spine is one row per (committee, date) — the witness LEFT JOINs aggregate
    # to lists and must never inflate it.
    n, distinct = con.execute(
        "SELECT count(*), count(DISTINCT (committee_code, date)) FROM v_committee_meetings"
    ).fetchone()
    assert n == distinct, f"v_committee_meetings not one-row-per-(committee, date): {n} rows, {distinct} distinct"


def test_committee_meetings_crosswalk_folds_typographic_apostrophe(tmp_path):
    """Regression: a meeting whose committee name carries the typographic apostrophe
    (' U+2019, as the Oireachtas API's showAs renders it) MUST attach to the
    membership register's committee selection, which uses the ASCII apostrophe
    (' U+0027). A plain lower() left the two unequal, so the meeting silently
    vanished from the Committees page ("Meeting history not yet available" despite
    the row existing in gold — observed for "Committee on Members' Interests of
    Dáil Éireann").

    Self-contained (does not skip in CI): synthesises the three gold parquets with a
    curly-apostrophe committee name, loads the REAL view SQL over them, then drives
    the REAL production retrieval query with an ASCII-apostrophe selection — locking
    both halves of the crosswalk (the view's committee_key AND the query's parameter
    normalization) so the bug cannot reappear in either place independently.
    """
    from dail_tracker_core.queries import committees as _committees_q

    curly = "Committee on Members’ Interests of Dáil Éireann"  # API showAs glyph
    ascii_sel = "Committee on Members' Interests of Dáil Éireann"  # register selection
    assert curly != ascii_sel and curly.lower() != ascii_sel.lower()  # the bug's premise

    gen = duckdb.connect()
    meetings_pq = (tmp_path / "committee_meetings.parquet").as_posix()
    witnesses_pq = (tmp_path / "committee_witnesses.parquet").as_posix()
    persons_pq = (tmp_path / "committee_witness_persons.parquet").as_posix()
    gen.execute(
        "COPY (SELECT 'mi_dail' AS committee_code, ? AS committee_name, 1 AS house_no,"
        " DATE '2025-06-26' AS date,"
        " 'https://data.oireachtas.ie/akn/ie/debateRecord/committee_on_members_interests_of_dail_eireann/2025-06-26/x.xml' AS source_xml,"
        " ['Standards in Public Office']::VARCHAR[] AS topics, 1 AS n_topics, 1 AS n_orgs, 1 AS n_persons"
        f") TO '{meetings_pq}' (FORMAT parquet)",
        [curly],
    )
    gen.execute(
        "COPY (SELECT 'mi_dail' AS committee_code, DATE '2025-06-26' AS date, 'SIPO' AS witness_org)"
        f" TO '{witnesses_pq}' (FORMAT parquet)"
    )
    gen.execute(
        "COPY (SELECT 'mi_dail' AS committee_code, DATE '2025-06-26' AS date, 'Mr. A. Witness' AS witness_person)"
        f" TO '{persons_pq}' (FORMAT parquet)"
    )

    sql = _view_path("committee_evidence_meetings.sql").read_text(encoding="utf-8")
    sql = sql.replace("data/gold/parquet/committee_meetings.parquet", meetings_pq)
    sql = sql.replace("data/gold/parquet/committee_witnesses.parquet", witnesses_pq)
    sql = sql.replace("data/gold/parquet/committee_witness_persons.parquet", persons_pq)

    con = _con()
    con.execute(sql)

    # the page selects with the ASCII apostrophe — the production query must still
    # find the curly-apostrophe meeting.
    res = _committees_q.meetings(con, ascii_sel)
    assert res.ok, res.unavailable_reason
    assert len(res.data) == 1, "ASCII-apostrophe selection must retrieve the curly-apostrophe meeting"
    assert res.data.iloc[0]["committee_name"] == curly

    # negative control: the ORIGINAL crosswalk (committee_key := lower(committee_name),
    # matched against lower(selection)) genuinely missed — proving the normalization is
    # what bridges the glyphs, not some incidental equality.
    would_have_missed = con.execute(
        "SELECT count(*) FROM v_committee_meetings WHERE lower(committee_name) = lower(?)", [ascii_sel]
    ).fetchone()[0]
    assert would_have_missed == 0, "test premise broken: raw lower() should NOT match (else nothing is verified)"


def test_committee_meetings_crosswalk_merges_joint_and_select_formations(tmp_path):
    """Regression: the Oireachtas runs each committee in two FORMATIONS — the Joint
    committee (both houses) and the Select committee (one house, bill stages) — and
    records meetings under whichever sat. A citizen sees one committee, so EITHER
    register page (Joint or Select) must surface BOTH formations' meetings. Before the
    formation-stem crosswalk, a Select committee with no own sittings showed "not yet
    available" even though its Joint twin had dozens of meetings (observed for Select
    Cttees on AI, Drugs Use, EU Affairs, Public Petitions, Traveller Community, Good
    Friday).

    Self-contained: synthesises one Joint meeting + one Select meeting for the same
    committee body, loads the REAL view SQL, and drives the REAL retrieval query from
    BOTH the Joint and the Select register name — each must return both meetings.
    """
    from dail_tracker_core.queries import committees as _committees_q

    joint_name = "Joint Committee on Drugs Use"
    select_name = "Select Committee on Drugs Use"

    gen = duckdb.connect()
    meetings_pq = (tmp_path / "committee_meetings.parquet").as_posix()
    witnesses_pq = (tmp_path / "committee_witnesses.parquet").as_posix()
    persons_pq = (tmp_path / "committee_witness_persons.parquet").as_posix()
    gen.execute(
        "COPY (SELECT * FROM (VALUES"
        " ('drugs_joint', ?, 1, DATE '2025-05-01',"
        "  'https://data.oireachtas.ie/akn/ie/debateRecord/joint_committee_on_drugs_use/2025-05-01/x.xml',"
        "  ['Engagement']::VARCHAR[], 1, 0, 0),"
        " ('drugs_select', ?, 1, DATE '2025-05-08',"
        "  'https://data.oireachtas.ie/akn/ie/debateRecord/select_committee_on_drugs_use/2025-05-08/x.xml',"
        "  ['Committee Stage']::VARCHAR[], 1, 0, 0)"
        ") AS t(committee_code, committee_name, house_no, date, source_xml, topics, n_topics, n_orgs, n_persons))"
        f" TO '{meetings_pq}' (FORMAT parquet)",
        [joint_name, select_name],
    )
    # No witnesses/persons for these meetings, but the view LEFT-JOINs both parquets,
    # so they must exist with the right schema (zero rows is fine).
    gen.execute(
        "COPY (SELECT 'x' AS committee_code, DATE '2099-01-01' AS date, 'x' AS witness_org WHERE 1=0)"
        f" TO '{witnesses_pq}' (FORMAT parquet)"
    )
    gen.execute(
        "COPY (SELECT 'x' AS committee_code, DATE '2099-01-01' AS date, 'x' AS witness_person WHERE 1=0)"
        f" TO '{persons_pq}' (FORMAT parquet)"
    )

    sql = _view_path("committee_evidence_meetings.sql").read_text(encoding="utf-8")
    sql = sql.replace("data/gold/parquet/committee_meetings.parquet", meetings_pq)
    sql = sql.replace("data/gold/parquet/committee_witnesses.parquet", witnesses_pq)
    sql = sql.replace("data/gold/parquet/committee_witness_persons.parquet", persons_pq)
    con = _con()
    con.execute(sql)

    for selection in (joint_name, select_name):
        res = _committees_q.meetings(con, selection)
        assert res.ok, res.unavailable_reason
        names = sorted(res.data["committee_name"].tolist())
        assert names == [joint_name, select_name], (
            f"selecting {selection!r} must return BOTH formations' meetings, got {names}"
        )

    # both formations must share one crosswalk key (the topic stem).
    keys = {r[0] for r in con.execute("SELECT DISTINCT committee_key FROM v_committee_meetings").fetchall()}
    assert keys == {"drugs use"}, f"Joint+Select must collapse to one stem key, got {keys}"
