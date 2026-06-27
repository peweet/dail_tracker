"""Unit tests for the pure polars transforms in extractors/participation_extract.py.

The attendance / participation feature has a documented HISTORY of denominator bugs
(see doc/ATTENDANCE_PARTICIPATION_REDESIGN.md and the member-overview attendance
denominator regression notes). The honest "turnout" signal is only as trustworthy as
its denominator, so these tests pin the arithmetic of:

  * build_participation  — total_divisions (the per-house/year denominator),
                           voted_in, missed, turnout_pct, and the role/leader gating.
  * build_absence_gaps   — longest INTERIOR run of consecutive plenary sitting days a
                           member was physically absent (recess-proof, bracketed runs).
  * build_presence       — TAA presence × vote turnout → 120-day compliance, the 1%/day
                           deduction, and the divergence flag (which EXCLUDES office
                           holders and party leaders so they are never shamed).
  * build_absence_news   — curated, code-resolved explanation lookup only.

Every fixture is a tiny in-memory polars DataFrame built with the EXACT columns each
function reads (derived by reading the module), and every expected value is computed by
hand in the test so a silent denominator regression fails loudly.

`run` is intentionally skipped: it is pure IO/network glue (reads gold parquet, writes
gold parquet) and composes the pure functions tested here.

polars-WMI gotcha: this repo can hang if hardware/system probing touches WMI at import
time. A plain `import polars as pl` is safe and is all we do here.
"""

from __future__ import annotations

import datetime as dt

import polars as pl

from extractors.participation_extract import (
    build_absence_gaps,
    build_absence_news,
    build_participation,
    build_presence,
)


# ── fixture builders ─────────────────────────────────────────────────────────


def _votes_df(rows: list[dict]) -> pl.DataFrame:
    """A votes frame with the columns build_participation reads:
    house, year, vote_id, unique_member_code, full_name, party, vote_type, d (Date)."""
    return pl.DataFrame(
        rows,
        schema={
            "house": pl.Utf8,
            "year": pl.Int64,
            "vote_id": pl.Int64,
            "unique_member_code": pl.Utf8,
            "full_name": pl.Utf8,
            "party": pl.Utf8,
            "vote_type": pl.Utf8,
            "d": pl.Date,
        },
    )


def _office_df(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "unique_member_code": pl.Utf8,
            "holds_office": pl.Boolean,
            "is_minister": pl.Boolean,
            "is_chair": pl.Boolean,
            "office_name": pl.Utf8,
        },
    )


def _leaders_df(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "full_name": pl.Utf8,
            "house": pl.Utf8,
            "is_leader": pl.Boolean,
            "leader_note": pl.Utf8,
        },
    )


def _empty_office() -> pl.DataFrame:
    return _office_df([])


def _empty_leaders() -> pl.DataFrame:
    return _leaders_df([])


def _vote(code, name, vid, *, party="IND", vtype="Tá", year=2025, house="Dáil",
          day=dt.date(2025, 1, 1)):
    return {
        "house": house, "year": year, "vote_id": vid, "unique_member_code": code,
        "full_name": name, "party": party, "vote_type": vtype, "d": day,
    }


# ── build_participation: the DENOMINATOR ─────────────────────────────────────


def test_participation_denominator_is_distinct_divisions_per_house_year():
    """total_divisions is the DENOMINATOR for turnout and must equal the number of
    DISTINCT vote_ids that occur in the (house, year) cohort — not the row count, and
    not per-member. Two members, two divisions: A votes in both, B only in division 1.

    This is the exact place the historic denominator bug lived: if the denominator were
    computed per-member (e.g. counting only the divisions a member appears in) turnout
    would always be 100% and 'missed' would always be 0. Pinning total_divisions=2 for
    BOTH members guards that."""
    votes = _votes_df([
        _vote("A", "Ann A", 1),
        _vote("A", "Ann A", 2),
        _vote("B", "Ben B", 1),  # B is simply absent from division 2 (no row)
    ])
    out = build_participation(votes, _empty_office(), _empty_leaders())

    by_code = {r["unique_member_code"]: r for r in out.iter_rows(named=True)}
    # Same denominator for every member in the cohort.
    assert by_code["A"]["total_divisions"] == 2
    assert by_code["B"]["total_divisions"] == 2

    # A voted in both → 0 missed, 100%.
    assert by_code["A"]["voted_in"] == 2
    assert by_code["A"]["missed"] == 0
    assert by_code["A"]["turnout_pct"] == 100.0

    # B voted in 1 of 2 → 1 missed (absence = the MISSING row), 50%.
    assert by_code["B"]["voted_in"] == 1
    assert by_code["B"]["missed"] == 1
    assert by_code["B"]["turnout_pct"] == 50.0


def test_participation_denominator_is_scoped_per_house_and_year():
    """The denominator must be partitioned by (house, year): a Dáil 2025 member's
    turnout cannot be diluted by Seanad divisions or by a different year's divisions.
    Here Dáil-2025 has 2 divisions and Seanad-2025 has 1 — each member's denominator
    must reflect only its own (house, year) slice."""
    votes = _votes_df([
        _vote("D1", "Dee 1", 1, house="Dáil"),
        _vote("D1", "Dee 1", 2, house="Dáil"),
        _vote("D2", "Dee 2", 1, house="Dáil"),
        _vote("S1", "Sue 1", 9, house="Seanad"),
    ])
    out = build_participation(votes, _empty_office(), _empty_leaders())
    by_code = {r["unique_member_code"]: r for r in out.iter_rows(named=True)}

    assert by_code["D1"]["total_divisions"] == 2
    assert by_code["D2"]["total_divisions"] == 2  # shares the Dáil denominator, not 1
    assert by_code["S1"]["total_divisions"] == 1  # isolated Seanad denominator
    assert by_code["D2"]["turnout_pct"] == 50.0   # voted 1 of the 2 Dáil divisions
    assert by_code["S1"]["turnout_pct"] == 100.0


def test_participation_duplicate_vote_rows_do_not_inflate_counts():
    """vote_id is counted with n_unique on BOTH the denominator and the per-member
    numerator, so a duplicated member-division row (a known data hazard) must not push
    voted_in or total_divisions above the true distinct count, nor turnout above 100%."""
    votes = _votes_df([
        _vote("A", "Ann A", 1),
        _vote("A", "Ann A", 1),  # exact duplicate row
        _vote("A", "Ann A", 2),
    ])
    out = build_participation(votes, _empty_office(), _empty_leaders())
    row = out.row(0, named=True)
    assert row["total_divisions"] == 2
    assert row["voted_in"] == 2
    assert row["missed"] == 0
    assert row["turnout_pct"] == 100.0


def test_participation_abstentions_count_as_voted_in():
    """An 'Abstained' division is still a recorded presence in the lobby (a row exists),
    so it must count toward voted_in / turnout, while ALSO being tallied separately in
    `abstentions`. Abstaining is participation, not absence — conflating the two would
    re-introduce the censoring the redesign set out to remove."""
    votes = _votes_df([
        _vote("A", "Ann A", 1, vtype="Tá"),
        _vote("A", "Ann A", 2, vtype="Abstained"),
    ])
    out = build_participation(votes, _empty_office(), _empty_leaders())
    row = out.row(0, named=True)
    assert row["voted_in"] == 2          # abstention counted as participation
    assert row["abstentions"] == 1
    assert row["turnout_pct"] == 100.0


def test_participation_role_priority_and_leader_join():
    """Role classification is gated chair > minister > leader > '' and the role_note is
    coalesced office_name → leader_note. Members are NEVER shamed for structurally-low
    turnout, so these flags must attach correctly:

      * a chair (is_chair) → role 'chair' even if also flagged minister,
      * a minister (no chair) → role 'minister' with the office_name note,
      * a party leader (joined on full_name+house, not in the office feed) → 'party_leader'
        with the curated leader_note,
      * a plain backbencher → role '' and the missing office/leader flags fill to False.
    """
    votes = _votes_df([
        _vote("CHAIR", "Chair Person", 1),
        _vote("MIN", "Min Ister", 1),
        _vote("LEAD", "Lead Er", 1),
        _vote("BACK", "Back Bencher", 1),
    ])
    office = _office_df([
        # chair that is ALSO a minister → chair must win the priority chain
        {"unique_member_code": "CHAIR", "holds_office": True, "is_minister": True,
         "is_chair": True, "office_name": "Ceann Comhairle"},
        {"unique_member_code": "MIN", "holds_office": True, "is_minister": True,
         "is_chair": False, "office_name": "Minister for Finance"},
    ])
    leaders = _leaders_df([
        {"full_name": "Lead Er", "house": "Dáil", "is_leader": True,
         "leader_note": "Party leader — votes via pairing"},
    ])
    out = build_participation(votes, office, leaders)
    by_code = {r["unique_member_code"]: r for r in out.iter_rows(named=True)}

    assert by_code["CHAIR"]["role"] == "chair"
    assert by_code["CHAIR"]["is_chair"] is True

    assert by_code["MIN"]["role"] == "minister"
    assert by_code["MIN"]["role_note"] == "Minister for Finance"

    assert by_code["LEAD"]["role"] == "party_leader"
    assert by_code["LEAD"]["is_leader"] is True
    assert by_code["LEAD"]["role_note"] == "Party leader — votes via pairing"

    # backbencher: no office, no leader → flags fill False, empty role/note
    assert by_code["BACK"]["role"] == ""
    assert by_code["BACK"]["holds_office"] is False
    assert by_code["BACK"]["is_minister"] is False
    assert by_code["BACK"]["is_chair"] is False
    assert by_code["BACK"]["is_leader"] is False
    assert by_code["BACK"]["role_note"] == ""


# ── build_absence_gaps: physical-absence run length ──────────────────────────


def _att_dates_df(rows: list[dict]) -> pl.DataFrame:
    """attendance-dates frame as _attendance_dates() produces it:
    identifier, house, year, present_date (Date), is_plenary (bool)."""
    return pl.DataFrame(
        rows,
        schema={
            "identifier": pl.Utf8,
            "house": pl.Utf8,
            "year": pl.Int64,
            "present_date": pl.Date,
            "is_plenary": pl.Boolean,
        },
    )


def _code_map_df(rows: list[dict]) -> pl.DataFrame:
    return pl.DataFrame(
        rows,
        schema={
            "identifier": pl.Utf8,
            "house": pl.Utf8,
            "year": pl.Int64,
            "unique_member_code": pl.Utf8,
            "full_name": pl.Utf8,
        },
    )


def test_absence_gap_counts_interior_missed_sitting_days():
    """The plenary calendar is the set of DISTINCT plenary sitting dates across all
    members. With 5 sitting days (indices 0..4) a member present on days 0 and 4 but
    absent on 1,2,3 has an INTERIOR run of 3 missed sitting days (idx 4 - idx 0 - 1).

    Crucially the run is measured in SITTING-DAY INDICES, not calendar days, so recesses
    don't inflate it (recess days are not plenary sitting dates and never enter the
    calendar). This is the recess-proof property the redesign requires."""
    # member M is present days 0 and 4; member FILL anchors the full 5-day calendar.
    cal = [dt.date(2025, 1, d) for d in (6, 7, 8, 9, 10)]  # 5 plenary sitting dates
    rows = []
    for d in cal:  # FILL is present every sitting day → defines the calendar
        rows.append({"identifier": "FILL", "house": "Dáil", "year": 2025,
                     "present_date": d, "is_plenary": True})
    rows.append({"identifier": "M", "house": "Dáil", "year": 2025,
                 "present_date": cal[0], "is_plenary": True})
    rows.append({"identifier": "M", "house": "Dáil", "year": 2025,
                 "present_date": cal[4], "is_plenary": True})
    att = _att_dates_df(rows)
    code_map = _code_map_df([
        {"identifier": "M", "house": "Dáil", "year": 2025,
         "unique_member_code": "M.CODE", "full_name": "Em Member"},
        {"identifier": "FILL", "house": "Dáil", "year": 2025,
         "unique_member_code": "FILL.CODE", "full_name": "Fill Er"},
    ])
    out = build_absence_gaps(att, code_map)
    m = out.filter(pl.col("unique_member_code") == "M.CODE").row(0, named=True)

    assert m["longest_run_sitting_days"] == 3       # days 1,2,3 missed between 0 and 4
    assert m["run_start"] == cal[0]
    assert m["run_end"] == cal[4]
    # calendar span between the two present dates = 10 Jan - 6 Jan = 4 days
    assert m["run_calendar_days"] == 4


def test_absence_gap_present_committee_day_breaks_the_run():
    """'Present' on a sitting date means badged in AT ALL that day — sitting OR committee.
    A non-plenary (committee) badge-in that falls on a plenary sitting date must therefore
    BREAK the absence run: the member was physically in Leinster House. Here the member is
    plenary-present on days 0 and 4 and committee-present on day 2, so the longest interior
    run is 1 (day 1 alone), NOT 3."""
    cal = [dt.date(2025, 1, d) for d in (6, 7, 8, 9, 10)]
    rows = []
    for d in cal:
        rows.append({"identifier": "FILL", "house": "Dáil", "year": 2025,
                     "present_date": d, "is_plenary": True})
    # M plenary-present on days 0 and 4
    rows.append({"identifier": "M", "house": "Dáil", "year": 2025,
                 "present_date": cal[0], "is_plenary": True})
    rows.append({"identifier": "M", "house": "Dáil", "year": 2025,
                 "present_date": cal[4], "is_plenary": True})
    # M committee-present (non-plenary) on day 2 — in the building, breaks the run
    rows.append({"identifier": "M", "house": "Dáil", "year": 2025,
                 "present_date": cal[2], "is_plenary": False})
    att = _att_dates_df(rows)
    code_map = _code_map_df([
        {"identifier": "M", "house": "Dáil", "year": 2025,
         "unique_member_code": "M.CODE", "full_name": "Em Member"},
    ])
    out = build_absence_gaps(att, code_map)
    m = out.filter(pl.col("unique_member_code") == "M.CODE").row(0, named=True)
    # runs are day1 (between idx0 and idx2) and day3 (between idx2 and idx4): each length 1
    assert m["longest_run_sitting_days"] == 1


def test_absence_gap_perfect_attendance_is_zero_not_null():
    """A member present on every plenary sitting date has no interior gap. The output
    must fill that to 0 (a real 'no notable absence'), never null — a null would break
    downstream numeric ranking / sorting in the gold table."""
    cal = [dt.date(2025, 1, d) for d in (6, 7, 8)]
    rows = [{"identifier": "M", "house": "Dáil", "year": 2025,
             "present_date": d, "is_plenary": True} for d in cal]
    att = _att_dates_df(rows)
    code_map = _code_map_df([
        {"identifier": "M", "house": "Dáil", "year": 2025,
         "unique_member_code": "M.CODE", "full_name": "Em Member"},
    ])
    out = build_absence_gaps(att, code_map)
    m = out.filter(pl.col("unique_member_code") == "M.CODE").row(0, named=True)
    assert m["longest_run_sitting_days"] == 0


def test_absence_gap_drops_members_without_code():
    """The final select filters to rows where unique_member_code is non-null: a badge-in
    identifier that the gold roster cannot resolve to a member code is dropped rather than
    emitted with a null key that would corrupt the member-join downstream."""
    cal = [dt.date(2025, 1, d) for d in (6, 7, 8)]
    rows = []
    for d in cal:
        rows.append({"identifier": "FILL", "house": "Dáil", "year": 2025,
                     "present_date": d, "is_plenary": True})
    rows.append({"identifier": "GHOST", "house": "Dáil", "year": 2025,
                 "present_date": cal[0], "is_plenary": True})
    rows.append({"identifier": "GHOST", "house": "Dáil", "year": 2025,
                 "present_date": cal[2], "is_plenary": True})
    att = _att_dates_df(rows)
    # code_map only resolves FILL — GHOST has no code.
    code_map = _code_map_df([
        {"identifier": "FILL", "house": "Dáil", "year": 2025,
         "unique_member_code": "FILL.CODE", "full_name": "Fill Er"},
    ])
    out = build_absence_gaps(att, code_map)
    assert "GHOST" not in out["full_name"].to_list()
    assert out.filter(pl.col("unique_member_code").is_null()).height == 0


# ── build_presence: 120-day compliance + divergence gating ───────────────────


def _att_presence_df(rows: list[dict]) -> pl.DataFrame:
    """TAA attendance frame with the columns build_presence reads."""
    return pl.DataFrame(
        rows,
        schema={
            "unique_member_code": pl.Utf8,
            "house": pl.Utf8,
            "year": pl.Int64,
            "full_name": pl.Utf8,
            "party_name": pl.Utf8,
            "constituency": pl.Utf8,
            "sitting_days": pl.Int64,
            "other_days": pl.Int64,
            "total_days": pl.Int64,
        },
    )


def _part_for_presence(rows: list[dict]) -> pl.DataFrame:
    """The slim participation frame build_presence consumes."""
    return pl.DataFrame(
        rows,
        schema={
            "unique_member_code": pl.Utf8,
            "house": pl.Utf8,
            "year": pl.Int64,
            "voted_in": pl.Int64,
            "total_divisions": pl.Int64,
            "turnout_pct": pl.Float64,
        },
    )


def _att_row(code, name, total, *, sitting=None, other=0, house="Dáil", year=2025,
             party="IND", const="Dublin"):
    sitting = total - other if sitting is None else sitting
    return {
        "unique_member_code": code, "house": house, "year": year, "full_name": name,
        "party_name": party, "constituency": const, "sitting_days": sitting,
        "other_days": other, "total_days": total,
    }


def test_presence_120_day_compliance_and_deduction():
    """The TAA allowance basis: meets_120 = total_days >= 120, days_below_minimum =
    max(120 - total_days, 0), deduction_pct = days_below clipped to 100 (1%/day, capped).

    Three members pin the boundary and the cap:
      * exactly 120 days → meets, 0 below, 0 deduction (boundary is INCLUSIVE),
      * 110 days        → 10 below → 10% deduction,
      * 5 days          → 115 below → deduction CAPPED at 100, not 115.
    Getting the cap or the >= boundary wrong mis-states real money owed/deducted."""
    att = _att_presence_df([
        _att_row("EXACT", "Ex Act", 120),
        _att_row("UNDER", "Un Der", 110),
        _att_row("TINY", "Ti Ny", 5),
    ])
    part = _part_for_presence([])  # no votes needed for the compliance arithmetic
    out = build_presence(att, part, _empty_office(), _empty_leaders())
    by_code = {r["unique_member_code"]: r for r in out.iter_rows(named=True)}

    assert by_code["EXACT"]["meets_120"] is True
    assert by_code["EXACT"]["days_below_minimum"] == 0
    assert by_code["EXACT"]["deduction_pct"] == 0

    assert by_code["UNDER"]["meets_120"] is False
    assert by_code["UNDER"]["days_below_minimum"] == 10
    assert by_code["UNDER"]["deduction_pct"] == 10

    assert by_code["TINY"]["days_below_minimum"] == 115
    assert by_code["TINY"]["deduction_pct"] == 100  # capped, not 115


def test_presence_divergence_excludes_office_holders_and_leaders():
    """The divergence headline ('badged in a lot, barely voted') is a BACKBENCHER signal.
    It fires only when total_days >= 100 AND turnout < 50 AND NOT holds_office AND NOT
    is_leader. Ministers/chairs and party leaders structurally vote less (gov't business,
    pairing) so flagging them would be a false accusation — exactly the shaming the
    redesign forbids.

    Four members all at 150 days / 30% turnout:
      * plain backbencher → divergence TRUE,
      * office holder      → FALSE (gated out),
      * party leader       → FALSE (gated out),
      * backbencher at 51% turnout → FALSE (turnout threshold not met)."""
    att = _att_presence_df([
        _att_row("BACK", "Back Bencher", 150),
        _att_row("OFFICE", "Off Holder", 150),
        _att_row("LEADER", "Lead Er", 150),
        _att_row("OKVOTE", "Ok Voter", 150),
    ])
    part = _part_for_presence([
        {"unique_member_code": "BACK", "house": "Dáil", "year": 2025,
         "voted_in": 30, "total_divisions": 100, "turnout_pct": 30.0},
        {"unique_member_code": "OFFICE", "house": "Dáil", "year": 2025,
         "voted_in": 30, "total_divisions": 100, "turnout_pct": 30.0},
        {"unique_member_code": "LEADER", "house": "Dáil", "year": 2025,
         "voted_in": 30, "total_divisions": 100, "turnout_pct": 30.0},
        {"unique_member_code": "OKVOTE", "house": "Dáil", "year": 2025,
         "voted_in": 51, "total_divisions": 100, "turnout_pct": 51.0},
    ])
    office = _office_df([
        {"unique_member_code": "OFFICE", "holds_office": True, "is_minister": True,
         "is_chair": False, "office_name": "Minister for Health"},
    ])
    leaders = _leaders_df([
        {"full_name": "Lead Er", "house": "Dáil", "is_leader": True,
         "leader_note": "leader"},
    ])
    out = build_presence(att, part, office, leaders)
    by_code = {r["unique_member_code"]: r for r in out.iter_rows(named=True)}

    assert by_code["BACK"]["divergence_present_low_vote"] is True
    assert by_code["OFFICE"]["divergence_present_low_vote"] is False   # office gate
    assert by_code["LEADER"]["divergence_present_low_vote"] is False   # leader gate
    assert by_code["OKVOTE"]["divergence_present_low_vote"] is False   # turnout gate


def test_presence_divergence_requires_minimum_presence_and_handles_null_turnout():
    """Two more divergence boundaries:
      * total_days < 100 → not enough presence to call it 'present but not voting', so a
        low-turnout member who barely attended is NOT flagged (their problem is absence,
        captured elsewhere). 99 days @ 10% → FALSE; 100 days @ 10% → TRUE (>= boundary).
      * a member with NO vote rows joins to null turnout_pct; fill_null(0) treats that as
        0% so a heavily-present non-voter still divergence-flags rather than slipping
        through on a null comparison."""
    att = _att_presence_df([
        _att_row("LOWATT", "Low Att", 99),
        _att_row("ATBOUND", "At Bound", 100),
        _att_row("NOVOTE", "No Voter", 150),
    ])
    part = _part_for_presence([
        {"unique_member_code": "LOWATT", "house": "Dáil", "year": 2025,
         "voted_in": 10, "total_divisions": 100, "turnout_pct": 10.0},
        {"unique_member_code": "ATBOUND", "house": "Dáil", "year": 2025,
         "voted_in": 10, "total_divisions": 100, "turnout_pct": 10.0},
        # NOVOTE deliberately absent from participation → null turnout after the left join
    ])
    out = build_presence(att, part, _empty_office(), _empty_leaders())
    by_code = {r["unique_member_code"]: r for r in out.iter_rows(named=True)}

    assert by_code["LOWATT"]["divergence_present_low_vote"] is False   # < 100 days
    assert by_code["ATBOUND"]["divergence_present_low_vote"] is True   # exactly 100 days
    # null turnout → coalesced to 0% < 50 → present non-voter is flagged
    assert by_code["NOVOTE"]["turnout_pct"] is None
    assert by_code["NOVOTE"]["divergence_present_low_vote"] is True


def test_presence_left_join_preserves_attendance_rows_without_votes():
    """build_presence is attendance-driven via a LEFT join onto participation: a member in
    the TAA record who cast no votes must SURVIVE (height preserved) with null vote columns,
    not be dropped. Dropping them would hide the worst non-voters from the feature."""
    att = _att_presence_df([
        _att_row("HASVOTE", "Has Vote", 130),
        _att_row("NOVOTE", "No Vote", 130),
    ])
    part = _part_for_presence([
        {"unique_member_code": "HASVOTE", "house": "Dáil", "year": 2025,
         "voted_in": 90, "total_divisions": 100, "turnout_pct": 90.0},
    ])
    out = build_presence(att, part, _empty_office(), _empty_leaders())
    assert out.height == 2
    novote = out.filter(pl.col("unique_member_code") == "NOVOTE").row(0, named=True)
    assert novote["voted_in"] is None
    assert novote["total_days"] == 130  # attendance side intact


# ── build_absence_news: curated-only resolution ──────────────────────────────


def test_absence_news_returns_curated_schema_when_no_csv(monkeypatch, tmp_path):
    """The live news fallback was REMOVED (a name search surfaced same-name strangers as
    false 'explanations'). With no curated CSV present the function must return an EMPTY
    frame carrying the exact news schema — never invent an explanation. We point the
    module's EXPLANATIONS_CSV at a non-existent path to exercise the no-CSV branch."""
    import extractors.participation_extract as mod

    monkeypatch.setattr(mod, "EXPLANATIONS_CSV", tmp_path / "does_not_exist.csv")
    name_to_code = pl.DataFrame(
        {"full_name": ["Em Member"], "house": ["Dáil"], "unique_member_code": ["M.CODE"]}
    )
    out = build_absence_news(name_to_code)
    assert out.height == 0
    assert set(out.columns) == {
        "unique_member_code", "year", "reason_label",
        "source_title", "source_url", "outlet", "is_curated",
    }


def test_absence_news_resolves_curated_csv_to_member_code(monkeypatch, tmp_path):
    """A curated explanation is keyed by (full_name, house) and must resolve to the member
    CODE via name_to_code, dropping any curated row whose name does not match a known member
    (so a typo can never display against the wrong person). The resolved row is marked
    is_curated=True with outlet='curated'."""
    import extractors.participation_extract as mod

    csv = tmp_path / "explanations.csv"
    pl.DataFrame(
        {
            "full_name": ["Em Member", "Unknown Person"],
            "house": ["Dáil", "Dáil"],
            "year": [2025, 2025],
            "reason_label": ["Maternity leave", "Illness"],
            "source_title": ["Title A", "Title B"],
            "source_url": ["http://a", "http://b"],
        }
    ).write_csv(csv)
    monkeypatch.setattr(mod, "EXPLANATIONS_CSV", csv)

    name_to_code = pl.DataFrame(
        {"full_name": ["Em Member"], "house": ["Dáil"], "unique_member_code": ["M.CODE"]}
    )
    out = build_absence_news(name_to_code)
    # only the matched member survives; the unknown-name curated row is dropped
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["unique_member_code"] == "M.CODE"
    assert row["reason_label"] == "Maternity leave"
    assert row["is_curated"] is True
    assert row["outlet"] == "curated"
    assert row["year"] == 2025
