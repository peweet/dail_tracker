"""Unit tests for the gold-enrichment transforms in votes/enrich.py.

Each ``_build_*`` helper is a pure silver->gold transform that reads small
CSV/parquet inputs and writes a gold artefact. The tests drive them with
tiny in-memory fixtures written to ``tmp_path`` and read the result back,
so they need no pipeline output and no network.

These lock the behaviour the docstring promises: name-key normalisation,
the LEFT-join shape (members preserved when attendance is missing),
year-of-election extraction, the (member, date, vote_id) vote dedup, and
the since-2020 payment-ranking filter + rank ordering.
"""

from __future__ import annotations

import polars as pl

from shared.normalise_join_key import normalise_df_td_name
from votes.enrich import (
    _build_attendance_by_year,
    _build_enriched_attendance,
    _build_members_and_master,
    _build_payment_rankings,
    _build_vote_history,
)


def _members_csv(tmp_path, rows: list[dict]):
    p = tmp_path / "flattened_members.csv"
    pl.DataFrame(rows).write_csv(p)
    return p


_MEMBER_COLS = dict(
    full_name="Mary Lou McDonald",
    year_elected="2011",
    ministerial_office=None,
    constituency_name="Dublin Central",
    party="Sinn Féin",
    constituency_code="dublin-central",
    dail_number=34,
)


def _member_row(first, last, code, **over):
    return {"first_name": first, "last_name": last, "unique_member_code": code, **_MEMBER_COLS, **over}


# ── _build_members_and_master ────────────────────────────────────────────────


def test_master_renames_and_join_key(tmp_path):
    csv = _members_csv(
        tmp_path,
        [
            _member_row("Mary Lou", "McDonald", "MaryLou.McDonald.D.2011-02-25"),
            _member_row("Micheál", "Martin", "Micheal.Martin.D.1989-06-15", constituency_name="Cork South-Central"),
        ],
    )
    members_wide, master = _build_members_and_master(csv)

    # Columns are renamed to the gold contract; old names are gone.
    assert {"identifier", "constituency", "position"} <= set(master.columns)
    assert not ({"unique_member_code", "constituency_name", "ministerial_office"} & set(master.columns))
    assert master.height == 2

    # join_key is the normalise_df_td_name sorted-char key, not the raw name.
    expected = normalise_df_td_name(pl.DataFrame({"jk": ["Mary LouMcDonald"]}), "jk")["join_key"][0]
    mary = master.filter(pl.col("identifier") == "MaryLou.McDonald.D.2011-02-25")
    assert mary["join_key"][0] == expected
    assert " " not in expected and expected == "".join(sorted(expected))


def test_master_dedup_keeps_highest_dail_number(tmp_path):
    # Same identifier across two terms (different display name → different
    # join_key, so the members_wide name-dedup keeps both); master then dedups
    # on identifier after sorting dail_number descending → the later term wins.
    csv = _members_csv(
        tmp_path,
        [
            _member_row("Mary", "McDonald", "DUP.CODE.1", dail_number=33, party="OLD"),
            _member_row("Mary Lou", "McDonald", "DUP.CODE.1", dail_number=34, party="NEW"),
        ],
    )
    _, master = _build_members_and_master(csv)
    assert master.height == 1
    assert master["party"][0] == "NEW"


def test_master_name_collision_dedups_members_wide(tmp_path):
    # Two rows that normalise to the same join_key collapse to one in
    # members_wide (unique on join_key, keep first).
    csv = _members_csv(
        tmp_path,
        [
            _member_row("Mary Lou", "McDonald", "CODE.A"),
            _member_row("Mary Lou", "McDonald", "CODE.B"),
        ],
    )
    members_wide, _ = _build_members_and_master(csv)
    assert members_wide.height == 1


# ── _build_enriched_attendance ───────────────────────────────────────────────


def test_enriched_left_join_preserves_members_and_extracts_year(tmp_path):
    members_csv = _members_csv(
        tmp_path,
        [
            _member_row("Mary Lou", "McDonald", "MaryLou.McDonald.D.2011-02-25"),
            _member_row("Noel", "Nomatch", "Noel.Nomatch.D.2016-03-09"),
        ],
    )
    members_wide, _ = _build_members_and_master(members_csv)

    # Attendance fact has a row for McDonald only.
    fact = tmp_path / "fact.csv"
    pl.DataFrame(
        {"first_name": ["Mary Lou"], "last_name": ["McDonald"], "identifier": ["id1"], "year": [2023]}
    ).write_csv(fact)

    enriched = _build_enriched_attendance(members_wide, fact)

    # LEFT join: both members survive even though Nomatch has no attendance row.
    assert enriched.height == 2
    # year_elected is re-derived from the 4-digit token in unique_member_code.
    by_code = {r["unique_member_code"]: r["year_elected"] for r in enriched.iter_rows(named=True)}
    assert by_code["MaryLou.McDonald.D.2011-02-25"] == "2011"
    assert by_code["Noel.Nomatch.D.2016-03-09"] == "2016"


# ── _build_attendance_by_year ────────────────────────────────────────────────


def test_attendance_by_year_totals_and_filters_null_year(tmp_path):
    """Attendance-spine build: the FACT drives the rows (so a member who sat but is
    not on the current roster is kept), roster metadata is left-joined, null-year
    rows are dropped, the 'Member Services' junk row is excluded, and counts are
    max(distinct) per (member, year)."""
    members_csv = _members_csv(
        tmp_path,
        [_member_row("Mary Lou", "McDonald", "MaryLou.McDonald.D.2011-02-25", party="SF")],
    )
    members_wide, _ = _build_members_and_master(members_csv)

    fact = tmp_path / "fact.csv"
    pl.DataFrame(
        {
            # McDonald is on the roster (matched); Doe is a FORMER TD with no roster
            # row; the null-year McDonald row must be filtered; Memberservices is junk.
            "first_name": ["Mary Lou", "Mary Lou", "Mary Lou", "John", "AP"],
            "last_name": ["McDonald", "McDonald", "McDonald", "Doe", "Memberservices"],
            "identifier": [
                "McDonald_Mary Lou",
                "McDonald_Mary Lou",
                "McDonald_Mary Lou",
                "Doe_John",
                "Memberservices_AP",
            ],
            "year": [2023, 2023, None, 2022, 2023],
            "sitting_days_count": [10, 8, 99, 5, 4],
            "other_days_count": [2, 2, 1, 1, 1],
        }
    ).write_csv(fact)

    csv_path = tmp_path / "aby.csv"
    parquet_path = tmp_path / "aby.parquet"
    _build_attendance_by_year(members_wide, fact, csv_path, parquet_path)

    out = pl.read_parquet(parquet_path).sort("full_name")
    # null-year row dropped + junk dropped → McDonald(2023) + Doe(2022) = 2 rows.
    assert out.height == 2, out

    mary = out.filter(pl.col("full_name") == "Mary Lou McDonald")
    assert mary.height == 1  # roster full_name used for the matched member
    assert mary["sitting_days"][0] == 10  # max(10, 8)
    assert mary["total_days"][0] == 12
    assert mary["party_name"][0] == "SF"  # metadata left-joined from the roster

    # Former TD: retained, name reconstructed from the PDF identifier, null party.
    doe = out.filter(pl.col("full_name") == "Doe John")
    assert doe.height == 1
    assert doe["total_days"][0] == 6
    assert doe["party_name"][0] is None

    # Junk administrative row never reaches the gold table.
    assert not any("member" in n.lower() for n in out["full_name"].to_list())


def test_attendance_resolves_former_member_via_historic_roster(tmp_path):
    """A TD who sat in a given year but has left the Dáil is off the CURRENT roster, so the
    current-spine join leaves unique_member_code empty. The historic-roster fallback resolves
    them (it carries former members + their codes), while current members are unaffected. This
    pins the fix for the 117 unmatched 2023-24 rows (Coveney/Howlin/… were former members)."""
    members_csv = _members_csv(
        tmp_path, [_member_row("Mary Lou", "McDonald", "MaryLou.McDonald.D.2011-02-25", party="SF")]
    )
    members_wide, _ = _build_members_and_master(members_csv)

    # historic roster: a FORMER TD (Coveney) absent from the current roster, with a real code.
    historic_csv = tmp_path / "historic.csv"
    pl.DataFrame(
        {
            "unique_member_code": ["Simon.Coveney.D.2011-02-25"],
            "first_name": ["Simon"],
            "last_name": ["Coveney"],
            "constituency_name": ["Cork South-Central"],
            "full_name": ["Simon Coveney"],
            "party": ["FG"],
            "ministerial_office": [None],
        }
    ).write_csv(historic_csv)

    fact = tmp_path / "fact.csv"
    pl.DataFrame(
        {
            "first_name": ["Mary Lou", "Simon"],
            "last_name": ["McDonald", "Coveney"],
            "identifier": ["McDonald_Mary Lou", "Coveney_Simon"],
            "year": [2023, 2023],
            "sitting_days_count": [10, 7],
            "other_days_count": [2, 1],
        }
    ).write_csv(fact)

    parquet_path = tmp_path / "aby.parquet"
    _build_attendance_by_year(members_wide, fact, tmp_path / "aby.csv", parquet_path, historic_csv)
    out = pl.read_parquet(parquet_path)

    # former member now resolves to the historic code + party (was empty before the fallback)
    coveney = out.filter(pl.col("full_name") == "Simon Coveney")
    assert coveney.height == 1
    assert coveney["unique_member_code"][0] == "Simon.Coveney.D.2011-02-25"
    assert coveney["party_name"][0] == "FG"
    # current member unaffected: keeps its current-roster code, not overwritten by the fallback
    assert (
        out.filter(pl.col("full_name") == "Mary Lou McDonald")["unique_member_code"][0]
        == "MaryLou.McDonald.D.2011-02-25"
    )


# ── _build_vote_history ──────────────────────────────────────────────────────


def test_vote_history_joins_metadata_dedups_and_drops_join_key(tmp_path):
    votes_csv = tmp_path / "pretty_votes.csv"
    pl.DataFrame(
        {
            "unique_member_code": ["X", "X", "X"],
            "date": ["2024-01-01", "2024-01-01", "2024-02-02"],
            "vote_id": [1, 1, 2],  # first two are a true duplicate row
            "outcome": ["Tá", "Tá", "Níl"],
        }
    ).write_csv(votes_csv)

    enriched_csv = tmp_path / "enriched_td_attendance.csv"
    pl.DataFrame(
        {
            "join_key": ["k"],
            "unique_member_code": ["X"],
            "year_elected": ["2011"],
            "last_name": ["McDonald"],
            "dail_term": ["34"],
            "dail_number": [34],
            "full_name": ["Mary Lou McDonald"],
            "first_name": ["Mary Lou"],
            "party": ["SF"],
            "constituency_name": ["Dublin Central"],
        }
    ).write_csv(enriched_csv)

    out_csv = tmp_path / "vote_history.csv"
    out_parquet = tmp_path / "vote_history.parquet"
    _build_vote_history(votes_csv, enriched_csv, out_csv, out_parquet)

    out = pl.read_parquet(out_parquet)
    # dedup on (code, date, vote_id) collapses the duplicate → 2 rows.
    assert out.height == 2
    # member metadata joined on; internal join_key dropped.
    assert "join_key" not in out.columns
    assert set(out["full_name"].unique()) == {"Mary Lou McDonald"}
    assert out["party"][0] == "SF"


# ── _build_payment_rankings ──────────────────────────────────────────────────


def test_payment_rankings_filters_pre_2020_and_ranks_desc(tmp_path):
    payments = tmp_path / "payments.parquet"
    pl.DataFrame(
        {
            "member_name": ["Mary McDonald", "Mary McDonald", "John Doe", "Ghost Member"],
            "date_paid": pl.Series(["2021-05-01", "2019-12-31", "2022-01-01", "2023-01-01"]).str.to_date(),
            "amount": [100.0, 9999.0, 250.0, 50.0],
        }
    ).write_parquet(payments)

    # master only knows McDonald + Doe (Ghost has no master row → inner-join drop).
    def jk(name):
        return normalise_df_td_name(pl.DataFrame({"n": [name]}), "n")["join_key"][0]

    master = pl.DataFrame(
        {
            "join_key": [jk("Mary McDonald"), jk("John Doe")],
            "identifier": ["X", "Y"],
            "party": ["SF", "FF"],
            "constituency": ["Dublin", "Cork"],
        }
    )

    out_csv = tmp_path / "rankings.csv"
    out_parquet = tmp_path / "rankings.parquet"
    _build_payment_rankings(master, payments, out_csv, out_parquet)

    out = pl.read_parquet(out_parquet)
    # Ghost dropped (inner join); 2 ranked TDs.
    assert out.height == 2
    # pre-2020 €9999 excluded → McDonald total is 100, Doe 250 → Doe ranks #1.
    by_id = {r["identifier"]: r for r in out.iter_rows(named=True)}
    assert by_id["X"]["total_amount_paid_since_2020"] == 100.0
    assert by_id["Y"]["total_amount_paid_since_2020"] == 250.0
    assert by_id["Y"]["rank"] == 1
    assert by_id["X"]["rank"] == 2


def test_payment_rankings_excludes_null_amounts(tmp_path):
    payments = tmp_path / "payments.parquet"
    pl.DataFrame(
        {
            "member_name": ["Mary McDonald", "Mary McDonald"],
            "date_paid": pl.Series(["2021-05-01", "2022-06-01"]).str.to_date(),
            "amount": [None, 75.0],
        },
        schema={"member_name": pl.Utf8, "date_paid": pl.Date, "amount": pl.Float64},
    ).write_parquet(payments)

    def jk(name):
        return normalise_df_td_name(pl.DataFrame({"n": [name]}), "n")["join_key"][0]

    master = pl.DataFrame(
        {"join_key": [jk("Mary McDonald")], "identifier": ["X"], "party": ["SF"], "constituency": ["Dublin"]}
    )
    out_csv = tmp_path / "r.csv"
    out_parquet = tmp_path / "r.parquet"
    _build_payment_rankings(master, payments, out_csv, out_parquet)

    out = pl.read_parquet(out_parquet)
    # null amount dropped → only the €75 row contributes.
    assert out.height == 1
    assert out["total_amount_paid_since_2020"][0] == 75.0
