"""Unit tests for the pure DataFrame-transform functions in
``extractors/judiciary_bench_extract.py``.

These are the PRE-PROMOTION cleanup primitives that turn the validated judiciary
sandbox into the committed gold tables. The existing suites cover other layers:
  * ``test/dail_tracker_core/test_judiciary_bench.py`` exercises the *SQL views*
    (v_judiciary_*) over the live gold parquet — it never calls the build_* funcs;
  * ``test/extractors/test_judiciary_privacy.py`` locks ``legal_diary_extract.py``
    (a DIFFERENT module — case anonymisation), not this extractor.
So the pure transforms here (name keying, the appointment/nomination/courts builders)
were previously UNTESTED. These tests construct minimal synthetic inputs with exactly
the columns each function reads and assert on shape / columns / key rows, with expected
values derived from the code logic (COURT_RANK, SALARY_BY_COURT, the alias/honorific
strip rules), not guesses.

Skipped deliberately:
  * ``main`` — IO (reads sandbox parquet, writes gold + coverage json);
  * ``build_courts_waiting`` — reads ``data/_meta/courts_waiting_context.csv`` from
    disk (not a pure transform) and raises on a curated-CSV drift guard.

Run:  python -m pytest test/extractors/test_judiciary_bench_extract.py -q
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "extractors"))

from judiciary_bench_extract import (  # noqa: E402
    COURT_RANK,
    SALARY_BY_COURT,
    build_appointments,
    build_bench,
    build_courthouses,
    build_courts_clearance,
    build_nominations,
    normalise_key,
)


# ───────────────────────────────────────────────────────── normalise_key
def test_normalise_key_strips_accents_and_honorifics():
    # NFD accent fold + honorific ('justice') strip + lowercase. The apostrophe becomes a
    # space, so "O'Leary" -> tokens ['o','leary']; the single-letter 'o' is dropped by the
    # len(t) > 1 filter, leaving just 'leary' (the code drops bare initials, including 'O').
    assert normalise_key("Ms Justice Síofra O'Leary", {}) == "siofra leary"


def test_normalise_key_casing_and_punctuation_collapse():
    # casing + punctuation must collapse to the same key regardless of input form.
    # "O'Shea": apostrophe -> space, single-letter 'o' dropped -> 'shea'.
    assert normalise_key("BRIAN O'SHEA", {}) == normalise_key("Brian O'Shea", {}) == "brian shea"


def test_normalise_key_drops_initials_and_notice_suffixes():
    # bare middle initial ('G.') and Irish notice suffix (', AS') are stop tokens — dropping
    # both lets the three published spellings collapse to ONE key (per the module docstring).
    base = normalise_key("Michael MacGrath", {})
    assert base == "michael macgrath"
    assert normalise_key("Michael G. MacGrath", {}) == base
    assert normalise_key("Michael MacGrath, AS", {}) == base


def test_normalise_key_applies_alias_map_per_token():
    # alias map is applied per token (Liz -> Elizabeth, Gabett -> Gabbett typo fixes).
    aliases = {"liz": "elizabeth", "gabett": "gabbett"}
    assert normalise_key("Liz Gabett", aliases) == "elizabeth gabbett"
    # without aliases the raw tokens survive
    assert normalise_key("Liz Gabett", {}) == "liz gabett"


def test_normalise_key_handles_none_and_nan():
    # null-ish input -> empty key (used as a join key, must not blow up).
    assert normalise_key(None, {}) == ""
    assert normalise_key(float("nan"), {}) == ""


# ───────────────────────────────────────────────────────── build_appointments
def _appt_inputs():
    """Two real-court notices + one junk (is_real_court=False) + a multi-name notice.
    The join supplies current_court/status keyed on (appointee, issue_date)."""
    spine = pd.DataFrame(
        {
            "is_real_court": [True, True, False],
            # second notice has two appointees joined by ';' (and a 'None' to be skipped)
            "appointee": ["Brian O'Shea", "Anne Field; Carl Stone; none", "Junk Body"],
            "issue_date": ["2020-01-15", "2021-06-10", "2019-03-03"],
            "court": ["High Court", "Circuit Court", "Courts"],
            "role": ["Ordinary Judge", "Judge", "n/a"],
            "appointing_authority": ["President", "President", "n/a"],
            "notice_ref": ["IR001", None, "IRX"],
            "iris_source_pdf": ["IR150120.pdf", None, None],
        }
    )
    join = pd.DataFrame(
        {
            "appointee": ["Brian O'Shea"],
            "appointed_date": ["2020-01-15"],
            "current_court": ["Supreme Court"],  # elevation High -> Supreme
            "status": ["elevated"],
        }
    )
    return spine, join


def test_build_appointments_drops_junk_and_splits_names():
    spine, join = _appt_inputs()
    ev = build_appointments(spine, join, {})
    # junk (is_real_court=False) dropped; the 'none' name skipped; ';' split -> 2 names.
    # so 1 (O'Shea) + 2 (Field, Stone) = 3 event rows.
    assert len(ev) == 3
    assert "Junk Body" not in set(ev["appointee"])
    assert set(ev["appointee"]) == {"Brian O'Shea", "Anne Field", "Carl Stone"}
    # judge_key is the normalised name (single-letter 'o' from O'Shea dropped)
    assert ev.loc[ev["appointee"] == "Brian O'Shea", "judge_key"].iloc[0] == "brian shea"


def test_build_appointments_unmatched_status_filled():
    spine, join = _appt_inputs()
    ev = build_appointments(spine, join, {})
    # only O'Shea is in the join; the others get status='unmatched'.
    unmatched = ev[ev["appointee"].isin(["Anne Field", "Carl Stone"])]
    assert (unmatched["status"] == "unmatched").all()
    assert unmatched["is_elevation"].eq(False).all()


def test_build_appointments_elevation_to_senior_not_flagged():
    spine, join = _appt_inputs()
    ev = build_appointments(spine, join, {})
    oshea = ev[ev["appointee"] == "Brian O'Shea"].iloc[0]
    # High Court (rank 3) -> Supreme Court (rank 1): a legitimate (more senior) elevation.
    assert oshea["is_elevation"] is True or bool(oshea["is_elevation"]) is True
    assert oshea["elevated_to"] == "Supreme Court"
    # current rank (1) < appointed rank (3) -> NOT impossible -> no manual review.
    assert bool(oshea["requires_manual_review"]) is False


def test_build_appointments_impossible_junior_elevation_flagged():
    # an 'elevation' to a MORE JUNIOR court is a name-collision artefact -> flag it.
    spine = pd.DataFrame(
        {
            "is_real_court": [True],
            "appointee": ["Pat Collision"],
            "issue_date": ["2018-02-02"],
            "court": ["High Court"],  # appointed rank 3
            "role": ["Ordinary Judge"],
            "appointing_authority": ["President"],
            "notice_ref": ["IR9"],
            "iris_source_pdf": [None],
        }
    )
    join = pd.DataFrame(
        {
            "appointee": ["Pat Collision"],
            "appointed_date": ["2018-02-02"],
            "current_court": ["District Court"],  # rank 5 -> more junior, impossible
            "status": ["elevated"],
        }
    )
    ev = build_appointments(spine, join, {})
    row = ev.iloc[0]
    assert bool(row["is_elevation"]) is True
    assert bool(row["requires_manual_review"]) is True


# ───────────────────────────────────────────────────────── build_nominations
def test_build_nominations_keys_and_per_row_search_url():
    nom = pd.DataFrame(
        {
            "announce_date": ["2025-02-01", "2025-01-10"],
            "nominee": ["Síofra O'Leary", "Mary Quinn"],
            "target_court": ["Supreme Court", "High Court"],
            "prior_career": ["Barrister", "Solicitor"],
            "vacancy_cause": ["retirement", "new seat"],
            "predecessor": ["X", None],
        }
    )
    out = build_nominations(nom, {})
    # judge_key is normalised; every row carries a gov.ie SEARCH url scoped to nominee+court.
    assert set(out["judge_key"]) == {"siofra leary", "mary quinn"}
    # sorted by (announce_date, target_court, nominee): 2025-01-10 row comes first.
    assert out.iloc[0]["nominee"] == "Mary Quinn"
    assert (out["source_name"] == "gov.ie nomination announcement").all()
    assert out["source_url"].str.startswith("https://www.gov.ie/en/search/?q=").all()
    # the per-row url embeds both nominee and court (url-encoded)
    mary = out[out["nominee"] == "Mary Quinn"].iloc[0]
    assert "Mary+Quinn" in mary["source_url"]
    assert "High+Court" in mary["source_url"]
    # exact output column contract
    assert list(out.columns) == [
        "announce_date",
        "nominee",
        "judge_key",
        "target_court",
        "prior_career",
        "vacancy_cause",
        "predecessor",
        "source_name",
        "source_url",
    ]


# ───────────────────────────────────────────────────────── build_courts_clearance
def test_build_courts_clearance_canonicalises_casing_and_typing():
    clr = pd.DataFrame(
        {
            # 'Court Of Appeal' (criminal table casing) must collapse to 'Court of Appeal'.
            "JURISDICTION": ["Court Of Appeal", "Court of Appeal", "High Court"],
            "AREA_OF_LAW": ["Criminal", "Civil", "Civil"],
            "YEAR": ["2024", "2024", "2023"],  # string year -> cast to int
            "CATEGORY": ["Appeals", "Appeals", "Judicial Review"],
            "INCOMING": ["100", "50", "200"],
            "RESOLVED": [90, 60, 180],
        }
    )
    out = build_courts_clearance(clr)
    # the two Court-of-Appeal rows now share one canonical jurisdiction spelling.
    assert set(out["jurisdiction"]) == {"Court of Appeal", "High Court"}
    assert (out[out["area_of_law"] == "Criminal"]["jurisdiction"] == "Court of Appeal").all()
    # YEAR / INCOMING cast to int; counts untouched.
    assert out["year"].dtype.kind == "i"
    assert out["incoming"].dtype.kind == "i"
    assert out[out["area_of_law"] == "Criminal"]["incoming"].iloc[0] == 100
    # provenance stamped on every row; no clearance_pct computed here (firewall).
    assert (out["source_name"] == "Courts Service annual statistics").all()
    assert "clearance_pct" not in out.columns
    # sorted by year asc -> the 2023 High Court row sorts first.
    assert out.iloc[0]["year"] == 2023


# ───────────────────────────────────────────────────────── build_courthouses
def test_build_courthouses_filters_active_and_requires_geocode():
    ch = pd.DataFrame(
        {
            "active_status": ["active", "closed", "active"],
            "court_house": ["Zeta Courthouse", "Old Courthouse", "Alpha Courthouse"],
            "court_house_address": ["Z St", "O St", "A St"],
            "court_house_eircode": ["Z00 0000", "O00 0000", "A00 0000"],
            "region": ["R1", "R2", "R1"],
            "county": ["C1", "C2", "C1"],
            "circuit": ["Cir1", "Cir2", "Cir1"],
            "latitude": [53.1, 53.2, None],  # active-but-ungeocoded must be dropped
            "longitude": [-6.1, -6.2, -6.3],
        }
    )
    out = build_courthouses(ch)
    # 'closed' dropped; the active-but-null-lat row dropped -> only Zeta survives.
    assert out["court_house"].tolist() == ["Zeta Courthouse"]
    assert out["latitude"].notna().all()
    assert out["longitude"].notna().all()
    # renamed/curated columns + provenance present
    assert "address" in out.columns and "eircode" in out.columns
    assert (out["source_name"] == "Courts Service court-office register").all()


def test_build_courthouses_sorted_by_name():
    ch = pd.DataFrame(
        {
            "active_status": ["active", "active"],
            "court_house": ["Zeta Courthouse", "Alpha Courthouse"],
            "court_house_address": ["Z St", "A St"],
            "court_house_eircode": ["Z00 0000", "A00 0000"],
            "region": ["R", "R"],
            "county": ["C", "C"],
            "circuit": ["Cir", "Cir"],
            "latitude": [53.1, 53.2],
            "longitude": [-6.1, -6.2],
        }
    )
    out = build_courthouses(ch)
    assert out["court_house"].tolist() == ["Alpha Courthouse", "Zeta Courthouse"]


# ───────────────────────────────────────────────────────── build_bench
def _bench_inputs():
    """A roster with an ex-officio cross-listing to exercise the seat dedup + salary
    suppression, plus a matching appointment spine and an HC assignment."""
    roster = pd.DataFrame(
        {
            "judge_name": [
                "Brian O'Shea",  # Supreme Court, ex-officio cross-listing (President)
                "Brian O'Shea",  # ALSO listed on High Court (same person, 2 seats)
                "Mary Quinn",  # plain High Court ordinary judge
            ],
            "court": ["Supreme Court", "High Court", "High Court"],
            "is_ex_officio_or_multi": [True, False, False],
        }
    )
    appts = build_appointments(*_appt_inputs(), {})  # O'Shea has a spine; Field/Stone too
    hc = pd.DataFrame(
        {
            "judge": ["Mary Quinn"],
            "assignment": ["Commercial List"],
            "term": ["Hilary 2026"],
        }
    )
    return roster, appts, hc


def test_build_bench_resolves_ex_officio_to_one_seat_per_judge():
    roster, appts, hc = _bench_inputs()
    bench = build_bench(roster, appts, set(), hc, {})
    # one row per judge_key (the two O'Shea rows collapse).
    assert bench["judge_key"].is_unique
    assert set(bench["judge_key"]) == {"brian shea", "mary quinn"}
    oshea = bench[bench["judge_key"] == "brian shea"].iloc[0]
    # seat_count keeps the raw count of listings (2) even after dedup.
    assert int(oshea["seat_count"]) == 2
    # dedup keeps the substantive (non ex-officio) High Court seat, not the Supreme ex-officio.
    assert oshea["court"] == "High Court"
    assert bool(oshea["is_ex_officio_or_multi"]) is False


def test_build_bench_salary_band_by_court_and_provenance():
    roster, appts, hc = _bench_inputs()
    bench = build_bench(roster, appts, set(), hc, {})
    mary = bench[bench["judge_key"] == "mary quinn"].iloc[0]
    # ordinary High Court band straight from SALARY_BY_COURT.
    assert mary["salary_band_eur"] == SALARY_BY_COURT["High Court"]
    assert mary["salary_office"] == "Ordinary Judge, High Court"
    # HC specialist-list assignment joined by key.
    assert mary["assignment"] == "Commercial List"
    assert mary["assignment_term"] == "Hilary 2026"
    # current_court mirrors the roster court; roster provenance stamped.
    assert mary["current_court"] == "High Court"
    assert mary["source_url"]  # non-empty roster URL


def test_build_bench_ex_officio_salary_suppressed():
    # a purely ex-officio/president seat must have its salary band suppressed (premium
    # can't be attributed to a named person from the roster alone).
    roster = pd.DataFrame(
        {
            "judge_name": ["Pres Ident"],
            "court": ["Supreme Court"],
            "is_ex_officio_or_multi": [True],
        }
    )
    # an appointment spine for a DIFFERENT judge -> the President has no spine row.
    other_appts = build_appointments(
        pd.DataFrame(
            {
                "is_real_court": [True],
                "appointee": ["Some Other Judge"],
                "issue_date": ["2000-01-01"],
                "court": ["High Court"],
                "role": ["Ordinary Judge"],
                "appointing_authority": ["President"],
                "notice_ref": [None],
                "iris_source_pdf": [None],
            }
        ),
        pd.DataFrame({"appointee": [], "appointed_date": [], "current_court": [], "status": []}),
        {},
    )
    hc = pd.DataFrame({"judge": [], "assignment": [], "term": []})
    bench = build_bench(roster, other_appts, set(), hc, {})
    bench = bench[bench["judge_key"] == normalise_key("Pres Ident", {})]
    row = bench.iloc[0]
    assert pd.isna(row["salary_band_eur"]) or row["salary_band_eur"] is None
    assert "ex-officio" in str(row["salary_office"]).lower()
    assert bool(row["has_spine"]) is False


def test_build_bench_court_rank_sort_and_columns():
    roster, appts, hc = _bench_inputs()
    bench = build_bench(roster, appts, set(), hc, {})
    # output ordered by (court_rank, judge_name): both judges resolve to High Court (rank 3).
    assert (bench["court_rank"] == COURT_RANK["High Court"]).all()
    # the documented gold contract columns are all present.
    for col in (
        "judge_key",
        "judge_name",
        "court",
        "current_court",
        "seat_count",
        "salary_band_eur",
        "has_spine",
        "requires_manual_review",
        "source_url",
    ):
        assert col in bench.columns
