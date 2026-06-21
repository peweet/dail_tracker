"""Build + invariant tests for the ministerial-diary SQL views.

These views (sql_views/diary/ministerial_diary_*.sql) are the logic-firewall layer:
all joins / flags / the state-body split / corroboration live here, read by the page
via data_access.ministerial_diary_data. They read the COMMITTED gold parquet produced
by extractors/diary_promote_gold.py, so this runs against real data (no fixture).

Pins the two invariants that matter for honest presentation:
  * `corroborated` is a POSITIVE-only flag (true ⇒ the org lobbied + met the same
    minister) — we never expose the unreliable "never lobbied" negative;
  * the engagements drill-down excludes travel/media (an org on a flight line is not
    a meeting with that org — the Aer-Lingus-flight contamination).
"""

from __future__ import annotations

import pytest

from dail_tracker_core.db import connect_with_views

# Runs in CI's `sql-contracts` job (executes registered views against the COMMITTED
# gold parquet under data/gold/). Same marker convention as test_sql_views.py — the
# views read gold directly, so no silver fixtures are needed.
pytestmark = pytest.mark.sql


@pytest.fixture(scope="module")
def conn():
    c = connect_with_views(["ministerial_diary_*.sql"], swallow_errors=False)
    yield c
    c.close()


def test_all_views_build(conn) -> None:
    views = {
        r[0]
        for r in conn.execute("select view_name from duckdb_views() where view_name like 'v_ministerial%'").fetchall()
    }
    assert views == {
        "v_ministerial_diary_org_overlap",
        "v_ministerial_diary_engagements",
        "v_ministerial_diary_meetings",
        "v_ministerial_diary_company_influence",
    }


def test_overlap_has_expected_columns(conn) -> None:
    cols = {d[0] for d in conn.execute("select * from v_ministerial_diary_org_overlap limit 0").description}
    assert {
        "organisation",
        "sector",
        "is_state_body",
        "meetings",
        "ministers_met",
        "ministers_lobbied_and_met",
        "corroborated",
        "first_meeting",
        "last_meeting",
    } <= cols
    # the unreliable negative flag must NOT be exposed
    assert "access_without_return" not in cols


def test_corroborated_is_positive_only(conn) -> None:
    # every corroborated row genuinely has a lobbied-and-met minister (no false trues)
    bad = conn.execute(
        "select count(*) from v_ministerial_diary_org_overlap where corroborated and ministers_lobbied_and_met = 0"
    ).fetchone()[0]
    assert bad == 0
    assert conn.execute("select count(*) from v_ministerial_diary_org_overlap where corroborated").fetchone()[0] > 0


def test_state_body_split_partitions(conn) -> None:
    n_state = conn.execute("select count(*) from v_ministerial_diary_org_overlap where is_state_body").fetchone()[0]
    n_outside = conn.execute("select count(*) from v_ministerial_diary_org_overlap where not is_state_body").fetchone()[
        0
    ]
    assert n_state > 0 and n_outside > 0  # both buckets populated (page leads with outside)


def test_engagements_excludes_travel_and_media(conn) -> None:
    leaked = conn.execute(
        "select count(*) from v_ministerial_diary_engagements where entry_class in ('travel','media')"
    ).fetchone()[0]
    assert leaked == 0


def test_education_landed_and_fully_attributed(conn) -> None:
    # Education was promoted to gold in session 4 (WAF fingerprint fix + 2nd grid parser +
    # orientation re-OCR → 14,567 entries, 2016-2025). Guard that it stays in gold AND that
    # every Education engagement that reaches the view is attributed to a minister (the lineage
    # date-rules: O'Sullivan/Bruton/McHugh/Foley/McEntee + MoS Moynihan). A regression in the
    # rules or the merge would drop the dept or null its minister.
    n_rows, n_min, n_null = conn.execute(
        "select count(*), count(distinct minister), count(*) filter (where minister is null) "
        "from v_ministerial_diary_engagements where department = 'EDUCATION'"
    ).fetchone()
    assert n_rows > 0  # Education present in gold
    assert n_min >= 5  # full lineage represented (not collapsed to one stale name)
    assert n_null == 0  # no unattributed Education engagement reaches the page


def test_minister_resolution_from_filename() -> None:
    # the promotion-time fix: canonical minister from the source filename, covering the
    # cases the old single-token regex missed (multi-token names, "…-Calendar", possessives)
    from datetime import date

    from extractors._diary_minister import minister_from_filename, resolve_minister

    # multi-token names + apostrophes (the 1,968-Housing-meeting bug)
    assert minister_from_filename("minister-darragh-obriens-diary-may-to-june-2022.pdf") == "O'Brien"
    assert minister_from_filename("minister-of-state-peter-burkes-diary-2021.pdf") == "Burke"
    # "…-Calendar" files (not the literal "diary")
    assert minister_from_filename("Minister_Brownes_Calendar_-_June_2025.pdf") == "Browne"
    # split possessive token "…-s-diary"
    assert minister_from_filename("minister-breen-s-diary-q1-2018.pdf") == "Breen"
    # genuine trailing-'s' surnames must NOT be truncated
    assert minister_from_filename("minister-cummins-diary-may-2025.pdf") == "Cummins"
    assert minister_from_filename("Ministers_Diary_-_October_2022.pdf") is None  # no surname in name

    # dept+date fallback for name-less generic files (verified via who_was_minister)
    assert resolve_minister("ministers-diary-may-december-2021.pdf", "HEALTH", date(2022, 1, 1)) == "Donnelly"
    assert resolve_minister("April_2025.pdf", "EDUCATION", date(2025, 4, 1)) == "McEntee"
    assert resolve_minister("Q4_2025_MoS_Diary_2.pdf", "DCCS", date(2025, 11, 1)) is None  # un-named MoS — stays None
