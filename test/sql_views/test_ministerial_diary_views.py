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

from dail_tracker_core.db import PROJECT_ROOT, connect_with_views

# CWD-independent path for raw read_csv() in tests (views get theirs absolutized at registration)
_SUPPLEMENT_CSV = (PROJECT_ROOT / "data/_meta/diary_state_bodies_supplement.csv").as_posix()

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
        # period-grain rollups (2026-07 logic-firewall graduation of the page's
        # groupby faceting — see the ministerial_diary_zz_*.sql headers)
        "v_ministerial_diary_minister_period",
        "v_ministerial_diary_dept_period",
        "v_ministerial_diary_dept_minister_period",
        "v_ministerial_diary_top_orgs",
    }


# ── period-grain rollups (the page's Year/Month filter as a WHERE clause) ────────────────


def test_minister_period_all_grain_matches_meetings(conn) -> None:
    # the 'all' grain of the rollup must agree exactly with a direct count over the
    # meetings view (per named minister) — a drifted GROUPING SETS cell would lie on cards
    bad = conn.execute(
        "WITH direct AS ("
        "  SELECT minister, COUNT(*) AS n, MIN(entry_date) AS first_m, MAX(entry_date) AS last_m"
        "  FROM v_ministerial_diary_meetings WHERE minister IS NOT NULL AND minister <> ''"
        "  GROUP BY minister)"
        " SELECT count(*) FROM direct d"
        " JOIN v_ministerial_diary_minister_period p ON p.minister = d.minister AND p.period_grain = 'all'"
        " WHERE p.meetings <> d.n OR p.first_meeting <> d.first_m OR p.last_meeting <> d.last_m"
    ).fetchone()[0]
    assert bad == 0


def test_minister_period_grains_are_disjoint_and_complete(conn) -> None:
    # year cells sum back to the all cell per minister (no double counting across grains)
    bad = conn.execute(
        "WITH yr AS ("
        "  SELECT minister, SUM(meetings) AS n FROM v_ministerial_diary_minister_period"
        "  WHERE period_grain = 'year' GROUP BY minister)"
        " SELECT count(*) FROM yr"
        " JOIN v_ministerial_diary_minister_period p ON p.minister = yr.minister AND p.period_grain = 'all'"
        " WHERE p.meetings <> yr.n"
    ).fetchone()[0]
    assert bad == 0
    # period columns are null exactly per the grain contract
    n_bad = conn.execute(
        "SELECT count(*) FROM v_ministerial_diary_minister_period WHERE"
        " (period_grain = 'all' AND (period_year IS NOT NULL OR period_month IS NOT NULL))"
        " OR (period_grain = 'year' AND (period_year IS NULL OR period_month IS NOT NULL))"
        " OR (period_grain = 'month' AND (period_year IS NULL OR period_month IS NULL))"
    ).fetchone()[0]
    assert n_bad == 0


def test_minister_period_depts_is_sorted_portfolio(conn) -> None:
    # multi-portfolio ministers carry every department they logged under (e.g. Ryan
    # held Transport + Climate) — comma-joined, sorted, no blanks
    row = conn.execute(
        "SELECT depts FROM v_ministerial_diary_minister_period WHERE minister = 'Ryan' AND period_grain = 'all'"
    ).fetchone()
    if row is not None:  # a future gold re-cut may rename; the shape contract below still holds
        depts = row[0].split(",")
        assert len(depts) >= 2 and depts == sorted(depts)
    n_blank = conn.execute(
        "SELECT count(*) FROM v_ministerial_diary_minister_period WHERE depts IS NULL OR depts = ''"
    ).fetchone()[0]
    assert n_blank == 0


def test_dept_period_ministers_counts_named_only(conn) -> None:
    # ministers = DISTINCT named ministers; unattributed rows may add meetings but never ministers
    bad = conn.execute(
        "WITH direct AS ("
        "  SELECT department, COUNT(*) AS n,"
        "         COUNT(DISTINCT minister) FILTER (WHERE minister IS NOT NULL AND minister <> '') AS m"
        "  FROM v_ministerial_diary_meetings WHERE department IS NOT NULL AND department <> ''"
        "  GROUP BY department)"
        " SELECT count(*) FROM direct d"
        " JOIN v_ministerial_diary_dept_period p ON p.department = d.department AND p.period_grain = 'all'"
        " WHERE p.meetings <> d.n OR p.ministers <> d.m"
    ).fetchone()[0]
    assert bad == 0


def test_dept_minister_period_slices_the_minister_rollup(conn) -> None:
    # a minister's per-dept cells sum to their minister_period total (same grain), and the
    # attached portfolio matches the minister rollup's depts for the same period
    bad = conn.execute(
        "WITH per AS ("
        "  SELECT minister, SUM(meetings) AS n FROM v_ministerial_diary_dept_minister_period"
        "  WHERE period_grain = 'all' GROUP BY minister)"
        " SELECT count(*) FROM per"
        " JOIN v_ministerial_diary_minister_period p ON p.minister = per.minister AND p.period_grain = 'all'"
        " WHERE p.meetings <> per.n"
    ).fetchone()[0]
    assert bad == 0
    drift = conn.execute(
        "SELECT count(*) FROM v_ministerial_diary_dept_minister_period dm"
        " JOIN v_ministerial_diary_minister_period mp"
        "   ON mp.minister = dm.minister AND mp.period_grain = dm.period_grain"
        "  AND mp.period_year IS NOT DISTINCT FROM dm.period_year"
        "  AND mp.period_month IS NOT DISTINCT FROM dm.period_month"
        " WHERE dm.depts <> mp.depts"
    ).fetchone()[0]
    assert drift == 0


def test_top_orgs_rank_is_dense_and_ordered(conn) -> None:
    # rnk is a proper 1..n ranking within each (entity_kind, entity, period) partition:
    # starts at 1, no gaps, and n never increases as rnk increases
    n_bad = conn.execute(
        "WITH w AS ("
        "  SELECT entity_kind, entity, period_grain, period_year, period_month, n, rnk,"
        "         lag(rnk)  OVER win AS prev_rnk,"
        "         lag(n)    OVER win AS prev_n"
        "  FROM v_ministerial_diary_top_orgs"
        "  WINDOW win AS ("
        "    PARTITION BY entity_kind, entity, period_grain, period_year, period_month ORDER BY rnk))"
        " SELECT count(*) FROM w WHERE"
        " (prev_rnk IS NULL AND rnk <> 1) OR (prev_rnk IS NOT NULL AND rnk <> prev_rnk + 1)"
        " OR (prev_n IS NOT NULL AND n > prev_n)"
    ).fetchone()[0]
    assert n_bad == 0
    kinds = {r[0] for r in conn.execute("SELECT DISTINCT entity_kind FROM v_ministerial_diary_top_orgs").fetchall()}
    assert kinds == {"minister", "department"}


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


def test_state_body_supplement_overrides_gold(conn) -> None:
    # The curated data/_meta/diary_state_bodies_supplement.csv corrects gold's sector-derived
    # is_state_body for verifiable statutory / State-owned bodies (2026-07-13 MCP-sweep DQ #3:
    # LDA, NCSE, National Concert Hall, Heritage Council, Dublin Port Company, Arts Council read
    # as outside interests). Every supplement org that reaches the view must be flagged state.
    n_bad = conn.execute(
        "SELECT count(*) FROM v_ministerial_diary_org_overlap o "
        f"JOIN read_csv('{_SUPPLEMENT_CSV}', header = true, AUTO_DETECT = true) s "
        "ON lower(trim(o.organisation)) = lower(trim(s.organisation)) WHERE NOT o.is_state_body"
    ).fetchone()[0]
    assert n_bad == 0
    # the override actually bit on the six the sweep reproduced (present in current gold)
    for org in ("Land Development Agency", "Arts Council", "Dublin Port Company"):
        row = conn.execute(
            "SELECT is_state_body FROM v_ministerial_diary_org_overlap WHERE organisation = ?", [org]
        ).fetchone()
        if row is not None:  # a future gold re-cut may rename; the invariant above still holds
            assert row[0] is True, org


def test_company_influence_quarantines_supplement_state_bodies(conn) -> None:
    # v_ministerial_diary_company_influence documents "state/semi-state bodies are excluded" —
    # the curated supplement is WHERE-quarantined at the view so a body the upstream sector tag
    # missed (e.g. Waterways Ireland) can't be served as an outside company paid public money.
    n = conn.execute(
        "SELECT count(*) FROM v_ministerial_diary_company_influence c "
        f"JOIN read_csv('{_SUPPLEMENT_CSV}', header = true, AUTO_DETECT = true) s "
        "ON lower(trim(c.organisation)) = lower(trim(s.organisation))"
    ).fetchone()[0]
    assert n == 0


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


def test_ocr_batch_departments_landed(conn) -> None:
    # The 2026-06 OCR batch landed HOUSING/DCCS scans (Outlook day-pair grids) into gold. Guard that
    # those departments carry a substantial, attributed presence (a regression in the day-grid parser,
    # the orientation re-OCR, or the merge would shrink them back toward born-digital-only counts).
    for dept in ("HOUSING", "DCCS"):
        n_rows, n_null = conn.execute(
            "select count(*), count(*) filter (where minister is null) "
            "from v_ministerial_diary_engagements where department = ?",
            [dept],
        ).fetchone()
        assert n_rows > 0, f"{dept} missing from gold"
        # the bulk is attributed; the only null-by-design source is a Minister-of-State file with no
        # surname (deliberately not given the senior date rule), so allow a small minority.
        assert n_null / n_rows < 0.2, f"{dept} attribution regressed ({n_null}/{n_rows} null)"


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
