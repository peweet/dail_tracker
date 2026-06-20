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


def test_both_views_build(conn) -> None:
    views = {
        r[0]
        for r in conn.execute("select view_name from duckdb_views() where view_name like 'v_ministerial%'").fetchall()
    }
    assert views == {"v_ministerial_diary_org_overlap", "v_ministerial_diary_engagements"}


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


def test_minister_defragmentation() -> None:
    # the promotion-time fix that merges the Ryans/Ryan filename-guess split
    from extractors.diary_promote_gold import minister_display

    assert minister_display("Ryans") == "Ryan"
    assert minister_display("Martins") == "Martin"
    assert minister_display("Burke") == "Burke"  # 'e' ending — untouched
    assert minister_display("Ross") == "Ross"  # short — untouched
    assert minister_display(None) is None
