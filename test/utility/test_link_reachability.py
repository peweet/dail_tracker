"""Cross-page link reachability — every clickable entity must resolve.

Each internal click in the app is an ``<a href>`` built by a helper in
``utility/ui/entity_links.py``. The href carries an entity id in a query param
(``?member=``, ``?vote=``, ``?bill=`` …) to a TARGET page that must turn that id
back into a displayed record. When a SOURCE page surfaces an id the target can't
resolve, the click dead-ends — e.g. the "couldn't find this member" callout, or a
blank detail view. This is exactly the regression class that let former members
(surfaced by the all-time interests index, absent from the current-roster
registry) become dead links from the "What They Own" page.

This is a DATA-CONTRACT test, not a browser crawl: for each link type it builds
the full set of ids the source view(s) make clickable and the full set the
target's resolver can return, then asserts ``source ⊆ resolvable``. Set-based, so
it checks EVERY id (no sampling) and stays fast.

The single ``api_conn()`` registers every source and resolver view used here, so
one connection serves all link types. Skips cleanly when a domain's parquet is
absent (fresh clone / CI without data), mirroring the other integration tests.

Link types covered (the full ``entity_links.py`` internal surface):
  member_profile_url   ?member=  → member-overview      (3-tier identity)   [cross-view: REAL risk]
  bill_detail_url      ?bill=    → rankings-legislation (v_legislation_detail) [cross-view: REAL risk]
  division_url         ?vote=    → rankings-votes       (v_vote_index)
  member_votes_url     ?member=  → rankings-votes       (v_td_vote_summary)
  si_detail_url        ?si=      → statutory-instruments(v_statutory_instruments)
  company_profile_url  ?supplier=→ company              (v_procurement_supplier_summary)
"""

from __future__ import annotations

import duckdb
import pytest

# (view, column) pairs. A "source" set is the union of every id a page turns into
# a link of this type; a "resolver" set is the union of every id the target page
# can display. A link type passes iff source ⊆ resolver. Views that don't exist
# on this checkout are skipped per-view, so a domain with no data never fails the
# unrelated link types.
_LINK_CONTRACTS = [
    {
        "id": "member_profile_url",
        # Sources: current-roster browse, historic-toggle browse / constituency,
        # and the What They Own interests index (the one that surfaces FORMER
        # members — member_id is the canonical code the card links on).
        "source": [
            ("v_member_registry", "unique_member_code"),
            ("v_member_registry_all", "unique_member_code"),
            ("v_member_interests_index_alltime", "member_id"),
            ("v_member_interests_index", "member_id"),
        ],
        # Resolver: member_overview._identity tries attendance → registry →
        # registry_all in turn (dail_tracker_core.queries.member_overview).
        "resolver": [
            ("v_attendance_member_year_summary", "unique_member_code"),
            ("v_member_registry", "unique_member_code"),
            ("v_member_registry_all", "unique_member_code"),
        ],
    },
    {
        "id": "bill_detail_url",
        # Cross-view: the index lists bills, the detail view resolves them.
        "source": [("v_legislation_index", "bill_id")],
        "resolver": [("v_legislation_detail", "bill_id")],
    },
    {
        "id": "division_url",
        "source": [("v_vote_index", "vote_id")],
        "resolver": [("v_vote_index", "vote_id")],
    },
    {
        "id": "member_votes_url",
        "source": [("v_td_vote_summary", "member_id")],
        "resolver": [("v_td_vote_summary", "member_id")],
    },
    {
        "id": "si_detail_url",
        "source": [("v_statutory_instruments", "si_id")],
        "resolver": [("v_statutory_instruments", "si_id")],
    },
    {
        "id": "company_profile_url",
        "source": [("v_procurement_supplier_summary", "supplier_norm")],
        "resolver": [("v_procurement_supplier_summary", "supplier_norm")],
    },
    {
        # The company dossier links each buyer name (repeat-buyers + top-buyer +
        # framework call-offs) to /rankings-procurement?authority=<contracting_authority>,
        # resolved by _render_authority_profile against the authority summary view.
        "id": "authority_profile_url",
        "source": [
            ("v_procurement_incumbency", "contracting_authority"),
            ("v_procurement_supplier_dependency", "top_authority"),
        ],
        "resolver": [("v_procurement_authority_summary", "contracting_authority")],
    },
    # ── In-page procurement drill params ────────────────────────────────────────
    # These are same-domain ?param= filters on the Procurement page (soft-nav), but
    # the link VALUES come from one view and the router resolves them against another
    # (a real cross-view risk), so they earn a contract. NOTE: ?ted_winner= is
    # deliberately NOT contracted here — winner_join_norm in v_procurement_ted_winner_history
    # is broader than v_procurement_ted_supplier_summary (~4k history rows have no summary
    # row, e.g. pre-2024 / non-company winners); the precise rendered-link source is a
    # subset, so a naive whole-history contract would false-fail. Scope + add separately.
    {
        # ?cpv= — award rows / category chips → _render_cpv_profile (cpv summary).
        "id": "cpv_drill",
        "source": [("v_procurement_awards", "cpv_code")],
        "resolver": [("v_procurement_cpv_summary", "cpv_code")],
    },
    {
        # ?paid_supplier= — payment rows → _render_payments_supplier_profile (supplier summary).
        "id": "paid_supplier_drill",
        "source": [("v_procurement_payments", "supplier_normalised")],
        "resolver": [("v_procurement_payments_supplier_summary", "supplier_normalised")],
    },
    {
        # ?paid_publisher= — payment rows → _render_payments_publisher_profile (publisher summary).
        "id": "paid_publisher_drill",
        "source": [("v_procurement_payments", "publisher_name")],
        "resolver": [("v_procurement_payments_publisher_summary", "publisher_name")],
    },
]


@pytest.fixture(scope="module")
def conn():
    """One connection with every view the app's links source from / resolve to."""
    try:
        from dail_tracker_core.connections import api_conn
    except Exception as exc:  # noqa: BLE001 — config import side-effects
        pytest.skip(f"api_conn not importable: {exc}")
    try:
        return api_conn()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"could not build api_conn: {exc}")


def _view_exists(conn: duckdb.DuckDBPyConnection, view: str) -> bool:
    return (
        conn.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
            [view],
        ).fetchone()[0]
        > 0
    )


def _id_set(conn: duckdb.DuckDBPyConnection, pairs) -> tuple[set[str], list[str]]:
    """Union of non-null/non-blank ids across the (view, column) pairs that exist.
    Returns (ids, present_views) so callers can skip when no source/resolver view
    is on this checkout."""
    ids: set[str] = set()
    present: list[str] = []
    for view, col in pairs:
        if not _view_exists(conn, view):
            continue
        present.append(view)
        rows = conn.execute(
            f"SELECT DISTINCT {col} AS v FROM {view} WHERE {col} IS NOT NULL AND TRIM(CAST({col} AS VARCHAR)) <> ''"
        ).fetchall()
        ids.update(str(r[0]) for r in rows)
    return ids, present


@pytest.mark.parametrize("contract", _LINK_CONTRACTS, ids=lambda c: c["id"])
def test_clickable_ids_resolve(conn, contract):
    """Every id a source page turns into a link must resolve on its target page."""
    source_ids, source_views = _id_set(conn, contract["source"])
    resolvable, resolver_views = _id_set(conn, contract["resolver"])

    if not source_views:
        pytest.skip(f"{contract['id']}: no source view present on this checkout")
    if not resolver_views:
        pytest.skip(f"{contract['id']}: no resolver view present on this checkout")
    if not source_ids:
        pytest.skip(f"{contract['id']}: source views present but empty (no data)")

    dead_ends = sorted(source_ids - resolvable)
    assert not dead_ends, (
        f"{contract['id']}: {len(dead_ends)} of {len(source_ids)} clickable ids "
        f"do NOT resolve on the target page (dead links).\n"
        f"  source views:   {source_views}\n"
        f"  resolver views: {resolver_views}\n"
        f"  first dead ids: {dead_ends[:10]}"
    )


# ── Constituency → local-government council link ────────────────────────────────
#
# council_accountability_url(local_authority) → /local-government?la=<la>, resolved
# by the local-government page against v_la_chief_executives. These council-grain
# views live ONLY on the constituency connection (constituency_*.sql is NOT in the
# api_conn glob set), so this contract needs its own connection, but reuses the same
# set-based "every clickable id must resolve" check as the api_conn link types above.
_COUNCIL_LINK = {
    "id": "council_accountability_url",
    # Sources: every serving council the constituency dossier surfaces. The council-
    # context grid + drill-down render the "Who runs this council" link today; the
    # housing-performance grid names the same councils, so it's included to guard a
    # future link there — both sets must resolve to a CE dossier.
    "source": [
        ("v_constituency_council_context", "local_authority"),
        ("v_constituency_council_housing_performance", "local_authority"),
    ],
    # Resolver: the 31-LA Chief Executive roster the local-government page reads.
    "resolver": [("v_la_chief_executives", "local_authority")],
}


@pytest.fixture(scope="module")
def cons_conn():
    """Constituency connection — hosts the constituency + council-grain LA views that
    api_conn does not register. Skips cleanly when the data/config isn't on this
    checkout, mirroring the api_conn fixture."""
    try:
        from dail_tracker_core.connections import constituency_conn
    except Exception as exc:  # noqa: BLE001 — config import side-effects
        pytest.skip(f"constituency_conn not importable: {exc}")
    try:
        return constituency_conn()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"could not build constituency_conn: {exc}")


def test_council_accountability_link_resolves(cons_conn):
    """Every serving council the constituency page links must resolve to a Chief
    Executive dossier on the local-government page (NAV graph defect #6 wiring)."""
    source_ids, source_views = _id_set(cons_conn, _COUNCIL_LINK["source"])
    resolvable, resolver_views = _id_set(cons_conn, _COUNCIL_LINK["resolver"])

    if not source_views:
        pytest.skip("council link: no source view present on this checkout")
    if not resolver_views:
        pytest.skip("council link: no resolver view present on this checkout")
    if not source_ids:
        pytest.skip("council link: source views present but empty (no data)")

    dead_ends = sorted(source_ids - resolvable)
    assert not dead_ends, (
        f"council_accountability_url: {len(dead_ends)} of {len(source_ids)} serving "
        f"councils do NOT resolve on /local-government (dead links).\n"
        f"  source views:   {source_views}\n"
        f"  resolver views: {resolver_views}\n"
        f"  first dead ids: {dead_ends[:10]}"
    )
