"""Contract tests for the IPAS / international-protection accommodation views.

These lock the invariants that make this corpus safe to publish. Each is a rule that,
if it silently broke, would put a WRONG or UNSAFE figure in front of the public:

  * the never-sum grain (audit narrative must never be summable / unioned with money facts)
  * the identity gate (only exactly-resolved operators may be named)
  * the standards join (a compliance code must always resolve to its meaning)
  * unknowns survive (where the State publishes nothing, we must not silently imply zero)
  * the LA map reconciles to the source's own published total
"""

from __future__ import annotations

import duckdb
import pytest

from dail_tracker_core.db import register_views

pytestmark = pytest.mark.sql

IPAS_VIEWS = (
    "v_ipas_facts",
    "v_ipas_la_profile",
    "v_ipas_operators",
    "v_ipas_centre_compliance",
    "v_ipas_property_rates",
    "v_ipas_entitlements",
)


@pytest.fixture(scope="module")
def conn() -> duckdb.DuckDBPyConnection:
    c = duckdb.connect()
    # swallow_errors=False: a broken view must FAIL the test, not vanish silently
    # (register_views defaults to swallowing, which would hide a typo'd view).
    register_views(c, ["housing_*.sql"], swallow_errors=False)
    return c


@pytest.mark.parametrize("view", IPAS_VIEWS)
def test_view_registers_and_has_rows(conn, view):
    assert conn.execute(f"SELECT count(*) FROM {view}").fetchone()[0] > 0


@pytest.mark.parametrize("view", IPAS_VIEWS)
def test_never_sum_invariant(conn, view):
    """The whole corpus is audit-report narrative grain. Not one row may be summable —
    these figures must never be added up or unioned with payments/awards/grants."""
    bad = conn.execute(
        f"SELECT count(*) FROM {view} WHERE value_safe_to_sum"
    ).fetchone()[0]
    assert bad == 0, f"{view}: {bad} rows claim value_safe_to_sum=TRUE"


def test_operators_are_identity_gated(conn):
    """Only operators whose name resolved EXACTLY across both the compliance and payment
    sides may be named. One wrong name is worse than ten omitted."""
    rows = conn.execute(
        "SELECT count(*) FROM v_ipas_operators WHERE match_confidence <> 'exact'"
    ).fetchone()[0]
    assert rows == 0, "an unresolved operator would be named on the page"


def test_every_compliance_judgment_resolves_to_a_standard(conn):
    """A judgment rendered as a bare code ('Standard 4.3') is meaningless to a reader.
    The National Standards lookup must resolve all of them."""
    unresolved, total = conn.execute(
        "SELECT count(*) FILTER (WHERE standard_statement IS NULL), count(*) "
        "FROM v_ipas_centre_compliance"
    ).fetchone()
    assert total > 2000
    assert unresolved == 0, f"{unresolved}/{total} judgments have no standard text"


def test_unknowns_are_preserved(conn):
    """Where the State does not publish a figure, we carry an explicit unknown. If these
    ever vanish, someone has started imputing — which would be fabrication."""
    unknown = conn.execute(
        "SELECT count(*) FROM v_ipas_facts WHERE is_unknown"
    ).fetchone()[0]
    assert unknown > 400, "explicit unknowns disappeared from the fact store"

    # An unknown row must never carry a NUMERIC value — that would mean we published a
    # figure while claiming we could not establish it.
    #
    # It MAY carry `value_text`, and that is deliberate: some unknowns are PARTIAL. E.g.
    # IGEES prints four staff-number data labels but no x-axis categories — the values are
    # known, the periods are not; recording the values in value_text while flagging the row
    # unknown is more honest than either guessing the periods or discarding the numbers.
    # Likewise a 2015 finding the 2024 chapter never re-examines carries the text marker
    # 'not_assessed_in_2024' rather than an invented verdict.
    contradictory = conn.execute(
        "SELECT count(*) FROM v_ipas_facts WHERE is_unknown AND value_numeric IS NOT NULL"
    ).fetchone()[0]
    assert contradictory == 0, (
        "a row is flagged unknown yet carries a numeric value — either the flag is wrong "
        "or a figure was imputed"
    )


def test_la_profile_reconciles_to_the_published_total(conn):
    """The 31 local-authority counts must still sum to the IPAS weekly report's own Grand
    Total (32,702 at the 2024-12-29 snapshot). If this drifts, the map is lying."""
    n_la, total = conn.execute(
        "SELECT count(*), sum(ip_applicants) FROM v_ipas_la_profile"
    ).fetchone()
    assert n_la == 31, f"expected 31 local authorities, got {n_la}"
    assert total == 32702, f"LA counts sum to {total}, not the source's Grand Total 32,702"


def test_per_capita_is_never_imputed(conn):
    """Per-capita must be NULL where population could not be mapped — never guessed."""
    bad = conn.execute(
        "SELECT count(*) FROM v_ipas_la_profile "
        "WHERE population_2022 IS NULL AND ip_per_1000_population IS NOT NULL"
    ).fetchone()[0]
    assert bad == 0, "a per-capita rate exists without a population — it was imputed"


def test_property_rates_keep_unknown_rates_null(conn):
    """The C&AG recorded 4 of its 20 sampled properties as 'Unclear'/Department-run. Those
    rates must stay NULL rather than be filled in."""
    bad = conn.execute(
        "SELECT count(*) FROM v_ipas_property_rates "
        "WHERE NOT rate_known AND contracted_rate_eur_per_person_night IS NOT NULL"
    ).fetchone()[0]
    assert bad == 0, "an 'unclear' rate has been given a number"
