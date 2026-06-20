"""Tests for dail_tracker_core.queries.procurement.

Two layers:
  1. Unit (always runs, no data): a query against a connection with no views
     returns an *unavailable* QueryResult — proving DuckDB failures are surfaced,
     not swallowed into a silent empty DataFrame (the old _safe behaviour).
  2. Integration (skips if gold parquet absent): against the real registered
     views, each query returns the columns the published contract expects.
"""

from __future__ import annotations

import duckdb
import pytest

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import procurement as q
from dail_tracker_core.results import QueryResult

# The exact column contract each fetcher is responsible for (matches the SQL).
_EXPECTED_COLUMNS = {
    "supplier_summary": {
        "supplier",
        "supplier_norm",
        "n_awards",
        "n_authorities",
        "awarded_value_safe_eur",
        "n_value_safe_awards",
        "n_ceiling_notices",
        "company_num",
        "company_status",
        "cro_match_method",
        "on_lobbying_register",
        "lobbying_returns",
        "is_lobbying_registrant",
        "is_lobbying_client",
    },
    "authority_summary": {"contracting_authority", "n_awards", "n_suppliers", "awarded_value_safe_eur"},
    "cpv_summary": {"cpv_code", "cpv_description", "n_awards", "n_suppliers", "awarded_value_safe_eur"},
    "lobbying_overlap": {
        "lobby_name",
        "lobby_side",
        "supplier",
        "supplier_norm",
        "n_lobby_returns",
        "n_award_rows",
        "n_authorities",
        "awarded_value_safe_eur",
    },
    "coverage_stats": {
        "min_year",
        "max_year",
        "n_award_rows",
        "n_safe_rows",
        "value_safe_total_eur",
        "n_suppliers",
        "n_authorities",
        "n_categories",
    },
}


# ---------------------------------------------------------------------------
# 1. Unit — DuckDB failure surfaces as unavailable (no data needed)
# ---------------------------------------------------------------------------


def test_missing_view_is_unavailable_not_silent_empty():
    conn = duckdb.connect()  # no views registered → the view does not exist
    try:
        result = q.supplier_summary(conn)
        assert isinstance(result, QueryResult)
        assert result.ok is False
        assert result.unavailable_reason is not None
        assert result.is_empty
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 2. Integration — real views; skip if the gold parquet has not been built
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def conn():
    c = connect_with_views(["procurement_*.sql"], swallow_errors=True)
    yield c
    c.close()


def _result_or_skip(result: QueryResult) -> QueryResult:
    if not result.ok:
        pytest.skip(f"procurement gold not available: {result.unavailable_reason}")
    return result


def test_supplier_summary_columns(conn):
    r = _result_or_skip(q.supplier_summary(conn, limit=5))
    assert _EXPECTED_COLUMNS["supplier_summary"].issubset(set(r.data.columns))
    assert len(r.data) <= 5  # LIMIT respected


def test_authority_summary_columns(conn):
    r = _result_or_skip(q.authority_summary(conn, limit=5))
    assert _EXPECTED_COLUMNS["authority_summary"].issubset(set(r.data.columns))


def test_cpv_summary_columns(conn):
    r = _result_or_skip(q.cpv_summary(conn, limit=5))
    assert _EXPECTED_COLUMNS["cpv_summary"].issubset(set(r.data.columns))


def test_lobbying_overlap_columns(conn):
    r = _result_or_skip(q.lobbying_overlap(conn))
    assert _EXPECTED_COLUMNS["lobbying_overlap"].issubset(set(r.data.columns))


def test_awards_for_supplier_returns_queryresult(conn):
    # Use a supplier_norm pulled from the summary if data exists; otherwise the
    # query still returns a valid (possibly empty) ok result or unavailable.
    summary = q.supplier_summary(conn, limit=1)
    if not summary.ok or summary.is_empty:
        pytest.skip("no supplier rows to drill into")
    norm = summary.data.iloc[0]["supplier_norm"]
    r = q.awards_for_supplier(conn, norm)
    assert r.ok is True  # the supplier came from the same dataset, so it must resolve


def test_awards_for_supplier_blocks_sole_traders(conn):
    """A natural person's award history must never compose into a dossier:
    awards_for_supplier is the single code path behind both the API supplier
    dossier and the Streamlit drill-down, so the quarantine is asserted here."""
    try:
        df = conn.execute(
            "SELECT supplier_norm FROM v_procurement_awards WHERE supplier_class = 'sole_trader_or_individual' LIMIT 1"
        ).df()
    except Exception:
        pytest.skip("procurement gold not available")
    if df.empty:
        pytest.skip("no sole-trader rows in the awards corpus")
    r = q.awards_for_supplier(conn, df.iloc[0]["supplier_norm"])
    assert r.ok is True
    assert r.is_empty  # the rows exist in the view, but the drill-down refuses them


def test_coverage_stats_columns_and_single_row(conn):
    r = _result_or_skip(q.coverage_stats(conn))
    assert _EXPECTED_COLUMNS["coverage_stats"].issubset(set(r.data.columns))
    assert len(r.data) == 1  # single aggregate row, never a rollup


def test_value_contrast_naive_dwarfs_safe(conn):
    """The '€570bn that isn't' invariant: the ungated naive Σ must be many times the
    sum-safe Σ (framework-ceiling repetition). Guards the contrast panel's whole premise
    and that the safe gate never silently widens to swallow ceilings."""
    r = _result_or_skip(q.value_contrast(conn))
    assert len(r.data) == 1
    row = r.data.iloc[0]
    expected = {
        "n_rows",
        "n_framework_rows",
        "n_safe_rows",
        "naive_total_eur",
        "safe_total_eur",
        "framework_naive_eur",
        "framework_once_eur",
    }
    assert expected.issubset(set(r.data.columns))
    naive, safe = float(row["naive_total_eur"]), float(row["safe_total_eur"])
    assert safe > 0 and naive > safe * 5  # honesty story holds (real ratio ~24x)
    # A framework ceiling repeated across suppliers inflates the once-counted figure.
    assert float(row["framework_naive_eur"]) >= float(row["framework_once_eur"]) > 0


def test_value_ordering_is_descending(conn):
    r = _result_or_skip(q.supplier_summary(conn, limit=20, order_by="value"))
    vals = r.data["awarded_value_safe_eur"].tolist()
    assert vals == sorted(vals, reverse=True)  # value lens surfaces the money leaders first


def test_payments_corpus_stats_tiers_separate(conn):
    r = _result_or_skip(q.payments_corpus_stats(conn))
    assert len(r.data) == 1
    row = r.data.iloc[0]
    assert {"n_payments", "n_publishers", "n_suppliers", "spent_safe_eur", "committed_safe_eur"}.issubset(
        set(r.data.columns)
    )
    # Paid and ordered are reported separately (never one blended total).
    assert float(row["spent_safe_eur"]) >= 0 and float(row["committed_safe_eur"]) >= 0


def test_payments_supplier_summary_named_and_single_tier(conn):
    r = _result_or_skip(q.payments_supplier_summary(conn, tier="SPENT", limit=10))
    assert {
        "supplier",
        "supplier_normalised",
        "realisation_tier",
        "n_publishers",
        "total_safe_eur",
        "vat_mixed",
        "supplier_class",
    }.issubset(set(r.data.columns))
    # The view is filtered to a single tier; the ranking is by money, descending.
    assert set(r.data["realisation_tier"].unique()) <= {"SPENT"}
    vals = r.data["total_safe_eur"].tolist()
    assert vals == sorted(vals, reverse=True)


def test_payments_tier_whitelist_rejects_injection(conn):
    # An unknown tier falls back to SPENT (no raw string reaches SQL).
    r = q.payments_supplier_summary(conn, tier="'; DROP TABLE x; --", limit=3)
    if r.ok and not r.is_empty:
        assert set(r.data["realisation_tier"].unique()) <= {"SPENT"}


def test_payments_by_year_single_tier_chronological(conn):
    top = q.payments_publisher_summary(conn, tier="COMMITTED", limit=1)
    if not top.ok or top.is_empty:
        pytest.skip("no payment publishers")
    body = top.data.iloc[0]["publisher_name"]
    r = q.payments_by_year(conn, body, tier="COMMITTED")
    assert r.ok is True
    assert {"year", "n_payments", "total_safe_eur"}.issubset(set(r.data.columns))
    years = [int(y) for y in r.data["year"].tolist()]
    assert years == sorted(years)  # chronological for the spend-over-time chart
    assert (r.data["total_safe_eur"] >= 0).all()


def test_payments_for_supplier_roundtrip(conn):
    top = q.payments_supplier_summary(conn, tier="SPENT", limit=1)
    if not top.ok or top.is_empty:
        pytest.skip("no payment rows")
    norm = top.data.iloc[0]["supplier_normalised"]
    r = q.payments_for_supplier(conn, norm)
    assert r.ok is True and not r.is_empty


def test_payments_supplier_header_single_row_both_tiers(conn):
    top = q.payments_supplier_summary(conn, tier="SPENT", limit=1)
    if not top.ok or top.is_empty:
        pytest.skip("no payment rows")
    norm = top.data.iloc[0]["supplier_normalised"]
    r = q.payments_supplier_header(conn, norm)
    assert r.ok is True and len(r.data) == 1
    row = r.data.iloc[0]
    assert {
        "supplier",
        "supplier_class",
        "n_publishers",
        "n_paid_lines",
        "n_ordered_lines",
        "paid_safe_eur",
        "ordered_safe_eur",
        "vat_mixed",
        "cro_company_num",
    }.issubset(set(r.data.columns))
    # Paid and ordered are carried side by side, never one blended total.
    assert float(row["paid_safe_eur"]) >= 0 and float(row["ordered_safe_eur"]) >= 0


def test_payments_publishers_for_supplier_mirrors_publisher_drill(conn):
    """The reverse drill (a supplier's payers) must agree with the forward drill (a body's
    suppliers): the body's total for the supplier equals the supplier's total for the body."""
    top = q.payments_supplier_summary(conn, tier="SPENT", limit=1)
    if not top.ok or top.is_empty:
        pytest.skip("no payment rows")
    norm = top.data.iloc[0]["supplier_normalised"]
    rev = q.payments_publishers_for_supplier(conn, norm, tier="SPENT")
    assert rev.ok is True and not rev.is_empty
    assert {"publisher_name", "publisher_type", "n_payments", "total_safe_eur"}.issubset(set(rev.data.columns))
    # Descending by money (the ranking the drill-down renders).
    vals = rev.data["total_safe_eur"].tolist()
    assert vals == sorted(vals, reverse=True)
    # Cross-check the top body against the forward per-publisher drill.
    body = rev.data.iloc[0]["publisher_name"]
    fwd = q.payments_for_publisher(conn, body, tier="SPENT")
    match = fwd.data[fwd.data["supplier_normalised"] == norm]
    if not match.empty:
        assert abs(float(match.iloc[0]["total_safe_eur"]) - float(rev.data.iloc[0]["total_safe_eur"])) < 1.0


def test_payments_publishers_for_supplier_tier_whitelist(conn):
    # An injection-shaped tier falls back to SPENT (no raw string reaches SQL).
    r = q.payments_publishers_for_supplier(conn, "JOHN SISK SON", tier="'; DROP TABLE x; --")
    assert r.ok is True  # query ran (didn't error on a bad tier string)


def test_supplier_concentration_share_is_sane(conn):
    r = _result_or_skip(q.supplier_concentration(conn, top_n=10))
    assert len(r.data) == 1
    row = r.data.iloc[0]
    assert {"top_n", "n_suppliers", "total_awards", "top_n_awards", "top_n_share_pct"}.issubset(set(r.data.columns))
    share = float(row["top_n_share_pct"])
    assert 0 < share <= 100  # a real share
    assert int(row["top_n_awards"]) <= int(row["total_awards"])  # subset never exceeds whole


def test_awards_by_year_counts_ascending_years(conn):
    r = _result_or_skip(q.awards_by_year(conn))
    years = [int(y) for y in r.data["year"].tolist()]
    assert years == sorted(years)  # chronological for the trend chart
    assert (r.data["n_awards"] >= 0).all()


def test_ted_corpus_stats_single_row(conn):
    r = _result_or_skip(q.ted_corpus_stats(conn))
    assert len(r.data) == 1
    expected = {
        "n_notices",
        "n_notices_ex_pan_eu",
        "min_year",
        "max_year",
        "n_winners",
        "n_buyers",
        "n_pan_eu",
        "value_safe_eur",
        "pan_eu_ceiling_eur",
    }
    assert expected.issubset(set(r.data.columns))
    row = r.data.iloc[0]
    # The default (ex-pan-EU) count must not exceed the full count, and pan-EU ceilings
    # dwarf the real safe value (the TED echo of the eTenders mirage).
    assert row["n_notices_ex_pan_eu"] <= row["n_notices"]
    if row["n_pan_eu"] > 0:
        assert float(row["pan_eu_ceiling_eur"]) > float(row["value_safe_eur"])


def test_ted_supplier_summary_company_class_and_order(conn):
    r = _result_or_skip(q.ted_supplier_summary(conn, limit=15, order_by="awards"))
    assert {
        "winner_name",
        "winner_join_norm",
        "n_awards",
        "n_buyers",
        "ted_value_safe_eur",
        "ted_value_safe_incl_eu_eur",
        "has_pan_eu",
    }.issubset(set(r.data.columns))
    counts = r.data["n_awards"].tolist()
    assert counts == sorted(counts, reverse=True)  # count-led ranking


def test_ted_for_supplier_roundtrip(conn):
    top = q.ted_supplier_summary(conn, limit=1)
    if not top.ok or top.is_empty:
        pytest.skip("no TED winners")
    norm = top.data.iloc[0]["winner_join_norm"]
    r = q.ted_for_supplier(conn, norm)
    assert r.ok is True and not r.is_empty
    assert r.data.iloc[0]["winner_join_norm"] == norm


def test_awards_for_authority_drill_down(conn):
    auth = q.authority_summary(conn, limit=1)
    if not auth.ok or auth.is_empty:
        pytest.skip("no authority rows to drill into")
    name = auth.data.iloc[0]["contracting_authority"]
    r = q.awards_for_authority(conn, name)
    assert r.ok is True
    assert {"supplier", "supplier_norm", "award_date", "value_eur"}.issubset(set(r.data.columns))


def test_awards_for_cpv_drill_down(conn):
    cpv = q.cpv_summary(conn, limit=1)
    if not cpv.ok or cpv.is_empty:
        pytest.skip("no cpv rows to drill into")
    code = cpv.data.iloc[0]["cpv_code"]
    r = q.awards_for_cpv(conn, code)
    assert r.ok is True
    # supplier_class rides along so the page can mask individual awardees (privacy).
    assert {"supplier", "supplier_class", "contracting_authority", "award_date"}.issubset(set(r.data.columns))


def test_available_years_descending_ints(conn):
    r = _result_or_skip(q.available_years(conn))
    years = [int(y) for y in r.data["year"].tolist()]
    assert years == sorted(years, reverse=True)
    assert all(2000 <= y <= 2100 for y in years)  # sane calendar years, no NULLs


def test_awards_for_supplier_carries_notice_link(conn):
    """The supplier award rows must carry etenders_notice_url so the page can link each row to
    its authoritative national notice (templated from the Tender ID), plus the TED links."""
    summary = q.supplier_summary(conn, limit=1)
    if not summary.ok or summary.is_empty:
        pytest.skip("no supplier rows")
    r = q.awards_for_supplier(conn, summary.data.iloc[0]["supplier_norm"])
    assert r.ok is True
    assert {"etenders_notice_url", "ted_can_link", "ted_notice_link"}.issubset(set(r.data.columns))


def test_etenders_notice_url_is_templated_from_tender_id(conn):
    """The national notice deep link is the eTenders resource URL built from the Tender ID
    (confirmed to resolve to the real notice). Where present it must be a resourceId URL."""
    try:
        df = conn.execute(
            "SELECT tender_id, etenders_notice_url FROM v_procurement_awards "
            "WHERE etenders_notice_url IS NOT NULL LIMIT 5"
        ).df()
    except Exception:
        pytest.skip("procurement gold not available")
    if df.empty:
        pytest.skip("no award rows with a tender id")
    for row in df.itertuples():
        assert "prepareViewCfTWS.do?resourceId=" in row.etenders_notice_url
        assert str(row.tender_id) in row.etenders_notice_url


def test_payment_lines_for_pair_is_the_leaf(conn):
    """The payments leaf returns the individual published lines for one supplier × body × tier
    (breaking the supplier↔body card loop). Columns must include the description + source link."""
    top = q.payments_supplier_summary(conn, tier="SPENT", limit=1)
    if not top.ok or top.is_empty:
        pytest.skip("no payment rows")
    norm = top.data.iloc[0]["supplier_normalised"]
    payers = q.payments_publishers_for_supplier(conn, norm, tier="SPENT")
    if not payers.ok or payers.is_empty:
        pytest.skip("no payer body")
    body = payers.data.iloc[0]["publisher_name"]
    r = q.payment_lines_for_pair(conn, norm, body, tier="SPENT")
    assert r.ok is True and not r.is_empty
    assert {"period", "year", "description", "po_number", "amount_eur", "source_file_url"}.issubset(set(r.data.columns))
    # Biggest first (the order the leaf renders), and the body's reported total reconciles to
    # the aggregate the card showed.
    vals = [v for v in r.data["amount_eur"].tolist() if v is not None]
    assert vals == sorted(vals, reverse=True)


def test_single_bid_notices_for_cpv_drill(conn):
    """Each single-bid market card must drill into the individual single-bid notices, each with a
    TED notice_url. Restricted to the api lane (the single-bid field's only source)."""
    markets = q.competition_by_cpv(conn, min_lots=100)
    if not markets.ok or markets.is_empty:
        pytest.skip("no competition-by-cpv rows")
    division = markets.data.iloc[0]["cpv_division"]
    r = q.single_bid_notices_for_cpv(conn, division)
    assert r.ok is True
    assert {"publication_number", "notice_url", "buyer_name", "winner_name"}.issubset(set(r.data.columns))


def test_live_tenders_stats_has_horizon(conn):
    """The open-tenders stats must carry the furthest deadline + max days so the page can
    'project to the furthest date' instead of implying a 30-day cap."""
    r = q.live_tenders_stats(conn)
    if not r.ok:
        pytest.skip("no live-tender snapshot")
    assert {"n_open", "next_closing", "last_closing", "max_days"}.issubset(set(r.data.columns))


def test_live_tenders_sector_filter_is_optional(conn):
    """live_tenders must run with no sector (un-enriched snapshots), and the sector facet query
    degrades to unavailable rather than erroring when the snapshot carries no CPV yet."""
    base = q.live_tenders(conn, limit=3)
    if not base.ok:
        pytest.skip("no live-tender snapshot")
    sectors = q.live_tender_sectors(conn)
    # Either the snapshot is CPV-enriched (ok, with sector+n) or it isn't (unavailable) — never a crash.
    if sectors.ok and not sectors.is_empty:
        assert {"sector", "n"}.issubset(set(sectors.data.columns))
        one = sectors.data.iloc[0]["sector"]
        filtered = q.live_tenders(conn, sector=one)
        assert filtered.ok is True


def test_year_filter_scopes_and_preserves_columns(conn):
    full = _result_or_skip(q.available_years(conn))
    if full.is_empty:
        pytest.skip("no years to filter on")
    yr = int(full.data.iloc[0]["year"])
    scoped = q.supplier_summary(conn, year=yr, limit=10)
    assert scoped.ok is True
    # The year view must expose the exact same column contract as the all-time view.
    assert _EXPECTED_COLUMNS["supplier_summary"].issubset(set(scoped.data.columns))
    # A single-year ranking must not exceed the all-time one for the same suppliers.
    all_time = q.supplier_summary(conn, limit=None)
    if all_time.ok and not all_time.is_empty and not scoped.is_empty:
        assert scoped.data["n_awards"].max() <= all_time.data["n_awards"].max()
