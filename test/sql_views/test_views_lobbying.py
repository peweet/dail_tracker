"""
SQL view contract tests — lobbying / DPO views.

Split out of the former monolithic test_sql_views.py (REFACTORING_CANDIDATES C6).
Shared fixtures/helpers live in _view_test_helpers.py.
"""


import pytest

from ._view_test_helpers import (
    GOLD_PARQUET_DIR,
    LOBBY_PARQUET_DIR,
    SILVER_DIR,
    _con,
    _load,
    _skip_missing,
    _result,
    _src,
    _assert_cols,
)


# ---------------------------------------------------------------------------
# LOBBYING VIEWS
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_v_lobbying_base_member_codes_executes():
    """The shared normalised member-name → unique_member_code resolver that the four
    v_lobbying_* views LEFT JOIN (extracted from a CTE that was copy-pasted into each).
    Locks the two-column contract and the one-row-per-normalised-name grain — a dup here
    would fan out every consumer's member join."""
    _skip_missing(*_src("data/silver/parquet/flattened_members.parquet"))
    con = _con()
    con.execute(_load("lobbying_base_member_codes.sql"))
    result = _result(con, "v_lobbying_base_member_codes")
    _assert_cols(result, "norm_name", "unique_member_code")
    assert len(result) > 0
    dup = con.execute(
        "SELECT COUNT(*) FROM (SELECT norm_name, COUNT(*) c"
        " FROM v_lobbying_base_member_codes GROUP BY norm_name HAVING c > 1)"
    ).fetchone()[0]
    assert dup == 0, "v_lobbying_base_member_codes must be one row per normalised name"


@pytest.mark.sql
def test_v_lobbying_index_executes():
    _skip_missing(
        GOLD_PARQUET_DIR / "most_lobbied_politicians.parquet",
        GOLD_PARQUET_DIR / "politician_policy_exposure.parquet",
        GOLD_PARQUET_DIR / "bilateral_relationships.parquet",
    )
    con = _con()
    con.execute(_load("lobbying_index.sql"))
    result = _result(con, "v_lobbying_index")
    assert "member_name" in result.columns
    assert "return_count" in result.columns
    assert "distinct_policy_areas" in result.columns
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_persistence_executes():
    _skip_missing(LOBBY_PARQUET_DIR / "lobbyist_persistence.parquet")
    con = _con()
    con.execute(_load("lobbying_persistence.sql"))
    result = _result(con, "v_lobbying_persistence")
    assert "lobbyist_name" in result.columns
    assert "first_return_date" in result.columns
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_org_intensity_executes():
    _skip_missing(LOBBY_PARQUET_DIR / "bilateral_relationships.parquet")
    con = _con()
    con.execute(_load("lobbying_base_member_codes.sql"))  # consumer LEFT JOINs it
    con.execute(_load("lobbying_org_intensity.sql"))
    result = _result(con, "v_lobbying_org_intensity")
    assert "lobbyist_name" in result.columns
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_org_index_exposes_register_fields():
    """website / profile_url come from the lobbying.ie org register via gold."""
    _skip_missing(
        GOLD_PARQUET_DIR / "top_lobbyist_organisations.parquet",
        GOLD_PARQUET_DIR / "lobbyist_persistence.parquet",
    )
    con = _con()
    con.execute(_load("lobbying_org_index.sql"))
    result = _result(con, "v_lobbying_org_index")
    _assert_cols(result, "lobbyist_name", "website", "profile_url", "main_activities")
    # At least one org must carry a real website — guards against the columns
    # silently reverting to the old hardcoded '' literals.
    populated = con.execute(
        "SELECT COUNT(*) FROM v_lobbying_org_index WHERE website IS NOT NULL AND website <> ''"
    ).fetchone()[0]
    assert populated > 0, "no website populated — gold join may have regressed"


@pytest.mark.sql
def test_v_experimental_org_index_enriched_exposes_website():
    """The org detail panel reads `website` from this view."""
    _skip_missing(
        GOLD_PARQUET_DIR / "top_lobbyist_organisations.parquet",
        GOLD_PARQUET_DIR / "lobbyist_persistence.parquet",
        SILVER_DIR / "charities" / "charity_resolved.parquet",
        SILVER_DIR / "cro" / "companies.parquet",
        SILVER_DIR / "cro" / "financial_statements.parquet",
    )
    con = _con()
    con.execute(_load("lobbying_experimental_org_index_enriched.sql"))
    result = _result(con, "v_experimental_lobbying_org_index_enriched")
    _assert_cols(result, "lobbyist_name", "website", "sector_label", "lobbying_profile_url")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_clients_executes():
    _skip_missing(SILVER_DIR / "lobbying" / "client_company_returns_detail.csv")
    con = _con()
    con.execute(_load("lobbying_clients.sql"))
    result = _result(con, "v_lobbying_clients")
    assert "client_name" in result.columns
    assert "period_start_date" in result.columns
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_revolving_door_executes():
    _skip_missing(GOLD_PARQUET_DIR / "revolving_door_dpos.parquet")
    con = _con()
    con.execute(_load("lobbying_base_member_codes.sql"))  # consumer LEFT JOINs it
    con.execute(_load("lobbying_revolving_door.sql"))
    result = _result(con, "v_lobbying_revolving_door")
    assert "return_count" in result.columns
    assert "chamber_display" in result.columns
    assert len(result) > 0


# ---------------------------------------------------------------------------
# LOBBYING VIEWS (backfill — only 5 of ~21 were previously tested)
# ---------------------------------------------------------------------------

_LOB_SILVER = "data/silver/lobbying/parquet"


@pytest.mark.sql
def test_v_lobbying_summary_executes():
    _skip_missing(*_src("data/gold/parquet/policy_area_breakdown.parquet"))
    con = _con()
    con.execute(_load("lobbying_summary.sql"))
    result = _result(con, "v_lobbying_summary")
    _assert_cols(result, "total_returns", "total_orgs", "total_politicians", "first_period", "last_period")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_org_index_executes():
    _skip_missing(*_src("data/gold/parquet/top_lobbyist_organisations.parquet"))
    con = _con()
    con.execute(_load("lobbying_org_index.sql"))
    result = _result(con, "v_lobbying_org_index")
    _assert_cols(result, "lobbyist_name", "return_count", "politicians_targeted", "distinct_policy_areas")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_dpo_clients_executes():
    _skip_missing(*_src(f"{_LOB_SILVER}/revolving_door_returns_detail.parquet"))
    con = _con()
    con.execute(_load("lobbying_dpo_clients.sql"))
    result = _result(con, "v_lobbying_dpo_clients")
    _assert_cols(result, "individual_name", "client_name", "return_count")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_dpo_firms_executes():
    _skip_missing(*_src(f"{_LOB_SILVER}/revolving_door_returns_detail.parquet"))
    con = _con()
    con.execute(_load("lobbying_dpo_firms.sql"))
    result = _result(con, "v_lobbying_dpo_firms")
    _assert_cols(result, "individual_name", "lobbyist_name", "return_count")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_dpo_politicians_executes():
    _skip_missing(*_src(f"{_LOB_SILVER}/revolving_door_returns_detail.parquet"))
    con = _con()
    con.execute(_load("lobbying_dpo_politicians.sql"))
    result = _result(con, "v_lobbying_dpo_politicians")
    _assert_cols(result, "individual_name", "member_name", "chamber", "return_count")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_dpo_returns_executes():
    _skip_missing(*_src(f"{_LOB_SILVER}/revolving_door_returns_detail.parquet"))
    con = _con()
    con.execute(_load("lobbying_dpo_returns.sql"))
    result = _result(con, "v_lobbying_dpo_returns")
    _assert_cols(
        result,
        "individual_name",
        "return_id",
        "lobbyist_name",
        "client_name",
        "public_policy_area",
        "period_start_date",
    )
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_contact_detail_executes():
    """The per-contact return view; unique_member_code is the cross-page key the
    member-overview lobbying card joins on."""
    _skip_missing(*_src(f"{_LOB_SILVER}/politician_returns_detail.parquet"))
    con = _con()
    con.execute(_load("lobbying_base_member_codes.sql"))  # consumer LEFT JOINs it
    con.execute(_load("lobbying_contact_detail.sql"))
    result = _result(con, "v_lobbying_contact_detail")
    _assert_cols(
        result,
        "return_id",
        "member_name",
        "unique_member_code",
        "chamber",
        "lobbyist_name",
        "public_policy_area",
        "period_start_date",
    )
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_contact_detail_with_dpo_executes():
    """Reads v_lobbying_dpo_returns + v_lobbying_contact_detail — both first."""
    _skip_missing(*_src(f"{_LOB_SILVER}/politician_returns_detail.parquet"))
    con = _con()
    con.execute(_load("lobbying_base_member_codes.sql"))  # contact_detail LEFT JOINs it
    con.execute(_load("lobbying_dpo_returns.sql"))
    con.execute(_load("lobbying_contact_detail.sql"))
    con.execute(_load("lobbying_zz_contact_detail_with_dpo.sql"))
    result = _result(con, "v_lobbying_contact_detail_with_dpo")
    _assert_cols(result, "return_id", "member_name", "dpo_individuals", "dpo_count")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_dpo_politician_returns_executes():
    """Reads v_lobbying_dpo_returns + v_lobbying_contact_detail — both first."""
    _skip_missing(*_src(f"{_LOB_SILVER}/politician_returns_detail.parquet"))
    con = _con()
    con.execute(_load("lobbying_base_member_codes.sql"))  # contact_detail LEFT JOINs it
    con.execute(_load("lobbying_dpo_returns.sql"))
    con.execute(_load("lobbying_contact_detail.sql"))
    con.execute(_load("lobbying_zz_dpo_politician_returns.sql"))
    result = _result(con, "v_lobbying_dpo_politician_returns")
    _assert_cols(result, "individual_name", "member_name", "unique_member_code", "return_id", "lobbyist_name")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_policy_area_summary_executes():
    _skip_missing(*_src("data/gold/parquet/policy_area_breakdown.parquet"))
    con = _con()
    con.execute(_load("lobbying_policy_area_summary.sql"))
    result = _result(con, "v_lobbying_policy_area_summary")
    _assert_cols(result, "public_policy_area", "return_count", "distinct_orgs", "distinct_politicians")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_policy_exposure_executes():
    _skip_missing(*_src("data/gold/parquet/politician_policy_exposure.parquet"))
    con = _con()
    con.execute(_load("lobbying_base_member_codes.sql"))  # consumer LEFT JOINs it
    con.execute(_load("lobbying_policy_exposure.sql"))
    result = _result(con, "v_lobbying_policy_exposure")
    _assert_cols(
        result, "member_name", "unique_member_code", "public_policy_area", "returns_targeting", "distinct_lobbyists"
    )
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_recent_returns_executes():
    _skip_missing(*_src(f"{_LOB_SILVER}/returns_master.parquet"))
    con = _con()
    con.execute(_load("lobbying_recent_returns.sql"))
    result = _result(con, "v_lobbying_recent_returns")
    _assert_cols(result, "period_start_date", "lobbyist_name", "member_name", "public_policy_area", "source_url")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_return_documents_executes():
    _skip_missing(*_src("data/silver/parquet/lobbying_return_documents.parquet"))
    con = _con()
    con.execute(_load("lobbying_return_documents.sql"))
    result = _result(con, "v_lobbying_return_documents")
    _assert_cols(result, "return_id", "lobbyist_name", "pdf_url", "public_policy_area")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_sources_executes():
    _skip_missing(*_src(f"{_LOB_SILVER}/politician_returns_detail.parquet"))
    con = _con()
    con.execute(_load("lobbying_sources.sql"))
    result = _result(con, "v_lobbying_sources")
    _assert_cols(result, "return_id", "member_name", "lobbyist_name", "source_url", "official_pdf_url")
    assert len(result) > 0


@pytest.mark.sql
def test_v_lobbying_topic_search_executes():
    """searchable_text is the concatenated column the topic-search box scans."""
    _skip_missing(*_src(f"{_LOB_SILVER}/returns_master.parquet"))
    con = _con()
    con.execute(_load("lobbying_topic_search.sql"))
    result = _result(con, "v_lobbying_topic_search")
    _assert_cols(result, "return_id", "lobbyist_name", "public_policy_area", "searchable_text")
    assert len(result) > 0


@pytest.mark.sql
def test_v_experimental_lobbying_org_index_enriched_executes():
    """Experimental CRO/charity-enriched org index — depends on CRO + charities
    silver tables that may not be present even in a full local build."""
    _skip_missing(
        *_src(
            "data/gold/parquet/top_lobbyist_organisations.parquet",
            "data/silver/cro/companies.parquet",
            "data/silver/charities/charity_resolved.parquet",
        )
    )
    con = _con()
    con.execute(_load("lobbying_experimental_org_index_enriched.sql"))
    result = _result(con, "v_experimental_lobbying_org_index_enriched")
    _assert_cols(result, "lobbyist_name", "return_count", "sector_label", "funding_profile", "match_method", "flags")
    assert len(result) > 0
