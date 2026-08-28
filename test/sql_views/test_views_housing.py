"""
SQL view contract tests — constituency housing-enrichment, SSHA waiting-list,
accommodation-spend and constituency-map-layers views.

Split out of the former monolithic test_sql_views.py (REFACTORING_CANDIDATES C6).
Shared fixtures/helpers live in _view_test_helpers.py.
"""

import duckdb
import pytest

from ._view_test_helpers import (
    _DATA_BASE,
    GOLD_PARQUET_DIR,
    _con,
    _load,
    _result,
    _skip_missing,
)

# ---------------------------------------------------------------------------
# CONSTITUENCY HOUSING-ENRICHMENT TRIPWIRE (2026-06-19)
# These two views register with swallow_errors=True in constituency_conn(), so a
# break (renamed column, dropped parquet, mis-edited la_map) fails SILENTLY — the
# page section just disappears. This test fails LOUD instead. It also asserts the
# explicit la_map produces ZERO mis-joins (every serving council resolves to data).
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_constituency_housing_enrichment_views_build():
    _skip_missing(
        _DATA_BASE / "data" / "_meta" / "constituency_la_crosswalk.csv",
        GOLD_PARQUET_DIR / "ssha_a1_8_time_on_list_wide.parquet",
        GOLD_PARQUET_DIR / "noac_h2_vacancies_wide.parquet",
        GOLD_PARQUET_DIR / "noac_h1_stock_wide.parquet",
        GOLD_PARQUET_DIR / "noac_h7_retrofit_wide.parquet",
        GOLD_PARQUET_DIR / "noac_m2_collection_wide.parquet",
        GOLD_PARQUET_DIR / "derelict_sites_levy_wide.parquet",
    )
    con = _con()
    for fname in (
        "constituency_la_crosswalk.sql",
        "constituency_ssha_waiting_list.sql",
        "constituency_council_housing_performance.sql",
    ):
        try:
            con.execute(_load(fname))
        except duckdb.IOException as exc:
            pytest.skip(f"constituency housing: source not present for {fname}: {exc}")
        except Exception as exc:  # noqa: BLE001 — surface the offending file
            pytest.fail(f"constituency housing: {fname} failed to register: {type(exc).__name__}: {exc}")

    # SSHA waiting list — demand layer
    ssha = _result(con, "v_constituency_ssha_waiting_list", limit=100)
    assert ssha.height > 0
    for c in ("constituency_name", "local_authority", "waiting_total_2025", "long_wait_pct"):
        assert c in ssha.columns, f"v_constituency_ssha_waiting_list missing {c}"
    # explicit la_map => every serving council resolves (no NULL = no mis-join)
    assert ssha["waiting_total_2025"].null_count() == 0

    # NOAC council performance — operations layer, value + national-median benchmark
    perf = _result(con, "v_constituency_council_housing_performance", limit=100)
    assert perf.height > 0
    for c in ("vacancy_pct", "reletting_weeks", "retrofit_pct_of_stock", "nat_vacancy_pct"):
        assert c in perf.columns, f"v_constituency_council_housing_performance missing {c}"
    assert perf["vacancy_pct"].null_count() == 0
    # benchmark column is the national median (constant across all rows)
    assert perf["nat_vacancy_pct"].n_unique() == 1
    # collection + enforcement layer (NOAC M2 + Derelict Sites Levy) joins cleanly —
    # M2 shares NOAC naming, derelict has its own explicit map; both must resolve.
    for c in ("rent_collection_pct", "derelict_outstanding_eur", "nat_rent_collection_pct"):
        assert c in perf.columns, f"v_constituency_council_housing_performance missing {c}"
    assert perf["rent_collection_pct"].null_count() == 0
    assert perf["derelict_outstanding_eur"].null_count() == 0


@pytest.mark.sql
def test_ssha_waiting_list_national_views_build():
    """National Housing-screen views: composition (who's waiting) + totals (league
    table w/ per-capita). Guards the UNPIVOT labelling, the 3-grain rollup, and the
    LA->county->PEA08 maps (a name drift silently drops a county from the rollup)."""
    import polars as pl

    _skip_missing(
        GOLD_PARQUET_DIR / "ssha_a1_8_time_on_list_wide.parquet",
        GOLD_PARQUET_DIR / "ssha_a1_7_tenure_wide.parquet",
        GOLD_PARQUET_DIR / "ssha_a1_2_employment_wide.parquet",
        GOLD_PARQUET_DIR / "ssha_a1_4_household_size_wide.parquet",
        GOLD_PARQUET_DIR / "ssha_a1_9_citizenship_wide.parquet",
        GOLD_PARQUET_DIR / "cso_pea08.parquet",
        GOLD_PARQUET_DIR / "cso_vac14.parquet",
        GOLD_PARQUET_DIR / "cso_f2023b.parquet",
        GOLD_PARQUET_DIR / "cso_hap01.parquet",
        GOLD_PARQUET_DIR / "cso_ndq09.parquet",
        GOLD_PARQUET_DIR / "cso_hap17.parquet",
        GOLD_PARQUET_DIR / "cso_hap20.parquet",
        GOLD_PARQUET_DIR / "cso_hap32.parquet",
    )
    con = _con()
    for fname in (
        "housing_ssha_waiting_list_composition.sql",
        "housing_ssha_waiting_list_totals.sql",
        "housing_supply_national.sql",
        "housing_completions_trend.sql",
        "housing_rent_by_county.sql",
        "housing_hap_national.sql",
    ):
        try:
            con.execute(_load(fname))
        except duckdb.IOException as exc:
            pytest.skip(f"ssha national: source not present for {fname}: {exc}")
        except Exception as exc:  # noqa: BLE001
            pytest.fail(f"ssha national: {fname} failed to register: {type(exc).__name__}: {exc}")

    comp = con.execute("SELECT * FROM v_ssha_waiting_list_composition").pl()
    assert set(comp["grain"].unique()) == {"national", "county", "la"}
    assert set(comp["dimension"].unique()) == {
        "time_on_list",
        "tenure",
        "employment",
        "household",
        "citizenship",
        "age",
        "income",
        "main_need",
        "accom_need",
    }
    # every category is labelled — no SSHA column-slug leaked through as a category in ANY
    # dimension (a slug looks like lowercase_with_underscores; labels are Title Case)
    leaked = comp.filter(pl.col("category").str.contains("^[a-z0-9_]+$"))
    assert leaked.height == 0, f"unlabelled slugs leaked: {set(leaked['category'].unique())}"
    # ord set on every row (drives bar ordering)
    assert comp.filter(pl.col("ord").is_null()).height == 0
    # citizenship is exactly the 4 source categories (sensitivity: no surprise buckets)
    cit = set(comp.filter(pl.col("dimension") == "citizenship")["category"].unique())
    assert cit == {"Irish", "EEA", "Non-EEA", "UK"}
    # main_need: the 5 disability sub-types are rolled into one "Disability (any)" (legibility)
    needs = set(comp.filter(pl.col("dimension") == "main_need")["category"].unique())
    assert "Disability (any)" in needs
    assert not any("disability" in n.lower() and n != "Disability (any)" for n in needs)
    # a national distribution sums to ~100%
    nat_time = comp.filter(
        (pl.col("grain") == "national") & (pl.col("dimension") == "time_on_list") & (pl.col("year") == 2025)
    )
    assert abs(nat_time["pct"].sum() - 100.0) < 0.5

    tot = con.execute("SELECT * FROM v_ssha_waiting_list_totals").pl()
    nat = tot.filter(pl.col("grain") == "national")
    cty = tot.filter(pl.col("grain") == "county")
    la = tot.filter(pl.col("grain") == "la")
    assert cty.height == 26 and la.height == 31 and nat.height == 1
    # rollup integrity: county sum == LA sum == national (a dropped LA breaks this)
    national_total = nat["waiting_total"][0]
    assert cty["waiting_total"].sum() == national_total
    assert la["waiting_total"].sum() == national_total
    # per-capita present for every county + national, never faked at LA grain
    assert cty["waiters_per_1000"].null_count() == 0
    assert la["waiters_per_1000"].null_count() == la.height

    # supply & affordability — single national row, the three CSO metrics present
    sup = con.execute("SELECT * FROM v_housing_supply_national").pl()
    assert sup.height == 1
    s = sup.row(0, named=True)
    assert (s["vacant_dwellings"] or 0) > 0 and 0 < (s["vacancy_rate"] or 0) < 100
    assert (s["avg_weekly_private_rent"] or 0) > 0
    assert (s["hap_households"] or 0) > 0

    # completions trend — only complete years (no part-reported "drop"); ascending
    ct = con.execute("SELECT * FROM v_housing_completions_trend ORDER BY year").pl()
    assert ct.height >= 5 and (ct["completions"] > 0).all()
    assert ct["year"].is_sorted()

    # rent by county — 24 of 26 (Dublin + Galway split in F2023B, deliberately absent)
    rent = con.execute("SELECT * FROM v_housing_rent_by_county").pl()
    assert rent.height == 24
    assert "Dublin" not in rent["county"].to_list() and "Galway" not in rent["county"].to_list()
    assert (rent["avg_weekly_private_rent"] > 0).all()


@pytest.mark.sql
def test_accommodation_spend_views_build():
    """Asylum/Ukraine accommodation spend views — the precise spend-category filter must
    NOT pull in Homeless/Student/Conference accommodation or Coastal/Data Protection, and
    the Ukraine stream only appears from the first published 2022 register onward."""
    import polars as pl

    _skip_missing(
        GOLD_PARQUET_DIR / "procurement_payments_fact.parquet",
        GOLD_PARQUET_DIR / "dceidy_ipas_legacy_spend.parquet",
    )
    con = _con()
    for fname in ("housing_accommodation_spend_by_year.sql", "housing_accommodation_spend_providers.sql"):
        try:
            con.execute(_load(fname))
        except duckdb.IOException as exc:
            pytest.skip(f"accommodation spend: source not present for {fname}: {exc}")
        except Exception as exc:  # noqa: BLE001
            pytest.fail(f"accommodation spend: {fname} failed: {type(exc).__name__}: {exc}")

    yr = con.execute("SELECT * FROM v_accommodation_spend_by_year").pl()
    assert yr.height > 0
    assert (yr["total_eur"] > 0).all()
    # category filter is tight: no homeless/student/coastal leakage. A single year (IP +
    # Ukraine combined, incl. the 2023-2024 DCEDIY surge) tops out ~€1.8bn; a leak would
    # balloon it past ~€2.5bn (well over the C&AG IP+Ukraine envelope).
    assert yr["total_eur"].max() < 2_500_000_000
    # Ukraine-stream spend must not leak backwards into the pre-surge years. The earliest
    # Ukraine-category row in the source is 2023 — corrected 2026-08-28: this previously
    # asserted 2022, which no committed gold has ever satisfied (checked against both the
    # 2026-08-23 and the 2026-08-14 procurement_payments_fact: zero '%ukraine%' rows at or
    # before 2022 in each, so this was a false assertion, not a refresh regression). The
    # only 2022 row the view carries is a single Wexford CoCo asylum payment (€30,837),
    # which is IP stream, not Ukraine. The view header records the same thing: "2020-2022
    # remain thin (pre-surge; not separately published in a parsable register)".
    pre = yr.filter(pl.col("year") < 2023)
    assert pre["ukraine_eur"].fill_null(0).sum() == 0, "Ukraine spend must not predate 2023"
    assert yr.filter((pl.col("year") == 2023) & (pl.col("ukraine_eur") > 0)).height == 1

    prov = con.execute("SELECT * FROM v_accommodation_spend_providers").pl()
    assert prov.height > 50 and (prov["total_eur"] > 0).all()
    assert prov["total_eur"].is_sorted(descending=True)


# ---------------------------------------------------------------------------
# CONSTITUENCY CHOROPLETH TRIPWIRE (2026-06-19)
# v_constituency_map_layers feeds the national index choropleth. It JOINs
# v_constituency_registry + v_constituency_house_work and registers with
# swallow_errors=True in constituency_conn(), so a break (renamed source column,
# NTILE typo) fails SILENTLY — the map just disappears. This fails LOUD instead,
# and pins the quintile buckets to 1..5 (the page indexes a 5-colour palette with
# them — an out-of-range bucket would IndexError or mis-colour). Skips cleanly when
# the member/registry sources aren't present on this box.
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_constituency_map_layers_view_builds():
    try:
        from dail_tracker_core.connections import constituency_conn
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"constituency_conn import unavailable: {exc}")
    con = constituency_conn()
    try:
        df = con.execute("SELECT * FROM v_constituency_map_layers").pl()
    except duckdb.CatalogException:
        pytest.skip("v_constituency_map_layers absent — member/registry sources not on this box")

    assert df.height == 43, f"expected all 43 constituencies, got {df.height}"
    for c in (
        "constituency_name",
        "population_2022",
        "population_per_td",
        "pct_landlord_tds",
        "questions_per_td",
        "q_population",
        "q_population_per_td",
        "q_pct_landlord_tds",
        "q_questions_per_td",
    ):
        assert c in df.columns, f"v_constituency_map_layers missing {c}"

    # quintile buckets always land in 1..5 — the page maps them onto a 5-colour ramp.
    for qcol in ("q_population", "q_population_per_td", "q_pct_landlord_tds", "q_questions_per_td"):
        vals = set(df[qcol].drop_nulls().to_list())
        assert vals <= {1, 2, 3, 4, 5}, f"{qcol} out-of-range quintile(s): {vals - {1, 2, 3, 4, 5}}"

    # population is the Census-2022 spine: present for every constituency, fully bucketed.
    assert df["population_2022"].null_count() == 0
    assert df["q_population"].null_count() == 0
