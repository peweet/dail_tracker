"""
SQL view contract tests — procurement views (awards, real-terms deflation,
TED, expiring contracts, procurement x lobbying/charity overlap).

Split out of the former monolithic test_sql_views.py (REFACTORING_CANDIDATES C6).
Shared fixtures/helpers live in _view_test_helpers.py.
"""

import pytest

from ._view_test_helpers import (
    _USE_REAL_PATHS,
    SILVER_DIR,
    _assert_cols,
    _con,
    _fixture_only,
    _load,
    _skip_missing,
    _src,
    _view_path,
)

# ---------------------------------------------------------------------------
# PROCUREMENT VIEWS
# ---------------------------------------------------------------------------
#
# These assert the VALUE-IS-NOT-SPEND semantics, not just that the views run.
# The fixture (test/fixtures/sql_views/_generate.py) plants rows whose aggregates
# are known exactly, so a regression in the value_safe_to_sum filter, the privacy/
# truncation exclusions, the CRO join, or the lobbying-overlap dedup fails loudly.

_PROC_AWARDS = "data/gold/parquet/procurement_awards.parquet"
_PROC_CRO = "data/gold/parquet/procurement_supplier_cro_match.parquet"
_PROC_OVERLAP = "data/gold/parquet/procurement_lobbying_overlap.parquet"
_PROC_DEFLATOR = "data/gold/parquet/cso_cpi_deflator.parquet"
_PROC_TPI = "data/gold/parquet/scsi_tpi_deflator.parquet"
_PROC_GOV_DEFLATOR = "data/gold/parquet/cso_govt_consumption_deflator.parquet"
_PROC_PAYMENTS = "data/gold/parquet/procurement_payments_fact.parquet"


@pytest.mark.sql
def test_v_procurement_awards_executes():
    _fixture_only()
    _skip_missing(*_src(_PROC_AWARDS))
    con = _con()
    con.execute(_load("procurement_awards.sql"))
    df = con.execute("SELECT * FROM v_procurement_awards").pl()
    _assert_cols(
        df,
        "tender_id",
        "supplier",
        "supplier_norm",
        "supplier_class",
        "name_truncated",
        "contracting_authority",
        "cpv_code",
        "cpv_description",
        "award_date",
        "value_eur",
        "value_kind",
        "is_framework_or_dps",
        "value_shared_across_suppliers",
        "value_safe_to_sum",
        "is_call_off",
        "parent_agreement_id",
        # 2026-06-12 detail widening: title, classification fallback, competition detail,
        # pre-award estimate and the EU Official Journal deep links.
        "tender_title",
        "spend_category",
        "category_label",
        "contract_type",
        "procedure_type",
        "contract_duration_months",
        "n_bids_received",
        "n_sme_bids_received",
        "n_awarded_smes",
        "estimated_value_eur",
        "additional_cpv_codes",
        "ted_notice_link",
        "ted_can_link",
    )
    assert len(df) == 10  # raw passthrough — every award×supplier row, nothing filtered

    by_supplier = {r["supplier"]: r for r in df.to_dicts()}

    # DD/MM/YYYY parsed to a real DATE (TRY_STRPTIME)
    from datetime import date as _date

    assert by_supplier["Mason & Sons Ltd"]["award_date"] == _date(2023, 4, 4)

    # Detail fields: source strings TRY_CAST to honest ints; title/links/estimate carried.
    t001 = next(r for r in df.to_dicts() if r["tender_id"] == "T001")
    assert t001["tender_title"] == "N4 Road Improvement Works – Phase 2"
    assert t001["procedure_type"] == "Open Procedure"
    assert t001["contract_duration_months"] == 24
    assert t001["n_bids_received"] == 5
    assert t001["n_sme_bids_received"] == 3
    assert t001["n_awarded_smes"] == 1
    assert t001["estimated_value_eur"] == 120000.0
    assert t001["ted_can_link"].startswith("https://ted.europa.eu/")
    # category_label: CPV description wins when present…
    assert t001["category_label"] == "Construction work"
    # …and falls back to the OGP Spend Category when the row has no CPV (~70% of corpus).
    nullid = by_supplier["Nullid Co Ltd"]
    assert nullid["cpv_code"] is None
    assert nullid["category_label"] == "Information and Communication Technology"

    # Entity-split fix CONTRACT: a name with '&' survives whole — never fragmented
    # into "Mason" + "Sons Ltd". Guards against an ETL regression reaching gold.
    assert "Mason & Sons Ltd" in by_supplier
    assert "&" in by_supplier["Mason & Sons Ltd"]["supplier"]
    assert not any(r["supplier"] in {"Sons Ltd", "Company", "Co. Limited"} for r in df.to_dicts())

    # Tender ID literal "NULL" is now an honest null (2026-06-03 fix)
    assert by_supplier["Nullid Co Ltd"]["tender_id"] is None

    # A framework/DPS row is carried but flagged unsummable
    bigco = by_supplier["Bigco Services Ltd"]
    assert bigco["is_framework_or_dps"] is True
    assert bigco["value_safe_to_sum"] is False


@pytest.mark.sql
def test_v_procurement_supplier_summary_value_semantics():
    _fixture_only()
    _skip_missing(*_src(_PROC_AWARDS, _PROC_CRO, _PROC_OVERLAP))
    con = _con()
    con.execute(_load("procurement_supplier_summary.sql"))
    df = con.execute("SELECT * FROM v_procurement_supplier_summary").pl()
    _assert_cols(
        df,
        "supplier",
        "supplier_norm",
        "n_awards",
        "n_authorities",
        "awarded_value_safe_eur",
        "company_num",
        "company_status",
        "cro_match_method",
        "on_lobbying_register",
        "lobbying_returns",
        "is_lobbying_registrant",
        "is_lobbying_client",
        "has_epa_licence",
    )
    by = {r["supplier_norm"]: r for r in df.to_dicts()}

    # Privacy + quality exclusions: sole-trader and name_truncated never rank.
    assert "joemurphy" not in by, "sole trader leaked into supplier ranking"
    assert "eloittetruncnorm" not in by, "name_truncated supplier leaked into ranking"
    assert len(df) == 7

    # Clean multi-award supplier sums only its safe rows.
    acme = by["acmeconstructionltd"]
    assert acme["n_awards"] == 2
    assert acme["n_authorities"] == 2
    assert acme["awarded_value_safe_eur"] == 300000.0
    assert acme["company_num"] == 123456
    assert acme["company_status"] == "Normal"
    assert acme["on_lobbying_register"] is False
    # EPA flag (PR 4): the EPA fixture licenses ONLY company_num 123456 (acme), with
    # a second row at n_licences=0 that must NOT count. So exactly acme is flagged.
    assert acme["has_epa_licence"] is True, "CRO-matched EPA-licensed company must be flagged"
    assert sum(1 for r in df.to_dicts() if r["has_epa_licence"]) == 1, "only the n_licences>0 company is flagged"

    # KEY: a framework CEILING is counted but contributes ZERO to the value sum.
    bigco = by["bigcoservicesltd"]
    assert bigco["n_awards"] == 1
    assert bigco["awarded_value_safe_eur"] == 0.0

    # KEY: a value shared across co-suppliers on one tender is NOT summed.
    assert by["sharedcoaltd"]["awarded_value_safe_eur"] == 0.0
    # KEY: a NULL-Tender-ID row can't be verified unshared → not summed.
    assert by["nullidcoltd"]["awarded_value_safe_eur"] == 0.0

    # Lobbying overlap folded in: variant rows (registrant+client) aggregate per
    # supplier_norm — returns SUM to 8, both side flags true, value still 400k.
    lob = by["lobbycoltd"]
    assert lob["awarded_value_safe_eur"] == 400000.0
    assert lob["on_lobbying_register"] is True
    assert lob["lobbying_returns"] == 8  # 5 (registrant) + 3 (client)
    assert lob["is_lobbying_registrant"] is True
    assert lob["is_lobbying_client"] is True

    # Ordered by n_awards DESC → Acme (2 awards) leads.
    assert df["n_awards"].to_list()[0] == 2


@pytest.mark.sql
def test_v_procurement_authority_summary_value_semantics():
    _fixture_only()
    _skip_missing(*_src(_PROC_AWARDS))
    con = _con()
    con.execute(_load("procurement_authority_summary.sql"))
    df = con.execute("SELECT * FROM v_procurement_authority_summary").pl()
    _assert_cols(df, "contracting_authority", "n_awards", "n_suppliers", "awarded_value_safe_eur")
    by = {r["contracting_authority"]: r for r in df.to_dicts()}
    assert len(df) == 8

    # Two safe awards (Acme 100k + Lobbyco 400k) to two distinct suppliers.
    dcc = by["Dublin City Council"]
    assert dcc["n_awards"] == 2
    assert dcc["n_suppliers"] == 2
    assert dcc["awarded_value_safe_eur"] == 500000.0

    # Framework-only and shared-value-only authorities sum to ZERO.
    assert by["Health Service Executive"]["awarded_value_safe_eur"] == 0.0
    assert by["Office of Public Works (OPW)"]["awarded_value_safe_eur"] == 0.0


@pytest.mark.sql
def test_v_procurement_cpv_summary_value_semantics():
    _fixture_only()
    _skip_missing(*_src(_PROC_AWARDS))
    con = _con()
    con.execute(_load("procurement_cpv_summary.sql"))
    df = con.execute("SELECT * FROM v_procurement_cpv_summary").pl()
    _assert_cols(df, "cpv_code", "cpv_description", "n_awards", "n_suppliers", "awarded_value_safe_eur")
    by = {r["cpv_code"]: r for r in df.to_dicts()}
    # 4 real CPV groups; the CPV-less Nullid row (spend-category-only, like ~70% of the
    # corpus) must NOT grow a null/"NULL" bucket here.
    assert len(df) == 4
    assert None not in by

    construction = by["45000000"]
    assert construction["cpv_description"] == "Construction work"
    assert construction["n_awards"] == 3  # Acme×2 + Mason
    assert construction["awarded_value_safe_eur"] == 450000.0  # 100k + 200k + 150k

    # Business services: Bigco framework (5m) excluded; only eloitte 75k + Lobbyco 400k.
    assert by["79000000"]["awarded_value_safe_eur"] == 475000.0


# ---------------------------------------------------------------------------
# EXPERIMENTAL real-terms (CPI-adjusted) procurement views. v_cpi_deflator must
# register before v_procurement_awards_real (it LEFT JOINs the deflator); the
# alphabetical loader handles that via the procurement_aa_* filename. These run in
# both fixture and integration mode (the invariants + parity hold for either data).
# ---------------------------------------------------------------------------


def _load_awards_real(con):
    """Register the deflator views then v_procurement_awards_real (dependency order:
    v_cpi_deflator + v_scsi_tpi_deflator must precede the awards view that LEFT JOINs them)."""
    con.execute(_load("procurement_aa_cpi_deflator.sql"))
    con.execute(_load("procurement_ab_scsi_tpi_deflator.sql"))
    con.execute(_load("procurement_awards_real.sql"))


@pytest.mark.sql
def test_v_procurement_awards_real_invariants():
    """Additive real-terms columns + the never-break rules: ceilings/implausible/year-missing/
    no-value never carry a real figure, base-year is identity, and value_eur_real is non-NULL
    IFF real_caveat is OK/MULTI_YEAR_APPROX (one rule every consumer can trust)."""
    _skip_missing(*_src(_PROC_AWARDS, _PROC_DEFLATOR, _PROC_TPI))
    con = _con()
    _load_awards_real(con)
    df = con.execute("SELECT * FROM v_procurement_awards_real").pl()
    _assert_cols(
        df,
        "value_eur",
        "value_eur_real",
        "real_base_year",
        "deflator_index",
        "deflator_factor",
        "real_caveat",
        "award_year",
    )
    assert len(df) > 0
    # the framework/DPS ceiling in the fixture (Bigco, 5m) is never deflated
    assert (
        con.execute(
            "SELECT count(*) FROM v_procurement_awards_real WHERE is_framework_or_dps AND value_eur_real IS NOT NULL"
        ).fetchone()[0]
        == 0
    )
    # any non-adjustable caveat ⇒ NULL real value
    assert (
        con.execute(
            "SELECT count(*) FROM v_procurement_awards_real "
            "WHERE real_caveat IN ('NO_VALUE','CEILING_NOT_ADJUSTED','IMPLAUSIBLE','YEAR_MISSING') "
            "AND value_eur_real IS NOT NULL"
        ).fetchone()[0]
        == 0
    )
    # value_eur_real non-NULL IFF caveat adjustable
    assert (
        con.execute(
            "SELECT count(*) FROM v_procurement_awards_real "
            "WHERE (real_caveat IN ('OK','MULTI_YEAR_APPROX')) != (value_eur_real IS NOT NULL)"
        ).fetchone()[0]
        == 0
    )
    # base-year awards: real == nominal (factor 1.0)
    base = con.execute("SELECT base_year FROM v_cpi_deflator LIMIT 1").fetchone()[0]
    assert (
        con.execute(
            f"SELECT count(*) FROM v_procurement_awards_real WHERE award_year = {base} "
            "AND value_eur_real IS NOT NULL AND abs(value_eur_real - value_eur) > 0.005"
        ).fetchone()[0]
        == 0
    )
    # deflating to the latest base year never shrinks a past value (factor >= 1.0 ⇒ real >= nominal)
    assert (
        con.execute(
            "SELECT count(*) FROM v_procurement_awards_real WHERE value_eur_real IS NOT NULL "
            "AND deflator_factor >= 1.0 AND value_eur_real < value_eur - 0.005"
        ).fetchone()[0]
        == 0
    )
    # sector lens: construction CPVs (45*/71*) are tagged to the SCSI TPI, everything else to CPI
    _assert_cols(df, "value_eur_real_sector", "deflator_index_sector")
    assert (
        con.execute(
            "SELECT count(*) FROM v_procurement_awards_real WHERE "
            "(substr(cpv_code,1,2) IN ('45','71')) != (deflator_index_sector = 'SCSI_TPI_CONSTRUCTION')"
        ).fetchone()[0]
        == 0
    )


@pytest.mark.sql
def test_v_procurement_awards_real_parity_with_deflator_function():
    """SQL value_eur_real == services.deflator.Deflator.inflate, row-for-row — the single
    source-of-truth contract (the view precomputes exactly what the tested function computes).
    Deflators are loaded from the SAME parquets the views read, so parity holds in either mode —
    both the CPI baseline column and the construction-sector (SCSI TPI) column."""
    _skip_missing(*_src(_PROC_AWARDS, _PROC_DEFLATOR, _PROC_TPI))
    from services.deflator import Deflator

    gold = _src(_PROC_DEFLATOR)[0].parent
    cpi = Deflator.load_index("CSO_CPA07_CPI", gold_dir=gold)
    tpi = Deflator.load_index("SCSI_TPI_CONSTRUCTION", gold_dir=gold)
    con = _con()
    _load_awards_real(con)
    # CPI baseline column
    for value_eur, award_year, sql_real in con.execute(
        "SELECT value_eur, award_year, value_eur_real FROM v_procurement_awards_real WHERE value_eur_real IS NOT NULL"
    ).fetchall():
        py = cpi.inflate(value_eur, award_year)
        assert py is not None and abs(sql_real - py) <= 1e-9 * max(1.0, abs(py))
    # sector column: construction rows must match the SCSI TPI function exactly
    crows = con.execute(
        "SELECT value_eur, award_year, value_eur_real_sector FROM v_procurement_awards_real "
        "WHERE value_eur_real_sector IS NOT NULL AND substr(cpv_code,1,2) IN ('45','71')"
    ).fetchall()
    assert crows, "expected at least one adjustable construction award"
    for value_eur, award_year, sql_sector in crows:
        py = tpi.inflate(value_eur, award_year)
        assert py is not None and abs(sql_sector - py) <= 1e-9 * max(1.0, abs(py))


@pytest.mark.sql
def test_v_procurement_cpv_summary_real_band():
    """The per-CPV benchmark exposes a nominal band AND an inflation-adjusted band, with an
    honest n_real_excluded for awards whose year fell outside the index."""
    _skip_missing(*_src(_PROC_AWARDS, _PROC_DEFLATOR, _PROC_TPI))
    con = _con()
    _load_awards_real(con)
    con.execute(_load("procurement_cpv_summary_real.sql"))
    df = con.execute("SELECT * FROM v_procurement_cpv_summary_real").pl()
    _assert_cols(
        df,
        "cpv_code",
        "cpv_description",
        "median_award_eur",
        "median_award_real_eur",
        "p25_award_real_eur",
        "p75_award_real_eur",
        "n_real_excluded",
        "real_base_year",
        "deflator_index",
        "median_award_real_sector_eur",
        "deflator_index_sector",
    )
    assert len(df) > 0
    # sector index is the construction tender-price index for 45*/71* CPVs, CPI otherwise
    for code, idx in con.execute(
        "SELECT cpv_code, deflator_index_sector FROM v_procurement_cpv_summary_real"
    ).fetchall():
        assert idx == ("SCSI_TPI_CONSTRUCTION" if str(code)[:2] in ("45", "71") else "CSO_CPA07_CPI")
    # The real band is computed over a SUBSET of the nominal band (non-adjustable awards —
    # year outside the index / implausible — are dropped), so the two medians are NOT directly
    # comparable; n_real_excluded reconciles the sample sizes exactly. (This is why the UI must
    # show n_real_excluded beside a nominal-vs-real band.)
    for n_nom, n_real, n_excl in con.execute(
        "SELECT n_awards_valued, n_awards_valued_real, n_real_excluded FROM v_procurement_cpv_summary_real"
    ).fetchall():
        assert n_real <= n_nom
        assert n_excl == n_nom - n_real


@pytest.mark.sql
def test_v_govt_consumption_deflator_executes():
    """The agency-standard public-spend deflator, exposed as a view (base-year factor == 1.0)."""
    _skip_missing(*_src(_PROC_GOV_DEFLATOR))
    con = _con()
    con.execute(_load("procurement_ac_govt_consumption_deflator.sql"))
    df = con.execute("SELECT * FROM v_govt_consumption_deflator").pl()
    _assert_cols(df, "year", "gov_price_index", "deflator_to_base", "base_year", "index_code")
    assert len(df) > 0
    base = con.execute("SELECT base_year FROM v_govt_consumption_deflator LIMIT 1").fetchone()[0]
    assert (
        con.execute(f"SELECT deflator_to_base FROM v_govt_consumption_deflator WHERE year = {base}").fetchone()[0]
        == 1.0
    )


def _load_payments_real(con):
    """Register the gov-consumption deflator + payments view + payments-real (dependency order)."""
    con.execute(_load("procurement_ac_govt_consumption_deflator.sql"))
    con.execute(_load("procurement_payments.sql"))
    con.execute(_load("procurement_payments_real.sql"))


@pytest.mark.sql
def test_v_procurement_payments_real_invariants_and_parity():
    """Public spend deflated by the GOVERNMENT-CONSUMPTION index (not CPI). Integration-only (no
    synthetic payments fixture). Parity SQL == Deflator.load_index('CSO_GOV_CONSUMPTION'); non-OK
    caveat ⇒ NULL real."""
    if not _USE_REAL_PATHS:
        pytest.skip("payments fact has no fixture (set DAIL_INTEGRATION_TESTS=1)")
    _skip_missing(*_src(_PROC_PAYMENTS, _PROC_GOV_DEFLATOR))
    from services.deflator import Deflator

    g = Deflator.load_index("CSO_GOV_CONSUMPTION", gold_dir=_src(_PROC_GOV_DEFLATOR)[0].parent)
    con = _con()
    _load_payments_real(con)
    df = con.execute("SELECT * FROM v_procurement_payments_real LIMIT 5").pl()
    _assert_cols(df, "amount_eur", "amount_eur_real", "real_base_year", "deflator_index", "real_caveat")
    assert (
        con.execute(
            "SELECT count(*) FROM v_procurement_payments_real WHERE real_caveat <> 'OK' AND amount_eur_real IS NOT NULL"
        ).fetchone()[0]
        == 0
    )
    rows = con.execute(
        "SELECT amount_eur, year, amount_eur_real FROM v_procurement_payments_real "
        "WHERE amount_eur_real IS NOT NULL USING SAMPLE 500 ROWS"
    ).fetchall()
    assert rows
    for amount, year, sql_real in rows:
        py = g.inflate(amount, year)
        assert py is not None and abs(sql_real - py) <= 1e-9 * max(1.0, abs(py))


@pytest.mark.sql
def test_v_procurement_payments_real_by_year_keeps_tiers_separate():
    """The real-terms spend totals never collapse SPENT vs COMMITTED (or VAT bases): the grain is
    one row per (year, realisation_tier, vat_status)."""
    if not _USE_REAL_PATHS:
        pytest.skip("payments fact has no fixture (set DAIL_INTEGRATION_TESTS=1)")
    _skip_missing(*_src(_PROC_PAYMENTS, _PROC_GOV_DEFLATOR))
    con = _con()
    _load_payments_real(con)
    con.execute(_load("procurement_payments_real_by_year.sql"))
    df = con.execute("SELECT * FROM v_procurement_payments_real_by_year").pl()
    _assert_cols(df, "year", "realisation_tier", "vat_status", "total_nominal_eur", "total_real_eur", "n_real_excluded")
    assert len(df) > 0
    # grain integrity: exactly one row per (year, tier, vat) — tiers are never summed together
    assert (
        con.execute(
            "SELECT count(*) - count(DISTINCT (year, realisation_tier, vat_status)) "
            "FROM v_procurement_payments_real_by_year"
        ).fetchone()[0]
        == 0
    )


@pytest.mark.sql
def test_v_procurement_lobbying_overlap_executes():
    _fixture_only()
    _skip_missing(*_src(_PROC_OVERLAP))
    con = _con()
    con.execute(_load("procurement_lobbying_overlap.sql"))
    df = con.execute("SELECT * FROM v_procurement_lobbying_overlap").pl()
    _assert_cols(
        df,
        "lobby_name",
        "lobby_side",
        "supplier",
        "supplier_norm",
        "n_lobby_returns",
        "n_award_rows",
        "n_authorities",
        "awarded_value_safe_eur",
    )
    # Passthrough: one row per matched lobbying entity (registrant + client variant).
    assert len(df) == 2
    assert set(df["supplier"].to_list()) == {"Lobbyco Ltd"}
    # Anomaly #3 is INTENTIONAL in this two-keyed table: a naive row-sum
    # double-counts the same supplier's awarded value. Lock that so a consumer
    # never SUM()s this column without deduping by supplier first.
    assert df["awarded_value_safe_eur"].sum() == 800000.0  # 2 × 400k — NOT the true 400k


@pytest.mark.sql
def test_v_lobbying_org_procurement_dedups_to_registrant():
    _fixture_only()
    _skip_missing(*_src(_PROC_OVERLAP))
    con = _con()
    con.execute(_load("lobbying_org_procurement.sql"))
    df = con.execute("SELECT * FROM v_lobbying_org_procurement").pl()
    _assert_cols(df, "lobbyist_name", "supplier", "n_awards", "n_authorities", "awarded_value_safe_eur")
    # Registrant-side only (the client variant is filtered out), grouped per name.
    assert len(df) == 1
    row = df.to_dicts()[0]
    assert row["lobbyist_name"] == "Lobbyco Limited"
    assert row["n_awards"] == 1
    assert row["n_authorities"] == 1
    assert row["awarded_value_safe_eur"] == 400000.0


@pytest.mark.sql
def test_v_procurement_charity_overlap_grain_and_value_firewall():
    """Charity ↔ procurement co-occurrence, linked by a HARD CRO company number
    (charity_resolved.cro_number == supplier_cro_match.company_num). Locks the
    column contract the linkage surface reads, the one-row-per-(rcn, supplier_norm)
    grain (no fan-out), and the money-grain firewall: awarded_value_safe_eur is
    never negative and never sums more rows than the summable-award subset, so a
    framework/DPS ceiling can never inflate a charity's apparent award value.

    Integration-data only — this cross-domain join has no synthetic fixture (the
    registration smoke test already proves it parses/binds in the CI fixture run)."""
    if not _USE_REAL_PATHS:
        pytest.skip("charity×procurement overlap has no fixture (set DAIL_INTEGRATION_TESTS=1)")
    _skip_missing(
        *_src(_PROC_AWARDS, _PROC_CRO),
        SILVER_DIR / "charities" / "charity_resolved.parquet",
    )
    con = _con()
    con.execute(_load("procurement_charity_overlap.sql"))
    df = con.execute("SELECT * FROM v_procurement_charity_overlap").pl()
    _assert_cols(
        df,
        "rcn",
        "registered_charity_name",
        "company_num",
        "supplier_norm",
        "matched_supplier_name",
        "n_awards",
        "n_authorities",
        "awarded_value_safe_eur",
        "n_value_safe_awards",
        "n_ceiling_notices",
        "gov_funded_share_latest",
        "state_adjacent_flag",
    )

    # Grain: strictly one row per (rcn, supplier_norm) — a name-variant fan-out
    # would silently double-count a charity's award footprint.
    n, distinct = con.execute(
        "SELECT count(*), count(DISTINCT (rcn, supplier_norm)) FROM v_procurement_charity_overlap"
    ).fetchone()
    assert n == distinct, f"not one-row-per-(rcn, supplier_norm): {n} rows, {distinct} distinct"

    # Money-grain firewall: safe value never negative; the summable-award count
    # never exceeds the total award count (a ceiling notice can't be summed).
    bad = con.execute(
        "SELECT count(*) FROM v_procurement_charity_overlap "
        "WHERE awarded_value_safe_eur < 0 OR n_value_safe_awards > n_awards"
    ).fetchone()[0]
    assert bad == 0, "value firewall violated (negative safe value or safe>total awards)"

    # The link is a hard CRO identifier — company_num must always be present.
    assert df["company_num"].null_count() == 0


_TED_AWARDS_SILVER = "data/silver/parquet/ted_ie_awards.parquet"
_TED_TENDERS_SILVER = "data/silver/parquet/ted_ie_tenders.parquet"
_TED_WINNER_HISTORY_SILVER = "data/silver/parquet/ted_ie_winner_history.parquet"


@pytest.mark.sql
def test_v_procurement_ted_awards_competition_columns():
    """The TED award view must expose the eForms competition-intensity columns, and they must
    be internally consistent: is_single_bid is exactly (n_tenders_received == 1), tender counts
    are never < 1, and the flags stay boolean/null. Integration-data only (silver is gitignored,
    eForms-only so populated from ~2024)."""
    if not _USE_REAL_PATHS:
        pytest.skip("TED silver is gitignored and unfixtured (set DAIL_INTEGRATION_TESTS=1)")
    _skip_missing(*_src(_TED_AWARDS_SILVER))
    con = _con()
    con.execute(_load("procurement_ted_awards.sql"))
    df = con.execute("SELECT * FROM v_procurement_ted_awards LIMIT 5").pl()
    _assert_cols(
        df,
        "procedure_type",
        "is_uncompetitive_procedure",
        "n_tenders_received",
        "is_single_bid",
        "award_criteria_kind",
        "is_price_only",
    )
    # no nonsensical tender counts
    bad = con.execute(
        "SELECT count(*) FROM v_procurement_ted_awards WHERE n_tenders_received IS NOT NULL AND n_tenders_received < 1"
    ).fetchone()[0]
    assert bad == 0, "tenders-received below 1 — taxonomy/aggregation bug"
    # single-bid is exactly (min tenders == 1), wherever a count exists
    inconsistent = con.execute(
        "SELECT count(*) FROM v_procurement_ted_awards WHERE n_tenders_received IS NOT NULL "
        "AND is_single_bid <> (n_tenders_received = 1)"
    ).fetchone()[0]
    assert inconsistent == 0, "is_single_bid does not match (n_tenders_received = 1)"


@pytest.mark.sql
def test_v_procurement_ted_winner_history_union():
    """The full winner-history view UNIONs the 2024+ API lane and the 2016-2023 per-notice-XML
    backfill into one (notice x winner) feed. Both silvers gitignored → integration-data only."""
    if not _USE_REAL_PATHS:
        pytest.skip("TED silver is gitignored and unfixtured (set DAIL_INTEGRATION_TESTS=1)")
    _skip_missing(*_src(_TED_AWARDS_SILVER), *_src(_TED_WINNER_HISTORY_SILVER))
    con = _con()
    con.execute(_load("procurement_ted_awards_history.sql"))
    df = con.execute("SELECT * FROM v_procurement_ted_winner_history LIMIT 5").pl()
    _assert_cols(df, "source_lane", "winner_join_norm", "winner_name", "value_safe_to_sum", "procedure_type")
    # both ingestion lanes are present
    lanes = {r[0] for r in con.execute("SELECT DISTINCT source_lane FROM v_procurement_ted_winner_history").fetchall()}
    assert lanes == {"api", "per_notice_xml"}, f"unexpected lanes: {lanes}"
    # boundary dedupe: no publication_number may appear in BOTH lanes
    dup = con.execute(
        "SELECT count(*) FROM (SELECT publication_number FROM v_procurement_ted_winner_history "
        "GROUP BY 1 HAVING count(DISTINCT source_lane) > 1)"
    ).fetchone()[0]
    assert dup == 0, "publication_number present in both lanes — boundary dedupe failed"
    # the eForms competition fields exist only on the 2024+ API lane
    leaked = con.execute(
        "SELECT count(*) FROM v_procurement_ted_winner_history "
        "WHERE source_lane = 'per_notice_xml' AND procedure_type IS NOT NULL"
    ).fetchone()[0]
    assert leaked == 0, "competition field populated on a pre-2024 (legacy) row"
    # winner_name _NNNNN eForms suffix is stripped for display
    suffix = con.execute(
        r"SELECT count(*) FROM v_procurement_ted_winner_history WHERE regexp_matches(winner_name, '_[0-9]+$')"
    ).fetchone()[0]
    assert suffix == 0, "winner_name _NNNNN suffix not stripped"
    summable = con.execute("SELECT count(*) FROM v_procurement_ted_winner_history WHERE value_safe_to_sum").fetchone()[
        0
    ]
    assert summable == 0, "a TED award value was marked summable"


@pytest.mark.sql
def test_v_procurement_ted_tenders_pre_award_grain():
    """The TED tender-pipeline view (cn-standard) is a pre-award grain: value_safe_to_sum must
    be FALSE on every row (estimates are never summable across grains), value_kind is the
    pre-award marker, and the contract columns the tab reads are present. Integration-data only."""
    if not _USE_REAL_PATHS:
        pytest.skip("TED tenders silver is gitignored and unfixtured (set DAIL_INTEGRATION_TESTS=1)")
    _skip_missing(*_src(_TED_TENDERS_SILVER))
    con = _con()
    con.execute(_load("procurement_ted_tenders.sql"))
    df = con.execute("SELECT * FROM v_procurement_ted_tenders LIMIT 5").pl()
    _assert_cols(
        df,
        "publication_number",
        "buyer_name",
        "cpv_division",
        "procedure_type",
        "submission_deadline",
        "is_still_open",
        "estimated_value_eur",
        "value_safe_to_sum",
    )
    assert con.execute("SELECT count(*) FROM v_procurement_ted_tenders").fetchone()[0] > 0
    # FIREWALL: a pre-award estimate is never summable — not one row may be value_safe_to_sum.
    summable = con.execute("SELECT count(*) FROM v_procurement_ted_tenders WHERE value_safe_to_sum").fetchone()[0]
    assert summable == 0, "a tender estimate was marked value_safe_to_sum — three-grain firewall breach"
    # one row per notice (no fan-out)
    n, distinct = con.execute(
        "SELECT count(*), count(DISTINCT publication_number) FROM v_procurement_ted_tenders"
    ).fetchone()
    assert n == distinct, f"tenders view not one-row-per-notice: {n} rows, {distinct} distinct"


# --- NULL/EMPTY-STRING regression guard (2026-06-11 audit): procurement
# authority dirty-value filters — see test_views_legislation.py for the
# sibling guard against the same audit. ---


def _write_dirty_awards_fixture(tmp_path):
    """procurement_awards.parquet with the literal dirty authority values the
    eTenders source emits ('NULL', ''), plus an undated row."""
    import polars as pl

    pdir = tmp_path / "data" / "gold" / "parquet"
    pdir.mkdir(parents=True)
    df = pl.DataFrame(
        {
            "Contracting Authority": [
                "Dublin City Council",
                "Dublin City Council",
                "NULL",  # literal string — must be excluded by BOTH views
                "",  # empty string — must be excluded by BOTH views
                None,  # honest null — must be excluded by BOTH views
                "Health Service Executive",  # undated — summary only
            ],
            "Notice Published Date/Contract Created Date": [
                "01/02/2023",
                "15/03/2024",
                "01/02/2023",
                "01/02/2023",
                "01/02/2023",
                None,
            ],
            "supplier_norm": ["acme", "beta", "ghost", "ghost2", "ghost3", "gamma"],
            "value_eur": [100000.0, 50000.0, 1.0, 1.0, 1.0, 75000.0],
            "value_safe_to_sum": [True, True, True, True, True, True],
        }
    )
    df.write_parquet(pdir / "procurement_awards.parquet")


@pytest.mark.sql
def test_procurement_authority_views_agree_on_dirty_value_filters(tmp_path):
    """v_procurement_authority_summary and its per-year sibling must exclude the
    SAME dirty authority values ('', 'NULL', NULL). The year view once dropped
    only '' — harmless while gold is coerced upstream, but a silent universe
    split the moment a literal-NULL regresses. Dirty rows here make any future
    one-sided filter edit fail loudly."""
    _write_dirty_awards_fixture(tmp_path)
    con = _con()
    for fname in ("procurement_authority_summary.sql", "procurement_authority_year_summary.sql"):
        sql = _view_path(fname).read_text(encoding="utf-8")
        sql = sql.replace("'data/", f"'{tmp_path.as_posix()}/data/")  # mirror absolutize
        con.execute(sql)

    alltime = set(
        con.execute("SELECT contracting_authority FROM v_procurement_authority_summary").pl()["contracting_authority"]
    )
    yearly = con.execute("SELECT contracting_authority, year FROM v_procurement_authority_year_summary").pl()

    dirty = {"", "NULL", None}
    assert not (alltime & dirty), f"all-time view leaked dirty authorities: {alltime & dirty}"
    assert not (set(yearly["contracting_authority"]) & dirty), "year view leaked dirty authorities"

    # Same universe, modulo the documented difference: undated rows only exist all-time.
    assert alltime == {"Dublin City Council", "Health Service Executive"}
    assert set(yearly["contracting_authority"]) == {"Dublin City Council"}
    assert set(yearly["year"]) == {2023, 2024}


# --- v_procurement_expiring_contracts (TED advertised-term projection) ---


def _write_ted_awards_term_fixture(tmp_path):
    """Minimal ted_ie_awards.parquet rows covering every expiring-contracts rule."""
    import polars as pl

    pdir = tmp_path / "data" / "silver" / "parquet"
    pdir.mkdir(parents=True)
    base = {
        "notice_url": "u",
        "cpv_code": "45000000",
        "cpv_division": "Construction",
        "award_value_eur": 100000.0,
        "value_kind": "contract_award_value",
        "is_multi_supplier_framework": False,
        "is_pan_eu_outlier": False,
        "contract_conclusion_date": "2025-01-01",
        "contract_duration_months": 24.0,
        "renewal_max": None,
        "contract_end_date_est": "2027-01-01",
        "contract_end_basis": "conclusion_plus_duration",
        "dispatch_date": "2025-01-10",
        "year": 2025,
        "n_winners": 1,
        "supplier_class": "company",
    }
    rows = [
        # 1. plain company award with an estimate
        {**base, "publication_number": "1-2025", "buyer_name": "Dublin City Council_123", "winner_name": "Acme Ltd"},
        # 2. two winner-rows of ONE notice -> must collapse to one view row, names joined
        {**base, "publication_number": "2-2025", "buyer_name": "HSE", "winner_name": "Alpha Ltd", "n_winners": 2},
        {**base, "publication_number": "2-2025", "buyer_name": "HSE", "winner_name": "Beta Ltd", "n_winners": 2},
        # 3. sole-trader winner -> notice listed, name WITHHELD
        {
            **base,
            "publication_number": "3-2025",
            "buyer_name": "OPW",
            "winner_name": "Jane Bloggs",
            "supplier_class": "sole_trader_or_individual",
        },
        # 4. pan-EU outlier -> EXCLUDED entirely
        {
            **base,
            "publication_number": "4-2025",
            "buyer_name": "GÉANT",
            "winner_name": "MegaCo",
            "is_pan_eu_outlier": True,
        },
        # 5. no end estimate -> EXCLUDED
        {
            **base,
            "publication_number": "5-2025",
            "buyer_name": "Revenue",
            "winner_name": "NoTerm Ltd",
            "contract_end_date_est": None,
            "contract_end_basis": None,
        },
    ]
    pl.DataFrame(rows).write_parquet(pdir / "ted_ie_awards.parquet")


@pytest.mark.sql
def test_v_procurement_expiring_contracts_contract(tmp_path):
    """Locks the signal's honesty rules: notice grain (no winner-row inflation),
    sole-trader names withheld but notice kept, pan-EU outliers and no-estimate
    rows excluded, buyer-name artefact cleanup, basis carried for display."""
    _write_ted_awards_term_fixture(tmp_path)
    sql = _view_path("procurement_expiring_contracts.sql").read_text(encoding="utf-8")
    sql = sql.replace("'data/", f"'{tmp_path.as_posix()}/data/")  # mirror absolutize
    con = _con()
    con.execute(sql)
    df = con.execute("SELECT * FROM v_procurement_expiring_contracts ORDER BY publication_number").pl()

    assert df.height == 3, "expected notices 1,2,3 only (pan-EU + no-estimate excluded)"
    by = {r["publication_number"]: r for r in df.to_dicts()}
    assert set(by) == {"1-2025", "2-2025", "3-2025"}

    # buyer artefact suffix stripped; winners aggregated to one row per notice
    assert by["1-2025"]["buyer_name"] == "Dublin City Council"
    assert sorted(by["2-2025"]["winners_display"].split("; ")) == ["Alpha Ltd", "Beta Ltd"]

    # privacy: the sole trader's name never appears; the notice itself survives
    assert by["3-2025"]["winners_display"] is None
    assert "Jane Bloggs" not in str(df.to_dicts())

    # the estimate's provenance is carried for honest display
    assert by["1-2025"]["contract_end_basis"] == "conclusion_plus_duration"
    assert str(by["1-2025"]["contract_end_date_est"]) == "2027-01-01"
