"""Procurement retrieval — per-LA audited Annual Financial Statements + adopted budgets — the BUDGET/OUTTURN grain.

Split from the single 1,6xx-line queries/procurement.py by MONEY GRAIN
(2026-07-18) so the never-sum boundaries are module boundaries. Import surface is
unchanged: ``from dail_tracker_core.queries import procurement`` re-exports every
function; grain-shared constants live in ``._shared``.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import make_runner
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)

_run = make_runner("procurement", _log)


# ── AFS (per-LA audited Annual Financial Statement) — the BUDGET/accounts grain ──────────
# A SIBLING context fact for the local-authority dossier: the council's total audited revenue
# spend by service division (the denominator the named-supplier PO/payment slice sits inside).
# NEVER summed or unioned with PO/payment or award euros — different grain (see the view headers).
def afs_total_by_year(conn: duckdb.DuckDBPyConnection, council: str) -> QueryResult:
    """One council's audited REVENUE-account spend per year (2016–2025 where filed) — the
    "council accounts, all spending" spine of the local-authority dossier. gross_expenditure_eur
    is Σ operating expenditure by service (a budget actual, never the PO/award grain). Pre-
    aggregated in the view; the page renders, never computes."""
    return _run(
        conn,
        "SELECT year, gross_expenditure_eur, income_eur, net_expenditure_eur,"
        " n_divisions, printed_total_eur, reconciled, parser"
        " FROM v_procurement_afs_total_by_year WHERE council = ? ORDER BY year",
        [council],
    )


def afs_by_division(conn: duckdb.DuckDBPyConnection, council: str, year: int) -> QueryResult:
    """One council-year's revenue spending by service division (Housing / Roads / …), largest
    NET COST first — the "where your money goes" breakdown. Net cost (gross minus the service's
    own income/grants) is what the local taxpayer actually funds, so it leads; gross + the
    pipeline-computed pct_self_funded ride alongside. Display passthrough of the reconcile-gated
    fact; gross is operating expenditure by division, never the council's headline total."""
    return _run(
        conn,
        "SELECT division, gross_expenditure_eur, income_eur, net_expenditure_eur,"
        " pct_self_funded, reconciled"
        " FROM v_procurement_afs_by_division WHERE council = ? AND year = ?"
        " ORDER BY net_expenditure_eur DESC",
        [council, int(year)],
    )


def la_budget_vs_actual(conn: duckdb.DuckDBPyConnection, council: str) -> QueryResult:
    """One council's ADOPTED budget set beside its audited AFS outturn per (year, division) —
    the plan-vs-actual layer of the RUNNING lane. Two different money grains (BUDGETED plan vs
    accounts actual) joined SIDE-BY-SIDE in v_procurement_la_budget_vs_actual; the delta is
    computed in the view, never here or in the page, and is context — not an overspend verdict."""
    return _run(
        conn,
        "SELECT year, division, budget_expenditure_eur, afs_gross_expenditure_eur,"
        " outturn_minus_budget_eur, outturn_vs_budget_pct"
        " FROM v_procurement_la_budget_vs_actual WHERE council = ?"
        " ORDER BY year DESC, budget_expenditure_eur DESC",
        [council],
    )


def la_budget_by_division(conn: duckdb.DuckDBPyConnection, council: str) -> QueryResult:
    """One council's ADOPTED annual budget by service division, every published year —
    the pure BUDGETED grain (a plan, not spend), unlike la_budget_vs_actual this is NOT
    limited to years with an audited outturn to join to, so the newest adopted budget
    (published before its AFS exists) is present. NEVER summed with any other grain."""
    return _run(
        conn,
        "SELECT year, division, expenditure_adopted_eur, income_adopted_eur, source_url"
        " FROM v_procurement_la_budget_divisions WHERE council = ?"
        " ORDER BY year DESC, expenditure_adopted_eur DESC",
        [council],
    )


def afs_capital_by_year(conn: duckdb.DuckDBPyConnection, council: str) -> QueryResult:
    """One council's audited CAPITAL-account investment per year — the "what your council is
    building / acquiring" spine of the dossier's BUILDING lane. capital_expenditure_eur is Σ
    investment by service that year (a distinct fact: never summed with revenue net cost, PO/
    payment or award euros). Pre-aggregated in the view; the page renders, never computes."""
    return _run(
        conn,
        "SELECT year, capital_expenditure_eur, capital_income_eur, n_divisions, reconciled, parser"
        " FROM v_procurement_afs_capital_by_year WHERE council = ? ORDER BY year",
        [council],
    )


def afs_capital_by_division(conn: duckdb.DuckDBPyConnection, council: str, year: int) -> QueryResult:
    """One council-year's CAPITAL investment by service division (largest first) — the build/
    acquire programme, dominated by central housing grants. Display passthrough of the reconcile-
    gated fact; a distinct grain, never summed with the revenue or PO/award euros."""
    return _run(
        conn,
        "SELECT division, capital_expenditure_eur, capital_income_eur, reconciled"
        " FROM v_procurement_afs_capital_by_division WHERE council = ? AND year = ?"
        " ORDER BY capital_expenditure_eur DESC",
        [council, int(year)],
    )


def afs_coverage_by_council(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Per-council AFS corpus coverage — which of the 31 councils have audited accounts loaded,
    which years, and the reconciliation state. The scope-guard row set for 'is council X's AFS
    data here' before quoting a euro figure; also doubles as the council-label lookup for
    afs_total_by_year/afs_vs_po_coverage. latest_*_expenditure_eur is that council's most recent
    filed year only — never summed across councils or years."""
    return _run(
        conn,
        "SELECT council, region, COUNT(DISTINCT year) AS n_years, MIN(year) AS first_year,"
        " MAX(year) AS last_year, bool_and(reconciled) AS all_reconciled,"
        " arg_max(gross_expenditure_eur, year) AS latest_gross_expenditure_eur,"
        " arg_max(net_expenditure_eur, year) AS latest_net_expenditure_eur"
        " FROM v_procurement_afs_total_by_year GROUP BY council, region ORDER BY council",
    )


def afs_vs_po_coverage(conn: duckdb.DuckDBPyConnection, council: str, *, year: int | None = None) -> QueryResult:
    """Audited revenue spend (AFS) vs the slice traceable to named >€20k suppliers (POs), per
    year. Carries both tiers' PO totals and both pct_* ratios (the page reads the tier the
    council publishes). INDICATIVE ratio only — different thresholds/stages/grain, not a
    reconciliation (see the view header). ``year=None`` returns every year for the council."""
    sql = (
        "SELECT year, afs_gross_eur, afs_net_eur, po_spent_safe_eur, po_committed_safe_eur,"
        " n_spent_lines, n_committed_lines, n_named_suppliers, pct_spent_of_gross, pct_committed_of_gross"
        " FROM v_procurement_afs_vs_po_coverage WHERE council = ?"
    )
    params: list = [council]
    if year is not None:
        sql += " AND year = ?"
        params.append(int(year))
    return _run(conn, sql + " ORDER BY year", params)


def afs_national_by_division(conn: duckdb.DuckDBPyConnection, year: int | None = None) -> QueryResult:
    """The national amalgamated AFS — all 31 councils' audited net cost by service division, for
    one year (or every year if ``year`` is None). The complete audited local-government picture, a
    BUDGET grain never summed with the PO euros. Pre-ordered (year DESC, net DESC) in the view."""
    sql = (
        "SELECT year, division, gross_expenditure_eur, income_eur, net_expenditure_eur,"
        " net_expenditure_prior_yr_eur FROM v_procurement_afs_national_by_division"
    )
    params: list = []
    if year is not None:
        sql += " WHERE year = ?"
        params.append(int(year))
    return _run(conn, sql, params)


def afs_national_by_year(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """National amalgamated AFS totals per year (Σ across divisions) — the 2016–2024 spine."""
    return _run(
        conn,
        "SELECT year, gross_expenditure_eur, income_eur, net_expenditure_eur, n_divisions"
        " FROM v_procurement_afs_national_by_year ORDER BY year",
    )


# ── Balance-sheet financial POSITION (a STOCK grain) — NEVER summed with the spend facts above ──
# Loans Payable, assets, creditors — a debt/position at 31 Dec, not a flow. NOAC publishes none of
# this, so the AFS Balance Sheet is the only source. A stock is never added to a year's spending,
# nor summed across years for a council (that double-counts a carried balance).
def afs_loans_payable_by_year(conn: duckdb.DuckDBPyConnection, council: str) -> QueryResult:
    """One council's Loans Payable (borrowing outstanding @31 Dec) per year — the debt-trend
    spine. A STOCK: render the series, never sum it across years or with any spend euro."""
    return _run(
        conn,
        "SELECT year, loans_payable_eur, source_file_url, source_page_number"
        " FROM v_procurement_afs_loans_payable WHERE council = ? ORDER BY year",
        [council],
    )


def afs_debt_standing(conn: duckdb.DuckDBPyConnection, council: str) -> QueryResult:
    """One council's LATEST-year Loans Payable set against the national field: its euro figure,
    its rank (1 = most-indebted), the number of councils ranked, and the national median — so the
    page renders 'borrowing €Xm, Nth of M highest' with no computation. Each council contributes
    its own latest filed year only; a stock is never summed across councils, only ranked."""
    return _run(
        conn,
        "WITH latest AS ("
        "  SELECT council, arg_max(loans_payable_eur, year) AS loans_payable_eur,"
        "         max(year) AS year FROM v_procurement_afs_loans_payable GROUP BY council"
        "), ranked AS ("
        "  SELECT council, year, loans_payable_eur,"
        "         rank() OVER (ORDER BY loans_payable_eur DESC) AS debt_rank,"
        "         count(*) OVER () AS n_councils,"
        "         median(loans_payable_eur) OVER () AS national_median_eur"
        "  FROM latest"
        ") SELECT year, loans_payable_eur, debt_rank, n_councils, national_median_eur"
        " FROM ranked WHERE council = ?",
        [council],
    )


def afs_loan_movement_by_year(conn: duckdb.DuckDBPyConnection, council: str) -> QueryResult:
    """One council's Note-7 loan movement per year, pivoted: opening, NEW borrowings, principal
    repayments, early redemptions, closing. Every year shown is reconcile-gated at extract
    (opening + flows == closing). Borrowings is the 'ramping debt?' signal; repayments/redemptions
    are negative outflows. Mixed grain — never sum a flow row with an opening/closing stock."""
    return _run(
        conn,
        "SELECT year,"
        " max(value_eur) FILTER (WHERE item='opening_balance')        AS opening_eur,"
        " max(value_eur) FILTER (WHERE item='borrowings')             AS borrowings_eur,"
        " max(value_eur) FILTER (WHERE item='repayment_of_principal') AS repayment_eur,"
        " max(value_eur) FILTER (WHERE item='early_redemptions')      AS early_redemptions_eur,"
        " max(value_eur) FILTER (WHERE item='closing_balance')        AS closing_eur"
        " FROM v_procurement_afs_loan_movement WHERE council = ? GROUP BY year ORDER BY year",
        [council],
    )


def afs_financial_position(conn: duckdb.DuckDBPyConnection, council: str) -> QueryResult:
    """One council's LATEST-year Balance Sheet headline position, pivoted to one row: fixed assets
    (Σ components), long-term debtors, current assets (Σ), creditors & accruals (owed to suppliers),
    loans payable, net current assets — plus the source page. Every value is a STOCK at 31 Dec;
    the page displays them side by side and never sums across items or years."""
    return _run(
        conn,
        "WITH latest AS (SELECT max(year) AS y FROM v_procurement_afs_balance_sheet WHERE council = ?)"
        " SELECT year,"
        " sum(value_eur) FILTER (WHERE section='fixed_assets')                 AS fixed_assets_eur,"
        " sum(value_eur) FILTER (WHERE item='long_term_debtors')               AS long_term_debtors_eur,"
        " sum(value_eur) FILTER (WHERE section='current_assets')               AS current_assets_eur,"
        " sum(value_eur) FILTER (WHERE item='creditors_accruals')              AS creditors_accruals_eur,"
        " sum(value_eur) FILTER (WHERE item='loans_payable')                   AS loans_payable_eur,"
        " sum(value_eur) FILTER (WHERE item='net_current_assets')              AS net_current_assets_eur,"
        " any_value(source_file_url) AS source_file_url, any_value(source_page_number) AS source_page_number"
        " FROM v_procurement_afs_balance_sheet"
        " WHERE council = ? AND year = (SELECT y FROM latest) GROUP BY year",
        [council, council],
    )
