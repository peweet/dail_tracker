"""Tripwires for the three enrichment views (ISIF / CBI / EU-TAM).

Reads the real committed gold parquet (data/gold/parquet/, built by
extractors/enrichment_promote_to_gold.py). The parquet is git-tracked but these
build each view on a bare DuckDB connection from its .sql so they are independent
of the rest of the corporate/procurement view graph.

Guards the contracts that matter: the non-summable value semantics, the privacy
projection (no person-identifier column survives in the view), and basic shape.
"""

from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).parents[2]
GOLD = ROOT / "data" / "gold" / "parquet"

VIEWS = {
    "v_corporate_isif_portfolio": (
        ROOT / "sql_views" / "corporate" / "corporate_isif_portfolio.sql",
        "data/gold/parquet/isif_portfolio.parquet",
        GOLD / "isif_portfolio.parquet",
    ),
    "v_corporate_cbi_enforcement": (
        ROOT / "sql_views" / "corporate" / "corporate_cbi_enforcement.sql",
        "data/gold/parquet/cbi_enforcement_actions.parquet",
        GOLD / "cbi_enforcement_actions.parquet",
    ),
    "v_procurement_eu_tam_state_aid": (
        ROOT / "sql_views" / "procurement" / "procurement_eu_tam_state_aid.sql",
        "data/gold/parquet/eu_tam_state_aid.parquet",
        GOLD / "eu_tam_state_aid.parquet",
    ),
}

_PII_TOKENS = ("address", "national_id", "home", "dob", "birth", "pps", "phone", "email")


def _con(view: str) -> duckdb.DuckDBPyConnection:
    sql_path, rel_path, abs_path = VIEWS[view]
    if not abs_path.exists():
        pytest.skip(f"gold source absent (CI): {abs_path.name}")
    c = duckdb.connect()
    c.execute(sql_path.read_text(encoding="utf-8").replace(rel_path, str(abs_path).replace("\\", "/")))
    return c


@pytest.mark.parametrize("view", list(VIEWS))
def test_view_builds_and_has_rows(view):
    n = _con(view).execute(f"SELECT count(*) FROM {view}").fetchone()[0]
    assert n > 0, f"{view} is empty"


@pytest.mark.parametrize("view", list(VIEWS))
def test_value_safe_to_sum_is_all_false(view):
    """These facts are mixed-currency commitments / fines / awards — never summable."""
    n_true = _con(view).execute(f"SELECT count(*) FROM {view} WHERE value_safe_to_sum").fetchone()[0]
    assert n_true == 0


@pytest.mark.parametrize("view", list(VIEWS))
def test_no_person_identifier_column_survives(view):
    cols = [r[0].lower() for r in _con(view).execute(f"DESCRIBE {view}").fetchall()]
    leaked = [c for c in cols if any(t in c for t in _PII_TOKENS)]
    assert leaked == [], f"{view} exposes person-identifier column(s): {leaked}"


def test_isif_currency_constrained_when_amount_present():
    bad = _con("v_corporate_isif_portfolio").execute(
        "SELECT count(*) FROM v_corporate_isif_portfolio "
        "WHERE amount_stated IS NOT NULL AND amount_currency NOT IN ('EUR','USD','GBP')"
    ).fetchone()[0]
    assert bad == 0


def test_cbi_individual_flag_column_dropped():
    cols = [r[0] for r in _con("v_corporate_cbi_enforcement").execute("DESCRIBE v_corporate_cbi_enforcement").fetchall()]
    assert "party_is_individual_suspected" not in cols


def test_cbi_fine_non_negative_when_present():
    bad = _con("v_corporate_cbi_enforcement").execute(
        "SELECT count(*) FROM v_corporate_cbi_enforcement WHERE fine_amount_eur < 0"
    ).fetchone()[0]
    assert bad == 0


def test_eu_tam_cro_company_num_is_clean_or_null():
    """cro_company_num is the companies-only join key: 5-7 digits or NULL, never a raw id."""
    bad = _con("v_procurement_eu_tam_state_aid").execute(
        "SELECT count(*) FROM v_procurement_eu_tam_state_aid "
        "WHERE cro_company_num IS NOT NULL AND NOT regexp_matches(cro_company_num, '^[0-9]{5,7}$')"
    ).fetchone()[0]
    assert bad == 0


def test_eu_tam_is_awarded_tier():
    n_wrong = _con("v_procurement_eu_tam_state_aid").execute(
        "SELECT count(*) FROM v_procurement_eu_tam_state_aid WHERE realisation_tier <> 'AWARDED'"
    ).fetchone()[0]
    assert n_wrong == 0
