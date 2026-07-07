"""Unit tests for services/deflator.py — the single CPI deflation function.

Tests the MATH on a tiny hand-built index (no I/O) plus a parity check that the loaded
gold table agrees, and that SQL-side deflation matches this reference function (so the
precomputed column the app reads is provably identical to the tested code path).

Core properties asserted (the ones that protect "carefully assembled data"):
  identity at base · reversibility (round-trip) · order-preservation ·
  missing-year -> None (never a silent unadjusted number) · nominal column never mutated.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from services.deflator import Deflator, implausible_mask

# A tiny synthetic index: 2020=100, 2021=110 (+10%), 2022=121 (+10%). Base = 2022.
SYN = {2020: 100.0, 2021: 110.0, 2022: 121.0}
BASE = 2022


@pytest.fixture
def d() -> Deflator:
    return Deflator(SYN, BASE)


def test_base_year_is_identity(d):
    assert d.factor(BASE) == 1.0
    assert d.inflate(1234.0, BASE) == 1234.0


def test_known_factor_and_value(d):
    # 2020 -> 2022 : 121/100 = 1.21
    assert d.factor(2020) == pytest.approx(1.21)
    assert d.inflate(1000.0, 2020) == pytest.approx(1210.0)
    # 2021 -> 2022 : 121/110 = 1.1
    assert d.inflate(1000.0, 2021) == pytest.approx(1100.0)


def test_inflate_to_arbitrary_year(d):
    # 2020 -> 2021 : 110/100 = 1.1
    assert d.inflate(1000.0, 2020, to=2021) == pytest.approx(1100.0)
    # adjusting to the same year is identity
    assert d.inflate(777.0, 2021, to=2021) == pytest.approx(777.0)


def test_round_trip_is_lossless(d):
    # inflate to base then back to source year must recover the original.
    for yr in SYN:
        up = d.inflate(54321.0, yr)  # yr -> base
        back = d.inflate(up, BASE, to=yr)  # base -> yr
        assert back == pytest.approx(54321.0, rel=1e-12)


def test_order_preserving(d):
    # deflation is multiplication by a positive factor -> never reorders values.
    a, b = d.inflate(100.0, 2020), d.inflate(200.0, 2020)
    assert a < b
    assert d.inflate(100.0, 2021) < d.inflate(100.0, 2020)  # older year -> bigger factor


def test_missing_year_returns_none_not_one(d):
    # The danger the design guards against: a missing year must NOT behave like factor 1.0.
    assert d.factor(1999) is None
    assert d.inflate(1000.0, 1999) is None
    assert d.inflate(None, 2020) is None
    assert not d.has_year(None)


def test_deflate_series_nulls_missing_and_keeps_nominal(d):
    df = pl.DataFrame({"v": [100.0, 100.0, 100.0], "yr": [2020, 2021, 1999]})
    out = d.deflate_series(df, "v", "yr", "v_real")
    assert out["v"].to_list() == [100.0, 100.0, 100.0]  # nominal untouched
    assert out["v_real"][0] == pytest.approx(121.0)
    assert out["v_real"][1] == pytest.approx(110.0)
    assert out["v_real"][2] is None  # missing year -> null, not nominal


def test_constructor_rejects_absent_base():
    with pytest.raises(ValueError):
        Deflator({2020: 100.0}, base_year=2025)


# ── parity with the real precomputed gold table ─────────────────────────────
_DEFLATOR = Path("data/gold/parquet/cso_cpi_deflator.parquet")


@pytest.mark.skipif(not _DEFLATOR.exists(), reason="deflator gold not built")
def test_loaded_table_matches_published_cpi():
    d = Deflator.load()
    assert d.base_year == 2025
    assert d.factor(2025) == pytest.approx(1.0, abs=1e-9)
    # 2013 €100k -> ~€124,038 (hand-verified against the gold table)
    assert d.inflate(100_000.0, 2013) == pytest.approx(124_038.0, abs=2.0)
    # cumulative 2012->2025 ~ +24.66%
    assert d.factor(2012) == pytest.approx(1.2466, abs=0.001)


@pytest.mark.skipif(not _DEFLATOR.exists(), reason="deflator gold not built")
def test_function_matches_table_all_years():
    """The function recomputes factor from cpi_index_chained; the table stores it as
    deflator_to_base. Pin them equal so the two code paths can never drift."""
    d = Deflator.load()
    tbl = pl.read_parquet(_DEFLATOR)
    for year, stored in zip(tbl["year"].to_list(), tbl["deflator_to_base"].to_list(), strict=False):
        assert d.factor(int(year)) == pytest.approx(float(stored), rel=1e-9)


@pytest.mark.skipif(not _DEFLATOR.exists(), reason="deflator gold not built")
def test_sql_deflation_matches_python_function():
    """THE guarantee: the SQL precompute the app will read (value * deflator_to_base, joined
    on year) is byte-for-byte the tested Python function, on real award rows."""
    import duckdb

    d = Deflator.load()
    con = duckdb.connect()
    rows = con.execute(f"""
        WITH a AS (
            SELECT value_eur,
                   TRY_CAST(substr("Notice Published Date/Contract Created Date",7,4) AS INTEGER) AS yr
            FROM read_parquet('data/gold/parquet/procurement_awards.parquet')
            WHERE value_safe_to_sum AND value_eur > 0
        )
        SELECT a.value_eur, a.yr, a.value_eur * d.deflator_to_base AS sql_real
        FROM a JOIN read_parquet('{_DEFLATOR.as_posix()}') d ON a.yr = d.year
        USING SAMPLE 500 ROWS
    """).fetchall()
    con.close()
    assert rows, "no sampled rows"
    for value_eur, yr, sql_real in rows:
        py_real = d.inflate(value_eur, yr)
        assert py_real == pytest.approx(sql_real, rel=1e-9)


@pytest.mark.skipif(not _DEFLATOR.exists(), reason="deflator gold not built")
def test_implausible_mask_flags_low_end_artifacts():
    # The data-quality guard, exercised independently of the math.
    df = pl.DataFrame({"value_eur": [0.99, 50.0, 100.0, 250_000.0, 2.5e9]})
    flagged = df.filter(implausible_mask("value_eur")).height
    assert flagged == 3  # 0.99, 50.0 (< €100), and 2.5e9 (> €500m) ; 100 and 250k are fine


# ---------------------------------------------------------------------------
# Multi-index registry (CPI / government-consumption / construction TPI / materials).
# CPI is a household basket and is NOT the right index for public money or construction;
# the registry lets a caller pick the methodology-correct index per value type.
# ---------------------------------------------------------------------------
from services.deflator import DEFAULT_INDEX, INDEX_REGISTRY, list_indices  # noqa: E402


def test_registry_metadata_complete():
    # every entry must carry the fields a consumer needs to attach provenance + a caveat
    for _code, spec in INDEX_REGISTRY.items():
        assert {"file", "index_col", "label", "applies_to", "source", "caveat"} <= set(spec)
    codes = {it["code"] for it in list_indices()}
    assert codes == set(INDEX_REGISTRY)
    assert DEFAULT_INDEX == "CSO_CPA07_CPI"  # CPI stays the transparent default


def test_unknown_index_raises_never_silent_cpi():
    with pytest.raises(KeyError):
        Deflator.load_index("NOT_AN_INDEX")


@pytest.mark.skipif(not _DEFLATOR.exists(), reason="deflator gold not built")
def test_every_registered_index_loads_and_is_base_identity():
    """Each built index loads, base-year factor is exactly 1.0, and it tags itself — so a
    real-terms figure always knows which index produced it (and an absent year stays None)."""
    for code in INDEX_REGISTRY:
        gold = _DEFLATOR.parent / INDEX_REGISTRY[code]["file"]
        if not gold.exists():
            pytest.skip(f"{code} gold not built")
        d = Deflator.load_index(code)
        assert d.index_code == code and d.meta is not None
        assert d.factor(d.base_year) == 1.0
        # a year well outside any series stays None, never a silent x1.0
        assert d.factor(1800) is None


@pytest.mark.skipif(not _DEFLATOR.exists(), reason="deflator gold not built")
def test_indices_diverge_as_expected_construction_hotter_than_cpi():
    """Methodology sanity: for a pre-2020 year, construction tender prices (SCSI TPI) show MORE
    cumulative inflation than CPI, and the government-consumption deflator shows LESS — the whole
    reason a single CPI lens is wrong for these value types."""
    g = _DEFLATOR.parent
    if not all(
        (g / INDEX_REGISTRY[c]["file"]).exists()
        for c in ("SCSI_TPI_CONSTRUCTION", "CSO_GOV_CONSUMPTION", "CSO_CPA07_CPI")
    ):
        pytest.skip("not all index tables built")
    cpi = Deflator.load_index("CSO_CPA07_CPI").factor(2016)
    tpi = Deflator.load_index("SCSI_TPI_CONSTRUCTION").factor(2016)
    gov = Deflator.load_index("CSO_GOV_CONSUMPTION").factor(2016)
    assert tpi > cpi > gov  # construction hottest, government-consumption coolest
