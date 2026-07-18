"""Contract test for the eTenders AWARD-grain gold fact (``procurement_awards.parquet``).

This is the *award* grain — contract/framework ceilings, the money lane that must
NEVER be summed against the payment fact. The fact is typed (the consolidation has
already derived ``supplier_class`` / ``value_kind`` / ``value_safe_to_sum`` / a parsed
``value_eur`` on top of the raw eTenders export), so the drift we guard is:

  * ``value_kind`` is closed to the award kinds the classifier is designed to emit
    (``AWARD_VALUE_KIND`` in ``services.data_contracts`` — award vs. framework/DPS
    ceiling vs. call-off; the ceiling/call-off kinds are the never-sum boundary);
  * ``supplier_class`` reuses the SAME closed vocabulary as the payment fact (imported
    from ``services.data_contracts`` so the two contracts can never drift apart);
  * the never-sum invariant: any row flagged ``value_safe_to_sum`` MUST carry a real,
    positive ``value_eur``. A summable row with a null/≤0 value would inject a phantom
    into every ``SUM(value_eur WHERE value_safe_to_sum)``. Anchored 2026-06-27:
    0 violations across 16,404 summable rows.

``Tender ID`` is deliberately NOT asserted non-null — ~3.6k of 62.7k award rows have
no tender id (direct/legacy awards), which is real, not a defect.

Marked ``@sql`` — gold IS committed, runs against the real file on every push.
"""

import sys
from pathlib import Path

import pandera.polars as pa
import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))

from config import GOLD_PARQUET_DIR  # noqa: E402
from services.data_contracts import (  # noqa: E402  (single source of truth)
    AWARD_VALUE_KIND,
    SUPPLIER_CLASS,
    ContractViolation,
    guard_award_fact,
)


def _s(data) -> pl.Series:
    return data.lazyframe.select(pl.col(data.key)).collect()[data.key]


def _df(data) -> pl.DataFrame:
    return data.lazyframe.collect()


def _in_vocab(series: pl.Series, allowed: frozenset[str]) -> bool:
    nn = series.drop_nulls().cast(pl.Utf8)
    return bool(nn.is_in(list(allowed)).all()) if len(nn) else True


class ProcurementAwardsSchema(pa.DataFrameModel):
    value_eur: float = pa.Field(nullable=True)
    value_safe_to_sum: bool = pa.Field(nullable=True)
    value_kind: str = pa.Field(nullable=True)
    supplier_class: str = pa.Field(nullable=True)

    class Config:
        strict = False
        name = "procurement_awards"

    @pa.check("value_kind")
    def _value_kind_vocab(cls, data) -> bool:
        return _in_vocab(_s(data), AWARD_VALUE_KIND)

    @pa.check("supplier_class")
    def _supplier_class_vocab(cls, data) -> bool:
        return _in_vocab(_s(data), SUPPLIER_CLASS)

    @pa.dataframe_check
    def _safe_to_sum_implies_positive_value(cls, data) -> bool:
        df = _df(data)
        if "value_safe_to_sum" not in df.columns or "value_eur" not in df.columns:
            return True
        bad = df.filter(pl.col("value_safe_to_sum") & (pl.col("value_eur").is_null() | (pl.col("value_eur") <= 0)))
        return bad.height == 0

    @pa.dataframe_check
    def _safe_to_sum_implies_one_off_award(cls, data) -> bool:
        # Only a one-off contract_award_value may be summed — a summable framework/DPS
        # ceiling (repeated across N supplier rows) or call-off double-counts the money.
        df = _df(data)
        if "value_safe_to_sum" not in df.columns or "value_kind" not in df.columns:
            return True
        bad = df.filter(pl.col("value_safe_to_sum") & (pl.col("value_kind") != "contract_award_value"))
        return bad.height == 0


# --------------------------------------------------------------------------- samples
_GOOD = pl.DataFrame(
    {
        "value_eur": [125000.0, None],
        "value_safe_to_sum": [True, False],
        "value_kind": ["contract_award_value", "framework_or_dps_ceiling"],
        "supplier_class": ["company", "public_body"],
    }
)


def _bad(**overrides) -> pl.DataFrame:
    return _GOOD.with_columns(**{k: pl.lit(v) for k, v in overrides.items()})


def test_awards_schema_accepts_good_sample():
    ProcurementAwardsSchema.validate(_GOOD)


def test_awards_schema_rejects_unknown_value_kind():
    with pytest.raises(pa.errors.SchemaError):
        ProcurementAwardsSchema.validate(_bad(value_kind="mystery_value"))


def test_awards_schema_rejects_unknown_supplier_class():
    with pytest.raises(pa.errors.SchemaError):
        ProcurementAwardsSchema.validate(_bad(supplier_class="alien"))


def test_awards_schema_rejects_summable_row_without_positive_value():
    # value_safe_to_sum True but value_eur null → phantom in any SUM-where-summable.
    bad = pl.DataFrame(
        {
            "value_eur": [None],
            "value_safe_to_sum": [True],
            "value_kind": ["contract_award_value"],
            "supplier_class": ["company"],
        }
    ).with_columns(pl.col("value_eur").cast(pl.Float64))
    with pytest.raises(pa.errors.SchemaError):
        ProcurementAwardsSchema.validate(bad)


# --------------------------------------------------------------------------- runtime guard
# guard_award_fact is the WRITE-TIME twin of this test-side schema: same vocab constants,
# same invariants, but it runs inside the extractor and halts before gold is written.
_GUARD_GOOD = pl.DataFrame(
    {
        "supplier": ["Acme Ltd", "Framework Co"],
        "supplier_norm": ["acme", "framework co"],
        "supplier_class": ["company", "company"],
        "value_eur": [125000.0, 2_000_000.0],
        "value_kind": ["contract_award_value", "framework_or_dps_ceiling"],
        "value_safe_to_sum": [True, False],
    }
)


def test_guard_accepts_current_shape(tmp_path):
    report = guard_award_fact(_GUARD_GOOD, name="t_awards", hard=True, quarantine_dir=tmp_path)
    assert report.ok


def test_guard_halts_on_unknown_value_kind(tmp_path):
    bad = _GUARD_GOOD.with_columns(pl.lit("mystery_value").alias("value_kind"))
    with pytest.raises(ContractViolation):
        guard_award_fact(bad, name="t_awards", hard=True, quarantine_dir=tmp_path)


def test_guard_halts_on_summable_ceiling(tmp_path):
    # value_safe_to_sum on a framework ceiling re-opens the multi-supplier double-count.
    bad = _GUARD_GOOD.with_columns(pl.lit(True).alias("value_safe_to_sum"))
    with pytest.raises(ContractViolation):
        guard_award_fact(bad, name="t_awards", hard=True, quarantine_dir=tmp_path)


def test_guard_halts_on_dropped_derived_column(tmp_path):
    with pytest.raises(ContractViolation):
        guard_award_fact(_GUARD_GOOD.drop("value_safe_to_sum"), name="t_awards", hard=True, quarantine_dir=tmp_path)


# --------------------------------------------------------------------------- integration
@pytest.mark.sql
def test_procurement_awards_satisfies_contract():
    path = GOLD_PARQUET_DIR / "procurement_awards.parquet"
    if not path.exists():
        pytest.skip(f"{path} not found — run the procurement pipeline first")
    ProcurementAwardsSchema.validate(pl.read_parquet(path))


@pytest.mark.sql
def test_runtime_guard_passes_on_real_gold(tmp_path):
    # The write-time guard must be anchored to CURRENT gold — a guard that false-halts
    # a clean pipeline run is worse than no guard (it gets disabled).
    path = GOLD_PARQUET_DIR / "procurement_awards.parquet"
    if not path.exists():
        pytest.skip(f"{path} not found — run the procurement pipeline first")
    report = guard_award_fact(pl.read_parquet(path), name="procurement_awards_anchor", hard=True, quarantine_dir=tmp_path)
    assert report.ok
