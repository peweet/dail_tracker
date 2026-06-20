"""Contract tests for the procurement payment-grain facts (silver sources + gold fact).

Three layers, mirroring how the data is guarded in production:

  * **engine unit tests** (no marker — pure, run in the default CI lane): prove the
    runtime drift gate in ``services.data_contracts`` actually halts on an unknown
    enum value / structural break and quarantines the offending rows. This is the
    safety net the pipeline relies on, so it must itself be tested without any files.

  * **Pandera silver schema** (``@integration`` — silver is not committed): declarative
    column contract validating every silver payment fact on disk.

  * **Pandera + engine over the gold fact** (``@sql`` — gold IS committed, so this runs
    in the sql-contracts lane on every push): the consolidated fact must satisfy the
    same closed vocabularies the consolidation enforces at write time.

The vocabularies are imported from ``services.data_contracts`` so the test contract and
the runtime guard can never drift apart.
"""

import sys
from pathlib import Path

import pandera.polars as pa
import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))

from config import GOLD_PARQUET_DIR, SILVER_PARQUET_DIR  # noqa: E402
from services.data_contracts import (  # noqa: E402
    AMOUNT_SEMANTICS,
    EXTRACTION_CONFIDENCE,
    EXTRACTION_STATUS,
    PRIVACY_STATUS,
    REALISATION_TIER,
    SUPPLIER_CLASS,
    VALUE_KIND,
    VAT_STATUS,
    ContractViolation,
    check_structure,
    enforce_contract,
    guard_payment_fact,
    payment_fact_invariant_violations,
    reconciliation_violations,
)

# --------------------------------------------------------------------------- helpers


def _s(data) -> pl.Series:
    return data.lazyframe.select(pl.col(data.key)).collect()[data.key]


def _in_vocab(series: pl.Series, allowed: frozenset[str]) -> bool:
    """All non-null values fall inside ``allowed`` (case-sensitive)."""
    nn = series.drop_nulls().cast(pl.Utf8)
    return bool(nn.is_in(list(allowed)).all()) if len(nn) else True


# --------------------------------------------------------------------------- sample data

_GOOD = pl.DataFrame(
    {
        "publisher_id": ["ie_dept_x", "ie_dept_x"],
        "publisher_name": ["Dept X", "Dept X"],
        "publisher_type": ["department", "department"],
        "supplier_raw": ["ACME LTD", "BETA LTD"],
        "supplier_normalised": ["acme ltd", "beta ltd"],
        "amount_eur": [1000.0, 2500.0],
        "amount_semantics": ["payment_actual", "po_committed"],
        "value_kind": ["payment_actual", "po_committed"],
        "realisation_tier": ["SPENT", "COMMITTED"],
        "extraction_status": ["extracted", "extracted"],
        "extraction_confidence": ["high", "medium"],
        "vat_status": ["unknown", "incl_vat"],
        "supplier_class": ["company", "company"],
        "privacy_status": ["ok", "ok"],
        "value_safe_to_sum": [True, True],
        "public_display": [True, True],
        "paid_flag": ["Y", ""],
        "cro_company_num": [None, None],
    }
).with_columns(pl.col("cro_company_num").cast(pl.Int64))


def _bad(**overrides) -> pl.DataFrame:
    return _GOOD.with_columns(**{k: pl.lit(v) for k, v in overrides.items()})


# --------------------------------------------------------------------------- engine unit tests


def test_clean_frame_passes_with_no_quarantine():
    rep = enforce_contract(_GOOD, name="t_clean", write_quarantine=False)
    assert rep.ok
    assert rep.n_quarantined_rows == 0
    assert rep.vocab_breaches == {}


@pytest.mark.parametrize(
    "col,bad_value",
    [
        ("value_kind", "lease_imputed"),  # the consolidation's silent 'unknown' fallback class
        ("realisation_tier", "UNKNOWN"),
        ("amount_semantics", "accrual"),
        ("supplier_class", "charity"),
        ("extraction_status", "ocr_failed"),
        ("extraction_confidence", "guess"),
        ("vat_status", "maybe"),
        ("privacy_status", "secret"),
    ],
)
def test_unknown_enum_value_halts(col, bad_value):
    """Any unrecognised value in a closed-vocab column must HARD-fail the contract."""
    bad = _bad(**{col: bad_value})
    rep = enforce_contract(bad, name="t_drift", write_quarantine=False)
    assert not rep.ok
    assert rep.vocab_breaches[col]["severity"] == "hard"
    with pytest.raises(ContractViolation):
        rep.raise_if_failed()


def test_guard_raises_on_drift(tmp_path):
    with pytest.raises(ContractViolation):
        guard_payment_fact(_bad(supplier_class="alien"), name="t_guard", quarantine_dir=tmp_path)


def test_missing_required_column_is_structural_failure():
    rep = enforce_contract(_GOOD.drop("supplier_class"), name="t_struct", write_quarantine=False)
    assert not rep.ok
    assert any("supplier_class" in e for e in rep.structural_errors)


def test_null_in_nonnull_key_column_fails():
    bad = _GOOD.with_columns(pl.lit(None, dtype=pl.Float64).alias("amount_eur"))
    errors = check_structure(
        bad, required_columns=("amount_eur",), nonnull_columns=("amount_eur",)
    )
    assert any("amount_eur" in e for e in errors)


def test_paid_flag_dirt_quarantines_without_halting(tmp_path):
    """A leaked description in paid_flag is recorded for investigation but, below the
    escalation threshold (12%), does NOT halt the run."""
    n = 100
    flags = ["Building Mtce", "Constr Contract", "Drawdown", "Fitouts", "Part Paid"] + ["Y"] * (n - 5)
    big = pl.concat([_GOOD.head(1)] * n, how="vertical").with_columns(pl.Series("paid_flag", flags))
    rep = enforce_contract(big, name="t_paidflag", quarantine_dir=tmp_path)
    assert rep.ok, "5% paid_flag dirt is below the 12% threshold and must not halt"
    assert rep.vocab_breaches["paid_flag"]["severity"] == "quarantine"
    assert rep.vocab_breaches["paid_flag"]["escalated"] is False
    assert rep.n_quarantined_rows == 5
    q = pl.read_parquet(tmp_path / "t_paidflag_quarantine.parquet")
    assert q.height == 5
    assert all("paid_flag" in r for r in q["_quarantine_reason"].to_list())


def test_paid_flag_dirt_escalates_above_threshold(tmp_path):
    """A sudden jump in paid_flag contamination (past 12%) escalates to a halt."""
    n = 100
    flags = ["Building Mtce"] * 20 + ["Y"] * (n - 20)  # 20% dirty
    big = pl.concat([_GOOD.head(1)] * n, how="vertical").with_columns(pl.Series("paid_flag", flags))
    rep = enforce_contract(big, name="t_paidflag_esc", quarantine_dir=tmp_path)
    assert not rep.ok
    assert rep.vocab_breaches["paid_flag"]["escalated"] is True
    with pytest.raises(ContractViolation):
        rep.raise_if_failed()


# --------------------------------------------------------------------------- invariant tests


def test_invariants_clean_on_good_sample():
    assert payment_fact_invariant_violations(_GOOD) == []


@pytest.mark.parametrize(
    "mutation,expect_fragment",
    [
        ({"supplier_class": "public_body"}, "public-body"),  # summable transfer → double-count
        ({"supplier_normalised": ""}, "blank supplier"),  # summable un-identifiable row
        ({"value_kind": "po_committed"}, "disagree"),  # value_kind/tier mismatch (tier still SPENT)
        ({"amount_eur": -5.0}, "non-positive"),  # summable negative payment
    ],
)
def test_invariant_fires_on_corruption(mutation, expect_fragment):
    bad = _GOOD.with_columns(**{k: pl.lit(v) for k, v in mutation.items()})
    violations = payment_fact_invariant_violations(bad)
    assert any(expect_fragment in v for v in violations), violations


def test_invariant_cro_num_on_non_company_fires():
    bad = _GOOD.with_columns(
        pl.lit(123456).cast(pl.Int64).alias("cro_company_num"),
        pl.lit("sole_trader").alias("supplier_class"),
    )
    assert any("CRO" in v for v in payment_fact_invariant_violations(bad))


def test_guard_halts_on_invariant_violation(tmp_path):
    bad = _GOOD.with_columns(supplier_class=pl.lit("public_body"))  # summable public-body transfer
    with pytest.raises(ContractViolation):
        guard_payment_fact(bad, name="t_inv", quarantine_dir=tmp_path)


# --------------------------------------------------------------------------- reconciliation tests


def test_reconciliation_passes_when_preserved():
    exp = {"src_a": (100, 5000.0), "src_b": (50, 2500.0)}
    assert reconciliation_violations(exp, dict(exp)) == []


def test_reconciliation_flags_dropped_rows():
    exp = {"src_a": (100, 5000.0)}
    act = {"src_a": (60, 5000.0)}  # 40 rows vanished but € unchanged (a filtered-out subset)
    out = reconciliation_violations(exp, act)
    assert any("row count drift" in v for v in out)


def test_reconciliation_flags_money_drift():
    exp = {"src_a": (100, 5000.0)}
    act = {"src_a": (100, 4200.0)}
    out = reconciliation_violations(exp, act)
    assert any("total drift" in v for v in out)


def test_reconciliation_allows_documented_carry_forward():
    exp = {"la": (84706, 1.0e9)}
    act = {"la": (85116, 1.0e9)}  # +410 carried-forward rows, € identical
    assert reconciliation_violations(exp, act, allowed_row_delta={"la": 410}) == []


def test_reconciliation_flags_absent_source():
    out = reconciliation_violations({"src_a": (10, 1.0)}, {})
    assert any("ABSENT" in v for v in out)


def test_quarantine_summary_json_written(tmp_path):
    enforce_contract(_bad(supplier_class="alien"), name="t_json", quarantine_dir=tmp_path)
    summary = tmp_path / "t_json_quarantine.json"
    assert summary.exists()
    import json

    payload = json.loads(summary.read_text(encoding="utf-8"))
    assert payload["fact"] == "t_json"
    assert payload["n_rows_quarantined"] >= 1


# --------------------------------------------------------------------------- Pandera schemas


class PaymentFactSilverSchema(pa.DataFrameModel):
    """Declarative contract for a silver payment-grain fact. strict=False — only the
    integrity-critical + closed-vocab columns are declared (facts vary in width)."""

    publisher_id: str = pa.Field(nullable=False)
    publisher_name: str = pa.Field(nullable=False)
    supplier_raw: str = pa.Field(nullable=True)
    amount_eur: float = pa.Field(nullable=True)
    value_safe_to_sum: bool = pa.Field(nullable=True)
    public_display: bool = pa.Field(nullable=True)
    amount_semantics: str = pa.Field(nullable=True)
    extraction_status: str = pa.Field(nullable=True)
    extraction_confidence: str = pa.Field(nullable=True)
    supplier_class: str = pa.Field(nullable=True)
    privacy_status: str = pa.Field(nullable=True)

    class Config:
        strict = False
        name = "payment_fact_silver"

    @pa.check("amount_semantics")
    def _amount_semantics_vocab(cls, data) -> bool:
        return _in_vocab(_s(data), AMOUNT_SEMANTICS)

    @pa.check("extraction_status")
    def _extraction_status_vocab(cls, data) -> bool:
        return _in_vocab(_s(data), EXTRACTION_STATUS)

    @pa.check("extraction_confidence")
    def _extraction_confidence_vocab(cls, data) -> bool:
        return _in_vocab(_s(data), EXTRACTION_CONFIDENCE)

    @pa.check("supplier_class")
    def _supplier_class_vocab(cls, data) -> bool:
        return _in_vocab(_s(data), SUPPLIER_CLASS)

    @pa.check("privacy_status")
    def _privacy_status_vocab(cls, data) -> bool:
        return _in_vocab(_s(data), PRIVACY_STATUS)


class PaymentFactGoldSchema(PaymentFactSilverSchema):
    """The consolidated gold fact adds the 2-axis taxonomy + VAT/regime columns."""

    value_kind: str = pa.Field(nullable=True)
    realisation_tier: str = pa.Field(nullable=True)
    vat_status: str = pa.Field(nullable=True)

    class Config:
        strict = False
        name = "payment_fact_gold"

    @pa.check("value_kind")
    def _value_kind_vocab(cls, data) -> bool:
        return _in_vocab(_s(data), VALUE_KIND)

    @pa.check("realisation_tier")
    def _realisation_tier_vocab(cls, data) -> bool:
        return _in_vocab(_s(data), REALISATION_TIER)

    @pa.check("vat_status")
    def _vat_status_vocab(cls, data) -> bool:
        return _in_vocab(_s(data), VAT_STATUS)


class LaPaymentFactSchema(pa.DataFrameModel):
    """The 31-LA Purchase-Orders/Payments-over-€20k silver fact was built natively on the
    canonical 2-axis taxonomy, so it carries ``value_kind``/``realisation_tier`` directly
    and has NO ``amount_semantics`` / ``extraction_*`` columns (the consolidation adds
    those when it folds the LA fact into gold). Hence its own contract."""

    publisher_id: str = pa.Field(nullable=False)
    publisher_name: str = pa.Field(nullable=False)
    supplier_raw: str = pa.Field(nullable=True)
    amount_eur: float = pa.Field(nullable=True)
    value_safe_to_sum: bool = pa.Field(nullable=True)
    public_display: bool = pa.Field(nullable=True)
    value_kind: str = pa.Field(nullable=True)
    realisation_tier: str = pa.Field(nullable=True)
    supplier_class: str = pa.Field(nullable=True)
    privacy_status: str = pa.Field(nullable=True)

    class Config:
        strict = False
        name = "la_payment_fact"

    @pa.check("value_kind")
    def _value_kind_vocab(cls, data) -> bool:
        return _in_vocab(_s(data), VALUE_KIND)

    @pa.check("realisation_tier")
    def _realisation_tier_vocab(cls, data) -> bool:
        return _in_vocab(_s(data), REALISATION_TIER)

    @pa.check("supplier_class")
    def _supplier_class_vocab(cls, data) -> bool:
        return _in_vocab(_s(data), SUPPLIER_CLASS)

    @pa.check("privacy_status")
    def _privacy_status_vocab(cls, data) -> bool:
        return _in_vocab(_s(data), PRIVACY_STATUS)


def test_pandera_gold_schema_accepts_good_sample():
    PaymentFactGoldSchema.validate(_GOOD)


def test_pandera_gold_schema_rejects_bad_value_kind():
    with pytest.raises(pa.errors.SchemaError):
        PaymentFactGoldSchema.validate(_bad(value_kind="weird"))


# --------------------------------------------------------------------------- integration

# Five "semantics-style" silver facts share the amount_semantics + extraction_* schema.
SILVER_FACTS = [
    "public_payments_fact.parquet",
    "hse_tusla_payments_fact.parquet",
    "nta_payments_fact.parquet",
    "nphdb_payments_fact.parquet",
    "seai_payments_fact.parquet",
    "dept_readingorder_payments_fact.parquet",
]


def _read(base: Path, filename: str) -> pl.DataFrame:
    path = base / filename
    if not path.exists():
        pytest.skip(f"{filename} not found — run the pipeline first")
    return pl.read_parquet(path)


@pytest.mark.integration
@pytest.mark.parametrize("filename", SILVER_FACTS)
def test_silver_fact_satisfies_contract(filename):
    PaymentFactSilverSchema.validate(_read(SILVER_PARQUET_DIR, filename))


@pytest.mark.integration
def test_la_silver_fact_satisfies_contract():
    LaPaymentFactSchema.validate(_read(SILVER_PARQUET_DIR, "la_payments_fact.parquet"))


@pytest.mark.sql
def test_gold_fact_satisfies_pandera_contract():
    PaymentFactGoldSchema.validate(_read(GOLD_PARQUET_DIR, "procurement_payments_fact.parquet"))


@pytest.mark.sql
def test_gold_fact_passes_runtime_guard(tmp_path):
    """The committed gold fact must pass the SAME gate the consolidation runs — vocab +
    cross-column invariants — with no halt, only the known sub-threshold paid_flag quarantine."""
    df = _read(GOLD_PARQUET_DIR, "procurement_payments_fact.parquet")
    rep = guard_payment_fact(df, name="ci_gold_check", hard=False, quarantine_dir=tmp_path)
    assert rep.ok, (
        f"gold fact breaches contract: vocab={rep.vocab_breaches} "
        f"struct={rep.structural_errors} invariants={rep.invariant_errors}"
    )


@pytest.mark.sql
def test_gold_fact_invariants_hold():
    """The documented cross-column invariants must hold on committed gold (0 violations)."""
    df = _read(GOLD_PARQUET_DIR, "procurement_payments_fact.parquet")
    assert payment_fact_invariant_violations(df) == []
