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
    PAID_FLAG_CLEAN,
    PRIVACY_STATUS,
    REALISATION_TIER,
    SUPPLIER_CLASS,
    VALUE_KIND,
    VAT_STATUS,
    ColumnRule,
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


def test_contract_failure_message_preserves_reviewer_evidence(tmp_path):
    """A halt must say what failed, not merely signal that something failed."""
    report = enforce_contract(_bad(supplier_class="alien"), name="t_explained", quarantine_dir=tmp_path)

    with pytest.raises(ContractViolation) as exc:
        report.raise_if_failed()

    message = str(exc.value)
    assert "[HARD] supplier_class: 2 rows (100.0%)" in message
    assert "out-of-vocab" in message
    assert "offending rows quarantined ->" in message


def test_guard_raises_on_drift(tmp_path):
    with pytest.raises(ContractViolation):
        guard_payment_fact(_bad(supplier_class="alien"), name="t_guard", quarantine_dir=tmp_path)


def test_missing_required_column_is_structural_failure():
    rep = enforce_contract(_GOOD.drop("supplier_class"), name="t_struct", write_quarantine=False)
    assert not rep.ok
    assert any("supplier_class" in e for e in rep.structural_errors)


def test_null_in_nonnull_key_column_fails():
    bad = _GOOD.with_columns(pl.lit(None, dtype=pl.Float64).alias("amount_eur"))
    errors = check_structure(bad, required_columns=("amount_eur",), nonnull_columns=("amount_eur",))
    assert any("amount_eur" in e for e in errors)


def test_structure_rejects_empty_frames_and_skips_absent_optional_nonnull_columns():
    empty = pl.DataFrame(schema={"amount_eur": pl.Float64})
    assert "frame is empty (0 rows)" in check_structure(empty, required_columns=("amount_eur",))

    errors = check_structure(pl.DataFrame({"present": [1]}), required_columns=(), nonnull_columns=("not_here",))
    assert errors == []


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


def test_multiple_vocab_breaches_are_all_quarantined_and_explained(tmp_path):
    report = enforce_contract(
        _bad(supplier_class="alien", privacy_status="secret"),
        name="t_multiple_breaches",
        quarantine_dir=tmp_path,
    )

    assert set(report.vocab_breaches) == {"supplier_class", "privacy_status"}
    assert report.n_quarantined_rows == 2
    quarantined = pl.read_parquet(tmp_path / "t_multiple_breaches_quarantine.parquet")
    assert all(
        {"supplier_class", "privacy_status"} <= set(reason.split(";")) for reason in quarantined["_quarantine_reason"]
    )


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


@pytest.mark.parametrize(
    "mutation,expect_fragment",
    [
        ({"supplier_class": "sole_trader_or_individual"}, "likely-person"),
        ({"privacy_status": "review_personal_data"}, "likely-person"),
        ({"realisation_tier": "COMMITTED"}, "disagree"),
    ],
)
def test_remaining_payment_invariant_branches_fire(mutation, expect_fragment):
    violations = payment_fact_invariant_violations(_bad(**mutation))
    assert any(expect_fragment in violation for violation in violations), violations


def test_a_zero_payment_is_non_positive_not_merely_a_negative_one():
    """Exactly zero must trip the non-positive invariant.

    Closes surviving NumberReplacer/comparison mutants on
    ``pl.col("amount_eur") <= 0`` at services/data_contracts.py:420. The existing corruption test
    uses -5.0, which fires under both ``<= 0`` and ``< 0``, so it cannot tell the boundary
    operators apart. Zero is the only value that does — and a zero-euro row in a summable payment
    fact is a parse failure, which is precisely why the rule reads ``<=``.
    """
    zeroed = _bad(amount_eur=0.0)
    violations = payment_fact_invariant_violations(zeroed)
    assert any("non-positive" in v for v in violations), violations


def test_a_cro_number_on_any_non_company_class_fires_not_just_later_sorting_ones():
    """The CRO invariant keys on inequality, not on string ordering.

    Closes surviving NotEq_Gt / NotEq_GtE mutants at services/data_contracts.py:408. The existing
    CRO test uses "sole_trader", which sorts AFTER "company", so ``!= "company"`` and
    ``> "company"`` both fire on it. A class sorting BEFORE "company" separates them: it must
    still be caught, because what makes the row wrong is that it is not a company, not where its
    name falls in the alphabet.
    """
    bad = _GOOD.with_columns(
        pl.lit(123456).cast(pl.Int64).alias("cro_company_num"),
        pl.lit("charity").alias("supplier_class"),  # sorts before "company"
    )
    assert any("CRO" in v for v in payment_fact_invariant_violations(bad))


def test_public_display_on_a_class_after_sole_trader_is_not_a_person_row():
    """The likely-person rule keys on class identity, not alphabetical position.

    Closes an Eq_GtE mutant at services/data_contracts.py:414. "unknown" sorts after
    "sole_trader_or_individual", so under ``>=`` a publicly displayable unknown-class row would be
    branded a person — a false privacy violation on a legitimate row.
    """
    ordinary = _GOOD.with_columns(
        pl.lit("unknown").alias("supplier_class"),
        pl.lit(True).alias("public_display"),
        pl.lit("ok").alias("privacy_status"),
    )
    assert not any("likely-person" in v for v in payment_fact_invariant_violations(ordinary))


def test_a_sub_threshold_quarantine_breach_reports_but_does_not_halt(tmp_path):
    """A quarantine-severity breach below its escalation threshold must not raise.

    Closes an Eq_GtE mutant at services/data_contracts.py:250. Severity is only
    "hard" | "quarantine", and "quarantine" >= "hard", so under ``>=`` every quarantine breach
    would halt the pipeline — turning a tolerated, quarantined subset into a hard stop.
    """
    # The shipped paid_flag rule escalates past 12%, and one bad row in a two-row frame is 50%,
    # so state the tolerance explicitly rather than fighting the fixture size.
    tolerant = ColumnRule("paid_flag", PAID_FLAG_CLEAN, "quarantine", case_insensitive=True, max_offending_frac=1.0)
    frame = _GOOD.with_columns(pl.Series("paid_flag", ["y", "12/03/2024"]))
    rep = enforce_contract(frame, name="t_soft", rules=[tolerant], quarantine_dir=tmp_path)
    assert rep.vocab_breaches, "expected the quarantine rule to record a breach"
    assert rep.vocab_breaches["paid_flag"]["escalated"] is False
    assert rep.ok, f"a sub-threshold quarantine breach must leave the report ok: {rep.vocab_breaches}"
    rep.raise_if_failed()  # must not raise


def test_a_halt_message_names_only_the_breaches_that_caused_it():
    """A tolerated quarantine breach must not be listed in the halt message.

    Closes an Eq_GtE mutant at services/data_contracts.py:250. That line filters which breaches
    ``raise_if_failed`` prints, and severity is only "hard" | "quarantine" with
    "quarantine" >= "hard" — so under ``>=`` a tolerated breach is reported as HARD alongside the
    real cause, pointing whoever debugs the halt at the wrong column.
    """
    rules = [
        ColumnRule("supplier_class", SUPPLIER_CLASS, "hard"),
        ColumnRule("paid_flag", PAID_FLAG_CLEAN, "quarantine", case_insensitive=True, max_offending_frac=1.0),
    ]
    frame = _GOOD.with_columns(
        pl.Series("supplier_class", ["alien", "company"]),  # hard breach -> this is the halt
        pl.Series("paid_flag", ["y", "12/03/2024"]),  # tolerated quarantine breach
    )
    report = enforce_contract(frame, name="t_msg", rules=rules, write_quarantine=False)
    assert report.vocab_breaches["paid_flag"]["escalated"] is False
    with pytest.raises(ContractViolation) as excinfo:
        report.raise_if_failed()
    message = str(excinfo.value)
    assert "supplier_class" in message
    assert "paid_flag" not in message, f"a tolerated breach must not be blamed for the halt:\n{message}"


def test_escalation_needs_a_quarantine_rule_and_a_fraction_strictly_over_the_bound():
    """Escalation is quarantine-only and strictly above the bound.

    Closes two mutants at services/data_contracts.py:341. ``severity == "quarantine"`` → ``<=``
    would mark a hard breach escalated ("hard" <= "quarantine"), and ``frac >`` → ``>=`` would
    escalate a fraction sitting exactly on its documented tolerance.
    """
    rule = ColumnRule(column="paid_flag", allowed=PAID_FLAG_CLEAN, severity="quarantine", max_offending_frac=0.5)
    # Exactly half the rows offend, and the bound is 0.5 — on the line is not over it.
    on_the_line = _GOOD.with_columns(pl.Series("paid_flag", ["y", "12/03/2024"]))
    report = enforce_contract(on_the_line, name="t_edge", rules=[rule], write_quarantine=False)
    assert report.vocab_breaches["paid_flag"]["frac"] == 0.5
    assert report.vocab_breaches["paid_flag"]["escalated"] is False

    hard_rule = ColumnRule(column="paid_flag", allowed=PAID_FLAG_CLEAN, severity="hard", max_offending_frac=0.0)
    hard = enforce_contract(on_the_line, name="t_hard", rules=[hard_rule], write_quarantine=False)
    assert hard.vocab_breaches["paid_flag"]["escalated"] is False, "escalation is a quarantine concept"


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


def test_reconciliation_flags_duplicated_rows_and_every_absent_source():
    out = reconciliation_violations(
        {"duplicated": (10, 100.0), "missing_one": (1, 1.0), "missing_two": (1, 1.0)},
        {"duplicated": (11, 100.0)},
    )
    assert "duplicated: row count drift +1 (allowed +0)" in out[0]
    assert {"missing_one", "missing_two"} <= {line.split(":", 1)[0] for line in out}


def test_reconciliation_flags_money_drift():
    exp = {"src_a": (100, 5000.0)}
    act = {"src_a": (100, 4200.0)}
    out = reconciliation_violations(exp, act)
    assert any("total drift" in v for v in out)


def test_reconciliation_honours_the_exact_money_tolerance_boundary():
    expected = {"src": (1, 100.0)}
    assert reconciliation_violations(expected, {"src": (1, 101.0)}) == []
    out = reconciliation_violations(expected, {"src": (1, 101.01)})
    assert len(out) == 1
    assert out[0].startswith("src:")
    assert "+1.01" in out[0]
    assert out[0].endswith("money not preserved")


def test_reconciliation_treats_an_absent_money_total_as_zero():
    """A source whose € total is None must reconcile as zero, not crash or silently pass.

    Closes 8 surviving mutants from the Cosmic Ray data-contracts session, all NumberReplacer on
    the four ``or 0`` fallbacks at services/data_contracts.py:475-476. Nothing in the suite passed
    None for either side, so that branch never executed and any replacement value survived.
    None is reachable in practice: a per-source € sum over an all-null amount column returns None.
    """
    # Expected money, none delivered — the whole total went missing.
    out = reconciliation_violations({"src": (10, 5000.0)}, {"src": (10, None)})
    assert len(out) == 1
    assert out[0] == "src: € total drift -5,000.00 — money not preserved"

    # Nothing expected, money appeared — the mirror case.
    out = reconciliation_violations({"src": (10, None)}, {"src": (10, 250.0)})
    assert out == ["src: € total drift +250.00 — money not preserved"]

    # None on both sides is zero drift, not a violation.
    assert reconciliation_violations({"src": (10, None)}, {"src": (10, None)}) == []


def test_reconciliation_reports_the_arithmetic_difference_not_a_ratio():
    """The reported drift must be actual minus expected.

    Closes a surviving Sub_Div mutant at services/data_contracts.py:476. The existing boundary
    test cannot catch it: with 100.0 and 101.01, subtraction gives 1.01 and division gives 1.0101,
    and both render as "+1.01" under the message's ``.2f``, so a substring assertion passes either
    way. These values separate them — 500.00 against 1.50 — and the assertion is exact.
    """
    out = reconciliation_violations({"src": (1, 1000.0)}, {"src": (1, 1500.0)})
    assert out == ["src: € total drift +500.00 — money not preserved"]


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
    assert payload["n_rows_total"] == 2
    assert payload["n_rows_quarantined"] == 2
    assert payload["frac_quarantined"] == 1.0
    assert payload["breaches"]["supplier_class"]["n_offending"] == 2


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
