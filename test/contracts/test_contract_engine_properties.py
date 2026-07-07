"""Property-based fuzzing of the pure-Polars data-contracts engine.

The example tests in ``test_payment_fact_contract.py`` prove the engine catches a
handful of hand-picked drift cases. These properties assert the two behaviours the
WHOLE engine exists to guarantee hold for *arbitrary* inputs — the failure modes a
fixed set of examples can't exhaustively cover:

  1. **vocab-gate soundness + completeness** — across any multiset of values, the
     gate flags EXACTLY the out-of-vocabulary rows (never misses drift, never
     false-flags a valid value) and halts iff at least one exists;
  2. **never-sum invariant detection** — for any payment-like frame, the invariant
     checker reports a violation iff the frame actually contains a double-count
     (a summable public-body transfer) or a phantom (a summable non-positive
     amount). This is the guard that stops mis-classed money reaching a SUM().

Frames are generated with Hypothesis strategies (``polars.testing.parametric`` is
the native option for schema-shaped fuzzing; here we want tight control over the
vocab/invariant axes, so we build small frames directly). Pure + fast — no markers,
always runs.
"""

import sys
from pathlib import Path

import polars as pl
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

sys.path.insert(0, str(Path(__file__).parents[2]))

from services.data_contracts import (  # noqa: E402
    SUPPLIER_CLASS,
    ColumnRule,
    enforce_contract,
    payment_fact_invariant_violations,
)

# A pool mixing in-vocabulary classes with values that must be rejected (an empty
# string is non-null, so it counts as out-of-vocab; "UNKNOWN"/"charity" are the kind
# of silent fallback / new source value the gate exists to catch).
_OUT_OF_VOCAB = ("charity", "alien", "", "Company", "UNKNOWN", "ngo")
_VOCAB_POOL = sorted(SUPPLIER_CLASS) + list(_OUT_OF_VOCAB)


@settings(deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(values=st.lists(st.sampled_from(_VOCAB_POOL), min_size=1, max_size=40))
def test_vocab_gate_flags_exactly_out_of_vocab(values):
    df = pl.DataFrame({"supplier_class": values})
    rep = enforce_contract(
        df,
        name="prop_vocab",
        rules=(ColumnRule("supplier_class", SUPPLIER_CLASS, "hard"),),
        required_columns=(),
        nonnull_columns=(),
        write_quarantine=False,  # no files in a property test
    )
    expected_offending = sum(1 for v in values if v not in SUPPLIER_CLASS)
    if expected_offending:
        assert not rep.ok, "an out-of-vocab value must HARD-fail the contract"
        assert rep.vocab_breaches["supplier_class"]["n_offending"] == expected_offending
    else:
        assert rep.ok
        assert "supplier_class" not in rep.vocab_breaches


# (value_safe_to_sum, supplier_class, amount_eur) — amount pool spans the non-positive
# boundary so the "summable non-positive" invariant is exercised both ways.
_ROW = st.tuples(
    st.booleans(),
    st.sampled_from(["company", "public_body", "sole_trader"]),
    st.sampled_from([-50.0, 0.0, 100.0, 2500.0]),
)


@settings(deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(rows=st.lists(_ROW, min_size=1, max_size=30))
def test_never_sum_invariant_detects_iff_double_count_or_phantom(rows):
    safe = [r[0] for r in rows]
    classes = [r[1] for r in rows]
    amounts = [r[2] for r in rows]
    df = pl.DataFrame(
        {
            "value_safe_to_sum": safe,
            "supplier_class": classes,
            "amount_eur": amounts,
            # non-blank so the separate blank-supplier invariant never fires here,
            # isolating the two invariants under test.
            "supplier_normalised": ["acme ltd"] * len(rows),
        }
    )
    violations = payment_fact_invariant_violations(df)
    # Ground truth: a summable public-body row (double-count) or a summable
    # non-positive amount (phantom) is exactly what must be flagged.
    expect = any(s and (c == "public_body" or a <= 0) for s, c, a in zip(safe, classes, amounts, strict=False))
    assert bool(violations) == expect, (violations, expect)
