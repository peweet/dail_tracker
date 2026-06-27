"""Tests for the bronze-layer fidelity gate (tools.data_fidelity) and the engine
plausibility split it wraps (services.data_contracts.partition_implausible).

The gate's whole reason to exist: a single stray / fat-fingered / mis-OCR'd figure must
be held back from the published app, the rest of the data must flow, and a sudden SPIKE
of bad values must halt (the source changed shape). These tests pin all three behaviours
plus the split invariant.
"""

import sys
from pathlib import Path

import polars as pl
import pytest
from hypothesis import given
from hypothesis import strategies as st

sys.path.insert(0, str(Path(__file__).parents[2]))

from services.data_contracts import BoundRule, ImplausibleValueSpike, partition_implausible  # noqa: E402
from tools.data_fidelity import fidelity_gate  # noqa: E402

# A €1bn ceiling — absurd-only: no real Irish political donation approaches it.
_BILLION = (0, 1_000_000_000)


def _donations(n_normal: int = 60, absurd: float | None = None) -> pl.DataFrame:
    values = [500.0 + i for i in range(n_normal)]
    if absurd is not None:
        values.append(absurd)
    return pl.DataFrame({"donor": [f"d{i}" for i in range(len(values))], "value_eur": values})


def test_absurd_value_is_quarantined_and_does_not_propagate():
    df = _donations(absurd=5_000_000_000.0)  # €5bn fat-finger among 60 normal rows
    clean, report = fidelity_gate(
        df, name="t_donations", bounds={"value_eur": _BILLION}, write_quarantine=False, return_report=True
    )
    assert clean.height == 60
    assert 5_000_000_000.0 not in clean["value_eur"].to_list()
    assert report.n_quarantined_rows == 1


def test_clean_frame_passes_through_unchanged():
    df = _donations()
    clean = fidelity_gate(df, name="t_clean", bounds={"value_eur": _BILLION}, write_quarantine=False)
    assert clean.height == df.height


def test_open_sided_min_bound_catches_negative():
    df = pl.DataFrame({"amount_eur": [10.0, -5.0, 20.0, 30.0, 40.0]})
    clean = fidelity_gate(
        df, name="t_neg", bounds={"amount_eur": (0, None)}, write_quarantine=False, max_offending_frac=0.5
    )
    assert clean["amount_eur"].to_list() == [10.0, 20.0, 30.0, 40.0]


def test_spike_halts_with_implausible_value_spike():
    # 1 of 3 rows out of bounds = 33% > the 2% default tolerance → a structural change, halt.
    df = pl.DataFrame({"value_eur": [100.0, 200.0, 5_000_000_000.0]})
    with pytest.raises(ImplausibleValueSpike):
        fidelity_gate(df, name="t_spike", bounds={"value_eur": _BILLION}, write_quarantine=False)


def test_non_numeric_bounded_column_raises_valueerror():
    df = pl.DataFrame({"value_eur": ["1000", "2000"]})  # strings — a wiring error
    with pytest.raises(ValueError, match="not numeric"):
        fidelity_gate(df, name="t_str", bounds={"value_eur": _BILLION}, write_quarantine=False)


def test_absent_bounded_column_is_skipped():
    df = _donations()
    # gate references a column this loader doesn't carry → no-op, full frame returned.
    clean = fidelity_gate(df, name="t_absent", bounds={"not_here_eur": _BILLION}, write_quarantine=False)
    assert clean.height == df.height


def test_quarantine_artifacts_written_to_disk(tmp_path):
    df = _donations(absurd=9_000_000_000.0)
    _, implausible, report = partition_implausible(
        df,
        name="t_quarantine",
        bounds=(BoundRule("value_eur", 0, 1_000_000_000),),
        quarantine_dir=tmp_path,
    )
    pq = tmp_path / "t_quarantine_quarantine.parquet"
    js = tmp_path / "t_quarantine_quarantine.json"
    assert pq.exists() and js.exists()
    held = pl.read_parquet(pq)
    assert held.height == 1
    assert "_quarantine_reason" in held.columns
    assert held["_quarantine_reason"].to_list() == ["value_eur"]
    assert report.quarantine_parquet == str(pq)


# --------------------------------------------------------------------------- property
@given(
    values=st.lists(
        st.floats(allow_nan=False, allow_infinity=False, min_value=-1e12, max_value=1e12),
        min_size=1,
        max_size=60,
    )
)
def test_split_is_total_and_correct(values):
    df = pl.DataFrame({"x": values})
    plausible, implausible, _ = partition_implausible(
        df,
        name="prop",
        # max_offending_frac=1.0 so we test the SPLIT, never the spike halt.
        bounds=(BoundRule("x", min_value=0.0, max_value=1000.0, max_offending_frac=1.0),),
        write_quarantine=False,
    )
    # every row is accounted for, exactly once...
    assert plausible.height + implausible.height == df.height
    # ...and nothing out of bounds survives into the plausible frame.
    surviving = plausible["x"].to_list()
    assert all(0.0 <= v <= 1000.0 for v in surviving)
