"""Privacy-quarantine tests for the public-body payments sandbox fact.

Locks ``extractors/procurement_public_body_extract.py:classify_and_flag()`` — the
function that derives ``supplier_class`` / ``privacy_status`` / ``public_display`` for
``data/sandbox/parquet/public_payments_fact.parquet``. This fact is a gold-CANDIDATE one
promotion away from a procurement UI; if a sole-trader / individual supplier (personal
data) were left ``public_display=True`` it would be exposed on promotion (synthesis INC-4).

Invariant under test: NO ``public_display=True`` row may be ``sole_trader_or_individual``.
Classification errs toward over-quarantine (an org without a recognised company suffix is
treated as personal) — the privacy-safe direction. Supplier names below are invented.

Run:  pytest test/test_public_body_payments_privacy.py -v
"""

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "extractors"))

pl = pytest.importorskip("polars")
from procurement_public_body_extract import classify_and_flag  # noqa: E402


def _flag(suppliers):
    df = pl.DataFrame({
        "supplier_raw": suppliers,
        "amount_eur": [100_000.0] * len(suppliers),
        "amount_semantics": ["payment_actual"] * len(suppliers),
    })
    return classify_and_flag(df)


def test_sole_trader_is_quarantined():
    df = _flag(["Jonathan Oakfield"])  # no company suffix -> personal
    row = df.row(0, named=True)
    assert row["supplier_class"] == "sole_trader_or_individual"
    assert row["privacy_status"] == "review_personal_data"
    assert row["public_display"] is False


@pytest.mark.parametrize("name", [
    "Brightwater Solutions Limited",
    "Acme Engineering Ltd",
    "Northgate Holdings DAC",
])
def test_company_is_displayable(name):
    row = _flag([name]).row(0, named=True)
    assert row["supplier_class"] == "company"
    assert row["privacy_status"] == "ok"
    assert row["public_display"] is True


def test_public_body_is_displayable():
    row = _flag(["Cork County Council"]).row(0, named=True)
    assert row["supplier_class"] == "public_body"
    assert row["public_display"] is True


def test_invariant_no_personal_row_is_displayable():
    df = _flag([
        "Jonathan Oakfield",            # personal -> quarantined
        "Acme Engineering Ltd",         # company -> ok
        "Cork County Council",          # public body -> ok
        "Mary Quillfeather",            # personal -> quarantined
        "Brightwater Solutions Limited",
    ])
    leaked = df.filter(
        pl.col("public_display") & (pl.col("supplier_class") == "sole_trader_or_individual")
    )
    assert leaked.height == 0, f"{leaked.height} personal rows left displayable"
    # and the quarantine actually suppressed the two invented individuals
    assert df.filter(~pl.col("public_display")).height == 2


@pytest.mark.integration
def test_coverage_flag_is_applied():
    """The on-disk coverage JSON must record the quarantine as applied. Marked integration:
    requires a fresh extractor run (network crawl) — a stale pre-fix sandbox fails this,
    which is the intended signal to regenerate `public_payments_fact.parquet`."""
    import json
    cov = _ROOT / "data" / "_meta" / "public_payments_coverage.json"
    if not cov.exists():
        pytest.skip("coverage not generated; run the extractor first")
    data = json.loads(cov.read_text())
    assert data.get("privacy_quarantine_applied") is True
