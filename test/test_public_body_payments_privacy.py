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


# ----------------------------------------------------------------------------------------
# value_safe_to_sum: intergovernmental transfers must NOT be summable (DQ audit 2026-06-05).
# A payment whose recipient is itself a public body (e.g. TII -> county-council road grants,
# €2.5bn / 32% of the fact) is a transfer/grant, not private procurement; totalling it inflates
# "procurement spend" and triple-counts the same euro down the grant -> council -> contractor
# chain. Such rows are RETAINED (public_display=True) but excluded from value_safe_to_sum.
# ----------------------------------------------------------------------------------------
def test_public_body_recipient_is_not_summable():
    row = _flag(["Cork County Council"]).row(0, named=True)
    assert row["supplier_class"] == "public_body"
    assert row["value_safe_to_sum"] is False  # transfer, not procurement
    assert row["public_display"] is True       # but still retained/displayable


def test_company_payment_is_summable():
    row = _flag(["Acme Engineering Ltd"]).row(0, named=True)
    assert row["supplier_class"] == "company"
    assert row["value_safe_to_sum"] is True


@pytest.mark.parametrize("name", [
    "Transport Infrastructure Ireland",   # "...Ireland" — COMPANY_SUFFIX 'ireland' would mis-hit
    "Uisce Éireann",
    "Irish Water",
    "Tailte Éireann",
])
def test_named_state_agency_is_public_body_not_summable(name):
    # State agencies named "X Ireland"/"X Éireann" must classify public_body (tested before the
    # company check) so their intergovernmental transfers never leak into value_safe_to_sum.
    row = _flag([name]).row(0, named=True)
    assert row["supplier_class"] == "public_body", f"{name} misclassified as {row['supplier_class']}"
    assert row["value_safe_to_sum"] is False


def test_no_public_body_row_is_summable_in_mix():
    # Mirrors the real transfer pattern: TII pays county councils (recipient = public body),
    # which is the €2.5bn of intergovernmental transfers the fix excludes from summing.
    df = _flag([
        "Cork County Council",                # public body -> transfer, not summable
        "Donegal County Council",             # public body -> transfer, not summable
        "Acme Engineering Ltd",               # company -> summable
        "Brightwater Solutions Limited",      # company -> summable
    ])
    leaked = df.filter(
        pl.col("value_safe_to_sum") & (pl.col("supplier_class") == "public_body")
    )
    assert leaked.height == 0, f"{leaked.height} public_body transfer rows left summable"
    assert df.filter(pl.col("value_safe_to_sum")).height == 2  # only the two companies


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
