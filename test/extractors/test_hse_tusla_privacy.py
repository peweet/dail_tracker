"""Privacy + materialisation tests for the HSE/Tusla payments fact.

Locks ``extractors/procurement_hse_tusla_materialize.py`` — the thin writer that maps the
bespoke HSE/Tusla column-x parse into the shared ``public_payments_fact`` schema and writes
``data/gold/parquet/hse_tusla_payments_fact.parquet`` (pipeline chain ``hse_tusla_payments``).

HSE/Tusla is ``privacy_risk=high``: these bodies pay individual practitioners and carers, so a
sole-trader / individual supplier (personal data) left ``public_display=True`` would be exposed
on the served layer. The materialiser reuses ``pbe.classify_and_flag`` (locked separately in
test_public_body_payments_privacy.py) and adds a runtime write-invariant. This test asserts:
  1. the privacy gate quarantines individuals (the exact invariant expression yields 0 leaks);
  2. the layout-drift sanity gate rejects garbage files (the NTA lesson);
  3. value semantics map correctly (HSE=payment_actual, Tusla=po_committed).

Supplier names below are invented.

Run:  pytest test/extractors/test_hse_tusla_privacy.py -v
"""

import importlib.util
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "extractors"))

pl = pytest.importorskip("polars")
pytest.importorskip("fitz")


def _load_materialize():
    """Import the materialiser by path (it self-inserts extractors/ on sys.path and _loads pbe)."""
    path = _ROOT / "extractors" / "procurement_hse_tusla_materialize.py"
    spec = importlib.util.spec_from_file_location("hse_tusla_materialize", str(path))
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


mat = _load_materialize()


# --------------------------------------------------------------------------- privacy gate
def _flag(suppliers, semantics="payment_actual"):
    df = pl.DataFrame({
        "supplier_raw": suppliers,
        "amount_eur": [100_000.0] * len(suppliers),
        "amount_semantics": [semantics] * len(suppliers),
    })
    return mat.pbe.classify_and_flag(df)


def test_individual_supplier_is_quarantined():
    # HSE/Tusla pay named individuals (e.g. locum clinicians, foster carers) — no company suffix.
    row = _flag(["Dr Saoirse Lenihan"]).row(0, named=True)
    assert row["supplier_class"] == "sole_trader_or_individual"
    assert row["privacy_status"] == "review_personal_data"
    assert row["public_display"] is False


def test_company_supplier_is_displayable():
    row = _flag(["Medtronic Ireland Ltd"]).row(0, named=True)
    assert row["supplier_class"] == "company"
    assert row["public_display"] is True


def test_invariant_no_personal_row_is_displayable():
    """The exact runtime invariant from main(): no displayable row may be a likely person."""
    df = _flag([
        "Dr Saoirse Lenihan",          # individual -> quarantined
        "Medtronic Ireland Ltd",       # company -> ok
        "Beaumont Hospital",           # public body -> ok
        "Aoife Treacy",                # individual -> quarantined
        "Fresenius Medical Care Ltd",  # company -> ok
    ])
    leaked = df.filter(
        pl.col("public_display") & (pl.col("supplier_class") == "sole_trader_or_individual")
    )
    assert leaked.height == 0, f"{leaked.height} personal rows left displayable"
    assert df.filter(~pl.col("public_display")).height == 2  # the two invented individuals


# --------------------------------------------------------------------------- layout-drift gate
def _native(suppliers, amounts):
    return [{"supplier_raw": s, "amount_eur": a} for s, a in zip(suppliers, amounts, strict=True)]


def test_sanity_rejects_empty():
    ok, why = mat.sanity("ie_tusla", [])
    assert ok is False and "0 rows" in why


def test_sanity_rejects_implausible_median():
    # The real 2024 Tusla failure: column-x drift parsed €1-ish amounts -> reject, don't ship.
    ok, why = mat.sanity("ie_tusla", _native(["A Ltd"] * 5, [1, 1, 2, 1, 1]))
    assert ok is False and "implausible median" in why


def test_sanity_rejects_high_empty_supplier_share():
    native = _native([""] * 4 + ["A Ltd"] * 6, [50_000] * 10)
    ok, why = mat.sanity("ie_tusla", native)
    assert ok is False and "empty suppliers" in why


def test_sanity_passes_plausible():
    ok, why = mat.sanity("ie_hse", _native(["A Ltd"] * 5, [50_000, 120_000, 80_000, 200_000, 95_000]))
    assert ok is True and why == "ok"


# --------------------------------------------------------------------------- value semantics
def test_to_schema_semantics_and_period():
    meta = mat.PUBS["ie_hse"]
    native = {"supplier_raw": "Medtronic Ireland Ltd", "amount_eur": 120_000.0,
              "year": 2024, "quarter": "Q2", "description": "Devices",
              "doc_ref": "PO123", "source_row": 1, "source_page": 1}
    row = mat.to_schema("ie_hse", native, "http://x/f.pdf", "deadbeef", meta["semantics"], meta)
    assert row["amount_semantics"] == "payment_actual"   # HSE = payments
    assert row["period"] == "2024-Q2" and row["quarter"] == 2
    assert row["publisher_id"] == "ie_hse"

    tmeta = mat.PUBS["ie_tusla"]
    trow = mat.to_schema("ie_tusla", {**native, "quarter": "Q3"}, "http://x/t.pdf", "f00d",
                         tmeta["semantics"], tmeta)
    assert trow["amount_semantics"] == "po_committed"     # Tusla = purchase orders


@pytest.mark.integration
def test_coverage_flag_is_applied():
    """The on-disk coverage JSON must record the quarantine as applied. A stale pre-fix
    artifact fails this — the intended signal to regenerate the fact."""
    import json
    cov = _ROOT / "data" / "_meta" / "hse_tusla_payments_coverage.json"
    if not cov.exists():
        pytest.skip("coverage not generated; run the materialiser first")
    data = json.loads(cov.read_text())
    assert data.get("privacy_quarantine_applied") is True
