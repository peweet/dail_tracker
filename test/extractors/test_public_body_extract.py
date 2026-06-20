"""Unit tests for the public-body PO/payments extractor primitives
(extractors/procurement_public_body_extract.py). Pure/synthetic — no network, no PDFs.
"""

from __future__ import annotations

import polars as pl

from extractors.procurement_public_body_extract import canonicalise_supplier_raw


def test_canonicalise_supplier_raw_handles_all_null_column():
    # Regression: a body whose parse yields an all-null supplier column comes through as dtype Null,
    # and the str ops raised "expected String type, got: null", crashing the whole merge run
    # (hit on TUS 2026-06-19). The fix casts to Utf8 first, so the merge is a no-op here.
    df = pl.DataFrame(
        {
            "supplier_raw": pl.Series("supplier_raw", [None, None, None], dtype=pl.Null),
            "po_number": ["123", "NBI", None],
        }
    )
    out = canonicalise_supplier_raw(df)
    assert out.height == 3
    assert out.schema["supplier_raw"] == pl.Utf8
    assert out["supplier_raw"].to_list() == [None, None, None]


def test_canonicalise_supplier_raw_still_merges_nbi():
    # The NBI split-entity merge must keep working: po_number 'NBI' + 'Infrastructure DAC' →
    # 'NBI Infrastructure DAC' (so it normalises to the identifiable entity, not generic).
    df = pl.DataFrame(
        {
            "supplier_raw": ["Infrastructure DAC", "Some Other Ltd", "Infrastructure DAC"],
            "po_number": ["NBI", "NBI", "P0001"],
        }
    )
    out = canonicalise_supplier_raw(df)
    assert out["supplier_raw"].to_list() == [
        "NBI Infrastructure DAC",  # po=NBI + Infrastructure DAC → rewritten
        "Some Other Ltd",  # po=NBI but not Infrastructure DAC → untouched
        "Infrastructure DAC",  # Infrastructure DAC but po≠NBI → untouched
    ]


def test_canonicalise_supplier_raw_noop_without_required_columns():
    # Guard clause: missing po_number / supplier_raw → returns df unchanged (no crash).
    df = pl.DataFrame({"supplier_raw": ["A Ltd"]})
    assert canonicalise_supplier_raw(df).equals(df)
    assert canonicalise_supplier_raw(pl.DataFrame()).is_empty()
