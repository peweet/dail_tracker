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


# ── reading-order reader (DCEDIY / dept_children layout) ──────────────────────
def test_read_reading_order_both_column_orders():
    """The DCEDIY PO PDFs publish records as a 'ref supplier' line + a payment date + an
    '€amount description' line, in EITHER order (two layouts). The reader must parse both,
    carry a per-row payment date, and ignore page-header lines."""
    import fitz

    from extractors.procurement_public_body_extract import read_reading_order

    doc = fitz.open()
    # layout A: ref / date / amount+desc
    doc.new_page().insert_text(
        (40, 50),
        "Reference\nName\nPayment Date\nTotal Paid\nDescription\n"
        "70111 CAPE WRATH HOTEL UNLIMITED\n12/12/2024\n€4,028,036.00 IP Accommodation\n",
    )
    # layout B: ref / amount+desc / date (date last)
    doc.new_page().insert_text(
        (40, 50),
        "Reference\nSupplier Name\nAmount\nDescription\nPayment\nDate\n"
        "42958 MOSNEY HOLIDAYS PLC\n€3,255,828.09 Ukraine Accommodation\n06/07/2023\n",
    )
    recs = read_reading_order(doc.tobytes(), None)
    doc.close()

    by_ref = {r["ref"]: r for r in recs}
    assert by_ref["70111"]["supplier"] == "CAPE WRATH HOTEL UNLIMITED"
    assert by_ref["70111"]["amount"] == 4028036.00
    assert by_ref["70111"]["date"] == "2024-12-12"
    assert by_ref["42958"]["supplier"] == "MOSNEY HOLIDAYS PLC"  # date-last layout still pairs
    assert by_ref["42958"]["amount"] == 3255828.09
    assert by_ref["42958"]["date"] == "2023-07-06"
