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


# ── reading-order reader (Department of Defence layout) ───────────────────────
def _defence_pdf(header: str, body: str) -> bytes:
    """One-page synthetic Defence PO PDF: a header line then reading-order records."""
    import fitz

    doc = fitz.open()
    doc.new_page().insert_text((40, 50), header + "\n" + body)
    b = doc.tobytes()
    doc.close()
    return b


def test_read_defence_category_first_layout():
    """5-column NUMBER/CATEGORY/SUPPLIER/CURRENCY/AMOUNT (category before supplier). The reader
    anchors on the currency line, walks back to the PO digit line, and — given a category-first
    header — assigns the first mid line to category and the rest to the supplier."""
    from extractors.procurement_public_body_extract import read_defence

    b = _defence_pdf(
        "Number\nCategory\nSupplier\nCurrency\nAmount",
        "319124\nTRANSPORT EQUIPMENT\nA.G. BLOCK LTD\nEUR\n85,312.00\n"
        "320402\nSERVICES MISCELLANEOUS\nACME DEFENCE LTD\nEUR\n31,985.10\n",
    )
    recs = read_defence(b, None)
    by_ref = {r["ref"]: r for r in recs}
    assert by_ref["319124"]["supplier"] == "A.G. BLOCK LTD"
    assert by_ref["319124"]["category"] == "TRANSPORT EQUIPMENT"
    assert by_ref["319124"]["amount"] == 85312.00
    assert by_ref["320402"]["supplier"] == "ACME DEFENCE LTD"
    assert by_ref["320402"]["amount"] == 31985.10


def test_read_defence_supplier_first_layout_and_foreign_currency():
    """The OTHER published header order puts SUPPLIER before CATEGORY; the reader must flip the
    mid-line assignment. Also covers a non-EUR currency anchor (USD) — the amount is read as-is."""
    from extractors.procurement_public_body_extract import read_defence

    b = _defence_pdf(
        "Number\nSupplier\nCategory\nCurrency\nAmount",
        "440011\nAIRBUS DEFENCE & SPACE SAU SPAIN\nAIR CORPS\nUSD\n234,760.00\n",
    )
    recs = read_defence(b, None)
    assert len(recs) == 1
    r = recs[0]
    assert r["ref"] == "440011"
    assert r["supplier"] == "AIRBUS DEFENCE & SPACE SAU SPAIN"
    assert r["category"] == "AIR CORPS"
    assert r["amount"] == 234760.00


def test_read_defence_merged_po_name_layout():
    """Older quarters merge the PO and supplier onto one line ('PO NAME') with no category. The
    single mid line is taken as the supplier and category stays empty — no scrambling."""
    from extractors.procurement_public_body_extract import read_defence

    b = _defence_pdf(
        "Number\nCategory\nSupplier\nCurrency\nAmount",
        "500777 PILATUS AIRCRAFT LTD\nEUR\n754,600.00\n",
    )
    recs = read_defence(b, None)
    assert len(recs) == 1
    assert recs[0]["ref"] == "500777"
    assert recs[0]["supplier"] == "PILATUS AIRCRAFT LTD"
    assert recs[0]["category"] is None
    assert recs[0]["amount"] == 754600.00


# ── reading-order reader (Courts Service "PO analysis report" layout) ─────────
def test_read_courts_recovers_supplier_and_skips_total():
    """The Courts PDFs merge 'PO SupplierName' on one line, then amount / (blank) / description /
    Paid. The reader recovers PO+supplier+amount+paid and must DROP the period TOTAL summary line
    (which has no PO and would otherwise be ingested as a giant payment — DQ audit P1)."""
    import fitz

    from extractors.procurement_public_body_extract import read_courts

    doc = fitz.open()
    doc.new_page().insert_text(
        (40, 50),
        "PO No.\nSupplier Name\nAmount\nDescription\nPaid Yes/No\n"
        "94232 IPP CCC GP1 LTD\n1,833,172.55\nPPP Unitary Payment\nY\n"
        "93409 EIR\n149,005.68\nTelecoms\nN\n"
        "Total\n1,982,178.23\n",
    )
    b = doc.tobytes()
    doc.close()

    recs = read_courts(b, None)
    by_ref = {r["ref"]: r for r in recs}
    assert by_ref["94232"]["supplier"] == "IPP CCC GP1 LTD"
    assert by_ref["94232"]["amount"] == 1833172.55
    assert by_ref["94232"]["paid"] == "Y"
    assert by_ref["93409"]["supplier"] == "EIR"
    assert by_ref["93409"]["amount"] == 149005.68
    # the Total summary line carries no PO and must not become a supplier row
    assert all("total" not in (r["supplier"] or "").lower() for r in recs)
    assert 1982178.23 not in [r["amount"] for r in recs]


# ── reading-order/geometry reader (DHLGH / dept_housing layout) ───────────────
def _housing_pdf(rows: list[tuple], cols: dict) -> bytes:
    """Synthetic DHLGH payments PDF: each cell placed at its column's x so the reader must
    DERIVE the columns from x0s (not from header order). `rows` = list of (date, supplier,
    desc, amount) at the given column x-positions."""
    import fitz

    doc = fitz.open()
    page = doc.new_page(width=900, height=600)  # wide so distinct cells don't merge horizontally
    page.insert_text((cols["date"], 50), "Payment Date")
    page.insert_text((cols["supplier"], 50), "Supplier")
    page.insert_text((cols["desc"], 50), "Description")
    page.insert_text((cols["amount"], 50), "Payment Amount")
    y = 80
    for date, supplier, desc, amount in rows:
        if date:
            page.insert_text((cols["date"], y), date)
        if supplier:
            page.insert_text((cols["supplier"], y), supplier)
        if desc:
            page.insert_text((cols["desc"], y), desc)
        if amount:
            page.insert_text((cols["amount"], y), amount)
        y += 22
    b = doc.tobytes()
    doc.close()
    return b


def test_read_housing_derives_columns_clean_supplier_description():
    """Layout A: Date | Supplier | Description | Amount in four well-separated columns. The
    reader derives the columns from the data x0s and keeps supplier/description separate."""
    from extractors.procurement_public_body_extract import read_housing

    b = _housing_pdf(
        [
            ("04/10/2022", "BORD NA MONA ENERGY LTD", "Peatlands Restoration Works", "28,404.00"),
            ("06/10/2022", "EIR (EIRCOM)", "Telecoms", "32,650.24"),
        ],
        cols={"date": 40, "supplier": 140, "desc": 420, "amount": 720},
    )
    recs = read_housing(b, None)
    assert len(recs) == 2
    assert recs[0]["supplier"] == "BORD NA MONA ENERGY LTD"
    assert recs[0]["desc"] == "Peatlands Restoration Works"
    assert recs[0]["amount"] == 28404.00
    assert recs[0]["date"] == "04/10/2022"
    assert recs[1]["amount"] == 32650.24


def test_read_housing_handles_date_supplier_merged_cell():
    """Layout D: the date column holds 'DD/MM/YYYY SUPPLIER' in one cell (no separate supplier
    column), description and amount follow. The reader must split the date off, route the
    remainder to supplier, and NOT mislabel the description column as supplier."""
    from extractors.procurement_public_body_extract import read_housing

    b = _housing_pdf(
        [
            ("17/07/2024 BDO EATON SQUARE LTD", "", "CRM Platform Project", "66,572.77"),
            ("18/07/2024 CHARLES ALEXANDER LTD", "", "Branded Goods", "24,600.00"),
        ],
        cols={"date": 40, "supplier": 140, "desc": 420, "amount": 720},
    )
    recs = read_housing(b, None)
    assert len(recs) == 2
    assert recs[0]["supplier"] == "BDO EATON SQUARE LTD"
    assert recs[0]["desc"] == "CRM Platform Project"
    assert recs[0]["amount"] == 66572.77
