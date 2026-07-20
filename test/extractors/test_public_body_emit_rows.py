"""Characterization tests for emit_rows (extractors/procurement_public_body_extract.py).

emit_rows is the assembly stage of the public-body payments fact: (cf, file_url,
bytes, fmt) -> gold-schema row dicts + a per-file stat block. The sub-parsers
(read_courts, read_housing, ...) have their own tests; these tests pin the part
NOTHING else covers — the field wiring per reader branch, the confidence
thresholds, the geometry->fallback->unparsed chain, and the post-processing
repair/downgrade rules — so the dispatch-table refactor cannot silently change
behaviour. Written against the pre-refactor implementation; every assertion is
observed current behaviour, not aspiration.

Pure/synthetic: no network, no bronze files (same policy as
test_public_body_extract.py).
"""

from __future__ import annotations

import hashlib

import pytest

import extractors.procurement_public_body_extract as m

URL = "https://example.ie/files/payments-Q2-2024.pdf"  # -> period 2024-Q2 / 2024 / 2
FAKE_PDF = b"%PDF-fake-bytes"
FHASH = hashlib.sha256(FAKE_PDF).hexdigest()[:16]


def _cf(**over) -> dict:
    cf = {
        "id": "test_pub",
        "name": "Test Publisher",
        "ptype": "dept",
        "sector": "testing",
        "listing_url": "https://example.ie/listing",
        "amount_semantics": "payment_actual",
        "caveat": "",
        "reader": None,
    }
    cf.update(over)
    return cf


def _expected_row(**over) -> dict:
    """The full gold-schema row emit_rows produces for _cf/URL/FAKE_PDF defaults."""
    row = {
        "publisher_id": "test_pub",
        "publisher_name": "Test Publisher",
        "publisher_type": "dept",
        "sector": "testing",
        "source_landing_url": "https://example.ie/listing",
        "source_file_url": URL,
        "source_file_hash": FHASH,
        "period": "2024-Q2",
        "year": 2024,
        "quarter": 2,
        "supplier_raw": "ACME Consulting Ltd",
        "amount_eur": 25000.0,
        "amount_semantics": "payment_actual",
        "description": "Widget services",
        "po_number": "PO-77",
        "paid_flag": "Y",
        "source_row_number": 0,
        "source_page_number": 1,
        "parser_name": "public_body_pdf",
        "parser_version": m.PARSER_VERSION,
        "source_caveat": None,
        "extraction_status": "extracted",
        "extraction_confidence": "medium",
        "caveat_text_detected": False,
    }
    row.update(over)
    return row


def _rec(**over) -> dict:
    rec = {"supplier": "ACME Consulting Ltd", "amount": 25000.0, "desc": "Widget services", "ref": "PO-77", "paid": "Y"}
    rec.update(over)
    return rec


# ── bespoke reading-order branches: field wiring ──────────────────────────────
# Each branch is (reader_key, patched_fn, canned record, expected overrides).
# 4 records are emitted per case -> confidence "medium" (>3, not >20).

BRANCHES = [
    ("reading_order_courts", "read_courts", _rec(), {}),
    ("reading_order_revenue", "read_revenue", _rec(), {}),
    ("reading_order_dper", "read_dper", _rec(), {}),
    ("reading_order_tailte", "read_tailte", _rec(), {}),
    # no PO / paid columns in the Culture layout
    ("reading_order_culture", "read_culture", _rec(), {"po_number": None, "paid_flag": None}),
    # Defence: CATEGORY -> description, no paid flag
    (
        "reading_order_defence",
        "read_defence",
        {"supplier": "ACME Consulting Ltd", "amount": 25000.0, "category": "Widget services", "ref": "PO-77"},
        {"paid_flag": None},
    ),
    # LMETB always-fallback publishers: ref -> po, no paid flag
    ("reading_order_fallback", "read_pdf_reading_order_fallback", _rec(), {"paid_flag": None}),
    # DCEDIY: per-row ISO payment date overrides the URL period; page carried per row
    (
        "reading_order",
        "read_reading_order",
        {**_rec(), "date": "2024-11-05", "page": 3},
        {"period": "2024-Q4", "year": 2024, "quarter": 4, "source_page_number": 3, "paid_flag": None},
    ),
    # DHLGH: per-row DD/MM/YYYY date -> period; no PO / paid columns
    (
        "reading_order_housing",
        "read_housing",
        {**_rec(), "date": "05/11/2024"},
        {"period": "2024-Q4", "year": 2024, "quarter": 4, "po_number": None, "paid_flag": None},
    ),
]


@pytest.mark.parametrize(("reader_key", "fn", "rec", "over"), BRANCHES, ids=[b[0] for b in BRANCHES])
def test_reading_order_branch_wiring(monkeypatch, reader_key, fn, rec, over):
    monkeypatch.setattr(m, fn, lambda b, mp: [dict(rec) for _ in range(4)])
    rows, stat = m.emit_rows(_cf(reader=reader_key), URL, FAKE_PDF, "pdf", None)
    assert stat == {"status": "ok", "rows": 4, "confidence": "medium"}
    assert len(rows) == 4
    assert rows[0] == _expected_row(**over)
    assert [r["source_row_number"] for r in rows] == [0, 1, 2, 3]


def test_housing_malformed_date_falls_back_to_url_period(monkeypatch):
    monkeypatch.setattr(m, "read_housing", lambda b, mp: [{**_rec(), "date": "banana"} for _ in range(4)])
    rows, _ = m.emit_rows(_cf(reader="reading_order_housing"), URL, FAKE_PDF, "pdf", None)
    assert (rows[0]["period"], rows[0]["year"], rows[0]["quarter"]) == ("2024-Q2", 2024, 2)


def test_source_caveat_carries_configured_text(monkeypatch):
    monkeypatch.setattr(m, "read_courts", lambda b, mp: [_rec() for _ in range(4)])
    rows, _ = m.emit_rows(_cf(reader="reading_order_courts", caveat="incl. VAT"), URL, FAKE_PDF, "pdf", None)
    assert rows[0]["source_caveat"] == "incl. VAT"


@pytest.mark.parametrize(("n", "conf"), [(1, "low"), (3, "low"), (4, "medium"), (20, "medium"), (21, "high")])
def test_confidence_thresholds(monkeypatch, n, conf):
    monkeypatch.setattr(m, "read_courts", lambda b, mp: [_rec() for _ in range(n)])
    rows, stat = m.emit_rows(_cf(reader="reading_order_courts"), URL, FAKE_PDF, "pdf", None)
    assert stat["confidence"] == conf
    assert all(r["extraction_confidence"] == conf for r in rows)


# ── generic PDF branch: word-geometry -> reading-order fallback -> unparsed ───


def _geometry_pdf(note: str = "") -> bytes:
    """Digital PDF with a 3-column header the word-geometry reader resolves."""
    import fitz

    doc = fitz.open()
    page = doc.new_page()
    if note:
        page.insert_text((40, 30), note)
    page.insert_text((40, 55), "Supplier")
    page.insert_text((300, 55), "Amount")
    page.insert_text((420, 55), "Description")
    for i in range(6):
        y = 80 + i * 20
        page.insert_text((40, y), f"Geometry Supplier {i} Ltd")
        page.insert_text((300, y), f"{25000 + i},000.00")
        page.insert_text((420, y), f"Row {i} services")
    b = doc.tobytes()
    doc.close()
    return b


def test_generic_pdf_geometry_path():
    rows, stat = m.emit_rows(_cf(), URL, _geometry_pdf(), "pdf", None)
    assert stat == {"status": "ok", "rows": 6, "confidence": "medium"}
    r0 = rows[0]
    assert r0["supplier_raw"] == "Geometry Supplier 0 Ltd"
    assert r0["amount_eur"] == 25000000.0
    assert r0["description"] == "Row 0 services"
    assert r0["parser_name"] == "public_body_pdf"
    assert r0["source_page_number"] == 1
    assert r0["caveat_text_detected"] is False


def test_generic_pdf_caveat_detected_on_page0():
    rows, _ = m.emit_rows(_cf(), URL, _geometry_pdf(note="Note: figures exclude VAT"), "pdf", None)
    assert rows and all(r["caveat_text_detected"] for r in rows)


def test_generic_pdf_falls_back_to_reading_order_when_geometry_finds_nothing():
    """Field-per-line PDF (no column grid): geometry yields 0 rows -> the
    amount-anchored reading-order fallback parses it instead."""
    import fitz

    doc = fitz.open()
    doc.new_page().insert_text(
        (40, 50),
        "Supplier Name\nDescription\nAmount Excluding Vat\n"
        "Kevin Keogh Electrical Ltd\nElectrical Contracting Services\n208,410.00\n"
        "Marine Institute\nMesopelagic Resources Study\n135,000.00\n"
        "Fallback Supplier Three\nMore Services\n99,000.00\n"
        "Fallback Supplier Four\nOther Services\n88,000.00\n",
    )
    b = doc.tobytes()
    doc.close()
    rows, stat = m.emit_rows(_cf(), URL, b, "pdf", None)
    assert stat["status"] == "ok"
    assert stat["rows"] == 4
    suppliers = {r["supplier_raw"] for r in rows}
    assert "Kevin Keogh Electrical Ltd" in suppliers
    assert rows[0]["paid_flag"] is None  # fallback carries no paid column


def test_generic_pdf_unparsed_when_nothing_extracts():
    import fitz

    doc = fitz.open()
    doc.new_page()  # blank: not digital, no header, nothing for the fallback
    b = doc.tobytes()
    doc.close()
    rows, stat = m.emit_rows(_cf(), URL, b, "pdf", None)
    assert rows == []
    assert stat == {
        "status": "unparsed",
        "reason": "scanned/no-header/no-amount",
        "rows": 0,
        "confidence": "low",
        "pages": 1,
    }


def test_generic_pdf_drops_title_and_category_rows():
    """A banner row ('Payments greater than €20,000') and a 'Total' row must not
    survive as suppliers."""
    import fitz

    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((40, 55), "Supplier")
    page.insert_text((300, 55), "Amount")
    page.insert_text((420, 55), "Description")
    page.insert_text((40, 80), "Payments greater than")
    page.insert_text((300, 80), "20,000.00")
    page.insert_text((40, 100), "Total")
    page.insert_text((300, 100), "999,999.00")
    for i in range(4):
        y = 130 + i * 20
        page.insert_text((40, y), f"Real Supplier {i} Ltd")
        page.insert_text((300, y), f"{30 + i},000.00")
        page.insert_text((420, y), "Services")
    b = doc.tobytes()
    doc.close()
    rows, stat = m.emit_rows(_cf(), URL, b, "pdf", None)
    suppliers = [r["supplier_raw"] for r in rows]
    assert stat["rows"] == 4
    assert all(s.startswith("Real Supplier") for s in suppliers)


# ── tabular branch (csv exercises the shared tabular path end-to-end) ─────────


def _csv(body: str) -> bytes:
    return body.encode("utf-8")


def test_csv_wiring_and_parser_name():
    b = _csv(
        "Supplier Name,Amount,Description,Order No,Paid Y/N\n"
        'ACME Consulting Ltd,"25,000.00",Widget services,PO-77,Y\n'
        'Second Supplier Ltd,"30,000.00",Other services,PO-78,N\n'
        'Third Supplier Ltd,"31,000.00",Other services,PO-79,Y\n'
        'Fourth Supplier Ltd,"32,000.00",Other services,PO-80,Y\n'
    )
    rows, stat = m.emit_rows(_cf(), URL, b, "csv", None)
    assert stat == {"status": "ok", "rows": 4, "confidence": "medium"}
    assert rows[0] == _expected_row(
        source_file_hash=hashlib.sha256(b).hexdigest()[:16],
        parser_name="public_body_csv",
        source_page_number=None,
    )


def test_csv_no_amount_column_is_unparsed():
    b = _csv("Supplier Name,Description\nACME Consulting Ltd,Widget services\n")
    rows, stat = m.emit_rows(_cf(), URL, b, "csv", None)
    assert rows == []
    assert stat == {"status": "unparsed", "reason": "no-amount-col", "rows": 0, "confidence": "low"}


def test_csv_caveat_in_title_row_detected():
    # real gov.ie title rows pad to the column count with trailing commas; a bare
    # single-cell title makes polars infer a 1-column schema and drop the data
    b = _csv(
        "Payments over 20000 - note: figures exclude VAT,\n"
        "Supplier Name,Amount\n"
        'ACME Consulting Ltd,"25,000.00"\n'
    )
    rows, _ = m.emit_rows(_cf(), URL, b, "csv", None)
    assert rows and all(r["caveat_text_detected"] for r in rows)


def test_csv_category_total_supplier_skipped():
    b = _csv(
        "Supplier Name,Amount\n"
        'Grand Total,"999,999.00"\n'
        'ACME Consulting Ltd,"25,000.00"\n'
    )
    rows, stat = m.emit_rows(_cf(), URL, b, "csv", None)
    assert stat["rows"] == 1
    assert rows[0]["supplier_raw"] == "ACME Consulting Ltd"


# ── post-processing: blank-supplier repair + downgrade ────────────────────────


def test_blank_supplier_promoted_from_name_like_po(monkeypatch):
    """Mis-mapped column: supplier empty, the company name sits in po_number.
    A name-like PO with no big number is promoted back to supplier_raw."""
    monkeypatch.setattr(m, "read_courts", lambda b, mp: [_rec(supplier="", ref="AN POST LTD") for _ in range(4)])
    rows, _ = m.emit_rows(_cf(reader="reading_order_courts"), URL, FAKE_PDF, "pdf", None)
    assert rows[0]["supplier_raw"] == "AN POST LTD"
    assert rows[0]["po_number"] is None
    assert rows[0]["extraction_confidence"] == "medium"  # repaired row keeps its confidence
    assert rows[0]["caveat_text_detected"] is False


def test_blank_supplier_with_numeric_po_downgraded_not_promoted(monkeypatch):
    """A category-total line ('Meter Reading Services 3,823,410') must NOT become
    a supplier; the row is downgraded to low + caveat-flagged instead."""
    monkeypatch.setattr(
        m,
        "read_courts",
        lambda b, mp: [_rec(supplier="", ref="Meter Reading Services 3,823,410") for _ in range(4)],
    )
    rows, _ = m.emit_rows(_cf(reader="reading_order_courts"), URL, FAKE_PDF, "pdf", None)
    assert rows[0]["supplier_raw"] == ""
    assert rows[0]["extraction_confidence"] == "low"
    assert rows[0]["caveat_text_detected"] is True


def test_blank_supplier_blank_po_downgraded(monkeypatch):
    monkeypatch.setattr(m, "read_courts", lambda b, mp: [_rec(supplier="", ref="") for _ in range(4)])
    rows, stat = m.emit_rows(_cf(reader="reading_order_courts"), URL, FAKE_PDF, "pdf", None)
    assert rows[0]["extraction_confidence"] == "low"
    assert rows[0]["caveat_text_detected"] is True
    assert stat["confidence"] == "medium"  # stat keeps the branch confidence; downgrade is per-row


def test_empty_reader_result_is_empty_status(monkeypatch):
    monkeypatch.setattr(m, "read_courts", lambda b, mp: [])
    rows, stat = m.emit_rows(_cf(reader="reading_order_courts"), URL, FAKE_PDF, "pdf", None)
    assert rows == []
    assert stat == {"status": "empty", "rows": 0, "confidence": "low"}
