"""Tests for services.coverage_qa — the source↔output control-total / yield gate."""

from __future__ import annotations

import pytest

from services.coverage_qa import (
    CoverageError,
    amount_tokens,
    assert_yield,
    pdf_amount_yield,
    reconcile,
)


def test_amount_tokens_ignore_bare_reference_numbers():
    # The core caution: 5-digit PO refs (52355) are >= EUR20k but bare integers — they
    # must NOT be counted as amounts, or the control total doubles and fakes a shortfall.
    text = "52355 CAPE WRATH HOTEL\n€3,770,309.86 Ukraine Accommodation\n19/01/2024"
    amts = amount_tokens(text, threshold=20_000)
    assert amts == [3_770_309.86]  # the ref 52355 is excluded


def test_amount_tokens_threshold_and_grouping():
    text = "26,880.00\n19.99\n1200000.00\n31,741.38"
    assert amount_tokens(text, threshold=20_000) == [26_880.00, 1_200_000.00, 31_741.38]


def test_yield_report_math():
    r = reconcile(252, 1, "file.pdf", expected_eur=48_000_000, extracted_eur=20_000)
    assert r.missing == 251
    assert r.yield_frac == pytest.approx(1 / 252)
    assert r.eur_yield_frac < 0.001


def test_assert_yield_passes_healthy_and_trips_on_under_extraction():
    healthy = reconcile(2349, 2325, "ok.pdf")
    assert_yield(healthy, min_yield=0.90)  # ~99% -> ok
    broken = reconcile(252, 1, "broken.pdf")
    with pytest.raises(CoverageError):
        assert_yield(broken, min_yield=0.90)


def test_assert_yield_noop_when_no_source_signal():
    # No amount tokens found (e.g. image-only PDF) -> expected=0 -> cannot judge, never raises.
    assert_yield(reconcile(0, 0, "scanned.pdf"), min_yield=0.99)


def test_tabular_amount_rows_uses_detected_amount_column(tmp_path):
    # A ref/voucher column whose values exceed €20k must NOT be counted as amounts — only
    # the detected amount column. 3 rows, amounts 25k/30k/15k -> 2 disclosable (>=20k).
    from services.coverage_qa import tabular_amount_rows

    p = tmp_path / "po.csv"
    p.write_text(
        "Reference,Supplier,Amount,Description\n"
        "550001,Acme Ltd,25000.00,Services\n"
        "550002,Beta Ltd,30000.00,Goods\n"
        "550003,Gamma Ltd,15000.00,Minor\n"
    )
    assert tabular_amount_rows(p, threshold=20_000) == 2  # the 550001-3 refs are not counted


def test_tabular_amount_rows_none_for_unsupported(tmp_path):
    from services.coverage_qa import tabular_amount_rows

    p = tmp_path / "x.pdf"
    p.write_bytes(b"%PDF-1.4")
    assert tabular_amount_rows(p) is None


def test_pdf_amount_yield_on_synthetic_pdf(tmp_path):
    fitz = pytest.importorskip("fitz")
    p = tmp_path / "mini.pdf"
    doc = fitz.open()
    page = doc.new_page()
    # 3 PO lines (ref + amount) — refs must be ignored, 3 amounts counted.
    page.insert_text((72, 72), "26939\n26,880.00\n26940\n46,716.60\n26946\n31,741.38")
    doc.save(p)
    doc.close()
    rep = pdf_amount_yield(p, extracted_count=2, threshold=20_000)
    assert rep.expected == 3  # three amounts, zero refs miscounted
    assert rep.extracted == 2
    assert rep.missing == 1
