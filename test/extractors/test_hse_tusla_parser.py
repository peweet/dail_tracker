"""Unit tests for the PURE helpers in extractors/procurement_hse_tusla_parser.py.

These are the bespoke HSE/Tusla PO-PDF column-geometry parsers (PRE-ETL, not wired to
pipeline.py). We only exercise the side-effect-free functions: the x-cut column bucketer,
the light supplier normaliser, the per-publisher row builders, and the year->spec selectors.
Everything is built from synthetic word/cell inputs — no network, no PDF IO, no parquet, and
no assertions against real data files.

`parse`, `dq`, `main`, and `url_for` are deliberately skipped: they fetch PDFs / read the
probe JSON / build DataFrames (IO).

The module guards its own imports by inserting the repo ROOT on sys.path, but its sibling
import `from sample_extract_procurement_pdf import ...` only resolves when the `extractors/`
directory itself is importable, so we add it here before importing the module under test.
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
# `extractors/` is a namespace package (no __init__); the module under test does a bare
# `import sample_extract_procurement_pdf`, which only resolves with that dir on sys.path.
sys.path.insert(0, str(ROOT / "extractors"))

from extractors.procurement_hse_tusla_parser import (  # noqa: E402
    cols_by_xcuts,
    hse_row,
    hse_spec_for,
    norm_name,
    tusla_row,
    tusla_row_2025,
    tusla_spec_for,
)


def _w(x0: float, x1: float, text: str) -> tuple:
    """A minimal PyMuPDF word tuple: cols_by_xcuts only reads x0=[0], x1=[2], text=[4].

    The y/block/line slots are filler — keeping them present mirrors the real
    (x0, y0, x1, y1, "word", block, line, word_no) shape the production reader passes in.
    """
    return (x0, 0.0, x1, 0.0, text, 0, 0, 0)


# ─────────────────────────────────── cols_by_xcuts ───────────────────────────────────
def test_cols_by_xcuts_buckets_by_word_centre():
    """Words are bucketed by their CENTRE x ((x0+x1)/2) against the cut list, producing
    len(cuts)+1 columns. This is the core of every parser, so the bucket boundaries must be
    exact: a word straddling a cut belongs to whichever side its midpoint lands on."""
    words = [
        _w(100, 140, "ACME"),  # centre 120  -> bucket 0 (<180)
        _w(150, 170, "LTD"),  # centre 160  -> bucket 0 (<180)
        _w(200, 240, "PO123"),  # centre 220  -> bucket 1 (180-270)
        _w(300, 400, "GL"),  # centre 350  -> bucket 2 (270-450)
        _w(460, 500, "1,234.50"),  # centre 480 -> bucket 3 (450-515)
        _w(520, 560, "Q4"),  # centre 540  -> bucket 4 (>515)
        _w(565, 600, "2021"),  # centre 582  -> bucket 4 (>515)
    ]
    assert cols_by_xcuts(words, [180, 270, 450, 515]) == [
        "ACME LTD",
        "PO123",
        "GL",
        "1,234.50",
        "Q4 2021",
    ]


def test_cols_by_xcuts_sorts_by_x_independent_of_input_order():
    """Within a bucket, words are joined in left-to-right x order regardless of the order the
    reader hands them in. PyMuPDF does not guarantee word order, so a vendor like 'ACME LTD'
    must not come back scrambled as 'LTD ACME'."""
    words = [_w(150, 170, "LTD"), _w(100, 140, "ACME")]  # supplied right-to-left
    assert cols_by_xcuts(words, [180]) == ["ACME LTD", ""]


def test_cols_by_xcuts_strips_separator_chars_and_makes_empty_buckets():
    """Each joined bucket is stripped of leading/trailing space, '-', ':' and '|' (column-rule
    glyphs that bleed in from PDF borders), and an empty column comes back as '' (not dropped),
    so downstream positional unpacking stays aligned."""
    words = [_w(50, 90, "- foo |"), _w(600, 640, ": bar -")]
    # cut at 300: 'foo' lands left, 'bar' lands right, the middle implied buckets do not exist
    assert cols_by_xcuts(words, [300]) == ["foo", "bar"]
    # no words at all -> still one empty bucket per (cuts+1)
    assert cols_by_xcuts([], [100, 200]) == ["", "", ""]


# ───────────────────────────────────── norm_name ─────────────────────────────────────
def test_norm_name_collapses_whitespace_and_uppercases():
    """norm_name is the light dedup/DQ key: it upper-cases, collapses runs of whitespace to a
    single space, and trims surrounding spaces/dots/commas so '  acme   ltd.  ' and 'ACME LTD'
    count as one supplier."""
    assert norm_name("  acme   ltd.  ") == "ACME LTD"
    assert norm_name("a.b, ") == "A.B"


def test_norm_name_strips_leading_and_trailing_id_digits():
    """OCR/column-bleed often glues a doc-ref or year onto the vendor. norm_name drops a LEADING
    run of >=3 digits and a TRAILING run of >=4 digits so '12345 ACME LTD' and 'ACME LTD 67890'
    both collapse to 'ACME LTD' — but a short 2-digit prefix is NOT a ref, so it's kept."""
    assert norm_name("12345 ACME LTD") == "ACME LTD"
    assert norm_name("ACME LTD 67890") == "ACME LTD"
    # 2-digit lead is below the >=3 threshold -> preserved; 4-digit trailing year is stripped
    assert norm_name("99 ACME 2021") == "99 ACME"


def test_norm_name_preserves_accents_and_handles_empty():
    """This is NOT the CRO join key (the NFKD accent-fold lives elsewhere), so accents are kept
    as-is — only cased up. And a None/empty input must not raise; it yields ''."""
    assert norm_name("Tír na nÓg") == "TÍR NA NÓG"
    assert norm_name("") == ""
    assert norm_name(None) == ""  # `s or ""` guards the None path


# ────────────────────────────────────── hse_row ──────────────────────────────────────
def test_hse_row_happy_path_parses_amount_and_period():
    """The HSE 5-column layout (VENDOR|DOC REF|GL DESC|€AMOUNT|Qx YYYY) must map positionally,
    parse the euro amount, and split the period token into quarter+year. The period regex spans
    BOTH eras: 'Q4-2025' (hyphen) and 'Q42021' (no separator) via \\D*."""
    rec = hse_row(["ACME LTD", "PO123", "Imaging Services", "1,234.50", "Q4-2025"], page=2, idx=7)
    assert rec is not None
    assert rec["publisher_id"] == "ie_hse"
    assert rec["amount_eur"] == 1234.50
    assert rec["amount_semantics"] == "payment_incl_vat"
    assert rec["year"] == 2025
    assert rec["quarter"] == "Q4"
    assert rec["supplier_raw"] == "ACME LTD"
    assert rec["supplier_norm"] == "ACME LTD"
    assert rec["description"] == "Imaging Services"
    assert rec["doc_ref"] == "PO123"
    assert rec["source_page"] == 3  # 0-based page+1
    assert rec["source_row"] == 7


def test_hse_row_handles_separatorless_period_token():
    """Regression on the historic cumulative file: its period token is 'Q42021' with no
    separator. The \\D* between the quarter digit and the year must still split it correctly."""
    rec = hse_row(["VENDOR", "REF", "Desc", "10,000.00", "Q42021"], page=0, idx=0)
    assert rec is not None
    assert rec["quarter"] == "Q4"
    assert rec["year"] == 2021


def test_hse_row_missing_period_keeps_amount_but_nulls_period():
    """If the period column is junk, the row is still a valid payment (amount parsed) — only the
    quarter/year go None. We must NOT reject the row, or we'd silently drop real spend."""
    rec = hse_row(["VENDOR", "REF", "Desc", "500.00", "no-period-here"], page=0, idx=1)
    assert rec is not None
    assert rec["amount_eur"] == 500.00
    assert rec["quarter"] is None
    assert rec["year"] is None


def test_hse_row_rejects_when_amount_unparseable():
    """A row whose amount cell has no numeric token (e.g. a stray header/blank line) returns
    None and is dropped — the amount is the required anchor for a payment record."""
    assert hse_row(["VENDOR", "REF", "Desc", "", "Q4-2025"], page=0, idx=0) is None
    assert hse_row(["VENDOR", "REF", "Desc", "N/A", "Q4-2025"], page=0, idx=0) is None


# ───────────────────────────────────── tusla_row ─────────────────────────────────────
def test_tusla_row_happy_path():
    """The Tusla 6-column layout (YEAR|QTR|DATE|AMOUNT|VENDOR|DESC) maps positionally; the date
    cell becomes doc_ref and the quarter is matched by its digit (tolerating 'Q1-2024')."""
    rec = tusla_row(["2023", "Q1", "01/02/2023", "9,876.00", "BETA SUPPLIES LTD", "Childcare"], page=4, idx=2)
    assert rec is not None
    assert rec["publisher_id"] == "ie_tusla"
    assert rec["amount_semantics"] == "invoice_payment"
    assert rec["year"] == 2023
    assert rec["quarter"] == "Q1"
    assert rec["supplier_raw"] == "BETA SUPPLIES LTD"
    assert rec["supplier_norm"] == "BETA SUPPLIES LTD"
    assert rec["description"] == "Childcare"
    assert rec["doc_ref"] == "01/02/2023"
    assert rec["source_page"] == 5
    assert rec["source_row"] == 2


def test_tusla_row_tolerates_combined_quarter_token_and_nonyear_cell():
    """The 2024 file fuses the quarter as 'Q1-2024'; the digit must still be picked up. And a
    non-4-digit year cell (e.g. a blank/garbled value) yields year=None rather than crashing."""
    rec = tusla_row(["", "Q1-2024", "15/03/2024", "1,000.00", "GAMMA LTD", "Services"], page=0, idx=0)
    assert rec is not None
    assert rec["quarter"] == "Q1"
    assert rec["year"] is None  # '' is not a 4-digit year


def test_tusla_row_rejects_when_amount_unparseable():
    """No numeric amount token -> None (dropped), same anchor rule as HSE."""
    assert tusla_row(["2023", "Q1", "01/02/2023", "", "VENDOR", "Desc"], page=0, idx=0) is None


# ──────────────────────────────────── tusla_row_2025 ─────────────────────────────────
def test_tusla_row_2025_drops_year_column_and_hardcodes_2025():
    """The 2025 file dropped the per-row Year column (year is only in the page title) and split
    '€' into its own token, leaving 5 buckets: QTR|DATE|€AMOUNT|VENDOR|DESC. The builder pins
    year=2025 and reads the amount from the 3rd cell."""
    rec = tusla_row_2025(["Q2", "10/04/2025", "2,500.00", "DELTA LTD", "Fostering"], page=1, idx=3)
    assert rec is not None
    assert rec["publisher_id"] == "ie_tusla"
    assert rec["year"] == 2025
    assert rec["quarter"] == "Q2"
    assert rec["amount_eur"] == 2500.00
    assert rec["supplier_raw"] == "DELTA LTD"
    assert rec["doc_ref"] == "10/04/2025"
    assert rec["source_page"] == 2
    assert rec["source_row"] == 3


def test_tusla_row_2025_rejects_when_amount_unparseable():
    """If the 5-bucket split lands a non-numeric token in the amount slot, the row is dropped."""
    assert tusla_row_2025(["Q2", "10/04/2025", "N/A", "DELTA LTD", "Fostering"], page=0, idx=0) is None


# ─────────────────────────────── hse_spec_for / tusla_spec_for ───────────────────────
def test_hse_spec_for_selects_era_cuts_with_historic_fallback():
    """HSE re-typeset its PDFs: 2025/2026 files shifted every column left, so the spec selector
    must return the new cuts for those years and the original historic cuts for everything else
    (including None) — picking the wrong cuts drops the amount into the wrong bucket (0 rows)."""
    cuts_2025, builder_2025 = hse_spec_for(2025)
    assert cuts_2025 == [160, 225, 400, 500]
    assert builder_2025 is hse_row
    assert hse_spec_for(2026)[0] == [160, 225, 400, 500]
    # any unlisted year -> historic layout
    assert hse_spec_for(2021)[0] == [180, 270, 450, 515]
    assert hse_spec_for(None)[0] == [180, 270, 450, 515]


def test_tusla_spec_for_selects_yearly_cuts_and_2025_builder():
    """Tusla's geometry changes year to year, and 2025 even changes the bucket COUNT/builder
    (no Year column -> tusla_row_2025). The selector must return the right (cuts, builder) pair
    per year and fall back to the 2021 layout for unlisted years / None."""
    cuts_2021, b_2021 = tusla_spec_for(2021)
    assert cuts_2021 == [110, 160, 290, 375, 570]
    assert b_2021 is tusla_row
    assert tusla_spec_for(2022) == ([105, 145, 220, 320, 560], tusla_row)
    assert tusla_spec_for(2024) == ([72, 120, 180, 225, 360], tusla_row)
    # 2025 uniquely swaps in the 5-bucket builder
    cuts_2025, b_2025 = tusla_spec_for(2025)
    assert cuts_2025 == [80, 140, 205, 410]
    assert b_2025 is tusla_row_2025
    # unlisted year / None -> 2021 fallback
    assert tusla_spec_for(1999) == ([110, 160, 290, 375, 570], tusla_row)
    assert tusla_spec_for(None) == ([110, 160, 290, 375, 570], tusla_row)
