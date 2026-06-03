"""Tests for the LA Purchase-Orders/Payments extractor
(pipeline_sandbox/procurement_la_payments_extract.py).

Three layers, mirroring test_afs_amalgamated.py:
  1. Pure-function units — the parsing primitives that actually break (amount coercion,
     PO#/ID-prefix strip, the largest-x-gap PDF row split, the header-vs-title-row picker,
     period-from-URL, privacy classification). No network, no files → run in CI.
  2. Reader round-trip on a synthesised in-memory XLSX — proves the tabular reader +
     auto-debit-sign detection without committing a council's binary file.
  3. Data-integrity invariants on the committed golden parquet slice (taxonomy mapping,
     safe-to-sum gate, privacy gate, CRO band, no amount outliers).

Regenerate the golden slice: re-run the extractor, then test/fixtures/la_payments/_generate.py.
"""

from __future__ import annotations

import io
import sys
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline_sandbox"))
import procurement_la_payments_extract as m  # noqa: E402

FX = Path(__file__).resolve().parent / "fixtures" / "la_payments"
GOLDEN = FX / "la_payments_golden.parquet"


def _word(x0, x1, text):
    """A fitz 'words' tuple: (x0, y0, x1, y1, text, block, line, word_no)."""
    return (float(x0), 100.0, float(x1), 110.0, text, 0, 0, 0)


# ---- 1. pure-function units -------------------------------------------------
@pytest.mark.parametrize(("raw", "expected"), [
    ("€1,234.56", 1234.56),
    ("1,234.56", 1234.56),
    ("-61594.33", -61594.33),     # Limerick/Kilkenny debit sign
    ("(20,000.00)", -20000.0),    # parenthesised negative
    ("139,850.00", 139850.0),
    (22088.34, 22088.34),         # already numeric (xlsx cell)
    ("", None),
    ("n/a", None),
])
def test_to_eur(raw, expected):
    assert m.to_eur(raw) == (pytest.approx(expected) if expected is not None else None)


@pytest.mark.parametrize(("raw", "expected"), [
    ("539106 A HORTON LTD", "A HORTON LTD"),          # single ID prefix (Mayo/Donegal)
    ("400173 12 KEVIN THORPE LIMITED", "KEVIN THORPE LIMITED"),  # PO# + vendor ID (two runs)
    ("AECOM Ireland Limited", "AECOM Ireland Limited"),  # clean name unchanged
])
def test_strip_id_prefix(raw, expected):
    assert m.strip_id_prefix(raw) == expected


def test_split_row_largest_gap_amount_last():
    # "LAWLER BUILDERS LTD   <gap>   Construction   139,850.00"  (amount last = Galway shape)
    words = [_word(10, 40, "LAWLER"), _word(42, 70, "BUILDERS"), _word(72, 90, "LTD"),
             _word(200, 260, "Construction"), _word(400, 460, "139,850.00")]
    rec = m.split_row(words)
    assert rec is not None
    assert rec["supplier"] == "LAWLER BUILDERS LTD"
    assert rec["eur"] == pytest.approx(139850.0)
    assert "Construction" in (rec["description"] or "")


def test_split_row_no_money_returns_none():
    assert m.split_row([_word(10, 40, "SOME"), _word(42, 70, "TEXT")]) is None


def test_tabular_header_beats_title_row():
    """Regression: a Kilkenny-style title cell holding '…Orders Over €20,000…' must not be
    chosen as the header over the real multi-column header row beneath it."""
    grid = [
        ["Kilkenny County Council Purchase Orders Over €20,000 for Quarter 3 2024", None, None, None],
        ["Order No", "Supplier", "EURO", "Description"],
        ["400171680", "DAVID WALSH CIVIL ENGINEERING LTD", -21188.82, "Engineering"],
        ["400171809", "ST CANICES COMMUNITY LTD", -40000, "Community Grant"],
    ]
    rows, _ = m._tabular_rows(grid)
    assert rows, "expected rows from the real header"
    assert rows[0]["supplier"].startswith("DAVID WALSH")
    assert m.to_eur(rows[0]["eur"]) == pytest.approx(-21188.82)


@pytest.mark.parametrize(("url", "year", "quarter"), [
    ("https://x/purchase-orders-quarter-1-2026.pdf", 2026, 1),
    ("https://x/Qtr%204%202025%20%28ENG%29.pdf", 2025, 4),     # %20 must not read as "2020"
    ("https://x/purchase-order-over-20-000-quarter-4-20251.xlsx", 2025, 4),  # CMS dedup suffix
    ("https://x/2024.pdf", 2024, None),                         # Donegal yearly
])
def test_period_from_url(url, year, quarter):
    period, y, q = m.period_from_url(url)
    assert y == year
    assert q == quarter


# ---- 2. reader round-trip on a synthesised xlsx ----------------------------
def _build_xlsx(headers, rows) -> bytes:
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(headers)
    for r in rows:
        ws.append(r)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


def test_read_xlsx_roundtrip_and_debit_sign():
    b = _build_xlsx(
        ["Order No", "Supplier", "EURO", "Description"],
        [["400171680", "DAVID WALSH CIVIL ENGINEERING LTD", -21188.82, "Engineering"],
         ["400171809", "CARROLL QUARRIES LTD", -40000.0, "Materials"],
         ["400171999", "ENERGIA LTD", -25035.56, "Utilities"]],
    )
    cf = m.la("synthetic", "Synthetic", "Test", "county", fmt="xlsx",
              listing="https://x", value_kind="po_committed")
    rows, stat = m.emit_file(cf, "https://x/pos-q1-2025.xlsx", b, ".xlsx")
    assert stat["valid"] and len(rows) == 3
    # debit-sign auto-detected → amounts abs'd to positive
    assert all(r["amount_eur"] > 0 for r in rows)
    assert rows[0]["supplier_raw"].startswith("DAVID WALSH")
    assert rows[0]["value_kind"] == "po_committed"
    assert rows[0]["realisation_tier"] == "COMMITTED"


# ---- 3. privacy classification ---------------------------------------------
def test_classify_quarantines_personal_data():
    df = pl.DataFrame({
        "supplier_raw": ["AECOM IRELAND LIMITED", "JOHN MURPHY", "539106", "Cork County Council"],
        "supplier_is_id_code": [False, False, True, False],
        "amount_eur": [50000.0, 30000.0, 25000.0, 22000.0],
        "value_kind": ["po_committed"] * 4,
    })
    out = m.classify_and_flag(df)
    by = {r["supplier_raw"]: r for r in out.iter_rows(named=True)}
    assert by["AECOM IRELAND LIMITED"]["supplier_class"] == "company"
    assert by["AECOM IRELAND LIMITED"]["public_display"] is True
    assert by["JOHN MURPHY"]["supplier_class"] == "sole_trader_or_individual"
    assert by["JOHN MURPHY"]["public_display"] is False
    assert by["JOHN MURPHY"]["privacy_status"] == "quarantined"
    assert by["539106"]["supplier_class"] == "id_code"
    assert by["539106"]["public_display"] is False
    assert by["Cork County Council"]["supplier_class"] == "public_body"
    assert by["Cork County Council"]["public_display"] is True


def test_value_kind_tier_map():
    assert m.TIER == {"po_committed": "COMMITTED", "payment_actual": "SPENT"}


# ---- 4. golden parquet invariants ------------------------------------------
@pytest.fixture(scope="module")
def golden() -> pl.DataFrame:
    if not GOLDEN.exists():
        pytest.skip("golden slice not generated yet (run test/fixtures/la_payments/_generate.py)")
    return pl.read_parquet(GOLDEN)


def test_schema_has_master_taxonomy(golden: pl.DataFrame):
    for col in ("value_kind", "realisation_tier", "value_safe_to_sum",
                "supplier_class", "privacy_status", "public_display"):
        assert col in golden.columns


def test_realisation_tier_matches_value_kind(golden: pl.DataFrame):
    bad = golden.filter(pl.col("realisation_tier") != pl.col("value_kind").replace_strict(m.TIER, default=None))
    assert bad.height == 0


def test_safe_to_sum_gate(golden: pl.DataFrame):
    # value_safe_to_sum ⟺ value_kind ∈ {po_committed, payment_actual} AND amount > 0
    expected = (golden["value_kind"].is_in(["po_committed", "payment_actual"])
                & (golden["amount_eur"] > 0))
    assert (golden["value_safe_to_sum"] == expected).all()


def test_privacy_gate(golden: pl.DataFrame):
    # quarantined ⟹ not public_display ; personal-data classes ⟹ quarantined
    quar = golden.filter(pl.col("privacy_status") == "quarantined")
    assert (~quar["public_display"]).all()
    personal = golden.filter(pl.col("supplier_class").is_in(["sole_trader_or_individual", "id_code", "unknown"]))
    assert (personal["privacy_status"] == "quarantined").all()


def test_no_amount_outliers(golden: pl.DataFrame):
    # a council PO line over €50m is almost always a mis-parse (ID-as-amount); guard against
    # the €400m Kilkenny-order-number and €12bn Fingal-ID regressions.
    assert golden.filter(pl.col("amount_eur") > 50_000_000).height == 0


def test_cro_band_for_companies(golden: pl.DataFrame):
    # company-class suppliers should match CRO in the validated 30–75% band; a collapse to
    # ~0% means the supplier column drifted (the Fingal/Laois failure mode).
    cro = pl.read_parquet(ROOT / "data/silver/cro/companies.parquet").select(["name_norm", "company_num"])
    co = (golden.filter(pl.col("supplier_class") == "company")
          .select(pl.col("supplier_normalised").alias("name_norm")).unique())
    if co.height < 20:
        pytest.skip("too few company suppliers in the slice for a stable rate")
    hit = co.join(cro, on="name_norm", how="inner").height / co.height
    assert 0.25 <= hit <= 0.85, f"CRO company match rate {hit:.0%} outside sane band"
