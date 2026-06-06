"""Unit tests for legislation/bill_amendments_flatten.py.

Covers the pure URI-slug helpers and an end-to-end ``main()`` run against a
hand-built bronze JSON (paths monkeypatched to ``tmp_path``), asserting the
flattened silver schema: amendment_type / chamber slugs, the ``{year}_{no}``
bill_id, date parsing, and the pdf_url dropna.

(legislation/legislation.py and questions.py execute their transforms at
import time and so can't be unit-imported without pipeline data; this module
is the cleanly-importable transform in the package.)
"""

from __future__ import annotations

import datetime as dt
import json

import polars as pl
import pytest

import legislation.bill_amendments_flatten as m
from legislation.bill_amendments_flatten import _amend_type_slug, _chamber_slug


@pytest.mark.parametrize(
    "uri,expected",
    [
        ("https://data.oireachtas.ie/akn/ie/house/Dail", "dail"),
        ("https://data.oireachtas.ie/akn/ie/house/Seanad/", "seanad"),  # trailing slash
        (None, None),
        ("", None),
    ],
)
def test_chamber_slug(uri, expected):
    assert _chamber_slug(uri) == expected


@pytest.mark.parametrize(
    "uri,expected",
    [
        ("https://data.oireachtas.ie/akn/ie/.../numberedList", "numberedList"),  # case preserved
        ("https://x/creamList/", "creamList"),
        (None, None),
    ],
)
def test_amend_type_slug(uri, expected):
    assert _amend_type_slug(uri) == expected


def _amendment(pdf_uri, *, date="2025-06-01", type_uri="https://x/numberedList", chamber="https://x/house/Dail"):
    al = {
        "date": date,
        "showAs": "Numbered List [Dáil]",
        "stageNo": "3",
        "stage": {"showAs": "Committee Stage"},
        "amendmentTypeUri": {"uri": type_uri},
        "chamber": {"uri": chamber},
    }
    if pdf_uri is not None:
        al["formats"] = {"pdf": {"uri": pdf_uri}}
    return {"amendmentList": al}


def _write_source(tmp_path, bills):
    src = tmp_path / "legislation_results_unscoped.json"
    src.write_text(json.dumps([{"results": bills}]), encoding="utf-8")
    return src


def _bill(no, year, lists, **over):
    bill = {
        "billNo": no,
        "billYear": year,
        "billType": "Public",
        "shortTitleEn": f"Bill {no}",
        "status": "Current",
        "amendmentLists": lists,
    }
    bill.update(over)
    return {"bill": bill}


def test_main_flattens_amendment_to_silver_schema(tmp_path, monkeypatch):
    _write_source(tmp_path, [_bill("75", "2025", [_amendment("https://data.oireachtas.ie/amd/75.pdf")])])
    monkeypatch.setattr(m, "LEGISLATION_DIR", tmp_path)
    monkeypatch.setattr(m, "SILVER_PARQUET_DIR", tmp_path)

    rc = m.main()
    assert rc == 0

    out = pl.read_parquet(tmp_path / "bill_amendments.parquet")
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["bill_id"] == "2025_75"
    assert row["bill_no"] == "75" and row["bill_year"] == "2025"
    assert row["amendment_type"] == "numberedList"
    assert row["chamber"] == "dail"
    assert row["stage_show_as"] == "Committee Stage"
    assert row["amendment_date"] == dt.date(2025, 6, 1)
    assert row["pdf_url"] == "https://data.oireachtas.ie/amd/75.pdf"
    assert row["bill_short_title_en"] == "Bill 75"


def test_main_drops_amendments_without_pdf(tmp_path, monkeypatch):
    bills = [
        _bill(
            "75",
            "2025",
            [
                _amendment("https://x/has.pdf"),
                _amendment(None),  # no pdf → dropped
            ],
        )
    ]
    _write_source(tmp_path, bills)
    monkeypatch.setattr(m, "LEGISLATION_DIR", tmp_path)
    monkeypatch.setattr(m, "SILVER_PARQUET_DIR", tmp_path)

    assert m.main() == 0
    out = pl.read_parquet(tmp_path / "bill_amendments.parquet")
    assert out.height == 1
    assert out["pdf_url"][0] == "https://x/has.pdf"


def test_main_missing_source_returns_1(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "LEGISLATION_DIR", tmp_path)  # empty dir, no json
    monkeypatch.setattr(m, "SILVER_PARQUET_DIR", tmp_path)
    assert m.main() == 1


def test_main_no_amendment_lists_returns_0(tmp_path, monkeypatch):
    _write_source(tmp_path, [_bill("1", "2025", [])])
    monkeypatch.setattr(m, "LEGISLATION_DIR", tmp_path)
    monkeypatch.setattr(m, "SILVER_PARQUET_DIR", tmp_path)
    assert m.main() == 0
    # empty amendmentLists → nothing written.
    assert not (tmp_path / "bill_amendments.parquet").exists()
