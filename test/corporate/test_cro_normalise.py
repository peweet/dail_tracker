"""Unit tests for cro_normalise.normalise — the warning flags derived from status dates.

No bronze file is read. Each row carries the columns normalise() touches, as strings, the way
load_bronze() reads the CSV (try_parse_dates=False).

The future status date is real: the 4 September 2026 snapshot holds a Liquidation row whose
company_status_date is 2122-08-18, and before this test the flag called that distress recent.
"""

import datetime as dt
import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import corporate.cro_normalise as cn  # noqa: E402

TEXT_COLS = ["company_name", "company_type", "nace_v2_code", "eircode", *cn.ADDRESS_COLS, *cn.DATE_COLS]


def _rows(*rows: dict) -> pl.DataFrame:
    filled = [{"company_num": str(i + 1), **{c: None for c in TEXT_COLS}, **row} for i, row in enumerate(rows)]
    return pl.DataFrame(filled, schema={c: pl.Utf8 for c in ["company_num", "company_status", *TEXT_COLS]})


def _flags(*rows: dict) -> list[bool]:
    out, _ = cn.normalise(_rows(*rows))
    return out.sort("company_num")["recent_distress_flag"].to_list()


def _days_ago(n: int) -> str:
    return (dt.date.today() - dt.timedelta(days=n)).isoformat()


def test_recent_distress_flags_a_liquidation_inside_twelve_months():
    assert _flags({"company_status": "Liquidation", "company_status_date": _days_ago(30)}) == [True]


def test_recent_distress_ignores_old_distress_and_live_companies():
    assert _flags(
        {"company_status": "Liquidation", "company_status_date": _days_ago(400)},
        {"company_status": "Normal", "company_status_date": _days_ago(30)},
    ) == [False, False]


def test_recent_distress_does_not_count_a_status_date_in_the_future():
    assert _flags({"company_status": "Liquidation", "company_status_date": "2122-08-18"}) == [False]
