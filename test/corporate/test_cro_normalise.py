"""Unit tests for cro_normalise — the warning flags derived from status dates, and the snapshot date.

The flag tests read no bronze file. Each row carries the columns normalise() touches, as strings,
the way load_bronze() reads the CSV (try_parse_dates=False). The snapshot tests run main() on a
one-row bronze file in tmp_path.

The future status date is real: the 4 September 2026 snapshot holds a Liquidation row whose
company_status_date is 2122-08-18, and before this test the flag called that distress recent.
"""

import datetime as dt
import sys
from pathlib import Path

import polars as pl
import pytest

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


# ── snapshot date: the portal's date, carried from the bronze file name into silver ──────────
# Real case: the poller wrote companies_20261117.csv on 20 November 2026. The file's mtime said the
# 20th; the register's data is the portal's 17 November snapshot.
BRONZE_COLUMNS = [
    "company_num",
    "company_name",
    "company_status_code",
    "company_status",
    "company_type_code",
    "company_type",
    "company_reg_date",
    "last_ar_date",
    "company_address_1",
    "company_address_2",
    "company_address_3",
    "company_address_4",
    "comp_dissolved_date",
    "nard",
    "last_accounts_date",
    "company_status_date",
    "nace_v2_code",
    "eircode",
    "company_name_eff_date",
    "company_type_eff_date",
    "princ_object_code",
]


def test_silver_carries_the_snapshot_date_of_its_bronze_file(tmp_path, monkeypatch):
    bronze = tmp_path / "companies_20261117.csv"
    row = {c: "" for c in BRONZE_COLUMNS} | {"company_num": "102886", "company_status": "Normal"}
    pl.DataFrame([row]).write_csv(bronze)
    silver = tmp_path / "companies.parquet"
    monkeypatch.setattr(sys, "argv", ["cro_normalise", "--bronze", str(bronze), "--silver", str(silver)])
    assert cn.main() == 0
    assert pl.read_parquet(silver)["snapshot_date"].to_list() == [dt.date(2026, 11, 17)]


def test_a_bronze_file_with_no_date_in_its_name_is_refused(tmp_path, monkeypatch):
    bronze = tmp_path / "companies_nope.csv"
    pl.DataFrame([{c: "" for c in BRONZE_COLUMNS}]).write_csv(bronze)
    silver = tmp_path / "companies.parquet"
    monkeypatch.setattr(sys, "argv", ["cro_normalise", "--bronze", str(bronze), "--silver", str(silver)])
    with pytest.raises(SystemExit, match="snapshot date"):
        cn.main()
    assert not silver.exists()
