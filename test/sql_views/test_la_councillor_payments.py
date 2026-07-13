"""Guards for the s.142 councillor-payments gold CSV + v_la_councillor_payments view.

The fact is scope-capped to the councils that publish the statutory register as open data
(South Dublin quarterly, Dublin City monthly) — the tests pin that cap, the keep-as-printed
name rule's side effects, and the view's aggregation contract. CSV is git-tracked, so these
run in CI.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
CSV_PATH = ROOT / "data" / "_meta" / "la_councillor_payments.csv"

OPEN_DATA_COUNCILS = {"South Dublin", "Dublin City"}
KNOWN_UNITS = {"EUR", "meetings"}


@pytest.fixture(scope="module")
def rows() -> list[dict]:
    assert CSV_PATH.exists(), "la_councillor_payments.csv missing"
    with open(CSV_PATH, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_scope_capped_to_open_data_councils(rows):
    assert {r["local_authority"] for r in rows} <= OPEN_DATA_COUNCILS


def test_units_and_values_parse(rows):
    assert {r["unit"] for r in rows} <= KNOWN_UNITS
    for r in rows:
        float(r["value"])  # every value numeric
        assert r["year"].isdigit() and 2020 <= int(r["year"]) <= 2035


def test_no_duplicate_keys(rows):
    keys = [(r["local_authority"], r["councillor"], r["period"], r["category"]) for r in rows]
    assert len(keys) == len(set(keys))


def test_categories_are_canonical_snake_case(rows):
    import re

    for r in rows:
        assert re.fullmatch(r"[a-z0-9_]+", r["category"]), r["category"]
    # the salary category must be present (SDCC publishes it) — the headline citizen fact
    assert any(r["category"] == "representational_payment" for r in rows)


def test_no_title_prefixes_in_names(rows):
    assert not [r for r in rows if r["councillor"].lower().startswith(("councillor ", "cllr"))]


def test_view_aggregates_and_matches_csv():
    duckdb = pytest.importorskip("duckdb")
    sql = (ROOT / "sql_views" / "constituency" / "constituency_la_councillor_payments.sql").read_text(
        encoding="utf-8"
    )
    conn = duckdb.connect()
    conn.execute(f"SET file_search_path='{ROOT.as_posix()}'")
    conn.execute(sql)
    n, ncouncils, ymin, ymax = conn.execute(
        "SELECT COUNT(*), COUNT(DISTINCT local_authority), MIN(year), MAX(year) FROM v_la_councillor_payments"
    ).fetchone()
    assert n > 500 and ncouncils >= 2 and ymin >= 2022 and ymax >= 2024
    # spot anchor: a full SDCC year lands near the ~€40k national baseline (salary+allowances)
    tot = conn.execute(
        "SELECT amount_eur FROM v_la_councillor_payments WHERE local_authority='South Dublin' "
        "AND councillor='Alan Edge' AND year=2024 AND category='total_payment'"
    ).fetchone()
    assert tot and 30_000 < tot[0] < 60_000
