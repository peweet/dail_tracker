"""Data-integrity contract for v_supplier_entity_xref — the organisation-360 spine.

Runs in the @sql lane against committed gold (data/gold/parquet/supplier_entity_xref.parquet,
built by extractors/entity_xref_build.py). Pins the invariants the company dossier page and
the org-360 composition rely on: one row per supplier, the presence flags agree with their
counts, the cross-register tally is internally consistent, and counts never go negative.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

pytestmark = pytest.mark.sql

_PARQUET = Path("data/gold/parquet/supplier_entity_xref.parquet")


@pytest.fixture(scope="module")
def rel() -> str:
    if not _PARQUET.exists():
        pytest.skip("supplier_entity_xref gold not built (run extractors/entity_xref_build.py)")
    return f"read_parquet('{_PARQUET.as_posix()}')"


@pytest.fixture(scope="module")
def conn():
    c = duckdb.connect()
    yield c
    c.close()


def test_one_row_per_supplier_norm(conn, rel):
    total, distinct = conn.execute(f"SELECT count(*), count(DISTINCT supplier_norm) FROM {rel}").fetchone()
    assert total == distinct, "supplier_norm must be unique (the join key)"
    assert total >= 1000, "anchor is thousands of suppliers; a tiny frame means a broken input"


def test_presence_flags_match_counts(conn, rel):
    bad = conn.execute(
        f"SELECT count(*) FROM {rel} "
        f"WHERE has_corporate_notice != (corporate_notices > 0) "
        f"   OR on_lobbying_register != (lobby_returns > 0)"
    ).fetchone()[0]
    assert bad == 0, "a presence flag disagrees with its count"


def test_cross_register_count_consistent(conn, rel):
    bad = conn.execute(
        f"SELECT count(*) FROM {rel} WHERE cross_register_count != ("
        f"CAST(on_lobbying_register AS INT) + CAST(has_corporate_notice AS INT) "
        f"+ CAST(is_charity AS INT) + CAST(has_epa_licence AS INT))"
    ).fetchone()[0]
    assert bad == 0, "cross_register_count must equal the sum of the four extra-register flags"


def test_has_cro_matches_company_num(conn, rel):
    bad = conn.execute(f"SELECT count(*) FROM {rel} WHERE has_cro != (company_num IS NOT NULL)").fetchone()[0]
    assert bad == 0


def test_counts_nonneg(conn, rel):
    bad = conn.execute(
        f"SELECT count(*) FROM {rel} WHERE lobby_returns < 0 OR corporate_notices < 0 OR procurement_award_rows < 1"
    ).fetchone()[0]
    assert bad == 0
