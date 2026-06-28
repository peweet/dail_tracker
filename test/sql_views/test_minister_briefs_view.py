"""Tripwire for v_minister_briefs (sql_views/diary/minister_briefs.sql).

Reads the gold parquet (data/gold/parquet/minister_briefs.parquet, built by
extractors/ministerial_briefs_extract.py). Gold may be absent on a fresh checkout/CI → SKIP.
Guards: the view registers, exposes the agenda columns, returns one row per department with
list columns intact and no empty rows — the contract the ministerial_diaries page relies on.
"""

from pathlib import Path

import duckdb
import pytest

PROJECT_ROOT = Path(__file__).parents[2]
BRIEFS = PROJECT_ROOT / "data" / "gold" / "parquet" / "minister_briefs.parquet"
SQL = PROJECT_ROOT / "sql_views" / "diary" / "minister_briefs.sql"

pytestmark = pytest.mark.skipif(not BRIEFS.exists(), reason=f"gold source absent (CI): {BRIEFS.name}")


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    sql = SQL.read_text(encoding="utf-8").replace(
        "data/gold/parquet/minister_briefs.parquet", str(BRIEFS).replace("\\", "/")
    )
    c.execute(sql)
    return c


def test_view_registers_and_has_rows(con):
    n = con.execute("SELECT count(*) FROM v_minister_briefs").fetchone()[0]
    assert n >= 10, f"expected >=10 department briefs, got {n}"


def test_expected_columns(con):
    cols = {d[0] for d in con.execute("SELECT * FROM v_minister_briefs LIMIT 0").description}
    for c in ["department", "edition", "source_type", "strategic_goals", "immediate_priorities",
              "machinery_of_government", "key_issue_areas", "source_url"]:
        assert c in cols, f"view missing {c}"


def test_list_columns_are_lists(con):
    row = con.execute(
        "SELECT strategic_goals, immediate_priorities, machinery_of_government, key_issue_areas "
        "FROM v_minister_briefs WHERE department LIKE 'Justice%'"
    ).fetchone()
    assert all(isinstance(v, list) for v in row), "list columns must round-trip as lists"


def test_no_empty_rows(con):
    """Every department row carries some agenda content (goals OR priorities OR key areas)."""
    empties = con.execute(
        "SELECT department FROM v_minister_briefs "
        "WHERE len(strategic_goals)=0 AND len(immediate_priorities)=0 AND len(key_issue_areas)=0"
    ).fetchall()
    assert not empties, f"empty brief rows: {empties}"


def test_source_type_vocab(con):
    bad = con.execute(
        "SELECT DISTINCT source_type FROM v_minister_briefs "
        "WHERE source_type NOT IN ('born-digital','scanned')"
    ).fetchall()
    assert not bad, f"unexpected source_type: {bad}"
