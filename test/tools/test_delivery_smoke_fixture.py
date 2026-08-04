"""The wheel smoke fixture is tiny, external, and sufficient for readiness."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from tools.build_delivery_smoke_fixture import build_fixture


def test_build_fixture_writes_two_empty_schema_correct_facts(tmp_path: Path) -> None:
    outputs = build_fixture(tmp_path.resolve())

    assert all(path.is_file() for path in outputs)
    connection = duckdb.connect()
    try:
        relation = connection.read_parquet([str(path) for path in outputs], union_by_name=True)
        assert relation.count("*").fetchone() == (0,)
        assert {"member_name", "date_paid", "amount", "house"} <= set(relation.columns)
    finally:
        connection.close()


def test_build_fixture_rejects_a_cwd_relative_data_root() -> None:
    with pytest.raises(ValueError, match="must be absolute"):
        build_fixture(Path("data"))
