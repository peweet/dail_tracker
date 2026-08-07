"""Contract tests for privacy-safe council-minutes participation signals."""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

ROOT = Path(__file__).resolve().parents[2]


def _fixture(tmp_path: Path) -> Path:
    path = tmp_path / "council_minutes_corpus.parquet"
    pl.DataFrame(
        {
            "document_id": ["doc-1"],
            "entity_type": ["local_authority"],
            "council": ["Carlow"],
            "meeting": ["Minutes April 2026"],
            "meeting_date": ["2026-04-01"],
            "doc_type": ["plenary_minutes"],
            "meeting_scope": ["plenary"],
            "source_status": ["text"],
            "source_url": ["https://example.ie/minutes"],
            "chunk": [0],
            "participant_categories": [["residents_association"]],
            "issue_themes": [["traffic_access"]],
            "planning_references": [["2460125"]],
            "board_references": [["ABP-319198"]],
            "collective_organisation_names": [["Ballyboggan Residents Association"]],
            "body": ["A residents association discussed traffic."],
        }
    ).write_parquet(path)
    return path


def _load_view(relative_path: str, parquet: Path, con: duckdb.DuckDBPyConnection) -> None:
    sql = (ROOT / relative_path).read_text(encoding="utf-8")
    sql = sql.replace(
        "data/gold/parquet/council_minutes_corpus.parquet",
        parquet.as_posix(),
    )
    con.execute(sql)


def test_search_and_serving_views_preserve_signal_lists(tmp_path: Path) -> None:
    parquet = _fixture(tmp_path)
    con = duckdb.connect()
    try:
        _load_view("sql_views/council_minutes/council_minutes_docs.sql", parquet, con)
        _load_view(
            "sql_views/constituency/constituency_la_council_minutes.sql",
            parquet,
            con,
        )
        search_row = con.execute(
            "SELECT participant_categories, issue_themes, planning_references, "
            "board_references, collective_organisation_names FROM v_council_minutes_docs"
        ).fetchone()
        serving_row = con.execute(
            "SELECT participant_categories, issue_themes, planning_references, "
            "board_references, collective_organisation_names FROM v_la_council_minutes_docs"
        ).fetchone()
    finally:
        con.close()

    expected = (
        ["residents_association"],
        ["traffic_access"],
        ["2460125"],
        ["ABP-319198"],
        ["Ballyboggan Residents Association"],
    )
    assert search_row == expected
    assert serving_row == expected
