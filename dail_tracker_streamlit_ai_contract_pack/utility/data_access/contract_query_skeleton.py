"""Contract-aware retrieval SQL skeleton for Dáil Tracker."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import duckdb
import pandas as pd
import streamlit as st


FORBIDDEN_SQL_TOKENS = (
    " join ",
    " group by ",
    " having ",
    " over (",
    " create ",
    " insert ",
    " update ",
    " delete ",
    " read_parquet",
    " parquet_scan",
)


@dataclass(frozen=True)
class RetrievalQuery:
    relation: str
    columns: tuple[str, ...]
    where_sql: str = ""
    order_by: str | None = None
    order_direction: str = "ASC"
    limit: int = 500
    params: tuple[Any, ...] = ()

    def to_sql(self) -> str:
        column_sql = ", ".join(quote_ident(c) for c in self.columns)
        sql = f"SELECT {column_sql} FROM {quote_ident(self.relation)}"
        if self.where_sql:
            sql += f" WHERE {self.where_sql}"
        if self.order_by:
            direction = "DESC" if self.order_direction.upper() == "DESC" else "ASC"
            sql += f" ORDER BY {quote_ident(self.order_by)} {direction}"
        sql += " LIMIT ?"
        return sql


def quote_ident(identifier: str) -> str:
    if not identifier.replace("_", "").isalnum():
        raise ValueError(f"Unsafe identifier: {identifier!r}")
    return f'"{identifier}"'


def assert_retrieval_only(sql: str) -> None:
    lowered = f" {sql.lower()} "
    bad = [token.strip() for token in FORBIDDEN_SQL_TOKENS if token in lowered]
    if bad:
        raise ValueError(f"Forbidden SQL token(s) in Streamlit retrieval query: {bad}")


@st.cache_data(show_spinner=False)
def run_retrieval_query(database_path: str, query: RetrievalQuery) -> pd.DataFrame:
    sql = query.to_sql()
    assert_retrieval_only(sql)
    params = tuple(query.params) + (query.limit,)
    with duckdb.connect(database_path, read_only=True) as con:
        return con.execute(sql, params).fetchdf()


def validate_required_columns(df: pd.DataFrame, required_columns: Iterable[str]) -> list[str]:
    return [column for column in required_columns if column not in df.columns]
