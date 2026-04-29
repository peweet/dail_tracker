from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

import pandas as pd


@dataclass(frozen=True)
class RetrievalQuery:
    view_name: str
    columns: Sequence[str]
    where_sql: str = ""
    order_by: str | None = None
    limit: int = 1000


def build_retrieval_sql(query: RetrievalQuery) -> str:
    """Build retrieval-only SQL against a registered analytical view.

    This deliberately does not support joins, groupby, CTEs, read_parquet, or view creation.
    """
    columns = ", ".join(query.columns)
    sql = f"SELECT {columns} FROM {query.view_name}"
    if query.where_sql:
        sql += f" WHERE {query.where_sql}"
    if query.order_by:
        sql += f" ORDER BY {query.order_by}"
    sql += f" LIMIT {int(query.limit)}"
    return sql


def run_retrieval_query(con: Any, query: RetrievalQuery, params: Sequence[Any] | None = None) -> pd.DataFrame:
    sql = build_retrieval_sql(query)
    return con.execute(sql, params or []).df()
