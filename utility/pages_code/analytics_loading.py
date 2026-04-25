import duckdb

from config import GOLD_PARQUET_DIR


def get_gold_connection() -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB connection with gold Parquet files registered as views."""
    con = duckdb.connect()
    for parquet_file in sorted(GOLD_PARQUET_DIR.glob("*.parquet")):
        view_name = parquet_file.stem
        con.execute(
            f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet('{parquet_file.as_posix()}')"
        )
    return con
