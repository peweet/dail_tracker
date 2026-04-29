import duckdb

from config import SILVER_PARQUET_DIR ,GOLD_PARQUET_DIR


"""
Utility function to create an in-memory DuckDB connection with gold Parquet files registered as views for querying.
"""
def get_gold_connection() -> duckdb.DuckDBPyConnection:
    parquet_dirs = [GOLD_PARQUET_DIR, SILVER_PARQUET_DIR]
    print("Creating in-memory DuckDB connection and registering gold Parquet files as views...")
    """Return an in-memory DuckDB connection with gold Parquet files registered as views."""
    con = duckdb.connect()
    for parquet_dir in parquet_dirs:
        for parquet_file in sorted(parquet_dir.glob("*.parquet")):
            view_name = parquet_file.stem
            con.execute(
                f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet('{parquet_file.as_posix()}')"
                )
            print(f"Registered view '{view_name}' for {parquet_file.name}")
    return con

if __name__ == "__main__":
    conn = get_gold_connection()
    print("DuckDB connection created with gold Parquet files registered as views.")