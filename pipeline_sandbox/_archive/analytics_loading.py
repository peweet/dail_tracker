import duckdb

from config import GOLD_PARQUET_DIR, SILVER_PARQUET_DIR


def get_warehouse_connection() -> duckdb.DuckDBPyConnection:
    """Return an in-memory DuckDB connection with gold and silver Parquet files registered as views.

    Each parquet file in GOLD_PARQUET_DIR and SILVER_PARQUET_DIR is exposed as
    a view named after the file stem (e.g. payments_fact.parquet -> payments_fact).
    Gold is registered first; if a silver file shares a stem with a gold file,
    DuckDB will raise on the duplicate CREATE VIEW.
    """
    parquet_dirs = [GOLD_PARQUET_DIR, SILVER_PARQUET_DIR]
    print("Creating in-memory DuckDB connection and registering warehouse Parquet files as views...")
    con = duckdb.connect()
    for parquet_dir in parquet_dirs:
        for parquet_file in sorted(parquet_dir.glob("*.parquet")):
            view_name = parquet_file.stem
            con.execute(f"CREATE VIEW {view_name} AS SELECT * FROM read_parquet('{parquet_file.as_posix()}')")
            print(f"Registered view '{view_name}' for {parquet_file.name}")
    return con


if __name__ == "__main__":
    conn = get_warehouse_connection()
    print("DuckDB connection created with warehouse Parquet files registered as views.")
