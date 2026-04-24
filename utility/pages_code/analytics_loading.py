import duckdb

conn = duckdb.connect("data/gold/lobbyist.duckdb")

# Register silver parquets as views — one per file
agg_td = conn.execute("""

    CREATE OR REPLACE VIEW silver_attendance AS

    SELECT * FROM read_parquet('data/silver/parquet/aggregated_td_tables.parquet')
    LIMIT 100

""")
#C:\Users\pglyn\PycharmProjects\dail_extractor\data\silver\parquet\flattened_members.parquet
agg_members = conn.execute("""
    SELECT * FROM read_parquet('data/silver/parquet/flattened_members.parquet')
    LIMIT 100
""")

print(agg_members.fetchone())
# In Streamlit: query returns a pandas DataFrame directly

# df = conn.execute("SELECT * FROM fact_attendance WHERE year = 2024").df()

