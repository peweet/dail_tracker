import os

import duckdb

# Simple DuckDB script to load the cleaned lobbyist CSV into a DuckDB database, and to run some simple queries to check that the data has been loaded correctly. This is a key step in the pipeline, as it allows us to efficiently query and analyze the lobbyist data using SQL, and to join it with other tables (e.g. attendance, payments, etc.) in the future as we build out the enriched dataset for analysis. By loading the data into DuckDB at this stage, we can also take advantage of DuckDB's performance optimizations for analytical queries, which will be important as we start to work with larger datasets and more complex queries in later stages of the pipeline.
# start of the gold layer!
path = "C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\lobbyist\\output\\lobby_break_down_by_politician.csv"
gold_dir = "C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\data\\gold"

os.makedirs(gold_dir, exist_ok=True)

con = duckdb.connect(os.path.join(gold_dir, "lobbyist.duckdb"))

# Check if CSV path actually exists first
print("CSV exists:", os.path.exists(path))

# Create table
con.execute(f"""
    CREATE TABLE IF NOT EXISTS lobbyist AS 
    SELECT * FROM read_csv_auto('{path}')
""")

# See what tables actually got created
print("Tables in DB:", con.execute("SHOW TABLES").fetchdf())

# Now query
print(con.execute("SELECT * FROM lobbyist LIMIT 10").fetchdf())
print(con.execute("DESCRIBE lobbyist").fetchdf())

con.close()
