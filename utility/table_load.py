import duckdb
import os

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