import duckdb, polars as pl

con = duckdb.connect()
con.execute("CREATE VIEW activities AS SELECT * FROM read_parquet('data/silver/lobbying/parquet/lobby_break_down_by_politician.parquet')")
con.execute("CREATE VIEW returns   AS SELECT * FROM read_parquet('data/silver/lobbying/parquet/returns.parquet')")
# for other tables:
con.execute("CREATE VIEW current_dail_vote_history AS SELECT * FROM read_csv_auto('data/gold/current_dail_vote_history.csv')")

# test your SQL
sql = open("sql_queries/debate_summary.sql").read()
print(con.execute(sql).pl())