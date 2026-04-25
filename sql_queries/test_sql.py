import duckdb, polars as pl

con = duckdb.connect()
con.execute("CREATE VIEW activities AS SELECT * FROM read_parquet('data/silver/lobbying/parquet/lobby_break_down_by_politician.parquet')")
con.execute("CREATE VIEW returns   AS SELECT * FROM read_parquet('data/silver/lobbying/parquet/returns.parquet')")
# for other tables:
con.execute("CREATE VIEW current_dail_vote_history AS SELECT * FROM read_csv_auto('data/gold/current_dail_vote_history.csv')")

# test your SQL
sql = open("sql_queries/debate_summary.sql").read()
print(con.execute(sql).pl())






#### Interview-style queries you can practise on your own data

# **1. RANK / DENSE_RANK — Top N per group**

# "Find the top 3 TDs by sitting days within each party."

# ```sql
# SELECT party, first_name, last_name, sitting_days_count,
#        DENSE_RANK() OVER (PARTITION BY party ORDER BY sitting_days_count DESC) AS rank_in_party
# FROM td
# QUALIFY rank_in_party <= 3
# ORDER BY party, rank_in_party;
# ```

# `QUALIFY` is DuckDB/BigQuery syntax — filters on window function results without a subquery. Worth knowing for interviews.

# **2. CTE + conditional aggregation — party summary report**

# "For each party, show total TDs, average attendance, number of ministers, and number of TDs on 3+ committees."

# ```sql
# WITH td_stats AS (
#     SELECT *,
#            (CASE WHEN committee_1_name_english IS NOT NULL THEN 1 ELSE 0 END
#           + CASE WHEN committee_2_name_english IS NOT NULL THEN 1 ELSE 0 END
#           + CASE WHEN committee_3_name_english IS NOT NULL THEN 1 ELSE 0 END
#           + CASE WHEN committee_4_name_english IS NOT NULL THEN 1 ELSE 0 END
#           + CASE WHEN committee_5_name_english IS NOT NULL THEN 1 ELSE 0 END)
#            AS committee_count
#     FROM td
# )
# SELECT party,
#        COUNT(*) AS td_count,
#        ROUND(AVG(sitting_days_count), 1) AS avg_sitting_days,
#        SUM(CASE WHEN ministerial_office_filled = 'true' THEN 1 ELSE 0 END) AS ministers,
#        SUM(CASE WHEN committee_count >= 3 THEN 1 ELSE 0 END) AS heavy_committee_load
# FROM td_stats
# GROUP BY party
# ORDER BY avg_sitting_days DESC;
# ```

# **3. Self-join — TD pairs on the same committee**

# "Find all pairs of TDs who sit on the same committee (co-membership network)."

# ```sql
# WITH committees AS (
#     SELECT unique_member_code, first_name, last_name, party,
#            UNNEST([committee_1_name_english, committee_2_name_english,
#                    committee_3_name_english, committee_4_name_english,
#                    committee_5_name_english]) AS committee_name
#     FROM td
#     WHERE committee_name IS NOT NULL
# )
# SELECT a.first_name || ' ' || a.last_name AS td_1,
#        b.first_name || ' ' || b.last_name AS td_2,
#        a.committee_name,
#        a.party AS party_1,
#        b.party AS party_2
# FROM committees a
# JOIN committees b
#   ON a.committee_name = b.committee_name
#  AND a.unique_member_code < b.unique_member_code  -- avoid duplicates and self-joins
# ORDER BY a.committee_name;
# ```

# `UNNEST` with a literal array is a DuckDB feature — pivots wide columns into rows without a manual UNION ALL. Great for denormalized data like yours.

# **4. Window function — running total + percentage**

# "Show each TD's cumulative share of their party's total sitting days."

# ```sql
# SELECT party, first_name, last_name, sitting_days_count,
#        SUM(sitting_days_count) OVER (PARTITION BY party ORDER BY sitting_days_count DESC) 
#            AS running_total,
#        ROUND(100.0 * sitting_days_count / SUM(sitting_days_count) OVER (PARTITION BY party), 1)
#            AS pct_of_party_total
# FROM td
# WHERE sitting_days_count IS NOT NULL
# ORDER BY party, sitting_days_count DESC;
# ```

# **5. PIVOT — attendance by party and Dáil term (when you add historical data)**

# ```sql
# PIVOT (
#     SELECT party, dail_term, sitting_days_count FROM td
# )
# ON dail_term
# USING AVG(sitting_days_count)
# ORDER BY party;
# ```

# DuckDB's native `PIVOT` is cleaner than CASE WHEN pivots. This becomes powerful once you load 33rd + 34th Dáil data side by side.

# **6. Correlated subquery — TDs who sponsor bills but have below-average attendance**

# ```sql
# SELECT t.first_name, t.last_name, t.party, t.sitting_days_count,
#        b.bill_count
# FROM td t
# JOIN (
#     SELECT unique_member_code_raw, COUNT(*) AS bill_count
#     FROM bills
#     GROUP BY unique_member_code_raw
# ) b ON t.unique_member_code = b.unique_member_code_raw
# WHERE t.sitting_days_count < (SELECT AVG(sitting_days_count) FROM td WHERE sitting_days_count IS NOT NULL)
#   AND b.bill_count > 5
# ORDER BY b.bill_count DESC;
# ```

# This is a classic interview pattern: find entities that meet condition A in one table AND condition B in another.

# **7. LAG/LEAD — bill progression timeline (per bill)**

# Once you have bill stage dates:

# ```sql
# -- How many days between each bill stage?
# WITH stages AS (
#     SELECT bill_short_title_en,
#            bill_stages_0_event_showAs AS stage_name,
#            CAST(bill_stages_0_event_dates_0_date AS DATE) AS stage_date
#     FROM bills
#     WHERE bill_stages_0_event_dates_0_date IS NOT NULL
# )
# SELECT *,
#        LAG(stage_date) OVER (PARTITION BY bill_short_title_en ORDER BY stage_date) AS prev_stage_date,
#        stage_date - LAG(stage_date) OVER (PARTITION BY bill_short_title_en ORDER BY stage_date) AS days_between_stages
# FROM stages;
# ```

# **8. GROUPING SETS — multi-level aggregation in one pass**

# ```sql
# SELECT GROUPING_ID(party, member_constituency) AS grp,
#        COALESCE(party, '** ALL PARTIES **') AS party,
#        COALESCE(member_constituency, '** ALL CONSTITUENCIES **') AS constituency,
#        COUNT(*) AS td_count,
#        AVG(sitting_days_count) AS avg_attendance
# FROM td
# GROUP BY GROUPING SETS (
#     (party, member_constituency),
#     (party),
#     ()
# )
# ORDER BY grp, party, constituency;
# ```

# `GROUPING SETS` produces subtotals and grand totals in one query — the SQL equivalent of a pivot table. Interviewers love this because most candidates only know `GROUP BY`.