-- Grain: one row per TD (latest declared year)
-- Source: dail_member_interests_combined (silver Parquet — auto-registered)
-- Used by: interests.py landing page leaderboard and notable TD quick-select
--
-- Key concepts:
--   MAX / BOOL_OR aggregation     → https://duckdb.org/docs/sql/aggregates#general-aggregate-functions
--   FILTER clause on aggregates   → https://duckdb.org/docs/sql/aggregates#filtering
--   QUALIFY / window for latest   → https://duckdb.org/docs/sql/window_functions
--
-- Run this to see available columns:
--   SELECT * FROM dail_member_interests_combined LIMIT 1
--
-- Expected columns (verify): full_name, year_declared, interest_count,
--                             is_landlord, is_property_owner, party

SELECT
    full_name,
    -- TODO: which column holds the party? verify name
    MAX(???)                                                                          AS party,
    -- TODO: max interest count across all declared years
    MAX(???)                                                                          AS max_interest_count,
    -- TODO: how many distinct years has this TD filed a declaration?
    COUNT(DISTINCT ???)                                                               AS years_declared,
    -- TODO: latest year on record for this TD
    MAX(???)                                                                          AS latest_year,
    -- TODO: is this TD a landlord in any year? (boolean flag)
    -- Hint: MAX(CASE WHEN is_landlord THEN 1 ELSE 0 END) = 1  OR  BOOL_OR(is_landlord)
    ???                                                                               AS ever_landlord,
    -- TODO: same for property owner flag
    ???                                                                               AS ever_property_owner
FROM   dail_member_interests_combined
GROUP  BY full_name
ORDER  BY max_interest_count DESC
