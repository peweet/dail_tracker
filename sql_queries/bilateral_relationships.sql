-- Grain: one row per (lobbyist × politician × chamber) pair with more than one return
-- Signal: repeated targeting across multiple returns signals an ongoing relationship,
--         not a one-off contact.
-- Dedup on (primary_key, lobbyist_name, full_name) so activity-explosion does not
-- inflate return counts within a single return.
WITH deduped AS (
    SELECT DISTINCT
        primary_key,
        lobbyist_name,
        full_name,
        chamber,
        public_policy_area,
        lobbying_period_start_date
    FROM activities
)
SELECT
    lobbyist_name,
    full_name,
    chamber,
    COUNT(DISTINCT primary_key)                                              AS returns_in_relationship,
    COUNT(DISTINCT public_policy_area)                                       AS distinct_policy_areas,
    COUNT(DISTINCT
        YEAR(lobbying_period_start_date)::VARCHAR
        || '-Q' ||
        QUARTER(lobbying_period_start_date)::VARCHAR
    )                                                                        AS distinct_periods,
    MIN(lobbying_period_start_date)                                          AS relationship_start,
    MAX(lobbying_period_start_date)                                          AS relationship_last_seen
FROM   deduped
GROUP  BY lobbyist_name, full_name, chamber
HAVING COUNT(DISTINCT primary_key) > 1
ORDER  BY returns_in_relationship DESC, distinct_periods DESC
