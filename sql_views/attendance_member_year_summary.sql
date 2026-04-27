-- TODO: Switch read_csv_auto to parquet once available
CREATE OR REPLACE VIEW v_attendance_member_year_summary AS
WITH att AS (
    SELECT * FROM read_csv_auto('data/silver/aggregated_td_tables.csv')
),
mem AS (
    SELECT first_name, last_name, constituency_name, party
    FROM read_csv_auto('data/silver/flattened_members.csv')
),
joined AS (
    SELECT
        a.*, m.party, m.constituency_name
    FROM att a
    LEFT JOIN mem m
      ON a.first_name = m.first_name AND a.last_name = m.last_name
)
SELECT
    CONCAT(first_name, ' ', last_name)               AS member_name,
    identifier                                      AS member_id,
    year,
    MAX(sitting_days_count)                         AS attended_count,
    COALESCE(party, '')                             AS party_name,
    COALESCE(constituency_name, '')                 AS constituency
FROM joined
GROUP BY member_name, member_id, year, party_name, constituency;