-- v_lobbying_contact_detail_with_dpo — every contact-detail row with a
-- pre-joined dpo_individuals string column. Replaces the in-page
-- dict-join used to build CSV exports in lobbying_2.py (and the
-- dpo_count loop above the totals strip).
--
-- The join is one-to-one on return_id; multiple DPOs per return are
-- string-aggregated into a "; "-separated list (matching the format
-- lobbying_2.py used in csv_export["dpo_individuals"]).
--
-- File name prefix 'lobbying_zz_' so it loads after the two upstream
-- views (lobbying_contact_detail.sql, lobbying_dpo_returns.sql).

CREATE OR REPLACE VIEW v_lobbying_contact_detail_with_dpo AS
WITH dpo_by_return AS (
    SELECT
        return_id,
        string_agg(individual_name, '; ' ORDER BY individual_name) AS dpo_individuals,
        COUNT(*) AS dpo_count
    FROM v_lobbying_dpo_returns
    WHERE individual_name IS NOT NULL
    GROUP BY return_id
)
SELECT
    c.return_id,
    c.member_name,
    c.unique_member_code,
    c.chamber,
    c.position,
    c.lobbyist_name,
    c.public_policy_area,
    c.period_start_date,
    c.source_url,
    COALESCE(d.dpo_individuals, '') AS dpo_individuals,
    COALESCE(d.dpo_count, 0)        AS dpo_count
FROM v_lobbying_contact_detail c
LEFT JOIN dpo_by_return d ON c.return_id = d.return_id;
