-- v_member_interests_index_alltime — per-(house, member) "latest snapshot" rollup
-- ranked for the What They Own leaderboard. With the historic backfill (2011–
-- 2019 Dáil registers) this surfaces former TDs and senators alongside sitting
-- ones, each shown at their MOST RECENT declaration year on file.
--
-- IMPORTANT — does NOT sum across years. The Register of Members' Interests is a
-- full annual restatement (each year is a complete snapshot, not a delta), so
-- COUNT(*) pooled over every year multiplied a member's figure by the number of
-- years they appear (e.g. Seán Haughey 2017–2023 summed to 248 declarations vs a
-- true latest-year count of 52). We therefore restrict to each member's latest
-- declaration year and count THAT year only — the fixed last-declared figure.
--
-- One row per (house, member_name). Rank partitions by house only (the year is
-- collapsed to each member's latest), so it is a chamber-scoped leaderboard.
--
-- Adds member_id (canonical unique_member_code, arg_max'd to the latest year) so
-- the page can link a card straight to the member's profile WITHOUT a lossy
-- name→code re-lookup — this is what makes former members (e.g. Haughey, absent
-- from the current-roster v_member_registry) clickable. declaration_year is the
-- latest year the figures are drawn from. directorship_count stays 0 (no
-- directorship_flag in silver yet).
--
-- Sources: v_member_interests_detail (member_interests_detail.sql — alphabetically
-- earlier, so it registers first under the data-access glob).

CREATE OR REPLACE VIEW v_member_interests_index_alltime AS
WITH latest AS (
    -- each member's most recent declaration year — the snapshot we report
    SELECT house, member_name, MAX(declaration_year) AS latest_year
    FROM v_member_interests_detail
    WHERE member_name IS NOT NULL
    GROUP BY house, member_name
),
agg AS (
    SELECT
        d.house,
        d.member_name,
        arg_max(d.member_id, d.declaration_year)    AS member_id,
        arg_max(d.party_name, d.declaration_year)   AS party_name,
        arg_max(d.constituency, d.declaration_year) AS constituency,
        MAX(d.declaration_year)                      AS declaration_year,
        -- Count only actual declared interests, not 'No interests declared' Nil
        -- placeholders (~71% of rows). See member_zz_interests_index.sql for why.
        COUNT(*) FILTER (
            WHERE d.interest_text IS NOT NULL
              AND TRIM(d.interest_text) <> ''
              AND LOWER(TRIM(d.interest_text)) NOT IN ('no interests declared', 'nan')
        )                                            AS total_declarations,
        0                                            AS directorship_count,
        COUNT(DISTINCT CASE
                WHEN d.interest_category = 'Land (including property)'
                 AND d.interest_text IS NOT NULL
                 AND TRIM(d.interest_text) <> ''
                 AND LOWER(TRIM(d.interest_text)) <> 'no interests declared'
                THEN d.interest_text END)            AS property_count,
        COUNT(DISTINCT CASE
                WHEN d.interest_category = 'Shares'
                 AND d.interest_text IS NOT NULL
                 AND TRIM(d.interest_text) <> ''
                 AND LOWER(TRIM(d.interest_text)) <> 'no interests declared'
                THEN d.interest_text END)            AS share_count,
        BOOL_OR(d.landlord_flag)                     AS is_landlord,
        BOOL_OR(d.property_flag)                      AS is_property_owner
    FROM v_member_interests_detail d
    JOIN latest l
      ON d.house = l.house
     AND d.member_name = l.member_name
     AND d.declaration_year = l.latest_year
    WHERE d.member_name IS NOT NULL
    GROUP BY d.house, d.member_name
)
SELECT
    ROW_NUMBER() OVER (
        PARTITION BY house
        ORDER BY total_declarations DESC, member_name
    )                                   AS rank,
    house,
    member_name,
    member_id,
    party_name,
    constituency,
    declaration_year,
    total_declarations,
    directorship_count,
    property_count,
    share_count,
    is_landlord,
    is_property_owner
FROM agg;
