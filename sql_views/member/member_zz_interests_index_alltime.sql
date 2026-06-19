-- v_member_interests_index_alltime — per-(house, member) ALL-TIME rollup ranked
-- for the /interests leaderboard "historic" toggle. Pools every declaration
-- year a member has on record into one lifetime total and ranks within each
-- house. With the historic backfill (2011–2019 Dáil registers) this surfaces
-- former TDs and senators alongside sitting ones.
--
-- One row per (house, member_name). Rank partitions by house only (the year is
-- collapsed), so it is a chamber-scoped all-time leaderboard — the page keeps
-- the Dáil/Seanad selector and hides the year pills when the toggle is on.
--
-- Mirrors the column contract of v_member_interests_index (member_zz_interests_index.sql)
-- exactly, so the page renders the same card with no shape change. directorship_count
-- stays 0 for the same reason (no directorship_flag in silver yet).
--
-- Sources: v_member_interests_detail (member_interests_detail.sql — alphabetically
-- earlier, so it registers first under the data-access glob).
--
-- party_name / constituency come from the member's MOST RECENT declaration year
-- (arg_max on declaration_year), not an arbitrary MAX, so the card shows their
-- latest affiliation rather than whichever sorts highest.

CREATE OR REPLACE VIEW v_member_interests_index_alltime AS
WITH agg AS (
    SELECT
        house,
        member_name,
        arg_max(party_name, declaration_year)   AS party_name,
        arg_max(constituency, declaration_year) AS constituency,
        COUNT(*)                                 AS total_declarations,
        0                                        AS directorship_count,
        COUNT(DISTINCT CASE
                WHEN interest_category = 'Land (including property)'
                 AND interest_text IS NOT NULL
                 AND TRIM(interest_text) <> ''
                 AND LOWER(TRIM(interest_text)) <> 'no interests declared'
                THEN interest_text END)          AS property_count,
        COUNT(DISTINCT CASE
                WHEN interest_category = 'Shares'
                 AND interest_text IS NOT NULL
                 AND TRIM(interest_text) <> ''
                 AND LOWER(TRIM(interest_text)) <> 'no interests declared'
                THEN interest_text END)          AS share_count,
        BOOL_OR(landlord_flag)                   AS is_landlord,
        BOOL_OR(property_flag)                   AS is_property_owner
    FROM v_member_interests_detail
    WHERE member_name IS NOT NULL
    GROUP BY house, member_name
)
SELECT
    ROW_NUMBER() OVER (
        PARTITION BY house
        ORDER BY total_declarations DESC, member_name
    )                                   AS rank,
    house,
    member_name,
    party_name,
    constituency,
    total_declarations,
    directorship_count,
    property_count,
    share_count,
    is_landlord,
    is_property_owner
FROM agg;
