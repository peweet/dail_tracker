-- v_member_interests_index — per-(house, year, member) rollup ranked for the
-- leaderboard on /interests. Replaces the in-page GROUP BY + ROW_NUMBER OVER
-- pattern (formerly _fetch_member_index_fallback in interests.py).
--
-- One row per (house, declaration_year, member_name). Rank is partitioned
-- so each year × house leaderboard is self-contained.
--
-- Sources: v_member_interests_detail (defined in
-- member_interests_detail.sql — alphabetically earlier so it registers first).
--
-- TODO_PIPELINE_VIEW_REQUIRED: directorship_count — currently 0 because the
-- silver dataset has no directorship_flag yet; replace when the pipeline
-- exposes it.

CREATE OR REPLACE VIEW v_member_interests_index AS
WITH agg AS (
    SELECT
        house,
        declaration_year,
        member_name,
        MAX(member_id)                  AS member_id,
        MAX(party_name)                 AS party_name,
        MAX(constituency)               AS constituency,
        -- A "declaration" is an actual declared interest, NOT a 'No interests
        -- declared' placeholder. The register restates every category each year,
        -- so ~71% of rows are Nil boilerplate; counting them padded each member's
        -- tally (and the leaderboard rank) by their blank categories. Exclude Nil
        -- (matches the property/share filters below + the summary view's `real`).
        COUNT(*) FILTER (
            WHERE interest_text IS NOT NULL
              AND TRIM(interest_text) <> ''
              AND LOWER(TRIM(interest_text)) NOT IN ('no interests declared', 'nan')
        )                               AS total_declarations,
        0                               AS directorship_count,
        COUNT(DISTINCT CASE
                WHEN interest_category = 'Land (including property)'
                 AND interest_text IS NOT NULL
                 AND TRIM(interest_text) <> ''
                 AND LOWER(TRIM(interest_text)) <> 'no interests declared'
                THEN interest_text END) AS property_count,
        COUNT(DISTINCT CASE
                WHEN interest_category = 'Shares'
                 AND interest_text IS NOT NULL
                 AND TRIM(interest_text) <> ''
                 AND LOWER(TRIM(interest_text)) <> 'no interests declared'
                THEN interest_text END) AS share_count,
        BOOL_OR(landlord_flag)          AS is_landlord,
        BOOL_OR(property_flag)          AS is_property_owner
    FROM v_member_interests_detail
    WHERE member_name IS NOT NULL
    GROUP BY house, declaration_year, member_name
)
SELECT
    ROW_NUMBER() OVER (
        PARTITION BY house, declaration_year
        ORDER BY total_declarations DESC, member_name
    )                                   AS rank,
    house,
    declaration_year,
    member_name,
    member_id,
    party_name,
    constituency,
    total_declarations,
    directorship_count,
    property_count,
    share_count,
    is_landlord,
    is_property_owner
FROM agg;
