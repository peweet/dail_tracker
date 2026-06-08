-- v_member_interests_declarations + v_member_interests_member_year_summary
-- ───────────────────────────────────────────────────────────────────────────
-- Moves the per-TD interests business logic OUT of utility/ui/interests_panel.py
-- (the _real_descriptions() dedup/boilerplate filter, the set-difference
-- year-on-year diff, and the category/new/removed counts) into the pipeline
-- layer, so the panel becomes retrieval-only.
--
-- Source: v_member_interests_detail (member_interests_detail.sql — registers
-- first under the member_interests_* glob, before this member_zz_* file).
--
-- Parity contract with the old Python (must hold — see test/test_interests_diff_views.py):
--   * "real" declaration  = interest_text after TRIM is non-empty and not
--     one of {'no interests declared','nan'} (case-insensitive). De-duplicated.
--   * v_member_interests_declarations.change_status is PER-CATEGORY (matches the
--     render loop): 'new'  = this (category, text) not present the prior year,
--                   'unchanged' = present the prior year,
--                   'removed'   = present the prior year, absent this year
--                                 (emitted at the *later* year so the profile,
--                                  viewing year Y, sees what dropped since Y-1).
--   * The summary new_count / removed_count are GLOBAL (category-agnostic),
--     matching the editorial callout's set(current) − set(prior) maths.
--   * category_count is the RAW distinct-category count for the year (the panel
--     counted every category present, including boilerplate-only ones).
--   * has_prior_year is true when the member has ANY row in year-1 (the panel's
--     `has_prior = not prior_df.empty`, which includes boilerplate rows).

-- ── Deduplicated "real" declarations at (house, member, year, category) grain ──
CREATE OR REPLACE VIEW v_member_interests_declarations AS
WITH real AS (
    SELECT DISTINCT
        house,
        member_name,
        declaration_year,
        interest_category,
        TRIM(interest_text) AS interest_text
    FROM v_member_interests_detail
    WHERE member_name IS NOT NULL
      AND declaration_year IS NOT NULL
      AND interest_text IS NOT NULL
      AND TRIM(interest_text) <> ''
      AND LOWER(TRIM(interest_text)) NOT IN ('no interests declared', 'nan')
),
-- Every (member, year) that appears at all — including boilerplate-only rows —
-- so a "removed" item can be attributed to a year the member actually filed.
member_years AS (
    SELECT DISTINCT house, member_name, declaration_year
    FROM v_member_interests_detail
    WHERE member_name IS NOT NULL AND declaration_year IS NOT NULL
),
present AS (
    SELECT
        r.house,
        r.member_name,
        r.declaration_year,
        r.interest_category,
        r.interest_text,
        CASE WHEN prev.interest_text IS NOT NULL THEN 'unchanged' ELSE 'new' END AS change_status
    FROM real r
    LEFT JOIN real prev
      ON  prev.house            = r.house
      AND prev.member_name      = r.member_name
      AND prev.interest_category = r.interest_category
      AND prev.interest_text    = r.interest_text
      AND prev.declaration_year = r.declaration_year - 1
),
removed AS (
    SELECT
        prev.house,
        prev.member_name,
        prev.declaration_year + 1 AS declaration_year,  -- attribute to the viewing year
        prev.interest_category,
        prev.interest_text,
        'removed' AS change_status
    FROM real prev
    JOIN member_years y
      ON  y.house            = prev.house
      AND y.member_name      = prev.member_name
      AND y.declaration_year = prev.declaration_year + 1
    LEFT JOIN real cur
      ON  cur.house            = prev.house
      AND cur.member_name      = prev.member_name
      AND cur.interest_category = prev.interest_category
      AND cur.interest_text    = prev.interest_text
      AND cur.declaration_year = prev.declaration_year + 1
    WHERE cur.interest_text IS NULL
)
SELECT house, member_name, declaration_year, interest_category, interest_text, change_status FROM present
UNION ALL
SELECT house, member_name, declaration_year, interest_category, interest_text, change_status FROM removed;


-- ── Per (house, member, year) editorial summary ───────────────────────────────
CREATE OR REPLACE VIEW v_member_interests_member_year_summary AS
WITH real AS (
    SELECT DISTINCT
        house, member_name, declaration_year, interest_category,
        TRIM(interest_text) AS interest_text
    FROM v_member_interests_detail
    WHERE member_name IS NOT NULL
      AND declaration_year IS NOT NULL
      AND interest_text IS NOT NULL
      AND TRIM(interest_text) <> ''
      AND LOWER(TRIM(interest_text)) NOT IN ('no interests declared', 'nan')
),
real_global AS (  -- category-agnostic deduped texts (drives the global diff counts)
    SELECT DISTINCT house, member_name, declaration_year, interest_text FROM real
),
member_years AS (  -- raw grain incl. boilerplate; also carries the raw category count + flags
    SELECT
        house,
        member_name,
        declaration_year,
        MAX(party_name)                AS party_name,
        MAX(constituency)              AS constituency,
        COUNT(DISTINCT interest_category) AS category_count,
        BOOL_OR(landlord_flag)         AS is_landlord,
        BOOL_OR(property_flag)         AS is_property_owner
    FROM v_member_interests_detail
    WHERE member_name IS NOT NULL AND declaration_year IS NOT NULL
    GROUP BY house, member_name, declaration_year
),
totals AS (
    SELECT house, member_name, declaration_year, COUNT(*) AS total_declarations
    FROM real_global
    GROUP BY house, member_name, declaration_year
),
cat_counts AS (
    SELECT
        house, member_name, declaration_year,
        COUNT(*) FILTER (WHERE interest_category = 'Land (including property)') AS property_count,
        COUNT(*) FILTER (WHERE interest_category = 'Shares')                    AS share_count
    FROM real
    GROUP BY house, member_name, declaration_year
),
new_cte AS (
    SELECT
        cur.house, cur.member_name, cur.declaration_year,
        COUNT(*) FILTER (WHERE prev.interest_text IS NULL) AS new_count
    FROM real_global cur
    LEFT JOIN real_global prev
      ON  prev.house            = cur.house
      AND prev.member_name      = cur.member_name
      AND prev.interest_text    = cur.interest_text
      AND prev.declaration_year = cur.declaration_year - 1
    GROUP BY cur.house, cur.member_name, cur.declaration_year
),
removed_cte AS (
    SELECT
        prev.house, prev.member_name, prev.declaration_year + 1 AS declaration_year,
        COUNT(*) FILTER (WHERE cur.interest_text IS NULL) AS removed_count
    FROM real_global prev
    JOIN member_years y
      ON  y.house            = prev.house
      AND y.member_name      = prev.member_name
      AND y.declaration_year = prev.declaration_year + 1
    LEFT JOIN real_global cur
      ON  cur.house            = prev.house
      AND cur.member_name      = prev.member_name
      AND cur.interest_text    = prev.interest_text
      AND cur.declaration_year = prev.declaration_year + 1
    GROUP BY prev.house, prev.member_name, prev.declaration_year + 1
)
SELECT
    my.house,
    my.member_name,
    my.declaration_year,
    my.party_name,
    my.constituency,
    COALESCE(t.total_declarations, 0) AS total_declarations,
    my.category_count,
    -- new/removed are only meaningful (and only shown) when a prior year exists;
    -- the panel forces them to 0 for a member's first filing year.
    CASE WHEN py.declaration_year IS NOT NULL THEN COALESCE(n.new_count, 0) ELSE 0 END  AS new_count,
    CASE WHEN py.declaration_year IS NOT NULL THEN COALESCE(rm.removed_count, 0) ELSE 0 END AS removed_count,
    (py.declaration_year IS NOT NULL) AS has_prior_year,
    my.is_landlord,
    my.is_property_owner,
    COALESCE(cc.property_count, 0)    AS property_count,
    COALESCE(cc.share_count, 0)       AS share_count
FROM member_years my
LEFT JOIN totals     t  USING (house, member_name, declaration_year)
LEFT JOIN cat_counts cc USING (house, member_name, declaration_year)
LEFT JOIN new_cte    n  USING (house, member_name, declaration_year)
LEFT JOIN removed_cte rm USING (house, member_name, declaration_year)
LEFT JOIN member_years py
  ON  py.house            = my.house
  AND py.member_name      = my.member_name
  AND py.declaration_year = my.declaration_year - 1;
