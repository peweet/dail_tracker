-- v_news_mentions_recent — cross-member news feed (every member's recent coverage in one stream).
--
-- Powers the standalone "In the News" page. It is v_member_news_mentions (one row per
-- member × article, a NAME MATCH from a public news search) joined to v_member_registry_all
-- for the member's display name / party / constituency / current-vs-former flag, so the page
-- can render "headline → publisher" alongside "who was named → member profile" without the
-- Streamlit layer doing any join itself (logic firewall: views own joins).
--
-- A row is NOT an assertion the article is about the politician and is not an endorsement of
-- its content (same caveat as v_member_news_mentions). Most-recent first.
--
-- Depends on BOTH v_member_news_mentions (news-mentions phase) and v_member_registry_all
-- (registry phase). registry registers first; this file is appended to NEWS_MENTIONS_FILES
-- after member_news_mentions.sql, so both dependencies exist when it registers.

CREATE OR REPLACE VIEW v_news_mentions_recent AS
WITH member AS (
    -- One row per code (registry_all UNION-ALLs current + historic rosters, so a
    -- code can recur). Prefer the current-term row. Guarantees the join below can
    -- only return one member per article — no fan-out of the news rows.
    SELECT unique_member_code, member_name, party_name, constituency, house, is_current
    FROM v_member_registry_all
    WHERE member_name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY unique_member_code ORDER BY is_current DESC
    ) = 1
)
SELECT
    n.unique_member_code,
    r.member_name,
    r.party_name,
    r.constituency,
    r.house,
    r.is_current,
    n.outlet,
    n.outlet_tier,
    n.article_title,
    n.article_url,
    n.published_at,
    n.match_in_title
FROM v_member_news_mentions n
JOIN member r USING (unique_member_code)
ORDER BY n.published_at DESC NULLS LAST;
