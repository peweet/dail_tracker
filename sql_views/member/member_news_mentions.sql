-- v_member_news_mentions — recent news mentions per Oireachtas member.
-- Source: data/silver/parquet/news_mentions.parquet
--         (produced by extractors/news_mentions_extract.py — one Google-News RSS search
--          per member, keyed on unique_member_code; accumulates across runs).
--
-- One row per (member × article). A row is a NAME MATCH from a public news search, NOT an
-- assertion that the article is about this politician and not an endorsement of its content.
-- `match_in_title` is TRUE when the member's full name appears in the headline (high confidence);
-- ~83% of rows are body mentions (match_in_title = FALSE) the old headline scan could not see.
-- The UI renders headline + outlet + date + link only (no snippet), most-recent first, and shows
-- an empty state for members with no recent coverage.

CREATE OR REPLACE VIEW v_member_news_mentions AS
SELECT
    unique_member_code,
    matched_name,
    outlet,
    outlet_tier,
    article_title,
    article_url,
    published_at,
    match_in_title,
    is_current
FROM read_parquet('{NEWS_MENTIONS_PARQUET_PATH}')
WHERE unique_member_code IS NOT NULL
  AND article_title IS NOT NULL;
