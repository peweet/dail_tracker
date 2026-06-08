-- v_member_external_links — Wikidata-sourced socials + Wikipedia URL per TD.
-- Source: data/silver/parquet/member_external_links.parquet
--         (produced by wikidata_socials_etl.py — Wikidata SPARQL P4690 join)
--
-- The UI reads this view directly off `unique_member_code`. Every column
-- except the join key is nullable — sparse coverage is expected (Wikidata
-- has Wikipedia for ~95% of sitting TDs, Twitter for ~56%, Bluesky for ~2%).
-- The hero block renders chips only for non-null URLs.

CREATE OR REPLACE VIEW v_member_external_links AS
SELECT
    unique_member_code,
    wikidata_qid,
    wikipedia_url,
    twitter_handle,
    twitter_url,
    bluesky_handle,
    bluesky_url,
    facebook_id,
    facebook_url,
    instagram_handle,
    instagram_url,
    website_url
FROM read_parquet('{EXTERNAL_LINKS_PARQUET_PATH}')
WHERE unique_member_code IS NOT NULL;
