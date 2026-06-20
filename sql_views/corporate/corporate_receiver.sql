-- Receiver-appointer + operator-firm panels for the Corporate page.
-- Source: data/gold/parquet/corporate_receiver_appointers.parquet
--         data/gold/parquet/corporate_receiver_firms.parquet
--         data/gold/parquet/corporate_notices_enriched.parquet
--   all produced by extractors/corporate_receiver_enrich.py (run after the iris
--   chain builds corporate_notices gold). These panels are INDEPENDENT of the
--   page filters — they describe the full corpus — so the ranking/classification
--   is precomputed here rather than recomputed in pandas on every page load.
--   Graduated out of utility/pages_code/corporate.py per the logic-firewall audit
--   (2026-06-20).

-- 1. Receiver-appointers — funds/banks calling in loans, ranked by receivership
--    notices that name them. dominant_fund_type/type_bucket are precomputed.
CREATE OR REPLACE VIEW v_corporate_receiver_appointers AS
SELECT parent, n_notices, dominant_fund_type, type_bucket
FROM read_parquet('data/gold/parquet/corporate_receiver_appointers.parquet')
ORDER BY n_notices DESC, parent;

-- 2. Type-mix headline — share of parent mentions by bucket (vulture / bank /
--    servicer / state / other). Weighted by mention count, matching the page's
--    original pdf["bucket"].value_counts().
CREATE OR REPLACE VIEW v_corporate_receiver_bucket_mix AS
SELECT type_bucket, SUM(n_notices) AS n
FROM read_parquet('data/gold/parquet/corporate_receiver_appointers.parquet')
GROUP BY type_bucket
ORDER BY n DESC;

-- 3. Operator firms — professional firms named AS receiver, by notice presence.
CREATE OR REPLACE VIEW v_corporate_receiver_firms AS
SELECT firm, n_notices, is_big6
FROM read_parquet('data/gold/parquet/corporate_receiver_firms.parquet')
ORDER BY n_notices DESC, firm;

-- 3b. Receivership-notices-by-year — the featured-panel sparkline series.
--     year is precomputed in the enrichment with the SAME pandas parse the page
--     used, so the counts are identical.
CREATE OR REPLACE VIEW v_corporate_receiver_year_counts AS
SELECT CAST(year AS INTEGER) AS year, COUNT(*) AS n
FROM read_parquet('data/gold/parquet/corporate_notices_enriched.parquet')
WHERE is_receivership AND year IS NOT NULL
GROUP BY year
ORDER BY year;

-- 4. Headline scalars for the featured panel + operator strip. Scalar aggregates
--    only (COUNT FILTER) — presentation stats, not a business rollup.
CREATE OR REPLACE VIEW v_corporate_receiver_summary AS
SELECT
    COUNT(*) FILTER (WHERE is_receivership)                            AS n_recv,
    COUNT(*) FILTER (WHERE is_receivership AND is_spv)                 AS n_spv,
    COUNT(*) FILTER (WHERE is_receivership AND has_parent_mention)     AS n_tagged,
    COUNT(*) FILTER (WHERE is_receivership AND has_receiver_firm)      AS n_any_tagged
FROM read_parquet('data/gold/parquet/corporate_notices_enriched.parquet');
