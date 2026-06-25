-- v_la_noac_indicators — the FULL NOAC 2024 indicator set per council (~125 published
-- series across 12 service families), as published raw values. Feeds the single "All NOAC
-- indicators" reference drill-down (one expander) on the council dossier — the primary view
-- stays the curated headline cards. local_authority is already mapped to the page key by
-- the extractor, so this is a thin read; values are the published strings (€, %, MM:SS, Yes/No)
-- with a numeric_value alongside where it parses.
--
-- Source: data/gold/parquet/noac_indicators_long.parquet (extractors/noac_indicators_long_extract.py).
CREATE OR REPLACE VIEW v_la_noac_indicators AS
SELECT
    local_authority,
    family,
    indicator_code,
    series_label,
    raw_value,
    numeric_value,
    source_page,
    deep_link,
    year
FROM read_parquet('data/gold/parquet/noac_indicators_long.parquet')
ORDER BY local_authority, family, indicator_code, series_label;
