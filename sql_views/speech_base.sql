-- v_speech_base — member-attributed floor contributions (both chambers).
--
-- Single source: the unified gold speeches_fact parquet already carries `house`
-- (Dáil/Seanad) per row, so — unlike vote_base — no two-file union is needed;
-- one placeholder {SPEECH_FACT_PARQUET_PATH} is substituted from config in
-- dail_tracker_core.connections. ALL other speech_*.sql views read FROM
-- v_speech_base so the parquet read lives in exactly one place.
--
-- Adds a reconstructed public debate_url (the website path uses the bare section
-- number, dbsect_ prefix stripped — mirrors debate_url_web in the listings).

CREATE OR REPLACE VIEW v_speech_base AS
SELECT
    *,
    'https://www.oireachtas.ie/en/debates/debate/'
        || chamber || '/' || CAST("date" AS VARCHAR) || '/'
        || replace(debate_section_id, 'dbsect_', '') || '/' AS debate_url
FROM read_parquet('{SPEECH_FACT_PARQUET_PATH}');
