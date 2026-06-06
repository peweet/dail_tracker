-- v_si_current_state — the legal-state of each Statutory Instrument as recorded
-- by the eISB Legislation Directory chronological tables (whether it has been
-- revoked / partially revoked / amended, and by which SI). One row per SI.
-- Source: data/gold/parquet/si_current_state.parquet
-- (extractors/si_legislation_directory_extract.py).
--
-- DISCOVERY / INDEXING ONLY. This surfaces the *negative* states eISB explicitly
-- records (amended / revoked); it never positively asserts an SI is "in force".
-- A missing row therefore means "not checked", NOT "in force" — see the LEFT
-- JOIN in legislation_si_index.sql, which leaves current_state NULL for SIs the
-- directory crawl did not cover. The page renders NULL as "status not checked".
--
-- The file name starts with 'legislation_' so legislation_data.py's
-- get_legislation_conn() glob registers it. It sorts before
-- 'legislation_si_index.sql', so this view exists before v_statutory_instruments
-- LEFT-JOINs it (register_views loads matching files alphabetically — the
-- implicit dependency ordering the views rely on).

CREATE OR REPLACE VIEW v_si_current_state AS
SELECT
    si_id,
    si_year,
    si_number,
    current_state,
    affecting_sis,
    affecting_si_urls,
    this_si_eli_url,
    how_affected_raw,
    state_source,
    state_source_url,
    directory_updated_to,
    confidence
FROM read_parquet('data/gold/parquet/si_current_state.parquet');
