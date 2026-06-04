-- v_si_lrc_enrichment — Law Reform Commission subject classification for each
-- Statutory Instrument. One row per SI. Source:
-- data/gold/parquet/si_lrc_enrichment_summary.parquet
-- (pipeline_sandbox/si_lrc_enrichment_build.py, from the LRC Classified List of
-- In-Force Legislation: https://revisedacts.lawreform.ie/classlist/intro).
--
-- DISCOVERY / CLASSIFICATION ONLY — NOT a legal-status engine. A match means the
-- LRC lists this SI under the given subject in its Classified List, subject to
-- LRC accuracy warnings and an exact number/year match. lrc_enrichment_status is
-- matched_classified_list | not_matched — NEVER "in force". "not_matched" does
-- NOT mean the SI is not in force (the list omits spent/revoked SIs by design;
-- ~89% of unmatched SIs are recorded revoked in v_si_current_state). Legal state
-- lives in v_si_current_state; this view never asserts it.
--
-- The file name starts with 'legislation_' so get_legislation_conn()'s glob
-- registers it. Reads parquet directly, so load order is irrelevant.

CREATE OR REPLACE VIEW v_si_lrc_enrichment AS
SELECT
    si_year,
    si_number,
    si_number_year,
    has_lrc_classified_list_match,
    lrc_primary_subject,
    lrc_primary_leaf,
    lrc_subjects,
    lrc_n_subjects,
    lrc_enrichment_status,
    match_method,
    match_confidence,
    lrc_fills_empty_domain,
    lrc_caveat,
    lrc_list_updated_to,
    lrc_eisb_url
FROM read_parquet('data/gold/parquet/si_lrc_enrichment_summary.parquet');
