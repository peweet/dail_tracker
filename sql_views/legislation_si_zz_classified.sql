-- v_statutory_instruments_classified — v_statutory_instruments enriched with the
-- LRC subject classification (v_si_lrc_enrichment), for the SI page's subject
-- chip and topic-browse facet. One row per SI.
--
-- Kept SEPARATE from v_statutory_instruments (rather than joining LRC into the
-- core view) so the core SI surface and its tests/fixtures stay untouched, and
-- so the page can fall back to the unclassified view if the LRC gold table is
-- ever absent. The LEFT JOIN is one-row-per-SI on both sides (the summary is
-- unique per si_year/si_number), so it cannot inflate the row count.
--
-- 'zz_' prefix forces this to register AFTER both v_statutory_instruments
-- (legislation_si_index.sql) and v_si_lrc_enrichment
-- (legislation_si_lrc_enrichment.sql), which it reads.
--
-- DISCOVERY / CLASSIFICATION ONLY — see legislation_si_lrc_enrichment.sql. The
-- lrc_* columns never assert legal status; current_state (from the core view)
-- remains the sole legal-state signal.

CREATE OR REPLACE VIEW v_statutory_instruments_classified AS
SELECT
    si.*,
    lrc.has_lrc_classified_list_match,
    lrc.lrc_primary_subject,
    lrc.lrc_primary_leaf,
    lrc.lrc_n_subjects,
    lrc.lrc_enrichment_status,
    lrc.lrc_fills_empty_domain,
    lrc.lrc_caveat,
    lrc.lrc_list_updated_to
FROM v_statutory_instruments si
LEFT JOIN v_si_lrc_enrichment lrc
       ON lrc.si_year = si.si_year
      AND lrc.si_number = si.si_number;
