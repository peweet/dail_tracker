-- v_public_appointments — every public-appointment notice as a first-class row.
-- Source: data/gold/parquet/public_appointments.parquet (produced by
-- pipeline_sandbox/public_appointments_enrichment.py — curated Irish→English
-- template mapping over the Iris Oifigiuil `public_appointment` notices,
-- 2016+). Personal insolvency is excluded at the silver layer (privacy rule);
-- military commissions, election results, court-sittings and other
-- mis-bucketed notices are excluded by the enrichment.
--
-- Grain: one row per appointment notice. Page does its own filtering/facets
-- in pandas off this single frame (display_only, logic-firewall-safe).
CREATE OR REPLACE VIEW v_public_appointments AS
SELECT
    notice_ref,
    issue_date,
    appointing_authority,    -- President | Government | Minister | Unknown
    appointment_type,        -- state_board | special_adviser | judicial
    body,
    appointee,
    appointee_count,
    role,
    portfolio,               -- minister / department for special advisers
    english_summary,
    lang,                    -- Irish | English
    title,
    iris_source_pdf
FROM read_parquet('data/gold/parquet/public_appointments.parquet')
ORDER BY issue_date DESC NULLS LAST;
