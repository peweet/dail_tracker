-- v_ipas_centre_compliance — HIQA's per-centre, per-standard compliance judgments, with
-- each standard resolved to its human-readable statement.
--
-- Source: HIQA's 101 INDIVIDUAL IPAS centre inspection reports (2024-01 -> 2026-03), which
-- HIQA's own summary report does not reproduce. 2,668 judgments; every one extracted by two
-- independent paths (the report's Appendix-1 table and its inline narrative) which agree on
-- 99.5%. The National Standards lookup joins 100% of them, so "Standard 4.3" reads as its
-- binding statement rather than a code.
--
-- judgment_conflict = TRUE marks the 13 standards where HIQA CONTRADICTS ITSELF (its appendix
-- disagrees with its own narrative). Both readings are preserved; neither is silently picked.
--
-- ⚠️ Standards never judged in any report are ABSENT here. Absence of a judgment is NOT
-- compliance — do not render a missing standard as a pass.
--
-- GRAIN: one row per centre x inspection x standard. value_safe_to_sum=FALSE.
-- Provider names inherit the accommodation-providers public_display gate at the page layer.
CREATE OR REPLACE VIEW v_ipas_centre_compliance AS
SELECT
    c.centre_name,
    c.county,
    c.provider_name_canonical            AS operator,
    c.inspection_date,
    c.standard_ref,
    s.theme_no,
    s.theme_name,
    COALESCE(s.standard_text, c.standard_title) AS standard_statement,
    c.judgment,
    c.judgment_conflict,
    c.risk_rating,
    c.source_url,
    c.value_safe_to_sum
FROM read_parquet('data/gold/parquet/ipas_centre_compliance.parquet') c
LEFT JOIN read_parquet('data/gold/parquet/ipas_national_standards.parquet') s
       ON s.standard_ref = c.standard_ref;
