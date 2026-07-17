-- v_circular_si_crosswalk — the RULE CHAIN: a government circular (the operational
-- instruction, signed by a civil servant) → the Statutory Instrument it implements
-- (the law itself, signed by the Minister and laid before the Oireachtas).
--
-- A circular is how a department tells public bodies to APPLY a rule; the SI is the
-- rule. Pairing them exposes the two-tier structure of Irish rule-making — and the
-- accountability asymmetry (the circular carries no ministerial signature). This view
-- lets you answer, for a live SI, "which circular operationalises it?" and vice versa.
--
-- LEFT source: data/_meta/gov_circular_si_crosswalk.csv — FACTUAL CITATIONS ONLY
-- (circular N's text references SI M), extracted from the gov.ie circulars corpus
-- (PSI Licence / CC-BY, attribution: Government of Ireland). NOT the circular text.
-- RIGHT source: v_statutory_instruments (legislation_si_index) — hence this file is
-- `zz_`, so the glob registers it AFTER the SI index it joins. Same pattern as
-- legislation_si_zz_classified.sql.
--
-- Grain: one row per (circular, cited SI) pair. si_resolved = the SI is present in our
-- holdings (SI data starts 2016; older citations resolve to si_resolved = FALSE and
-- carry NULL SI attributes — a real reference we simply don't hold, not an error).
CREATE OR REPLACE VIEW v_circular_si_crosswalk AS
WITH xw AS (
    SELECT
        circular_no,
        circular_title,
        department,
        rule_type,
        TRY_CAST(published_on AS VARCHAR)   AS circular_published_on,
        CAST(si_year AS INTEGER)            AS si_year,
        CAST(si_number AS INTEGER)          AS si_number,
        si_id,
        circular_source_url,
        circular_pdf_url
    FROM read_csv('data/_meta/gov_circular_si_crosswalk.csv', header = true, AUTO_DETECT = true)
)
SELECT
    xw.circular_no,
    xw.circular_title,
    xw.department,
    xw.rule_type,
    xw.circular_published_on,
    xw.si_year,
    xw.si_number,
    xw.si_id,
    (si.si_year IS NOT NULL)                AS si_resolved,
    si.si_title,
    si.si_operation,
    si.si_minister_name,
    si.si_parent_legislation,
    si.this_si_eli_url,
    xw.circular_source_url,
    xw.circular_pdf_url
FROM xw
LEFT JOIN v_statutory_instruments si
       ON si.si_year = xw.si_year AND si.si_number = xw.si_number
ORDER BY xw.si_year DESC, xw.si_number DESC;
