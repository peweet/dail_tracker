-- v_la_lgas_audit — the Local Government Audit Service's statutory audit report on each
-- council's AFS, one row per (council, year), since 2012. VERBATIM material only: the
-- auditor's opinion paragraph and the report's own section headings; presence flags are
-- anchored to literal headings ("Emphasis of Matter", "Chief Executive's Response").
-- NO derived score or good/bad classification exists anywhere in this lane — the reader
-- sees what the auditor wrote (no-inference rule).
--
-- Source: gov.ie DHLGH audit-report publications (assets.gov.ie PDFs), harvested by
-- extractors/lgas_audit_reports_extract.py (discovery via gov.ie sitemaps — the collection
-- pages render client-side). Pairs 1:1 with la_afs_divisions on (council, year): the AFS
-- carries the numbers, this carries the auditor's words about them.
CREATE OR REPLACE VIEW v_la_lgas_audit AS
SELECT
    council            AS local_authority,
    year,
    audit_opinion_text,
    has_emphasis_of_matter,
    has_ce_response,
    section_headings,
    pages,
    report_page_url
FROM read_parquet('data/silver/parquet/la_lgas_audit_reports.parquet');
