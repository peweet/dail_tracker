-- v_ministerial_diary_meetings — the BROAD landscape: one row per external meeting,
-- WITHOUT requiring an org match. This is the deliberate counterweight to the org-overlap
-- view: the overlap only surfaces the ~23% of meetings whose counterparty matched the
-- gazetteer (lobbying register + state bodies + curated), so on its own it under-shows the
-- full diary. This view exposes every `external_meeting` engagement so the page can present
-- per-minister / per-department totals over ALL meetings, not just the matched subset.
-- Source: data/gold/parquet/ministerial_diary_engagements.parquet
--
-- One row per ENTRY (not per entry x org) — it does NOT join the mentions table, so there is
-- no org-match explosion and counts are honest. The org overlay (who lobbied/met) stays in
-- v_ministerial_diary_org_overlap; this is the denominator.
CREATE OR REPLACE VIEW v_ministerial_diary_meetings AS
SELECT
    minister_display AS minister,
    department,
    entry_date,
    subject,
    source_pdf_url
FROM read_parquet('data/gold/parquet/ministerial_diary_engagements.parquet')
WHERE entry_class = 'external_meeting'
ORDER BY entry_date DESC;
