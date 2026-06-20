-- v_ministerial_diary_engagements — one row per (engagement x matched organisation),
-- the DRILL-DOWN behind the org ranking: which minister met an org, when, and the
-- as-published subject. Also powers the per-minister rollup the page derives.
-- Sources (joined on entry_id):
--   data/gold/parquet/ministerial_diary_org_mentions.parquet  (the org match)
--   data/gold/parquet/ministerial_diary_engagements.parquet   (dept + class + minister_display)
--
-- Only ENGAGEMENT-bearing classes are exposed (the org name on a travel/media line is not a
-- meeting with that org) — matches the overlap builder's EXCLUDE_CLASSES. Subjects are the
-- minister's OWN published diary text (public record); the page frames them as co-occurrence
-- and links the source PDF. (Person-name review of free-text subjects = a pre-launch gate.)
CREATE OR REPLACE VIEW v_ministerial_diary_engagements AS
SELECT
    m.matched_org_name      AS organisation,
    m.gaz_origin,
    m.match_confidence,
    e.minister_display      AS minister,
    e.department,
    e.entry_class,
    m.entry_date,
    m.subject,
    e.source_pdf_url
FROM read_parquet('data/gold/parquet/ministerial_diary_org_mentions.parquet') AS m
JOIN read_parquet('data/gold/parquet/ministerial_diary_engagements.parquet') AS e
    USING (entry_id)
WHERE e.entry_class NOT IN ('travel', 'media')
ORDER BY m.entry_date DESC;
