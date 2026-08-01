-- v_la_council_power_events — where a council's ELECTED MEMBERS decide, versus where they
-- only noted a decision the Chief Executive had already taken. Source:
-- data/_meta/la_council_power_events.csv (1,066 rows / 25 councils, 2026-08-01).
--
-- This is the reserved/executive split that Irish local government turns on and that no other
-- view in the app carries. `power_type` is a mapping of the extractor's own class vocabulary,
-- not a fresh judgement: reserved_* and requisition_* (a s.140 requisition IS a members'
-- power — Local Government Act 2001 s.140) are 'reserved'; exec_* is 'executive'; anything
-- else stays '' and is reported as unclassified rather than guessed into a bucket.
--
-- BAND: Extracted, at DOCUMENT grain — one row is "this document contains N hits of this
-- power class", not "this decision happened". A citation reaches the document, never the
-- line, so these rows support COUNTS and never a quotation.
CREATE OR REPLACE VIEW v_la_council_power_events AS
SELECT
    local_authority,
    meeting,
    coalesce(doc_type, '')      AS doc_type,
    power_class,
    coalesce(power_type, '')    AS power_type,
    try_cast(n_hits AS BIGINT)  AS n_hits,
    coalesce(source_status, '') AS source_status
FROM read_csv('data/_meta/la_council_power_events.csv', header = true, AUTO_DETECT = true);

-- v_la_council_power_summary — per-council counts for the "what your councillors control"
-- section. Documents, not hits, is the honest denominator for the headline split: one
-- document mentioning s.183 eight times is one occasion, not eight.
CREATE OR REPLACE VIEW v_la_council_power_summary AS
SELECT
    local_authority,
    count(*)                                                AS class_rows,
    count(DISTINCT meeting)                                 AS documents,
    count(*) FILTER (WHERE power_type = 'reserved')         AS reserved_rows,
    count(*) FILTER (WHERE power_type = 'executive')        AS executive_rows,
    count(*) FILTER (WHERE power_type = '')                 AS unclassified_rows,
    count(DISTINCT power_class)                             AS distinct_classes
FROM v_la_council_power_events
GROUP BY local_authority
ORDER BY reserved_rows DESC;
