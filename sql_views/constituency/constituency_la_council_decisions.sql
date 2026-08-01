-- v_la_council_decisions — motion events (who proposed, who seconded, what was resolved)
-- parsed from council minute prose. Source: data/_meta/la_council_decisions.csv.
--
-- BAND: Extracted — regex-anchored parses of minute text, NOT a published decisions register.
-- The three things a caller must know before quoting a row (measured 2026-08-01, 6,497 rows /
-- 23 councils):
--   • `outcome` is EMPTY on 5,874 rows (90%). The minutes name the proposer and seconder far
--     more often than they record a resolution word. Empty means "the minutes do not record
--     it" — never "no decision was taken".
--   • Only 5 rows carry a tally. The named-vote record is v_la_councillor_votes; this view
--     does not substitute for it. `rollcall` marks the 292 rows whose text mentions one.
--   • `meeting_date` is present on 3,104 rows and is often month-grained ('2026 May'),
--     because that is the grain the source document name carries. It is display text, not a
--     DATE — cast before ordering.
-- source_status ∈ {text, ocr_winocr, html} carries how the minutes' text was obtained;
-- ocr_winocr rows inherit OCR risk and must not render as plain fact.
-- read_csv reads an empty CSV field as NULL, so the 5,874 rows with no recorded outcome would
-- arrive as NULL and a page filtering `outcome = ''` would match none of them. The text columns
-- are coalesced to '' so the documented contract — empty means "the minutes do not record it" —
-- is the one the view actually serves.
CREATE OR REPLACE VIEW v_la_council_decisions AS
SELECT
    local_authority,
    coalesce(meeting_date, '')   AS meeting_date,
    coalesce(item_context, '')   AS item_context,
    coalesce(motion_snippet, '') AS motion_snippet,
    coalesce(proposer, '')       AS proposer,
    coalesce(seconder, '')       AS seconder,
    coalesce(outcome, '')        AS outcome,
    -- ' | '-joined topic labels, from the same 6,435 events (motion_topics.jsonl describes
    -- THESE rows, not a separate dataset). Empty on ~77% of rows: the classifier matched no
    -- topic, which is not a statement that the motion was about nothing.
    coalesce(topics, '')         AS topics,
    -- Cast explicitly: only 5 rows carry a tally, so AUTO_DETECT types whichever of these
    -- columns happens to be entirely empty as VARCHAR and its siblings as BIGINT — the schema
    -- would then change shape with the data. try_cast keeps all three BIGINT permanently.
    try_cast(tally_for AS BIGINT)     AS tally_for,
    try_cast(tally_against AS BIGINT) AS tally_against,
    try_cast(tally_abstain AS BIGINT) AS tally_abstain,
    rollcall,
    coalesce(source_url, '')    AS source_url,
    coalesce(source_status, '') AS source_status
FROM read_csv('data/_meta/la_council_decisions.csv', header = true, AUTO_DETECT = true);

-- v_la_council_decision_coverage — per-council counts that let a page state what this data
-- can and cannot answer, instead of showing a motion count that reads like a complete register.
CREATE OR REPLACE VIEW v_la_council_decision_coverage AS
SELECT
    local_authority,
    count(*)                                              AS decision_rows,
    count(*) FILTER (WHERE outcome <> '')                 AS with_outcome,
    count(*) FILTER (WHERE rollcall)                      AS rollcall_mentioned,
    count(*) FILTER (WHERE meeting_date <> '')            AS dated_rows,
    count(*) FILTER (WHERE source_url <> '')              AS with_source_url,
    count(*) FILTER (WHERE source_status = 'ocr_winocr')  AS ocr_rows
FROM v_la_council_decisions
GROUP BY local_authority
ORDER BY decision_rows DESC;
