-- v_la_councillor_votes — per-councillor NAMED roll-call votes, extracted from council
-- minutes. Only exists where a council records votes by name (2026-08-01: Carlow, Cork City,
-- Fingal, Galway City, Kilkenny, Laois — v_la_council_meeting_coverage.tier = 'roll_call' is
-- the live list; never hardcode it). Most councils decide by agreement (proposer/seconder)
-- and publish no named tally. Source: data/_meta/la_councillor_votes.csv.
-- vote ∈ {for, against, abstain, absent}.
--
-- Two provenance columns travel with every row and must reach the UI:
--   source_status ∈ {text, ocr_winocr} — how the minutes' text was obtained. 'ocr_winocr'
--     rows are OCR-derived (Extracted band: OCR can mis-read a name), and every Galway City
--     row is one. They are shown with a caveat, never as plain fact.
--   join_status ∈ {resolved, printed_form} — whether `member` matches a gold roster name.
--     'printed_form' rows keep the name exactly as the minutes printed it because the roster
--     could not resolve it: mostly earlier-term councillors (all Galway City divisions are
--     2018-2023, before the current council) and Cork City seats missing from the roster.
--     They are NOT errors and are NOT filtered here — the extractor's reconcile gate proved
--     each division's names count to its printed tally, so dropping rows would break that
--     arithmetic. A councillor card joins on member name and therefore shows resolved rows
--     only; the printed-form count belongs on the Trust rail (v_la_councillor_vote_provenance).
CREATE OR REPLACE VIEW v_la_councillor_votes AS
SELECT local_authority, member, meeting_date, motion, vote, source_status, join_status
FROM read_csv('data/_meta/la_councillor_votes.csv', header = true, AUTO_DETECT = true);

-- v_la_councillor_vote_provenance — per-council Trust-rail counts for the vote record, so the
-- page states what it is NOT showing ("214 of 508 Galway City votes name a councillor the
-- roster cannot resolve") instead of silently omitting those rows.
CREATE OR REPLACE VIEW v_la_councillor_vote_provenance AS
SELECT
    local_authority,
    count(*)                                                      AS vote_rows,
    count(*) FILTER (WHERE join_status = 'resolved')              AS resolved_rows,
    count(*) FILTER (WHERE join_status = 'printed_form')          AS printed_form_rows,
    count(*) FILTER (WHERE source_status = 'ocr_winocr')          AS ocr_rows,
    count(DISTINCT member) FILTER (WHERE join_status = 'resolved') AS resolved_members,
    -- meeting_date is DISPLAY text (dd/mm/yyyy, and 'April 2026' where only a month is
    -- knowable), so it is cast before any min/max — string ordering on dd/mm/yyyy would put
    -- 09/02/2026 before 22/05/2023. Month-only forms cast to NULL and drop out of the range,
    -- which understates the span rather than inventing a day.
    min(try_strptime(meeting_date, '%d/%m/%Y'))::DATE               AS first_meeting,
    max(try_strptime(meeting_date, '%d/%m/%Y'))::DATE               AS last_meeting
FROM v_la_councillor_votes
GROUP BY local_authority
ORDER BY local_authority;
