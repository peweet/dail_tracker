-- v_la_meeting_agendas — what each council tabled for discussion (the AGENDA), per recent
-- meeting. Born-digital councils parsed with fitz; scanned councils (Galway/Wicklow) via
-- GPU OCR. `agenda` is a ` | `-delimited list of item titles (the page re-splits). Source:
-- data/_meta/la_meeting_agendas.csv. Louth absent (book-format scans don't parse).
CREATE OR REPLACE VIEW v_la_meeting_agendas AS
SELECT local_authority, meeting_date, agenda, source_url
FROM read_csv('data/_meta/la_meeting_agendas.csv', header = true, AUTO_DETECT = true);
