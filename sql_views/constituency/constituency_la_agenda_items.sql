-- v_la_agenda_items — each agenda ITEM, split out of v_la_meeting_agendas' ` | ` list and
-- classified so pages can lead with what is actually at stake instead of the procedural
-- boilerplate that opens every agenda. Categories are OUR display taxonomy (regex over the
-- council's own wording; first match wins); `item` stays verbatim as printed. is_highlight
-- marks the citizen-interesting classes: named motions, s.183/s.211 land disposals,
-- reserved planning decisions, money decisions. meeting_ts normalises the mixed date
-- formats ('09/02/2026', '2026-06-12', '12 January 2026', 'September 2025') for ordering;
-- unparseable dates sort last, they are never dropped. Source: data/_meta/la_meeting_agendas.csv
-- (read directly, same as the sibling view — no view-dependency ordering to maintain).
CREATE OR REPLACE VIEW v_la_agenda_items AS
SELECT *, category IN ('motion', 'disposal', 'planning', 'money') AS is_highlight
FROM (
    SELECT
        local_authority,
        meeting_date,
        COALESCE(
            try_strptime(trim(meeting_date), '%d/%m/%Y'),
            try_strptime(trim(meeting_date), '%Y-%m-%d'),
            try_strptime(trim(meeting_date), '%d %B %Y'),
            try_strptime('1 ' || trim(meeting_date), '%d %B %Y')
        ) AS meeting_ts,
        trim(u.item) AS item,
        source_url,
        CASE
            WHEN regexp_matches(u.item, '(?i)notices? of motion|\bc(?:oun)?cll?rs?\.?\s')
                THEN 'motion'
            WHEN regexp_matches(u.item, '(?i)section\s*183|s\.?\s*183\b|\bdisposal of|\bdispose of|\blease of')
                THEN 'disposal'
            WHEN regexp_matches(u.item, '(?i)material contravention|development plan|part\s*(viii|8)\b|local area plan|proposed variation')
                THEN 'planning'
            WHEN regexp_matches(u.item, '(?i)\bbudget\b|\bloans?\b|borrow|overdraft|casual trading|bye-?laws?\b|\brates\b|\bcharges\b')
                THEN 'money'
            WHEN regexp_matches(u.item, '(?i)management report|chief executive')
                THEN 'ce_report'
            WHEN regexp_matches(u.item, '(?i)^(to\s+)?(confirm|confirmation)|correspondence|votes? of (sympathy|congrat)|sympathy|conference|training|attendance at|schedule of dates|dates? (for|of) .*meeting|election of')
                THEN 'procedural'
            ELSE 'other'
        END AS category
    FROM read_csv('data/_meta/la_meeting_agendas.csv', header = true, AUTO_DETECT = true),
        UNNEST(string_split(agenda, ' | ')) AS u(item)
    WHERE length(trim(u.item)) > 0
);
