-- v_committee_meetings — committee meeting history (the timeline spine).
-- One row per (committee, meeting date), newest first, carrying:
--   topics (the session headings, e.g. "Appropriation Accounts 2024",
--   "Vote 45 - Further and Higher Education…"), the witness organisations and
--   named witness people who appeared, and the link to the official transcript.
--
-- Source: data/gold/parquet/committee_meetings.parquet (the spine) +
-- committee_witnesses.parquet (orgs) + committee_witness_persons.parquet (people),
-- produced by committee_witnesses_extract.py -> committee_evidence_promote_gold.py.
-- Committee identity is reconciled at extraction time (API committeeCode vs the
-- AKN FRBR path); unreconciled meetings are dropped upstream, never guessed.
--
-- CROSSWALK to the membership Committees page: the page selects a committee by
-- its human-readable name (e.g. "Committee of Public Accounts"); the API records
-- the same committee in a different case ("COMMITTEE OF PUBLIC ACCOUNTS"). We
-- expose `committee_key = lower(committee_name)` so the page can match its
-- selection case-insensitively (verified low-risk: names match after casefold).
--
-- Honest boundary (surfaced in the page, not here): the transcript gives what was
-- DISCUSSED, not formal committee outcomes/recommendations — those are a separate
-- Oireachtas source, not in transcripts.

CREATE OR REPLACE VIEW v_committee_meetings AS
WITH meetings AS (
    SELECT
        committee_code,
        committee_name,
        house_no,
        date,
        source_xml,
        topics,
        n_topics,
        n_orgs,
        n_persons
    FROM read_parquet('data/gold/parquet/committee_meetings.parquet')
),
orgs AS (
    SELECT
        committee_code,
        date,
        list(witness_org ORDER BY witness_org) AS witness_orgs
    FROM read_parquet('data/gold/parquet/committee_witnesses.parquet')
    GROUP BY committee_code, date
),
persons AS (
    SELECT
        committee_code,
        date,
        list(witness_person ORDER BY witness_person) AS witness_persons
    FROM read_parquet('data/gold/parquet/committee_witness_persons.parquet')
    GROUP BY committee_code, date
)
SELECT
    m.committee_code,
    m.committee_name,
    -- Crosswalk key to the membership register's committee name. Two glyph/formation
    -- gaps make a literal lower() match miss a committee's meetings:
    --   (1) the API's showAs renders apostrophes as the typographic ' (U+2019) while
    --       the register uses ASCII ' (U+0027) — e.g. "Committee on Members' Interests
    --       of Dáil Éireann";
    --   (2) the Oireachtas runs each committee in two FORMATIONS — the Joint committee
    --       (both houses, scrutiny) and the Select committee (one house, bill stages) —
    --       and records meetings under whichever sat. A citizen sees one committee, so
    --       both formations' meetings must surface on either page.
    -- The key therefore folds curly apostrophes to ASCII, collapses whitespace,
    -- lowercases, and strips the leading formation prefix down to the topic stem
    -- ("Joint/Select Committee on X" / "Comhchoiste/Roghchoiste X" -> "x"). Verified
    -- no two distinct committees collapse to the same stem (only Joint/Select twins do),
    -- and renamed previous-term committees keep distinct stems (they don't leak in).
    regexp_replace(
        lower(trim(regexp_replace(
            replace(replace(m.committee_name, chr(8217), ''''), chr(8216), ''''),
            '\s+', ' ', 'g'
        ))),
        '^(seanad |dail )?(joint |select )?(committee (on|of) |comhchoiste |roghchoiste )',
        ''
    )                                            AS committee_key,
    m.house_no,
    m.date,
    m.source_xml,
    -- Citizen-facing transcript page (the AKN XML is the machine source). The web
    -- debate URL is the same {committee-slug}/{date} the XML path carries, re-homed
    -- on oireachtas.ie/en/debates/debate/… (verified to resolve 200).
    regexp_replace(
        m.source_xml,
        '^https://data\.oireachtas\.ie/akn/ie/debateRecord/([^/]+)/(\d{4}-\d{2}-\d{2})/.*$',
        'https://www.oireachtas.ie/en/debates/debate/\1/\2/'
    )                                            AS transcript_url,
    m.topics,
    m.n_topics,
    m.n_orgs,
    m.n_persons,
    COALESCE(o.witness_orgs, []::VARCHAR[])       AS witness_orgs,
    COALESCE(p.witness_persons, []::VARCHAR[])    AS witness_persons
FROM meetings m
LEFT JOIN orgs o
       ON m.committee_code = o.committee_code AND m.date = o.date
LEFT JOIN persons p
       ON m.committee_code = p.committee_code AND m.date = p.date
ORDER BY m.committee_code, m.date DESC;
