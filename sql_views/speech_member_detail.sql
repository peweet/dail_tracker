-- v_member_speeches — per-contribution feed for the member-overview Debates
-- section. Projection over v_speech_base (member identity + language flag are
-- already on the gold row); the UI's retrieval query filters by
-- unique_member_code + the optional year / type / business / is_irish / text
-- filters. One row per floor contribution (speech / question / answer).

CREATE OR REPLACE VIEW v_member_speeches AS
SELECT
    unique_member_code,
    member_name,
    party,
    house,
    chamber,
    CAST("date" AS VARCHAR) AS speech_date,
    year,
    debate_section_id,
    section_heading,
    business,
    contribution_type,
    contribution_order,
    speaker_raw,
    recorded_time,
    speech_text,
    word_count,
    is_irish,
    irish_score,
    debate_url
FROM v_speech_base
WHERE unique_member_code IS NOT NULL;
