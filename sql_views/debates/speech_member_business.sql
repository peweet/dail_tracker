-- v_member_speech_business — per-member business groupings (the outermost debate
-- heading: "Commencement Matters", "Order of Business", a Bill's stages, …) with
-- contribution counts. Feeds the Debates section's topic selectbox / chips.
-- Pipeline-owned GROUP BY; UI does retrieval-only SELECT … WHERE.

-- Grouped by (member, business) only — house collapsed via any_value — so the
-- UI's retrieval query is a plain SELECT … WHERE (no re-aggregation, firewall-safe).
CREATE OR REPLACE VIEW v_member_speech_business AS
SELECT
    unique_member_code,
    business,
    any_value(house)             AS house,
    COUNT(*)                     AS contribution_count,
    MAX(CAST("date" AS VARCHAR)) AS last_date
FROM v_speech_base
WHERE unique_member_code IS NOT NULL
  AND business IS NOT NULL
  AND business <> ''
GROUP BY unique_member_code, business;
