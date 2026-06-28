-- v_minister_briefs — the incoming-minister BRIEF corpus: one row per department that publishes a
-- brief to its incoming minister (~10 of 18). The AGENDA layer that pairs with the diaries —
-- diary = what a minister DID (meetings); brief = what the department's stated GOALS / immediate
-- PRIORITIES / machinery-of-government changes ARE.
-- Source: data/gold/parquet/minister_briefs.parquet (built by extractors/ministerial_briefs_extract.py
--   — all curation lives THERE; this view is a thin read so the logic stays in the vetted pipeline).
--
-- READ HONESTLY (surfaced in the page provenance): these are the DEPARTMENT'S OWN words from the
-- published brief — display-only, never ranked or scored. born-digital fields are fitz-extracted;
-- scanned briefs (DECC/Education/Justice) were read from the rendered pages (no OCR). Empty list
-- columns = that field wasn't structured in this dept's brief (e.g. a goals-led vs priorities-led
-- brief), NOT an extraction failure.
CREATE OR REPLACE VIEW v_minister_briefs AS
SELECT
    department,
    slug,
    edition,
    source_type,
    source_url,
    vision_mission,
    strategic_goals,
    immediate_priorities,
    machinery_of_government,
    key_issue_areas,
    n_strategic_goals,
    n_priorities,
    n_mog_changes,
    extraction_method
FROM read_parquet('data/gold/parquet/minister_briefs.parquet')
ORDER BY (n_strategic_goals + n_priorities + n_mog_changes) DESC, department;
