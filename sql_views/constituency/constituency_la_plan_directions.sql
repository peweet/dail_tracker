-- v_la_plan_directions — every time the Planning Regulator (OPR) took a council's ADOPTED
-- development plan or Local Area Plan to the Minister, and what the Minister then did.
-- One row per (council, plan).
--
-- WHY THIS IS A COUNCILLOR FACT, NOT AN EXECUTIVE ONE. Making the development plan — and zoning
-- land in it — is a RESERVED function: one of the few decisions the ELECTED members actually
-- take. Where they adopt a plan inconsistent with the OPR's recommendations, the OPR must notify
-- the Minister (PDA 2000 s.31AM(8)), who may issue a Direction that is "deemed to be incorporated
-- into the plan", the members' provisions being "deemed not to be included" (s.31AN(11)). No
-- appeal; judicial review only.
--
-- ⚠️ NEVER MERGE WITH v_la_planning_overturn. That view measures An Coimisiún Pleanála overturning
-- the council's PLANNERS on appeal — the CHIEF EXECUTIVE's executive decisions, NOT councillors.
-- These are two structurally different override relationships and a combined "overruled" number
-- would be factually wrong. (Same discipline as the never-union money grains.)
--
-- ⚠️ NOT AN "OVERRIDES" COUNTER. plan_outcome carries the full taxonomy, because the register also
-- records the Minister DECLINING to follow the OPR (councillors upheld):
--     direction_issued   — the Minister overrode the members' plan
--     minister_declined  — the Minister refused to override them (Sligo CDP 2024-2030; Kilkenny Var 5)
--     in_progress        — the chain has not concluded
--     suspension_notice  — s.63(6) notice under the new Planning and Development Act 2024
-- Counting only the first value would misrepresent the process.
--
-- ⚠️ RESTRICTIVE ONLY: the Minister/OPR can strike a zoning; they cannot create one.
--
-- Source: the OPR's own published register (opr.ie) — the de-facto national record, since s.31
-- Directions are NOT centrally published on gov.ie. Built by extractors/opr_plan_directions_extract.py.
-- Research + statutory chain: doc/LOCAL_DEMOCRACY_OVERRIDE_RESEARCH.md
CREATE OR REPLACE VIEW v_la_plan_directions AS
SELECT
    local_authority,
    plan_name,
    plan_type,                              -- development_plan | local_area_plan | variation
    plan_outcome,
    MIN(doc_date)                    AS first_doc_date,   -- 'YYYY-MM' (the OPR's opening move)
    MAX(doc_date)                    AS last_doc_date,
    COUNT(*)                         AS n_documents,      -- the published paper trail
    -- the document that DECIDED it (Direction, decline, or suspension), for the source link
    MAX(CASE WHEN stage IN ('minister_final_direction', 'minister_declined', 'suspension_notice')
             THEN doc_url END)       AS outcome_doc_url
FROM read_csv('data/_meta/opr_plan_directions.csv', header = true, AUTO_DETECT = true)
GROUP BY local_authority, plan_name, plan_type, plan_outcome;
