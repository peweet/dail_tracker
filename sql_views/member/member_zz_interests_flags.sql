-- v_member_interests_flags — per-(member_id, declaration_year) interest FLAGS for
-- cross-register work (votes × Register of Members' Interests). THE single home of
-- the interest-classification predicates: landlord/property are pipeline-derived
-- booleans; director/shareholder are read off interest_category and MUST carry the
-- nil-text guard below (members file a return row for EVERY category, most of them
-- a nil "No interests declared" — a bare category-presence test over-counts wildly,
-- ≈half of Directorships rows are nil). Same guard v_member_interests_index uses.
-- Dedicated director/shareholder pipeline flags remain TODO_PIPELINE_VIEW_REQUIRED;
-- when they land, change ONLY this view.
-- zz_ prefix: must register AFTER member_interests_detail.sql (sorted-glob order).
-- Consumers: dail_tracker_core/queries/cross_ref.py (breakdown + voting_vs_interests).
CREATE OR REPLACE VIEW v_member_interests_flags AS
SELECT
    member_id,
    declaration_year,
    BOOL_OR(landlord_flag) AS is_landlord,
    BOOL_OR(property_flag) AS is_property_owner,
    BOOL_OR(
        interest_category = 'Directorships'
        AND interest_text IS NOT NULL AND TRIM(interest_text) <> ''
        AND LOWER(TRIM(interest_text)) <> 'no interests declared'
    ) AS is_director,
    BOOL_OR(
        interest_category = 'Shares'
        AND interest_text IS NOT NULL AND TRIM(interest_text) <> ''
        AND LOWER(TRIM(interest_text)) <> 'no interests declared'
    ) AS is_shareholder
FROM v_member_interests_detail
WHERE member_id IS NOT NULL
GROUP BY member_id, declaration_year;
