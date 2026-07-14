-- v_ipas_entitlements — what an international-protection applicant is ENTITLED TO in law,
-- beside what the auditor and the inspector actually FOUND.
--
-- This is the human end of a EUR 1.07bn number: the spend views show what the State PAYS;
-- this shows what a person is OWED, and whether they got it.
--
-- Every entitlement is quoted from SI 230/2018 (the instrument transposing Directive
-- 2013/33/EU) AS AMENDED — the right-to-work periods are the CURRENT ones (apply after 5
-- months, grantable at 6, valid 12), not the superseded 2018 text (8/9/6). Every reality
-- check is quoted from a NAMED source (C&AG RoAPS 2024 Ch.10, HIQA 2024, or the Government's
-- own Comprehensive Accommodation Strategy).
--
-- Where the State does not publish its performance, reality_status='NOT_PUBLISHED'. That is a
-- finding, not a gap to be filled — never infer it.
--
-- TONE (carried in the data, and binding on any page built from it): state the law and the
-- audited finding. Never editorialise about migration. Never name, age, locate or quote an
-- individual resident.
CREATE OR REPLACE VIEW v_ipas_entitlements AS
SELECT
    display_order,
    entitlement,
    legal_basis,
    what_the_law_says,
    timeframe,
    reality_status,
    reality_finding,
    legal_source_url,
    reality_source_url,
    reality_ref,
    tone_rule,
    value_safe_to_sum
FROM read_parquet('data/gold/parquet/ipas_entitlements.parquet')
ORDER BY display_order;
