-- v_ipas_operators — the named operators of IP accommodation centres, their HIQA
-- compliance record, and the public money they received.
--
-- WHY THIS EXISTS: the C&AG ANONYMISED its suppliers (A-G); HIQA's overview report never
-- names an operator. Only HIQA's 101 INDIVIDUAL inspection reports name the provider — and
-- they name one on every report. This view is that identification, joined to money.
--
-- IDENTITY GATE (non-negotiable): only operators whose name resolves EXACTLY on the house
-- normaliser (shared/name_norm.py) across BOTH the compliance and payment sides are here
-- (`match_confidence='exact'`). Operators we could not resolve with certainty are DROPPED
-- at promotion rather than named on a guess. One wrong name is worse than ten omitted.
--
-- ⚠️ NEVER CAUSAL. The compliance window (2024-01 -> 2026-03) and the payment windows
-- (DCEDIY Vote 40 2023-24; Dept of Justice 2025+, after IPAS transferred on 1 May 2025) are
-- DIFFERENT. The money is NOT "the price of that compliance record". Co-occurrence only.
--
-- ⚠️ The DCEDIY money is filtered upstream to stream='International Protection'. UKRAINE IS
-- EXCLUDED. Unfiltered, one provider reads EUR 46m against a true EUR 10.9m IP spend.
--
-- GRAIN: one row per resolved operator. value_safe_to_sum=FALSE — these are per-operator
-- totals over different windows and MUST NOT be summed into a headline, nor unioned with
-- procurement_payments_fact (which already contains the DoJ side).
CREATE OR REPLACE VIEW v_ipas_operators AS
SELECT
    display_name                AS operator,
    centres,
    judgments,
    not_compliant,
    pct_not_compliant,
    dcediy_ip_eur               AS ip_paid_dcediy_eur,   -- Vote 40, 2023-24, IP stream only
    doj_eur                     AS ip_paid_justice_eur,  -- Dept of Justice, 2025+
    match_confidence,
    caveat,
    value_safe_to_sum
FROM read_parquet('data/gold/parquet/ipas_operators.parquet')
WHERE match_confidence = 'exact'
ORDER BY pct_not_compliant DESC, centres DESC;
