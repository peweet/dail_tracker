-- v_procurement_awards_real_detail — AWARD-GRAIN real-terms detail: one row per awarded
-- contract/lot carrying supplier, date, bid count AND its inflation-adjusted value.
--
-- WHY THIS EXISTS. v_procurement_awards_real answers "what is the real-terms BAND for a
-- category?" — it reads the parquet directly and keeps only the columns an aggregate needs,
-- so it has no supplier, no award_date and no bid count. A bidder reading a live notice wants
-- the opposite: the individual PRIOR AWARDS behind that band — who won, when, for how much in
-- today's money, and against how many bidders. That is this view. It reads v_procurement_awards
-- (the canonical award view, which already parses the date and casts the bid counts) and LEFT
-- JOINs the same two deflator indices.
--
-- DRIFT GUARD. The honesty guards below are the SAME rules as v_procurement_awards_real:
-- framework/DPS ceilings are never deflated, implausible values are excluded, and a year absent
-- from an index yields NULL rather than a silent x1.0. Because the rules live in two views,
-- test/dail_tracker_core/test_publicsignal_market_intel.py pins the two to agree on the shared
-- OK/MULTI_YEAR_APPROX population — if one is edited without the other, that test fails.
--
-- SECTOR LENS. Construction and engineering CPVs (divisions 45 and 71) are adjusted by the SCSI
-- Tender Price Index, everything else by CSO CPA07 CPI — construction tender prices moved far
-- faster than general CPI, so one index across both badly understates construction. The index
-- actually used is named per row in deflator_index_sector, so a real figure always carries its
-- provenance. EXPERIMENTAL, like its two parents: gate consumption behind DAIL_EXPERIMENTAL.
CREATE OR REPLACE VIEW v_procurement_awards_real_detail AS
SELECT
    a.tender_id,
    a.contracting_authority,
    a.supplier,
    a.supplier_norm,
    a.supplier_class,
    a.cpv_code,
    a.cpv_description,
    substr(a.cpv_code, 1, 4)                                 AS trade_code,
    substr(a.cpv_code, 1, 2)                                 AS division_code,
    a.tender_title,
    a.category_label,
    a.procedure_type,
    a.competition_type,
    a.award_date,
    extract(year FROM a.award_date)::INTEGER                 AS award_year,
    a.contract_duration_months,
    a.n_bids_received,
    a.n_sme_bids_received,
    a.n_awarded_smes,
    a.value_eur,
    a.value_kind,
    a.value_safe_to_sum,
    a.is_framework_or_dps,
    a.is_call_off,
    a.etenders_notice_url,
    a.ted_notice_link,
    -- Same plausibility band as v_procurement_awards_real (services/deflator.py value_plausible
    -- expression with the awards €50m review ceiling).
    (a.value_eur >= 100 AND a.value_eur <= 50000000)          AS value_plausible,
    d.base_year                                              AS real_base_year,
    CASE WHEN substr(a.cpv_code, 1, 2) IN ('45', '71') THEN 'SCSI_TPI_CONSTRUCTION'
         ELSE 'CSO_CPA07_CPI' END                            AS deflator_index_sector,
    CASE
        WHEN a.value_eur IS NULL        THEN NULL
        WHEN a.is_framework_or_dps      THEN NULL
        WHEN NOT (a.value_eur >= 100 AND a.value_eur <= 50000000) THEN NULL
        WHEN substr(a.cpv_code, 1, 2) IN ('45', '71')
             THEN a.value_eur * t.deflator_to_base           -- NULL when TPI lacks the year
        ELSE a.value_eur * d.deflator_to_base                -- NULL when CPI lacks the year
    END                                                      AS value_eur_real_sector,
    CASE
        WHEN a.value_eur IS NULL        THEN NULL
        WHEN a.is_framework_or_dps      THEN NULL
        WHEN NOT (a.value_eur >= 100 AND a.value_eur <= 50000000) THEN NULL
        ELSE a.value_eur * d.deflator_to_base
    END                                                      AS value_eur_real_cpi,
    CASE
        WHEN a.value_eur IS NULL                    THEN 'NO_VALUE'
        WHEN a.is_framework_or_dps                  THEN 'CEILING_NOT_ADJUSTED'
        WHEN NOT (a.value_eur >= 100 AND a.value_eur <= 50000000) THEN 'IMPLAUSIBLE'
        WHEN d.deflator_to_base IS NULL             THEN 'YEAR_MISSING'
        WHEN a.contract_duration_months > 12        THEN 'MULTI_YEAR_APPROX'
        ELSE 'OK'
    END                                                      AS real_caveat
FROM v_procurement_awards a
LEFT JOIN v_cpi_deflator      d ON extract(year FROM a.award_date) = d.year
LEFT JOIN v_scsi_tpi_deflator t ON extract(year FROM a.award_date) = t.year;
