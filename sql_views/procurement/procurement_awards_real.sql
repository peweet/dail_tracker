-- v_procurement_awards_real — EXPERIMENTAL real-terms (inflation-adjusted) lens over the
-- eTenders awards. ADDITIVE: nominal value_eur is untouched and canonical; this view only
-- ADDS today's-money columns beside it, so no cited figure changes. The consuming
-- query/UI layer must gate this behind DAIL_EXPERIMENTAL, exactly like v_procurement_bid_signal.
-- Design + rationale: doc/PROCUREMENT_INFLATION_BENCHMARKING_DESIGN.md.
--
-- One machine-readable real_caveat so every consumer (UI / API / export) reads the rule in one
-- place. value_eur_real is non-NULL IFF real_caveat IN ('OK','MULTI_YEAR_APPROX'):
--   NO_VALUE             — award row carries no euro to adjust
--   CEILING_NOT_ADJUSTED — framework/DPS ceiling: multi-year legal headroom, NEVER deflated
--   IMPLAUSIBLE          — value outside [€100, €50m] (parse artefact; band matches the awards
--                          ceiling in services/deflator.py + the pipeline is_large_award_review)
--   YEAR_MISSING         — award year absent from the CPI index (e.g. 2026): real = NULL,
--                          NEVER a silent x1.0 (LEFT JOIN yields a NULL deflator_to_base)
--   MULTI_YEAR_APPROX    — contract > 12 months: deflated from its award year only (approx; the
--                          spend was actually spread across years)
--   OK                   — single-year, plausible, adjustable
CREATE OR REPLACE VIEW v_procurement_awards_real AS
WITH a AS (
    SELECT
        "Tender ID"                                          AS tender_id,
        "Contracting Authority"                              AS contracting_authority,
        "Main Cpv Code"                                      AS cpv_code,
        "Main Cpv Code Description"                          AS cpv_description,
        substr("Main Cpv Code", 1, 4)                        AS trade_code,
        value_eur,
        value_kind,
        value_safe_to_sum,
        is_framework_or_dps,
        TRY_CAST("Contract Duration (Months)" AS INTEGER)    AS contract_duration_months,
        -- Plausibility computed INLINE (not read from the optional, backfilled value_plausible
        -- column) so the view binds whether or not tools/patch_value_plausible_flag.py has run.
        -- Band == services/deflator.py:value_plausible_expr with the awards €50m review ceiling.
        (value_eur >= 100 AND value_eur <= 50000000)         AS value_plausible,
        TRY_CAST(substr("Notice Published Date/Contract Created Date", 7, 4) AS INTEGER) AS award_year
    FROM read_parquet('data/gold/parquet/procurement_awards.parquet')
)
SELECT
    a.*,
    d.base_year                                              AS real_base_year,
    'CSO_CPA07_CPI'                                          AS deflator_index,
    d.deflator_to_base                                       AS deflator_factor,
    -- real value only when it is honest to compute one
    CASE
        WHEN a.value_eur IS NULL          THEN NULL
        WHEN a.is_framework_or_dps        THEN NULL
        WHEN a.value_plausible IS FALSE   THEN NULL
        WHEN d.deflator_to_base IS NULL   THEN NULL
        ELSE a.value_eur * d.deflator_to_base
    END                                                      AS value_eur_real,
    CASE
        WHEN a.value_eur IS NULL             THEN 'NO_VALUE'
        WHEN a.is_framework_or_dps           THEN 'CEILING_NOT_ADJUSTED'
        WHEN a.value_plausible IS FALSE      THEN 'IMPLAUSIBLE'
        WHEN d.deflator_to_base IS NULL      THEN 'YEAR_MISSING'
        WHEN a.contract_duration_months > 12 THEN 'MULTI_YEAR_APPROX'
        ELSE 'OK'
    END                                                      AS real_caveat,
    -- ── Sector-aware lens (the methodology fix): construction CPVs (45* works / 71* eng) are
    --    adjusted by the SCSI Tender Price Index — construction prices moved ~2× faster than CPI,
    --    so general CPI badly understates them. Everything else stays on CPI. The index used is
    --    named in deflator_index_sector so a real-terms figure always carries its provenance.
    --    Same honesty guards as value_eur_real (ceiling/implausible/no-value/missing-year → NULL).
    CASE WHEN substr(a.cpv_code, 1, 2) IN ('45', '71') THEN 'SCSI_TPI_CONSTRUCTION'
         ELSE 'CSO_CPA07_CPI' END                            AS deflator_index_sector,
    CASE
        WHEN a.value_eur IS NULL        THEN NULL
        WHEN a.is_framework_or_dps      THEN NULL
        WHEN a.value_plausible IS FALSE THEN NULL
        WHEN substr(a.cpv_code, 1, 2) IN ('45', '71')
             THEN a.value_eur * t.deflator_to_base          -- NULL if TPI lacks the year
        ELSE a.value_eur * d.deflator_to_base                -- NULL if CPI lacks the year
    END                                                      AS value_eur_real_sector
FROM a
LEFT JOIN v_cpi_deflator d ON a.award_year = d.year
LEFT JOIN v_scsi_tpi_deflator t ON a.award_year = t.year;
