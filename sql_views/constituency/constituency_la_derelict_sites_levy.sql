-- v_la_derelict_sites_levy — per-council Derelict Sites Levy enforcement & collection.
-- The Derelict Sites Act 1990 lets each council put a site on its Derelict Sites
-- Register and charge an annual levy (7% of market value). Maintaining the register,
-- levying, and collecting are all EXECUTIVE functions — the Chief Executive's
-- administration, not the elected councillors. So a council that levies little or
-- collects almost nothing is an executive enforcement failure: the sharpest
-- "is this council doing its job?" signal (nationally ~€26m sits uncollected).
--
-- Source: DHLGH annual return (gov.ie), 2024 — one consolidated XLSX, per-LA, CC-BY.
-- Parsed by pipeline_sandbox/housing/derelict_sites_levy_extract_experimental.py
-- into data/gold/parquet/derelict_sites_levy_wide.parquet (fidelity GREEN: 31 LAs,
-- per-LA sums reconcile to the file's own Total row). ⚠️ experimental/sandbox source.
--
-- Grain: one row per council. la is normalised to the local_authority join key used
-- by v_la_chief_executives (collapsing "Limerick/Waterford City and County" to the
-- bare key). collection note: total_received can EXCEED amount_levied because it
-- includes arrears collected from prior-year levies, so collection_rate_pct can be
-- >100 and is NULL where nothing was levied. levied_nothing flags the councils that
-- issued no levy at all. National window totals give the benchmark/denominator.
CREATE OR REPLACE VIEW v_la_derelict_sites_levy AS
WITH base AS (
    SELECT
        CASE
            WHEN la = 'Limerick City and County' THEN 'Limerick'
            WHEN la = 'Waterford City and County' THEN 'Waterford'
            ELSE la
        END                                       AS local_authority,
        year,
        CAST(sites_on_register_end AS INTEGER)    AS sites_on_register,
        CAST(sites_levied AS INTEGER)             AS sites_levied,
        amount_levied_eur,
        total_received_eur,
        cumulative_outstanding_eur
    FROM read_parquet('data/gold/parquet/derelict_sites_levy_wide.parquet')
)
SELECT
    local_authority,
    year,
    sites_on_register,
    sites_levied,
    amount_levied_eur,
    total_received_eur,
    cumulative_outstanding_eur,
    (amount_levied_eur IS NULL OR amount_levied_eur = 0)              AS levied_nothing,
    CASE WHEN amount_levied_eur > 0
         THEN ROUND(100.0 * total_received_eur / amount_levied_eur, 1)
    END                                                              AS collection_rate_pct,
    SUM(amount_levied_eur)          OVER ()                          AS national_amount_levied_eur,
    SUM(total_received_eur)         OVER ()                          AS national_total_received_eur,
    SUM(cumulative_outstanding_eur) OVER ()                          AS national_outstanding_eur
FROM base
ORDER BY cumulative_outstanding_eur DESC NULLS LAST;
