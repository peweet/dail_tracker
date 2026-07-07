-- v_govt_consumption_deflator — the CSO government final consumption expenditure (GFCE) deflator
-- exposed as a view, so PUBLIC SPEND can be deflated by the agency-standard index instead of CPI.
-- Built by extractors/cso_pxstat_extract.py:build_govt_consumption_deflator (NA007 current ÷
-- NA008 constant, GFCE component). deflator_to_base = price[base]/price[year] (1.0 at base).
--
-- WHY this index for public money: HM Treasury / BEA deflate public expenditure with the GDP
-- deflator, NOT CPI (a household basket that includes imports and excludes government services).
-- We use the government-consumption COMPONENT specifically because a raw Irish GDP deflator is
-- distorted by multinational activity. Annual; currently covers years ≤2024 (no 2025 NA yet) →
-- later years get a NULL factor (never a silent x1.0). Named procurement_ac_* so the alphabetical
-- loader registers it before the payments-real view that LEFT JOINs it.
CREATE OR REPLACE VIEW v_govt_consumption_deflator AS
SELECT
    year,
    gov_price_index,
    deflator_to_base,
    base_year,
    'CSO_GOV_CONSUMPTION' AS index_code
FROM read_parquet('data/gold/parquet/cso_govt_consumption_deflator.parquet');
