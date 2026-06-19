-- v_member_salary — the STATUTORY SALARY RATE for each current member.
--
-- This is NOT earned/take-home pay and NOT the PSA expense allowances shown on
-- the Payments page. It is the published *set rate* a member is entitled to under
-- the Oireachtas (Allowances and Facilities) Regulations: the basic salary for
-- their House, plus — for office-holders — the salary allowance for the highest
-- office they CURRENTLY hold. Rates are fully citable (data/_meta/
-- oireachtas_salary_rates.csv, sourced from the Houses of the Oireachtas "Guide to
-- Salary and Allowances, 34th Dáil & 27th Seanad", 2 Dec 2024).
--
-- Honesty rails (see feedback_no_inference_in_app):
--   * CURRENT office only (end_date IS NULL). We never pro-rata a partial year or
--     reconstruct historical salary — that would be inference, not a published fact.
--   * One office allowance per member (the HIGHEST). Office-holders draw a single
--     office-holder allowance on top of basic salary, not one per portfolio — so a
--     two-department Minister is counted once at the Minister rate.
--   * Only offices unambiguously present in the Oireachtas member feed's
--     office_N_name fields are mapped: Taoiseach, Minister (senior), Minister of
--     State, Ceann Comhairle, Leas-Cheann Comhairle. The Tánaiste premium, Seanad
--     chair roles, committee chairs and party-whip allowances are NOT separately
--     identified here (the feed exposes the Tánaiste only via their ministerial
--     title, and "Cathaoirleach" in the committee fields means a committee chair,
--     not the Seanad Cathaoirleach). The page carries a caveat to that effect.
--
-- Grain: one row per (unique_member_code, house) in v_member_registry.

CREATE OR REPLACE VIEW v_member_salary AS
WITH rates AS (
    SELECT rate_key, rate_label, rate_type, house, CAST(annual_rate_eur AS BIGINT) AS annual_rate_eur,
           effective_from, source_doc, source_url
    FROM read_csv('data/_meta/oireachtas_salary_rates.csv', header = true, AUTO_DETECT = true)
),
basic AS (
    SELECT house, annual_rate_eur AS basic_rate, rate_label AS basic_label,
           effective_from, source_doc, source_url
    FROM rates
    WHERE rate_type = 'basic'
),
-- Unpivot the up-to-six office slots from the Dáil member feed; keep only
-- offices currently held (no end date).
offices AS (
    SELECT unique_member_code, office_name FROM (
        SELECT unique_member_code, CAST(office_1_name AS VARCHAR) AS office_name,
               CAST(office_1_end_date AS VARCHAR) AS end_date FROM read_parquet('{MEMBER_PARQUET_PATH}')
        UNION ALL
        SELECT unique_member_code, CAST(office_2_name AS VARCHAR),
               CAST(office_2_end_date AS VARCHAR) FROM read_parquet('{MEMBER_PARQUET_PATH}')
        UNION ALL
        SELECT unique_member_code, CAST(office_3_name AS VARCHAR),
               CAST(office_3_end_date AS VARCHAR) FROM read_parquet('{MEMBER_PARQUET_PATH}')
        UNION ALL
        SELECT unique_member_code, CAST(office_4_name AS VARCHAR),
               CAST(office_4_end_date AS VARCHAR) FROM read_parquet('{MEMBER_PARQUET_PATH}')
        UNION ALL
        SELECT unique_member_code, CAST(office_5_name AS VARCHAR),
               CAST(office_5_end_date AS VARCHAR) FROM read_parquet('{MEMBER_PARQUET_PATH}')
        UNION ALL
        SELECT unique_member_code, CAST(office_6_name AS VARCHAR),
               CAST(office_6_end_date AS VARCHAR) FROM read_parquet('{MEMBER_PARQUET_PATH}')
    )
    WHERE office_name IS NOT NULL AND TRIM(office_name) <> '' AND end_date IS NULL
),
classified AS (
    SELECT unique_member_code, office_name,
        CASE
            WHEN office_name = 'Taoiseach'                THEN 'taoiseach'
            WHEN office_name = 'Ceann Comhairle'          THEN 'ceann_comhairle'
            WHEN office_name = 'Leas-Cheann Comhairle'    THEN 'leas_cheann_comhairle'
            WHEN office_name LIKE 'Minister of State%'    THEN 'minister_of_state'
            WHEN office_name LIKE 'Minister for%'
              OR office_name LIKE 'Minister of %'         THEN 'minister'
            ELSE NULL
        END AS rate_key
    FROM offices
),
best_office AS (
    SELECT c.unique_member_code, c.office_name AS current_office,
           r.rate_label AS office_label, r.annual_rate_eur AS office_allowance
    FROM classified c
    JOIN rates r ON r.rate_key = c.rate_key AND r.rate_type = 'office_allowance'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY c.unique_member_code ORDER BY r.annual_rate_eur DESC, c.office_name
    ) = 1
)
SELECT
    reg.unique_member_code,
    reg.house,
    b.basic_label,
    b.basic_rate,
    bo.current_office,
    bo.office_label,
    bo.office_allowance,
    b.basic_rate + COALESCE(bo.office_allowance, 0) AS total_statutory_rate_eur,
    (bo.office_allowance IS NOT NULL)               AS is_office_holder,
    b.effective_from,
    b.source_doc,
    b.source_url
FROM v_member_registry reg
JOIN basic b ON b.house = reg.house
-- Office allowances are only mapped from the Dáil feed; never attach a Dáil
-- office to the Seanad row of the one person who sits in both houses.
LEFT JOIN best_office bo
       ON bo.unique_member_code = reg.unique_member_code
      AND reg.house = 'Dáil';
