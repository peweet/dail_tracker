-- ════════════════════════════════════════════════════════════════════════════
-- v_experimental_lobbying_org_index_enriched
-- ════════════════════════════════════════════════════════════════════════════
--
-- STATUS: EXPERIMENTAL — proves the CRO×Charity×Lobbying enrichment slice
--         from CRO/INTEGRATION_PLAN.md §9.2 end-to-end.
--         The view name is prefixed `experimental_` so it can be filtered out
--         of any production-only registry sweep with one grep.
--
-- Filename matches the `lobbying_*.sql` glob in
-- utility/data_access/lobbying_data.py — it auto-loads with the rest of the
-- lobbying views into the in-process DuckDB connection. No loader changes
-- required.
--
-- INPUTS:
--   data/gold/parquet/top_lobbyist_organisations.parquet  (existing pipeline gold)
--   data/gold/parquet/lobbyist_persistence.parquet        (existing pipeline gold)
--   data/silver/cro/companies.parquet                     (pipeline_sandbox/cro_normalise.py)
--   data/silver/charities/charity_resolved.parquet        (pipeline_sandbox/charity_resolved.py)
--
-- JOINS:
--   Tier B/C name match — the gold lobbyist_name is free text, so we compute
--   `name_norm` inline using exactly the same rule as the sandbox normalisers
--   (upper, strip punctuation, drop legal suffixes, alphanumeric only,
--   collapse whitespace) and join on equality.
--
--   At most one charity row and one company row per lobbyist:
--     - charity:  pick the row with the lowest RCN (deterministic; oldest first)
--     - company:  prefer Status='Normal'; then most-recent reg_date
--
--   This is the EXACT-name-match slice only. Fuzzy match + manual override CSV
--   are the next layer (per §4.4 / §10.1 step 6) and live in pipeline_sandbox.
--
-- OUTPUT COLUMNS — superset of §9.2 amended `v_lobbying_org_index`:
--   lobbyist_name, return_count, politicians_targeted, distinct_policy_areas,
--   first_period, last_period,
--   rcn, company_num,                      -- resolved entity IDs (nullable)
--   sector_label,                          -- governing_form (charity) or company_type (cro)
--   status,                                -- collapsed enum: active|in_distress|dead|registered|deregistered
--   funding_profile,                       -- state_funded|mostly_donations|mostly_trading|mixed|undisclosed
--   gov_funded_share_latest,
--   gross_income_latest_eur,
--   employees_band_latest,
--   entity_age_years,
--   newly_incorporated_flag,               -- TRUE if first_return_date - reg_date <= 24 months
--   state_adjacent_flag,
--   match_method,                          -- charity_name_exact | company_name_exact | both | none
--   flags                                  -- VARCHAR[] of warning-flag IDs; rendered as red/amber/info pills.
--                                          -- Stable IDs — UI labels live in lobbyist_poc.py:_FLAG_LABELS.
--
-- ════════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW v_experimental_lobbying_org_index_enriched AS
WITH
-- Gold — the existing org leaderboard
org_base AS (
    SELECT
        lobbyist_name,
        returns_filed                            AS return_count,
        distinct_politicians_targeted            AS politicians_targeted,
        distinct_policy_areas
    FROM read_parquet('data/gold/parquet/top_lobbyist_organisations.parquet')
),
persistence AS (
    SELECT
        lobbyist_name,
        CAST(first_return_date AS DATE) AS first_return_date,
        CAST(last_return_date  AS DATE) AS last_return_date
    FROM read_parquet('data/gold/parquet/lobbyist_persistence.parquet')
),
-- Inline name_norm — must stay in lock-step with the rule in
-- pipeline_sandbox/cro_normalise.py and pipeline_sandbox/charity_normalise.py.
-- If you change one, change all three.
org_norm AS (
    SELECT
        o.lobbyist_name,
        o.return_count,
        o.politicians_targeted,
        o.distinct_policy_areas,
        p.first_return_date,
        p.last_return_date,
        TRIM(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            UPPER(o.lobbyist_name),
                            '[\.,&''"]', ' ', 'g'),
                        '\b(THE|LIMITED|LTD|DAC|PLC|CLG|UC|COMPANY|DESIGNATED ACTIVITY COMPANY|COMPANY LIMITED BY GUARANTEE|UNLIMITED COMPANY|GROUP|HOLDINGS|IRELAND|IRL|OF)\b', ' ', 'g'),
                    '[^A-Z0-9 ]', ' ', 'g'),
                '\s+', ' ', 'g')
        ) AS name_norm
    FROM org_base o
    LEFT JOIN persistence p USING (lobbyist_name)
),
-- One charity row per name_norm — deterministic by lowest RCN
charity_pick AS (
    SELECT
        name_norm,
        rcn,
        registered_charity_name,
        status            AS charity_status,
        governing_form,
        classification_primary,
        country_established,
        has_cro_number_flag,
        gov_funded_share_latest,
        gross_income_latest_eur,
        employees_band_latest,
        funding_profile,
        state_adjacent_flag,
        period_end_latest,
        charity_filing_overdue_flag,
        charity_deficit_latest_flag,
        charity_insolvent_latest_flag
    FROM read_parquet('data/silver/charities/charity_resolved.parquet')
    WHERE name_norm IS NOT NULL AND name_norm != ''
    QUALIFY ROW_NUMBER() OVER (PARTITION BY name_norm ORDER BY rcn) = 1
),
-- One company row per name_norm — prefer Normal status, then most-recent reg
company_pick AS (
    SELECT
        name_norm,
        company_num,
        company_name,
        company_status,
        status_pill_value AS company_status_pill_value,
        company_type,
        company_reg_date,
        company_status_date,
        comp_dissolved_date,
        entity_age_years,
        annual_return_overdue_flag,
        accounts_overdue_flag,
        recent_distress_flag,
        no_registered_address_flag,
        recent_rename_flag,
        reg_date_invalid_flag
    FROM read_parquet('data/silver/cro/companies.parquet')
    WHERE name_norm IS NOT NULL AND name_norm != ''
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY name_norm
        ORDER BY
            CASE WHEN company_status = 'Normal' THEN 0 ELSE 1 END,
            company_reg_date DESC NULLS LAST
    ) = 1
)
SELECT
    o.lobbyist_name,
    o.return_count,
    o.politicians_targeted,
    o.distinct_policy_areas,
    CAST(o.first_return_date AS VARCHAR) AS first_period,
    CAST(o.last_return_date  AS VARCHAR) AS last_period,

    c.rcn,
    co.company_num,

    -- sector_label: prefer charity classification (more specific), fall back
    -- to company_type (commercial form), else null
    COALESCE(c.classification_primary, co.company_type)            AS sector_label,

    -- status: collapse charity + company enums into one display field
    --   active        — company Normal
    --   in_distress   — company Liquidation / Strike Off Listed / Receivership
    --   dead          — company Dissolved / Strike Off / Deregistered
    --   registered    — charity Registered (no matching company)
    --   deregistered  — charity Deregistered* (no matching company)
    --   unknown       — neither register matched
    CASE
        WHEN co.company_status_pill_value IN ('active','in_distress','dead')
            THEN co.company_status_pill_value
        WHEN c.charity_status = 'Registered' THEN 'registered'
        WHEN c.charity_status LIKE 'Deregistered%' THEN 'deregistered'
        ELSE 'unknown'
    END                                                            AS status,

    -- Charity-side enrichments (null when no charity match)
    c.funding_profile,
    c.gov_funded_share_latest,
    c.gross_income_latest_eur,
    c.employees_band_latest,
    c.state_adjacent_flag,
    c.country_established,

    -- Entity age (prefer CRO incorporation date — most reliable origin date)
    co.entity_age_years,

    -- Newly-incorporated flag — within 24 months of first lobbying return
    CASE
        WHEN co.company_reg_date IS NOT NULL
         AND o.first_return_date IS NOT NULL
         AND DATE_DIFF('month', co.company_reg_date, o.first_return_date) BETWEEN 0 AND 24
        THEN TRUE
        ELSE FALSE
    END                                                            AS newly_incorporated_flag,

    -- Match method, surfaced for editor review and uncertainty UI
    CASE
        WHEN c.rcn IS NOT NULL AND co.company_num IS NOT NULL THEN 'both_name_exact'
        WHEN c.rcn IS NOT NULL                                THEN 'charity_name_exact'
        WHEN co.company_num IS NOT NULL                       THEN 'company_name_exact'
        ELSE 'unmatched'
    END                                                            AS match_method,

    -- Warning flags — stable string IDs; UI labels and severity tier live in
    -- utility/pages_code/lobbyist_poc.py:_FLAG_LABELS. NULLs are stripped so an
    -- entity with no flags returns []. Each rule is one CASE → one flag string.
    list_filter([
        -- ── Composite (cross-dataset) ──────────────────────────────────────
        CASE WHEN co.company_status_pill_value = 'in_distress'
              AND CAST(o.last_return_date AS DATE) > co.company_status_date
             THEN 'lobbied_while_in_distress' END,
        CASE WHEN co.comp_dissolved_date IS NOT NULL
              AND CAST(o.last_return_date AS DATE) > co.comp_dissolved_date
             THEN 'lobbied_while_extinct' END,
        -- ── CRO-derived ────────────────────────────────────────────────────
        CASE WHEN co.annual_return_overdue_flag THEN 'annual_return_overdue' END,
        CASE WHEN co.accounts_overdue_flag      THEN 'accounts_overdue'      END,
        CASE WHEN co.recent_distress_flag       THEN 'recent_distress'       END,
        CASE WHEN co.no_registered_address_flag THEN 'no_registered_address' END,
        CASE WHEN co.recent_rename_flag         THEN 'recent_rename'         END,
        CASE WHEN co.reg_date_invalid_flag      THEN 'invalid_reg_date'      END,
        -- ── Charity-derived ────────────────────────────────────────────────
        CASE WHEN c.charity_filing_overdue_flag   THEN 'charity_filing_overdue'   END,
        CASE WHEN c.charity_deficit_latest_flag   THEN 'charity_deficit_latest'   END,
        CASE WHEN c.charity_insolvent_latest_flag THEN 'charity_insolvent_latest' END,
        -- Foreign domicile — match all Irish-jurisdiction variants the
        -- regulator actually emits (`Ireland`, `Republic of Ireland`,
        -- `Poblacht na hÉireann`) so the flag fires only on genuinely
        -- foreign-domiciled charities. `Northern Ireland` IS foreign
        -- (UK Charity Commission for NI jurisdiction).
        CASE WHEN c.country_established IS NOT NULL
              AND TRIM(c.country_established) <> ''
              AND NOT (
                  (c.country_established ILIKE '%IRELAND%'
                    OR c.country_established ILIKE '%ÉIREANN%'
                    OR c.country_established ILIKE '%EIREANN%')
                  AND c.country_established NOT ILIKE '%NORTHERN%'
              )
             THEN 'foreign_domicile' END,
        CASE WHEN c.has_cro_number_flag = FALSE
              AND c.governing_form ILIKE '%articles%'
             THEN 'cro_undisclosed' END
    ], x -> x IS NOT NULL)                                          AS flags
FROM org_norm o
LEFT JOIN charity_pick c  USING (name_norm)
LEFT JOIN company_pick co USING (name_norm)
ORDER BY o.return_count DESC;
