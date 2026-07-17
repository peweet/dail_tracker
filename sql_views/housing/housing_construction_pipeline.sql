-- v_housing_construction_pipeline — social-housing build programme per local authority.
-- The "are homes being built where I live?" counterpart to the demand-side waiting list.
-- One row per LA (31), ranked by the size of the not-yet-completed pipeline, with the
-- on-site subset (actively under construction now) and completed-to-date for context,
-- plus each LA's share of the national pipeline.
--
-- Source: housing_construction_pipeline.parquet (DHLGH Social Housing Construction
-- Status Report, from opendata.housing.gov.ie — see the extractor). A SNAPSHOT: the
-- same scheme recurs quarter to quarter until delivered, so pipeline_units is
-- value_safe_to_sum=False ACROSS quarters — this view is a single-quarter cross-section.
CREATE OR REPLACE VIEW v_housing_construction_pipeline AS
WITH src AS (
    SELECT
        local_authority,
        CAST(pipeline_units AS BIGINT)   AS pipeline_units,
        CAST(pipeline_schemes AS BIGINT) AS pipeline_schemes,
        CAST(units_on_site AS BIGINT)    AS units_on_site,
        CAST(schemes_on_site AS BIGINT)  AS schemes_on_site,
        CAST(units_completed AS BIGINT)  AS units_completed,
        report_period,
        source_name,
        source_url
    FROM read_parquet('data/gold/parquet/housing_construction_pipeline.parquet')
)
SELECT
    local_authority,
    pipeline_units,
    pipeline_schemes,
    units_on_site,
    schemes_on_site,
    units_completed,
    ROUND(100.0 * units_on_site / NULLIF(pipeline_units, 0), 1) AS pct_pipeline_on_site,
    SUM(pipeline_units) OVER ()                                  AS national_pipeline_units,
    SUM(units_on_site) OVER ()                                   AS national_units_on_site,
    SUM(pipeline_schemes) OVER ()                                AS national_pipeline_schemes,
    ROUND(100.0 * pipeline_units / NULLIF(SUM(pipeline_units) OVER (), 0), 1) AS pct_of_national,
    RANK() OVER (ORDER BY pipeline_units DESC)                   AS pipeline_rank,
    report_period,
    source_name,
    source_url
FROM src
ORDER BY pipeline_units DESC;
