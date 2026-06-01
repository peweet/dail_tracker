-- v_member_constituency_demographics — Census 2022 population per Dáil
-- constituency, on the CURRENT (2023 Electoral Commission) boundaries. One
-- row per constituency, joinable to v_member_registry.constituency.
--
-- Source: data/gold/parquet/ec_constituency_pop_2022.parquet
--   Electoral Commission, Constituency Review Report 2023, Appendix 2
--   ("Statistics Relating to Recommended Dáil Constituencies"). Built by
--   pipeline_sandbox/ec_constituency_pop_extract.py.
--
-- BOUNDARY NOTE: this REPLACES the earlier CSO PxStat FY005 source, which was
-- keyed on the 2017 boundaries (39 constituencies) and left the four split/new
-- constituencies (Dublin Fingal East/West, Tipperary North/South, Laois,
-- Offaly, Wicklow-Wexford) with no clean row. The Commission redrew the
-- boundaries to balance Census 2022 population, so its own report carries the
-- 2022 headcount on the current 43 boundaries — a verified 43/43 join to
-- v_member_registry with no aliasing required.
--
-- NO 2016 COMPARISON: Census 2016 was collected on yet-older boundaries, so a
-- 2016→2022 growth figure on the 2023 boundaries cannot be computed without
-- mixing boundary vintages — exactly the error this view exists to remove.
-- We therefore expose 2022 figures only; growth is intentionally omitted.

CREATE OR REPLACE VIEW v_member_constituency_demographics AS
SELECT
    constituency_name,
    CAST(population_2022 AS BIGINT)         AS population_2022,
    CAST(population_per_td_2022 AS BIGINT)  AS population_per_td,
    CAST(td_seats_2024 AS INTEGER)          AS td_seats,
    boundaries_label,
    source_key
FROM read_parquet('data/gold/parquet/ec_constituency_pop_2022.parquet')
ORDER BY constituency_name;
