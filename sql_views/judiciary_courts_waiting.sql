-- v_courts_waiting_times — published waiting-time lists, latest two years side by side.
-- Source: data/gold/parquet/judiciary_courts_waiting.parquet
--   (Courts Service Annual Report 2024, Waiting Times section pp.135–140; promoted by
--   extractors/judiciary_bench_extract.py). The wait_* columns are the published text
--   verbatim ("4 weeks", "Date immediately available", "1 week"); only the U+FFFD
--   extraction artefact was repaired upstream.
--
-- This view owns the sort metric (logic firewall): weeks_2024 / weeks_2023 parse the
-- leading number out of the published string so the UI can rank by longest wait.
-- "Date immediately available" -> 0 weeks; an unparseable label -> NULL (kept, shown
-- by its text). SCOPE: list throughput only — no judge named.
CREATE OR REPLACE VIEW v_courts_waiting_times AS
SELECT
    page,
    matter_or_venue,
    wait_2024,
    wait_2023,
    CASE WHEN lower(wait_2024) LIKE '%immediately%' THEN 0
         ELSE TRY_CAST(regexp_extract(wait_2024, '([0-9]+\.?[0-9]*)', 1) AS DOUBLE) END AS weeks_2024,
    CASE WHEN lower(wait_2023) LIKE '%immediately%' THEN 0
         ELSE TRY_CAST(regexp_extract(wait_2023, '([0-9]+\.?[0-9]*)', 1) AS DOUBLE) END AS weeks_2023,
    is_clean_label,
    source_name,
    source_url
FROM read_parquet('data/gold/parquet/judiciary_courts_waiting.parquet')
ORDER BY page, matter_or_venue;
