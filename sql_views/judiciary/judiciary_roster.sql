-- v_judiciary_roster — the sitting bench, one row per judge (identity grain).
-- Source: data/gold/parquet/judiciary_bench.parquet
--   (produced by extractors/judiciary_bench_extract.py: Courts Service "The Judges"
--   roster, ex-officio cross-listings resolved to one substantive seat per judge,
--   enriched from the Iris Oifigiúil appointment spine + SI 323/2021 salary bands +
--   High Court specialist-list assignments).
--
-- Grain: one row per sitting judge (judge_key). Court presidents listed ex-officio
-- under several courts are collapsed to their substantive seat (is_ex_officio_or_multi
-- marks them; seat_count keeps the raw listing count). salary_band_eur is the ordinary-
-- judge band for the court and is NULL for ex-officio/president seats (premium not
-- attributable to a named person from the roster alone) and for the Court of Appeal
-- (no separate ordinary band in SI 323/2021). has_spine is false for pre-2016 veterans
-- whose appointment predates the 2016+ Iris spine — surfaced, not hidden.
-- SCOPE: appointment / office / rank / assignment / salary BAND only. No performance,
-- conduct, or ranking data is joined here.
CREATE OR REPLACE VIEW v_judiciary_roster AS
SELECT
    judge_key,
    judge_name,
    court,
    current_court,
    court_rank,
    is_ex_officio_or_multi,
    seat_count,
    salary_band_eur,
    salary_office,
    salary_source,
    assignment,
    assignment_term,
    has_spine,
    first_appointed_date,
    first_appointing_authority,
    appointed_court,
    is_elevation,
    elevation_path,
    requires_manual_review,
    appt_source_url,
    source_url,
    source_published_at
FROM read_parquet('data/gold/parquet/judiciary_bench.parquet')
ORDER BY court_rank, judge_name;
