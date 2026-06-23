-- v_attendance_taa_compliance
-- Source: data/gold/parquet/participation_presence_year.parquet
--         (written by extractors/participation_extract.py).
--
-- The one honest use of the TAA presence record: did the member clear the
-- 120-day allowance threshold, and if not, what deduction applies? The Travel &
-- Accommodation Allowance is paid on a 150-day basis with a 1% deduction for each
-- day attended below 120 (days_below_minimum == deduction_pct). This is the money
-- angle — verifiable, no judgement. Office-holders (Taoiseach/ministers) are not
-- paid TAA on the attendance basis, so they legitimately sit outside this set.
CREATE OR REPLACE VIEW v_attendance_taa_compliance AS
SELECT
    COALESCE(unique_member_code, '') AS unique_member_code,
    full_name        AS member_name,
    COALESCE(party_name, '')   AS party_name,
    COALESCE(constituency, '') AS constituency,
    house,
    CAST(year AS INTEGER) AS year,
    total_days,
    sitting_days,
    other_days,
    meets_120,
    days_below_minimum,
    deduction_pct
FROM read_parquet('data/gold/parquet/participation_presence_year.parquet');
