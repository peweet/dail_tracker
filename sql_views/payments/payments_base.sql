-- v_payments_base — reads the full PSA gold Parquet produced by payments_full_psa_etl.py
--
-- Grain: one row per payment transaction (member_name, date_paid, narrative).
-- Covers all payment kinds: TAA, PSA_DUBLIN, PRA, PRA_MIN, PRA_FLAG_ONLY — both the
-- TAA-banded travel allowance and the previously-quarantined PRA-side rows.
--
-- Column aliases preserve the names expected by downstream views:
--   amount      → amount_num   (matches payments_summary, payments_member_detail, etc.)
--   date_paid   → synthesises payment_year
--
-- unique_member_code / party_name / constituency populated by
-- payments_member_enrichment.py after the PSA ETL runs.
-- Coverage: 172 of 176 current TDs (97.7%). The four unmatched (Daniel Ennis,
-- Frankie Feighan, Paul Nicholas Gogarty, Conor D McGuinness) are upstream
-- name-shape mismatches between the Oireachtas members API and the PSA
-- payment publications; they retain NULL and fall back to the "Not on file"
-- empty-state in the UI.

CREATE OR REPLACE VIEW v_payments_base AS
SELECT
    member_name,
    position,
    payment_kind,
    taa_band_raw,
    -- taa_band_label is re-derived here rather than taken from the Parquet.
    --
    -- The label baked into the gold Parquet by payments_full_psa_etl.py is wrong for
    -- bands 2-8 (ranges that appear in no statute, and an open-ended "over 210 km"
    -- Band 8 that truncated the scale) and absent for bands 9-12 ("Band N (unmapped)").
    -- The ETL's TAA_LABELS is now correct, but re-baking the full PSA parquet is a
    -- separate job, so this CASE serves the corrected labels off the existing bake.
    -- Drop it once the PSA ETL has been re-run and the parquet carries these labels.
    --
    -- Source of truth: Table to Reg. 4, Oireachtas (Allowances and Facilities)
    -- Regulations 2010 (S.I. 84/2010), as substituted by Reg. 4 of S.I. 149/2013.
    -- The 2013 instrument changed rates only; the distance boundaries are identical,
    -- so this table is correct across the whole 2020+ window the parquet covers.
    --   https://www.irishstatutebook.ie/eli/2010/si/84/made/en/print
    --   https://www.irishstatutebook.ie/eli/2013/si/149/made/en/print
    --   https://www.oireachtas.ie/en/members/salaries-and-allowances/parliamentary-standard-allowances/
    --
    -- ELSE preserves the Parquet's NULL for rows that carry no distance band at all:
    -- PRA / PRA_MIN / PRA_FLAG_ONLY rows ('', 'Vouched', 'MIN', 'CC', 'NoTAA'), the
    -- OCR mis-reads ('Dub', 'Dubin', 'Dulin'), and the composite mid-month role-change
    -- bands ('4/MIN', 'MIN/8', ...). Those are deliberately unlabelled.
    CASE taa_band_raw
        WHEN 'Dublin' THEN 'Dublin / under 25 km'
        WHEN '1'  THEN 'Band 1 — 25–60 km'
        WHEN '2'  THEN 'Band 2 — 60–90 km'
        WHEN '3'  THEN 'Band 3 — 90–120 km'
        WHEN '4'  THEN 'Band 4 — 120–150 km'
        WHEN '5'  THEN 'Band 5 — 150–180 km'
        WHEN '6'  THEN 'Band 6 — 180–210 km'
        WHEN '7'  THEN 'Band 7 — 210–240 km'
        WHEN '8'  THEN 'Band 8 — 240–270 km'
        WHEN '9'  THEN 'Band 9 — 270–300 km'
        WHEN '10' THEN 'Band 10 — 300–330 km'
        WHEN '11' THEN 'Band 11 — 330–360 km'
        WHEN '12' THEN 'Band 12 — 360 km or more'
        ELSE taa_band_label
    END                                   AS taa_band_label,
    date_paid,
    narrative,
    amount                                AS amount_num,
    EXTRACT(YEAR FROM date_paid)::INTEGER AS payment_year,
    source_pdf,
    schema,
    unique_member_code,
    party_name,
    constituency,
    COALESCE(house, 'Dáil')               AS house
-- Reads both houses. The Dáil parquet has no `house` column; the Senator one
-- (seanad_payments_full_psa.parquet, enriched by payments_member_enrichment)
-- does. union_by_name=true tolerates the differing column set and fills the
-- Dáil rows' house as NULL → coalesced to 'Dáil'. The per-member panel filters
-- by unique_member_code, so a member's code resolves to their own house's rows.
FROM read_parquet(
    ['data/gold/parquet/payments_full_psa.parquet',
     'data/gold/parquet/seanad_payments_full_psa.parquet'],
    union_by_name = true
);
