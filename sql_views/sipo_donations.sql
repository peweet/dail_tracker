-- v_sipo_donations — declared political donations to parties (SIPO), GE2024.
-- Reads data/gold/parquet/sipo_donations.parquet (promoted by
-- pipeline_sandbox/sipo_promote_to_gold.py).
--
-- Grain: one row per declared donation (> EUR 1,500 threshold). OCR-derived from
-- the official SIPO scanned return → every row carries `flag` + `min_confidence`
-- so the UI can show a "verify vs SIPO PDF" mark. Donor NAME + AMOUNT are the
-- public SIPO record; the donor's home ADDRESS is intentionally absent from gold
-- (stripped at promotion — never committed, never displayed). No-inference:
-- figures + source only.

CREATE OR REPLACE VIEW v_sipo_donations AS
SELECT
    election_event,
    party,
    donor_name,
    value_eur,
    date_received_raw,
    nature,
    description_of_donor,
    receipt_issued,
    donor_irish_citizen,
    flag,
    min_confidence,
    (flag <> 'ok') AS needs_verify,
    source_pdf,
    source_page
FROM read_parquet('data/gold/parquet/sipo_donations.parquet')
ORDER BY party, value_eur DESC;

-- v_sipo_donations_by_party — party-level rollup for the Donations-lens cards.
-- Aggregation lives in the pipeline view (not the Streamlit retrieval layer).
CREATE OR REPLACE VIEW v_sipo_donations_by_party AS
SELECT
    election_event,
    party,
    COUNT(*)                              AS donation_count,
    SUM(value_eur)                        AS total_value,
    MIN(value_eur)                        AS min_value,
    MAX(value_eur)                        AS max_value,
    SUM(CASE WHEN flag <> 'ok' THEN 1 ELSE 0 END) AS verify_count
FROM read_parquet('data/gold/parquet/sipo_donations.parquet')
GROUP BY election_event, party
ORDER BY total_value DESC;
