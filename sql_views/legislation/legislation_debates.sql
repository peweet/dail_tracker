-- v_legislation_debates — debate sessions linked to each bill
-- Source: data/silver/parquet/debates.parquet
-- Joined to bills via bill_year + bill_no composite key.

CREATE OR REPLACE VIEW v_legislation_debates AS
SELECT
    "bill.billYear" || '_' || "bill.billNo"  AS bill_id,
    TRY_CAST(date AS DATE)                   AS debate_date,
    showAs                                   AS debate_title,
    debate_url_web                           AS debate_url,
    "chamber.showAs"                         AS chamber,
    "bill.billNo"                            AS bill_no,
    "bill.billYear"                          AS bill_year
FROM read_parquet('data/silver/parquet/debates.parquet')
WHERE "bill.billNo" IS NOT NULL
  AND "bill.billYear" IS NOT NULL
ORDER BY debate_date ASC NULLS LAST;
