-- v_bill_si_operation_mix — operation-type counts per bill, used by the
-- "composition sentence" above the SI list on bill detail.
-- One row per (bill_id, si_operation). Source: v_bill_statutory_instruments
-- (already a registered view via legislation_*.sql).
--
-- File name starts with 'legislation_' so legislation_data.py picks it up.

CREATE OR REPLACE VIEW v_bill_si_operation_mix AS
SELECT bill_id,
       si_operation,
       COUNT(*) AS n
FROM v_bill_statutory_instruments
WHERE si_operation IS NOT NULL
GROUP BY bill_id, si_operation;
