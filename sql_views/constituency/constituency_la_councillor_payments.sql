-- v_la_councillor_payments — ACTUAL s.142 register payments to named councillors, by
-- (council, councillor, year, category), from the councils that publish the statutory
-- register as OPEN DATA: South Dublin (quarterly CSVs 2022→) and Dublin City (monthly CSV).
-- HARD SCOPE CAP (council-targeting assessment): structured open-data publishers only —
-- the other 29 councils publish PDFs/HTML or nothing, and the UI states that honestly.
--
-- Aggregation lives HERE (quarters/months summed to council-year-category), never in the
-- page. Amounts are the register's own figures; category sets differ per publisher (SDCC
-- includes the representational payment, DCC's file is expenses/allowances only) — so
-- cross-council comparison of TOTALS is not like-for-like; the page presents one
-- councillor's own record, never a ranking.
--
-- Councillor names are the register's printed form (keep-as-printed rule, same as the
-- named-votes fact); the page joins on the roster name where it matches exactly, no fuzzy.
-- FREE-CIVIC ONLY: elected-member personal data — never in the paid product, exports, or API
-- (diary precedent, see doc/COUNCIL_TARGETING_FABLE_ASSESSMENT.md §3).
CREATE OR REPLACE VIEW v_la_councillor_payments AS
SELECT
    local_authority,
    councillor,
    CAST(year AS INTEGER)                 AS year,
    category,
    ROUND(SUM(CAST(value AS DOUBLE)), 2)  AS amount_eur
FROM read_csv('data/_meta/la_councillor_payments.csv', header = true, AUTO_DETECT = true)
WHERE unit = 'EUR'
GROUP BY local_authority, councillor, year, category;
