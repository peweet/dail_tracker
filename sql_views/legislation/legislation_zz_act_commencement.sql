-- v_act_commencement — the commencement-order timeline for an enacted Act:
-- "brought into force by these orders, on these dates, by these ministers".
-- One row per (Act, commencement SI). Backs the "Commencement & in-force
-- history" section on the legislation detail page.
--
-- Composition (no new parquet):
--   * v_bill_statutory_instruments — the bill↔SI link (EXACT matches only,
--     score 1.0; iris_si_bill_enrichment.py) + si_commenced_sections.
--   * v_statutory_instruments      — the resolved signing minister
--     (si_minister_name / _member_code), department, and the order's own
--     legal state (current_state, LEFT-JOINed there from the eISB directory).
--
-- HONESTY: this is a commencement HISTORY, not a consolidated in-force status.
-- A NULL si_commenced_sections means the order did not name a provision in its
-- TITLE — never "the whole Act" (the order body, unparsed, usually carries the
-- detail). order_current_state is the state of the ORDER itself (e.g. revoked),
-- not of the Act. We never assert "fully in force / X still uncommenced" — that
-- needs the Act's section inventory, which we don't hold.
--
-- 'zz_' prefix so the alphabetical register loads this AFTER its dependency
-- views (v_bill_statutory_instruments, v_statutory_instruments).

CREATE OR REPLACE VIEW v_act_commencement AS
SELECT
    b.bill_id,
    b.bill_short_title,
    b.si_id,
    b.si_year,
    b.si_number,
    b.si_title,
    b.si_commenced_sections,
    b.si_signed_date,
    s.si_minister_name,            -- resolved person (nullable)
    s.si_minister_member_code,     -- → member profile link (nullable)
    s.si_responsible_actor,        -- role-string fallback when no named person
    s.si_department_label,
    s.current_state AS order_current_state,
    b.eisb_url
FROM v_bill_statutory_instruments b
JOIN v_statutory_instruments s ON s.si_id = b.si_id
WHERE b.si_operation = 'commencement'
ORDER BY b.bill_id, b.si_signed_date ASC NULLS LAST, b.si_number;
