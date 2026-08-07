-- v_constituency_housing_context_with_ssha -- one row per serving council,
-- combining the already-registered supply and SSHA-demand context views.  This
-- replaces the former pandas merge in the core query layer and preserves the
-- council-area grain (never apportioned to a constituency).
CREATE OR REPLACE VIEW v_constituency_housing_context_with_ssha AS
SELECT
    ctx.*,
    ssha.waiting_total_2025,
    ssha.waiting_yoy_pct,
    ssha.long_wait_pct,
    ssha.over_7yr_pct
FROM v_constituency_housing_context ctx
LEFT JOIN v_constituency_ssha_waiting_list ssha
  ON ssha.constituency_name = ctx.constituency_name
 AND ssha.local_authority = ctx.local_authority
 AND ssha.link_type = ctx.link_type;
