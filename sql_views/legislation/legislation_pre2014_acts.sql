-- v_legislation_pre2014_acts — curated table of synthetic 'act_<year>_<slug>'
-- entries used as enabling-act references for SIs whose primary Act predates
-- the Oireachtas bill index (pre-2014). One row per canonical_bill_id.
--
-- Source: data/_meta/pre2014_acts.csv (manually curated by domain editor;
-- match_residue is the keyword fragment used by the SI matcher to associate
-- raw enabling-act strings with a canonical ID).
--
-- File name starts with 'legislation_' so legislation_data.py's
-- get_legislation_conn() glob picks it up.

CREATE OR REPLACE VIEW v_legislation_pre2014_acts AS
SELECT DISTINCT
    canonical_bill_id,
    act_short_title,
    CAST(act_year AS INTEGER)  AS act_year,
    policy_domain
FROM read_csv_auto('data/_meta/pre2014_acts.csv', header=true)
WHERE canonical_bill_id IS NOT NULL;
