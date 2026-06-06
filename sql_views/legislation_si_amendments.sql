-- v_si_amendments — the SI→SI amendment / revocation graph as directed edges.
-- One row per (amending SI → affected SI) relationship. Derived purely by
-- INVERTING the affecting_sis lists that si_current_state records on the
-- *affected* side, so the page can finally show BOTH directions from one view:
--   "This SI was {revoked|amended} BY …"  → rows WHERE affected_(year,number)=X
--   "This SI {revokes|amends} …"          → rows WHERE amender_(year,number)=X
--
-- Source: data/gold/parquet/si_current_state.parquet (eISB Legislation Directory,
-- via extractors/si_legislation_directory_extract.py) + titles from
-- statutory_instruments.parquet. No new extractor — this is a relational view.
--
-- SCOPE — clean states only. We include exactly the states where eISB names a
-- definite affecting instrument with one clean effect:
--   revoked, partially_revoked, amended, amended_and_partially_revoked.
-- We DELIBERATELY EXCLUDE 'other_affected' — those rows ("rendered obsolete by
-- revocation of X", "references … construed") carry MIXED/INDIRECT references in
-- affecting_sis (a mentioned SI plus the affecting one), so inverting them would
-- assert amendment relationships that do not exist. All 26 multi-affecting SIs in
-- the data are 'other_affected'; the clean states are 100% single-affecting, so
-- per-edge effect = the affected SI's current_state (no ambiguity).
--
-- This is SI→SI only. SI→Act textual amendments (LRC Revised Acts F/C/E notes)
-- are a separate, deferred source and are NOT in scope here.
--
-- Loads via the legislation_* glob in get_legislation_conn(). It reads parquet
-- directly (not other views), so its alphabetical position does not matter.

CREATE OR REPLACE VIEW v_si_amendments AS
WITH affected AS (
    SELECT
        si_year                                   AS affected_year,
        si_number                                 AS affected_number,
        current_state,
        this_si_eli_url                           AS affected_eli_url,
        -- the part before '||' is the provision-level effect on the affected SI,
        -- e.g. "Reg. 2 amended" / "Sch., pt. B amended" / "Revoked"
        split_part(how_affected_raw, ' || ', 1)   AS provision_note,
        confidence,
        affecting_sis,
        affecting_si_urls
    FROM read_parquet('data/gold/parquet/si_current_state.parquet')
    WHERE current_state IN (
            'revoked', 'partially_revoked', 'amended', 'amended_and_partially_revoked'
          )
      AND affecting_sis IS NOT NULL
      AND len(affecting_sis) > 0
),
edges AS (
    SELECT
        affected_year,
        affected_number,
        affected_eli_url,
        current_state,
        CASE current_state
            WHEN 'revoked'                       THEN 'revokes'
            WHEN 'partially_revoked'             THEN 'partially revokes'
            WHEN 'amended'                       THEN 'amends'
            WHEN 'amended_and_partially_revoked' THEN 'amends and partially revokes'
        END                                       AS effect,
        provision_note,
        confidence,
        -- positional zip of the two parallel lists (clean states are 1-element)
        unnest(affecting_sis)                     AS amender_key,
        unnest(affecting_si_urls)                 AS amender_eli_url
    FROM affected
)
SELECT
    -- amending SI (the "this SI amends …" side)
    CAST(regexp_extract(e.amender_key, '^([0-9]+)/', 1) AS INTEGER) AS amender_number,
    CAST(regexp_extract(e.amender_key, '/([0-9]+)$', 1) AS INTEGER) AS amender_year,
    amender.si_title                              AS amender_title,
    e.amender_eli_url,
    -- the relationship
    e.effect,
    e.current_state,
    e.provision_note,
    e.confidence,
    -- affected SI (the "this SI was amended by …" side)
    e.affected_number,
    e.affected_year,
    affected_si.si_title                          AS affected_title,
    e.affected_eli_url
FROM edges e
LEFT JOIN read_parquet('data/gold/parquet/statutory_instruments.parquet') amender
       ON amender.si_number = CAST(regexp_extract(e.amender_key, '^([0-9]+)/', 1) AS INTEGER)
      AND amender.si_year   = CAST(regexp_extract(e.amender_key, '/([0-9]+)$', 1) AS INTEGER)
LEFT JOIN read_parquet('data/gold/parquet/statutory_instruments.parquet') affected_si
       ON affected_si.si_number = e.affected_number
      AND affected_si.si_year   = e.affected_year;
