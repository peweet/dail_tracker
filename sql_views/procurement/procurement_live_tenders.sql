-- v_procurement_live_tenders / _summary — the LIVE national tender pipeline (open opportunities now
-- accepting bids), from the new eTenders platform (etenders.gov.ie). This is the forward-looking lane
-- that the OGP quarterly open-data CSV and TED (EU-threshold only) cannot give us — incl. sub-EU-threshold
-- national contracts (schools, councils, water schemes).
--
-- SOURCE: data/silver/parquet/etenders_live_tenders.parquet, produced by the Playwright extractor
-- extractors/etenders_live_tenders_extract.py (promoted from sandbox 2026-06-14; refreshed by
-- tools/poll_live_tenders.ps1). The view registers via the procurement_*.sql glob and degrades to empty
-- (swallow_errors) when the snapshot is absent — it does NOT touch the awards/payments registers.
--
-- VALUE SEMANTICS — estimated_value_eur is a BUYER ESTIMATE at the PLANNED (pre-award) lifecycle stage:
-- realisation_tier='PLANNED', value_kind='estimate_advertised'. It is a NEW tier EARLIER than AWARDED,
-- and is NEVER summed — not with eTenders/TED awards, not with payments. It is a planning indicator only.
--
-- SCOPE: the source 'cft' feed lists current opportunities back to platform launch (2023), including
-- already-closed and DPS/Qualification-System records. This view keeps only the GENUINELY OPEN set
-- (a parseable deadline in the future) and excludes the 'notice' feed (award/contract notices, no deadline).
CREATE OR REPLACE VIEW v_procurement_live_tenders AS
WITH parsed AS (
    SELECT
        *,
        CASE
            -- The Irish portal renders an explicit GMT/IST abbreviation. Replace it with the
            -- corresponding numeric offset before parsing so DuckDB returns an honest instant
            -- instead of silently treating the source's local clock as UTC. The original source
            -- string remains available as deadline_raw.
            WHEN contains(deadline_raw, ' IST ') THEN TRY_STRPTIME(
                replace(deadline_raw, ' IST ', ' +0100 '),
                '%a %b %d %H:%M:%S %z %Y'
            )
            WHEN contains(deadline_raw, ' GMT ') THEN TRY_STRPTIME(
                replace(deadline_raw, ' GMT ', ' +0000 '),
                '%a %b %d %H:%M:%S %z %Y'
            )
            ELSE NULL
        END AS deadline_at_parsed
    FROM read_parquet('data/silver/parquet/etenders_live_tenders.parquet')
    WHERE feed = 'cft'
)
SELECT
    title,
    buyer,                                -- display name, cleaned in the extractor (org id / roll number stripped)
    buyer_org_id,                         -- eTenders internal org id, lifted off the name: a stable per-buyer join key
    TRY_CAST(published_date AS DATE)                          AS published_date,
    TRY_CAST(deadline_date AS DATE)                           AS submission_deadline,
    deadline_at_parsed                                       AS submission_deadline_at,
    deadline_raw,
    CASE WHEN deadline_at_parsed IS NOT NULL THEN 'Europe/Dublin' END AS deadline_timezone,
    regexp_extract(deadline_raw, ' (IST|GMT) ', 1)           AS deadline_timezone_abbreviation,
    CASE
        WHEN deadline_at_parsed IS NOT NULL THEN 'SOURCE_INSTANT'
        WHEN regexp_matches(COALESCE(deadline_raw, ''), '(^|[ T])[0-9]{1,2}:[0-9]{2}') THEN 'PARSE_FAILED'
        WHEN TRY_CAST(deadline_date AS DATE) IS NOT NULL THEN 'SOURCE_DATE_ONLY'
        ELSE 'NOT_STATED'
    END                                                       AS deadline_precision,
    DATE_DIFF('day', CURRENT_DATE, TRY_CAST(deadline_date AS DATE)) AS days_to_deadline,
    procedure,
    status,
    estimated_value_eur,                  -- buyer estimate, PLANNED tier, NEVER summed
    realisation_tier,                     -- 'PLANNED'
    value_kind,                           -- 'estimate_advertised'
    resource_id,
    detail_url,
    cpv_code,                             -- 8-digit CPV from the detail-page pass; NULL when that pass was skipped/capped
    cpv_division,                         -- 2-digit CPV division label, the sector facet key
    retrieved_utc
FROM parsed
WHERE (
        deadline_at_parsed > CURRENT_TIMESTAMP
        OR (
            deadline_at_parsed IS NULL
            AND NOT regexp_matches(COALESCE(deadline_raw, ''), '(^|[ T])[0-9]{1,2}:[0-9]{2}')
            AND TRY_CAST(deadline_date AS DATE) >= CURRENT_DATE
        )
    )                                                        -- exact instants close at the source-stated time
  AND TRY_CAST(deadline_date AS DATE) < CURRENT_DATE + INTERVAL 3 YEAR  -- exclude far-future DPS application windows
ORDER BY submission_deadline ASC, submission_deadline_at ASC; -- soonest-closing first

-- "Who is buying right now" — open opportunities by contracting authority. Planned estimates
-- remain on individual notices and are never aggregated.
CREATE OR REPLACE VIEW v_procurement_live_tenders_summary AS
SELECT
    buyer,
    COUNT(*)                                                 AS n_open_tenders,
    MIN(submission_deadline)                                 AS next_closing,
    MIN(submission_deadline_at)                              AS next_closing_at,
    COUNT(*) FILTER (WHERE days_to_deadline <= 14)           AS closing_within_14d,
    COUNT(*) FILTER (WHERE estimated_value_eur IS NOT NULL)  AS n_with_estimate
FROM v_procurement_live_tenders
GROUP BY buyer
ORDER BY n_open_tenders DESC;
