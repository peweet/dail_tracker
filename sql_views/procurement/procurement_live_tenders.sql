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
SELECT
    title,
    buyer,                                -- display name, cleaned in the extractor (org id / roll number stripped)
    buyer_org_id,                         -- eTenders internal org id, lifted off the name: a stable per-buyer join key
    TRY_CAST(published_date AS DATE)                          AS published_date,
    TRY_CAST(deadline_date AS DATE)                           AS submission_deadline,
    DATE_DIFF('day', CURRENT_DATE, TRY_CAST(deadline_date AS DATE)) AS days_to_deadline,
    procedure,
    status,
    estimated_value_eur,                  -- buyer estimate, PLANNED tier, NEVER summed
    realisation_tier,                     -- 'PLANNED'
    value_kind,                           -- 'estimate_advertised'
    resource_id,
    detail_url,
    retrieved_utc
FROM read_parquet('data/silver/parquet/etenders_live_tenders.parquet')
WHERE feed = 'cft'
  AND TRY_CAST(deadline_date AS DATE) >= CURRENT_DATE      -- open now (closing in the future)
  AND TRY_CAST(deadline_date AS DATE) < CURRENT_DATE + INTERVAL 3 YEAR  -- exclude far-future DPS application windows
ORDER BY submission_deadline ASC;                         -- soonest-closing first

-- "Who is buying right now" — open opportunities by contracting authority (counts + an indicative
-- estimated-value floor that is PLANNED-tier and must be labelled as such, never as committed/paid).
CREATE OR REPLACE VIEW v_procurement_live_tenders_summary AS
SELECT
    buyer,
    COUNT(*)                                                 AS n_open_tenders,
    MIN(submission_deadline)                                 AS next_closing,
    COUNT(*) FILTER (WHERE days_to_deadline <= 14)           AS closing_within_14d,
    SUM(estimated_value_eur)                                 AS est_value_floor_eur  -- PLANNED estimates only; never "spend"
FROM v_procurement_live_tenders
GROUP BY buyer
ORDER BY n_open_tenders DESC;
