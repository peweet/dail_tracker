-- v_corporate_isif_portfolio — Ireland Strategic Investment Fund (ISIF) portfolio,
--   one row per public investment commitment by the sovereign development fund.
--
-- Source: data/gold/parquet/isif_portfolio.parquet
--   produced by extractors/enrichment_promote_to_gold.py (transform-only promotion of the
--   vetted sandbox scrape pipeline_sandbox/isif_portfolio_extract.py — the public
--   https://isif.ie/portfolio cards). Investees are companies / funds; no natural-person data.
--
-- VALUE SEMANTICS (non-negotiable): amount_stated is an investment COMMITMENT
--   (realisation_tier=COMMITTED), parsed best-effort from the card's lead sentence in MIXED
--   currencies (amount_currency ∈ EUR/USD/GBP) and sometimes an "up to" cap (amount_is_up_to).
--   value_safe_to_sum is FALSE on every row — NEVER SUM amount_stated and never union it with
--   procurement awards or payment facts. Present individual commitments only.
CREATE OR REPLACE VIEW v_corporate_isif_portfolio AS
SELECT
    investee_name,
    commitment_date,
    commitment_year_label,
    description,
    amount_stated,
    amount_currency,
    amount_is_up_to,
    value_kind,
    realisation_tier,
    value_safe_to_sum,
    source_url,
    ingested_date
FROM read_parquet('data/gold/parquet/isif_portfolio.parquet')
WHERE investee_name IS NOT NULL;
