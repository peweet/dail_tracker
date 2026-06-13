-- Per-CANDIDATE GE2024 SIPO Election-Expenses views (the granular tier — individual
-- candidate spend down to each line item, e.g. Noel Grealish -> "Galway Advertiser"
-- EUR 2,799.48). DISTINCT from the party National-Agent tier in sipo_expenses_base.sql.
--
-- Reads the gold parquets produced by:
--   extractors/sipo_candidate_expenses_extract.py   (silver parse, no OCR)
--   extractors/sipo_candidate_expenses_aggregate.py (gold facts, vectorised polars)
--
-- These are ELECTION EXPENSES (money spent campaigning) for the 2024 general election
-- (34th Dáil) only — no year axis. OCR-derived from the official SIPO scanned returns:
--   * `reconciles` = Σ(5A-5H categories) == overall total on the page (a checksum).
--   * `min_confidence` / `item_confidence` carry per-cell OCR confidence.
-- The UI MUST surface a "verify against the official SIPO PDF" caveat and must NOT imply
-- influence from any figure (no-inference rule). Decimal-loss OCR mis-reads (a figure >
-- the statutory cap) are FLAGGED `*_suspect` in silver and ALREADY EXCLUDED from these
-- gold parquets — so every row here is within-cap, never a fabricated magnitude.
-- Provenance / statutory limits: data/_meta/sipo_ge2024_expenses_sources.md.


-- v_sipo_candidate_expenses — candidate grain (one row per candidate statement).
CREATE OR REPLACE VIEW v_sipo_candidate_expenses AS
SELECT
    election_event,
    candidate_name,
    constituency_name,
    party,            -- authoritative: registry party for elected TDs, else OCR-declared canonical
    party_declared,   -- raw OCR'd "declared party" field, kept for audit
    unique_member_code, -- canonical member ID for cross-linking (NULL = not a sitting TD)
    is_elected_td,
    total_spend_eur,
    spend_not_public_eur,
    spend_public_eur,
    cat_5A_eur, cat_5B_eur, cat_5C_eur, cat_5D_eur,
    cat_5E_eur, cat_5F_eur, cat_5G_eur, cat_5H_eur,
    reconciles,
    (COALESCE(reconciles, FALSE) = FALSE) AS needs_verify,
    min_confidence,
    ocr_complete,
    source_pdf_url
FROM read_parquet('data/gold/parquet/sipo_candidate_expenses_fact.parquet')
ORDER BY total_spend_eur DESC;


-- v_sipo_candidate_expenses_filed_unquantified — candidates who FILED a 2024 expenses
-- statement but for whom no trustworthy total can be shown: either the form's total cell
-- was blank/unreadable (`no_total_declared`), or the only figure is an OCR decimal-loss
-- artefact above the statutory cap (`figures_unreadable`). Carries NO amount by design —
-- showing the corrupt magnitude would be a fabricated number, and a blank total is never
-- asserted to be €0 (a genuine €0 nil-return parses cleanly and appears in the main view).
-- The page lists these as searchable "also filed" entries that link to the official PDF.
CREATE OR REPLACE VIEW v_sipo_candidate_expenses_filed_unquantified AS
SELECT
    election_event,
    candidate_name,
    constituency_name,
    party,
    party_declared,
    unique_member_code,
    is_elected_td,
    filed_status,        -- 'no_total_declared' | 'figures_unreadable'
    ocr_complete,
    source_pdf_url
FROM read_parquet('data/gold/parquet/sipo_candidate_expenses_unquantified.parquet')
ORDER BY constituency_name, candidate_name;


-- v_sipo_candidate_expense_items — line-item grain. `detail` is the SIPO form's free-text
-- "Details of item" column: a MIX of supplier names ("Galway Advertiser") and item
-- descriptions ("Posters", "Meta ads"). It is NOT a clean vendor field — never present
-- it as a payee/vendor without that caveat (no-inference).
CREATE OR REPLACE VIEW v_sipo_candidate_expense_items AS
SELECT
    candidate_name,
    constituency_name,
    party,
    unique_member_code,   -- cross-link a line item to a sitting TD (NULL if not elected)
    category,
    category_label,
    ref,
    detail,
    cost_eur,
    item_confidence,
    source_page
FROM read_parquet('data/gold/parquet/sipo_candidate_expense_items.parquet')
ORDER BY cost_eur DESC;


-- v_sipo_candidate_expenses_by_party — party-level rollup of candidate totals for the
-- lens cards (aggregation lives in the pipeline view, not the Streamlit layer). Groups on
-- the CANONICAL party; NULL party = the OCR'd field was a form placeholder / mis-grabbed
-- cell / unmatched (kept as one bucket, never guessed into a party).
CREATE OR REPLACE VIEW v_sipo_candidate_expenses_by_party AS
SELECT
    election_event,
    party,
    COUNT(*)                                            AS candidate_count,
    SUM(total_spend_eur)                                AS total_spend,
    AVG(total_spend_eur)                                AS mean_spend,
    MEDIAN(total_spend_eur)                             AS median_spend,
    MAX(total_spend_eur)                                AS max_spend,
    SUM(CASE WHEN needs_verify THEN 1 ELSE 0 END)       AS verify_count
FROM v_sipo_candidate_expenses
GROUP BY election_event, party
ORDER BY total_spend DESC;


-- v_sipo_candidate_expenses_by_category — spend split across the 8 statutory categories
-- (5A Advertising … 5H Campaign Workers), summed from the line items.
CREATE OR REPLACE VIEW v_sipo_candidate_expenses_by_category AS
SELECT
    category,
    category_label,
    COUNT(*)                          AS item_count,
    SUM(cost_eur)                     AS total_spend,
    COUNT(DISTINCT candidate_name)    AS candidate_count
FROM v_sipo_candidate_expense_items
GROUP BY category, category_label
ORDER BY category;


-- v_sipo_candidate_top_details — the campaign-spend graph: where the money went, by the
-- free-text detail line (payees + descriptions, see the caveat above). Drops the noisiest
-- 1-2 char OCR fragments.
CREATE OR REPLACE VIEW v_sipo_candidate_top_details AS
SELECT
    detail,
    SUM(cost_eur)                     AS total_spend,
    COUNT(*)                          AS item_count,
    COUNT(DISTINCT candidate_name)    AS candidate_count
FROM v_sipo_candidate_expense_items
WHERE detail IS NOT NULL AND LENGTH(detail) > 2
GROUP BY detail
ORDER BY total_spend DESC;
