-- v_corporate_brand_aliases — curated brand → parent_fund → fund_type alias map.
-- Used by the Corporate page methodology expander to make the panel's
-- brand-to-parent classification provenance visible (e.g. Beltany → Goldman
-- Sachs) without the reader inspecting the raw CSV.
--
-- Source: data/_meta/loan_book_fund_aliases.csv (manually curated by domain
-- editor). The `notes` column is OPTIONAL — a leaner alias file carrying only
-- brand/parent_fund/fund_type is valid, so this selects * rather than
-- enumerating columns (the page guards on `if "notes" in aliases.columns`).
--
-- File name starts with 'corporate_' so corporate_data.py's
-- get_corporate_conn() glob (corporate_*.sql) picks it up.

CREATE OR REPLACE VIEW v_corporate_brand_aliases AS
SELECT * FROM read_csv_auto('data/_meta/loan_book_fund_aliases.csv', header=true);
