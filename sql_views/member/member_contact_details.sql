-- v_member_contact_details — official office contact details per Oireachtas member.
-- Source: data/silver/parquet/member_contact_details.parquet
--         (produced by extractors/member_contact_extract.py — scraped from each
--          member's oireachtas.ie profile page, keyed on unique_member_code).
--
-- The Oireachtas members API exposes no contact fields; the profile page is the
-- only official publisher of a member's office address, phone and @oireachtas.ie
-- email. Every column except the join key is nullable — newly-elected members
-- commonly show only the Leinster House address, and a handful of stale/former
-- codes have no contact block at all. The UI renders only the rows/fields that
-- are populated and never imputes a missing value.

CREATE OR REPLACE VIEW v_member_contact_details AS
SELECT
    unique_member_code,
    address,
    phone_primary,
    phone_all,
    email,
    website_url,
    profile_url,
    source_url,
    scraped_date
FROM read_parquet('{CONTACT_DETAILS_PARQUET_PATH}')
WHERE unique_member_code IS NOT NULL;
