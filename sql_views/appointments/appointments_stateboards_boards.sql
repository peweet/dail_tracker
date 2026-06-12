-- v_stateboards_boards — the public-body universe: one row per state board in
-- the DPER register, with legal basis and gender-balance metadata.
-- Source: data/silver/parquet/stateboards_boards.parquet (produced by
-- extractors/stateboards_roster_extract.py from membership.stateboards.ie).
--
-- This is the body-universe spine behind Public-Body-Profile joins
-- (procurement publisher <-> C&AG-audited <-> AFS); join key = body name
-- (alias handling is the consumer's concern — names here are as published).
--
-- Grain: one row per board/body (~250).
CREATE OR REPLACE VIEW v_stateboards_boards AS
SELECT
    department,
    body,
    body_full,
    legal_basis,
    legal_basis_url,
    max_positions,
    gender_female_n,
    gender_male_n,
    gender_female_pct,
    gender_male_pct,
    members_listed,
    source_url
FROM read_parquet('data/silver/parquet/stateboards_boards.parquet')
ORDER BY department, body;
