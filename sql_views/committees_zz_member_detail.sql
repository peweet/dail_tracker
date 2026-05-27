-- v_committee_member_detail — per-committee rollup of seats, party mix and
-- chair identification. Replaces the in-page _committee_summary() function
-- in committees.py (pandas groupby + chair lookup + party_seats lookup).
--
-- One row per (chamber, committee). party_seats is a JSON-encoded list of
-- (party, seat_count) pairs ordered by seat count desc — exposed as VARCHAR
-- so the page's pd-side renderer can json.loads() and iterate. Embedding
-- a struct list keeps the view a single retrieval-friendly call (the page
-- does not need a separate query per committee).
--
-- Sorted (status ASC, members DESC) to match the page's display order.
--
-- Depends on v_committee_assignments — file name uses the 'zz_' prefix so it
-- registers after committees_assignments.sql (alphabetical glob order).

CREATE OR REPLACE VIEW v_committee_member_detail AS
WITH base AS (
    SELECT
        chamber,
        committee,
        party,
        is_chair,
        name,
        status,
        type,
        committee_url
    FROM v_committee_assignments
),
agg AS (
    SELECT
        chamber,
        committee,
        COUNT(*)                                AS members,
        COUNT(DISTINCT party)                   AS parties,
        SUM(CASE WHEN is_chair THEN 1 ELSE 0 END) AS chairs,
        ANY_VALUE(status)                       AS status,
        ANY_VALUE(type)                         AS type,
        ANY_VALUE(committee_url)                AS url
    FROM base
    GROUP BY chamber, committee
),
chairs AS (
    SELECT
        chamber, committee,
        ANY_VALUE(name)  AS chair_name,
        ANY_VALUE(party) AS chair_party
    FROM base
    WHERE is_chair
    GROUP BY chamber, committee
),
party_seats_long AS (
    SELECT
        chamber, committee, party,
        COUNT(*) AS seat_count
    FROM base
    GROUP BY chamber, committee, party
),
party_seats_collected AS (
    SELECT
        chamber, committee,
        list(struct_pack(party := party, seats := seat_count)
             ORDER BY seat_count DESC, party ASC) AS party_seats_struct
    FROM party_seats_long
    GROUP BY chamber, committee
)
SELECT
    a.chamber,
    a.committee,
    a.members,
    a.parties,
    a.chairs,
    a.status,
    a.type,
    a.url,
    COALESCE(c.chair_name,  '')                AS chair_name,
    COALESCE(c.chair_party, '')                AS chair_party,
    -- DuckDB renders nested STRUCTs as a JSON-ish string when cast to VARCHAR;
    -- to_json() gives a guaranteed JSON array of {party, seats} dicts that the
    -- page can json.loads() on the consumer side.
    to_json(ps.party_seats_struct)             AS party_seats_json
FROM agg a
LEFT JOIN chairs c
       ON a.chamber = c.chamber AND a.committee = c.committee
LEFT JOIN party_seats_collected ps
       ON a.chamber = ps.chamber AND a.committee = ps.committee
ORDER BY a.chamber, a.status ASC, a.members DESC;
