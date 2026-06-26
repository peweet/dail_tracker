-- v_la_standing_orders — how each council formulates its agenda + takes votes, parsed
-- VERBATIM from the council's adopted Standing Orders PDF: order_of_business (the agenda
-- template, ` | `-delimited), notice_of_motion (how councillors table an item), voting, quorum,
-- and records_named_votes (the structural reason named voting records exist for some councils).
-- Parsed for ~8/31 councils (rest: SO document not located → page shows a generic explainer).
-- Source: data/_meta/la_standing_orders.csv.
CREATE OR REPLACE VIEW v_la_standing_orders AS
SELECT local_authority, order_of_business, notice_of_motion, voting, quorum,
       records_named_votes, source_url
FROM read_csv('data/_meta/la_standing_orders.csv', header = true, AUTO_DETECT = true);
