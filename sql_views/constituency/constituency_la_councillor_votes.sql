-- v_la_councillor_votes — per-councillor NAMED roll-call votes, extracted from council
-- minutes. Only exists where a council records votes by name (Carlow). Most councils decide
-- by agreement (proposer/seconder) and publish no named tally. Source:
-- data/_meta/la_councillor_votes.csv. vote ∈ {for, against, abstain, absent}.
CREATE OR REPLACE VIEW v_la_councillor_votes AS
SELECT local_authority, member, meeting_date, motion, vote
FROM read_csv('data/_meta/la_councillor_votes.csv', header = true, AUTO_DETECT = true);
