"""P0-4 — Election results (SCAFFOLD — needs a source decision).

STATUS as of 2026-06-28: not built. The official Electoral Commission site
(electoralcommission.ie/general-elections/) is thin on STRUCTURED count-level
downloads. Count-by-count data lives with third parties (single-maintainer
continuity risk) or must be assembled per constituency.

Candidate retrieval routes (pick + document one as primary, cross-check a second):
  * electoralcommission.ie — official; structured downloads limited.
  * electionsireland.org — comprehensive history; single-maintainer (continuity risk).
  * RTÉ / Irish Times results hubs — HTML, per-election.
  * Oireachtas API — members (elected TDs) but NOT count data.

Scope to match existing SIPO finance coverage first: GE2024 + GE2020.

Proposed gold schema: election_results(election_event, constituency, candidate_name,
candidate_norm, party, count_number, first_pref, transfers, status[elected|eliminated],
final_count, quota, turnout, seats, boundary_review, source_url, + provenance).

Join keys: election_event, constituency, candidate_norm->unique_member_code
(reuse the project's NFKD name normaliser — do NOT invent matching), party.
Derived view v_spend_per_vote joins SIPO finance — SEPARATE facts, joined never summed.

NOT auto-run: choosing a primary source + its licence is a human decision
(continuity + ToS). This stub is the schema + the shortlist, not a scraper.
"""
from __future__ import annotations


def run() -> None:
    print("Election results: SCAFFOLD only — primary source + licence not yet chosen.")
    print("Shortlist: electoralcommission.ie (official, thin) / electionsireland.org")
    print("(history, single-maintainer) / RTÉ + Irish Times hubs. Target GE2024+GE2020.")


if __name__ == "__main__":
    run()
