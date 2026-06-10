---
name: project_election_2024_hub
description: GE2024 SIPO finance consolidated into one "Election 2024" page; donations/party-spend lenses REMOVED from Payments page
metadata:
  type: project
---

Shipped 2026-06-10: unified **Election 2024** hub (`utility/pages_code/election_2024.py`, url_path kept `rankings-election-spending`). It absorbs the THREE GE2024 SIPO surfaces that were previously scattered:
- **Donations** (was a lens on the Payments page) — `v_sipo_donations*`
- **Party spending** = national-agent per-candidate expenses (was the "Election expenses" lens on Payments) — `v_sipo_expenses*`
- **Candidates** = per-candidate own statements + Part-5 line items (was the standalone "Election Spending" page) — `v_sipo_candidate_*`

Tab strip via `?view=overview|donations|party|candidates`; drill params `?dparty`/`?eparty`/`?cand` take precedence in the router. Overview = a "money map" (3 big totals) + per-party "full picture" cards (3 aligned stream bars).

**Consequences (stale-doc traps):**
- The **Payments page** (`payments.py`, nav renamed "Payments & Donations" → **"Payments"**) is now PURELY parliamentary member payments — its segmented-control lens + `?dparty`/`?eparty` routing are GONE. Old audit memories that say "donations lens on Payments" are stale.
- `election_spending.py` was **deleted** (git rm); use `election_2024.py`.

**Keystone view:** `v_sipo_ge2024_party_finance` (`sql_views/sipo/sipo_ge2024_party_finance.sql`) FULL-picture per-party join of the three rollups via a party-spine + LEFT JOINs. File named `ge2024_*` deliberately so it sorts AFTER `sipo_candidate/donations/expenses_*.sql` in the alphabetical glob (dependency-order rule). ⚠ The 3 money columns are DIFFERENT grains and OVERLAP (agent vs candidate spend) — NEVER summed; the money-map copy says so explicitly. NaN stream = "—", never 0. Relates to [[reference_data_map]] 3-money-grain rule and [[project_sipo_candidate_expenses_corpus]].
