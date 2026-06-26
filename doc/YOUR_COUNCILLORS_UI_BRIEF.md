# Your Councillors — UI design brief

Status: **design brief (shape). No code, no gold.** Companion to `doc/YOUR_COUNCILLORS_PLAN.md`
(data/pipeline) and the corpus in `pipeline_sandbox/council_minutes/`.

## 1. User question this page answers
"**Who are my local councillors, what do they actually decide, what are they paid — and, where it
exists, how did mine vote?**" Distinct from `constituency.py` (your *TDs*) and `local_government.py`
(your *Chief Executive*). This is the elected **reserved-functions** tier.

## 2. Greenfield — no existing page
New page. Adjacent references (functional, not visual): `local_government.py` ("Who runs your county",
LA-keyed `?la=`), `constituency.py` (Dáil-constituency dossier). Reuse their card system, don't copy
their content.

## 3. Page placement & cross-linking (IA)
**Its own page** `your-councillors` in the **Your Area** nav group. Bidirectional links:
- `local_government.py` council dossier → "Your councillors (N) →" entry into this page pre-filtered to that LA's LEAs.
- `constituency.py` → "Councillors serving this area →" (constituency→LA→LEA via the crosswalk).
- Councillor dossier → back-links to the council (CE/accountability) and the Dáil constituency.
Rationale: ~949 councillor dossiers are too much to bolt onto the council page, but they are the
reserved-functions counterpart to the CE page — so: separate page, tightly woven, never isolated.

## 4. Bold layout — three screens

**Screen 1 — Picker (index).**
- Hero: "Who represents you on your local council?" + one sentence on reserved functions.
- Controls: `County [▼]` → `Local Electoral Area [▼]` (dependent select), CTA "Show my councillors →".
- No Eircode (licensed). Optional later: reuse the council choropleth as a visual picker.

**Screen 2 — LEA roster (`?county=&lea=`).**
- Header strip: "{N} councillors · {Council} · {LEA}" + **party-composition stripe** (`party_stripe_html`).
- Grid of councillor cards (reuse `ranked_member_card` shape): party stripe · name · party · status
  (sitting / co-opted) · "→". No photos.
- Card block "**What your councillors decide**": reserved vs executive (adopt Development Plan, set
  budget + commercial rates + LPT factor, appoint CE) → link to "Who runs your county".

**Screen 3 — Councillor dossier (`?councillor=`).**
- Hero: party stripe · name · party · LEA · council · status.
- **Voting record** card (the differentiator) — recent named roll-call votes: date · motion · how they
  voted (For/Against/Abstain) · result (Carried/Defeated) · source link. Tier-gated (see §8).
- **Meeting activity** card: motions proposed/seconded, attendance (lg-* stat rows).
- **Pay & allowances** card: schedule rows + "entitlement, not actual earnings" caveat (+ S142 actuals
  where the council publishes them).
- Provenance footer.

## 5. Interaction model
Two-stage, matching the project's member flow. Soft `?county=`/`?lea=`/`?councillor=` reruns via
`spa_links` (no full reload). `session_state` survives tile clicks (mind the known trap). Back-buttons
via `back_button`.

## 6. Temporal behaviour
Light. Council term 2024–2029 — no long time series. Voting record is **reverse-chronological recent
votes** with an optional **year pill** (2024/2025/2026) once a councillor accrues many. Pay schedule is
current-year with `effective_from`. No charts-over-time.

## 7. Source-link behaviour
Provenance footer **present** (no omission). Roster → Wikipedia + council; **each vote/meeting row →
its source minutes PDF/HTML** (per-record provenance, like payments rows); pay → DHLGH directions.
Honesty captions are first-class, not footnotes (entitlement vs actual; named-vote availability).

## 8. Chart & table strategy + empty states (the honest-degradation core)
No `st.dataframe` on primary views. Party composition = the stripe, not a chart. Voting record = card
list (a "see all votes" expander may use a secondary table). **Per-council-tier states for the Voting
record card** (this is the make-or-break design element):

- **Tier A — roll-call council (Carlow-class), data present:** real rows.
  Empty (no votes yet this term): *"No recorded roll-call votes for {name} yet this term."*
- **Tier B — proposer/seconder council (Galway/Kerry/Monaghan…):**
  *"{Council} records most decisions by agreement rather than by named vote, so individual councillor
  votes aren't published in its minutes. Showing motions proposed and seconded instead."* → fall back to
  the Meeting-activity card.
- **Tier C — minutes not yet processed (Galway/Louth scanned; Dublin City/DLR ModernGov):**
  *"{Council}'s minutes haven't been processed yet — meeting activity isn't available for {name}."*
  (Never implies zero activity.)
- **Roster empty (LEA picked, no data):** *"We don't have a councillor roster for {LEA} yet."*
- **Pay actuals absent (26 of 31 councils):** show the schedule; caption *"This council doesn't publish
  individual expense records as open data."*

## 9. Design differentiators
- **Honest per-council voting states** — the same card renders A/B/C truthfully; this is the integrity
  signature (`feedback_no_inference_in_app`), the way payments pages caption grain.
- **Reserved-vs-executive framing** — pairs with the CE page to complete "who holds local power".
- **Text-forward identity** (no photos): party stripe + name + LEA carry recognition.
- **Per-vote provenance** — every named vote deep-links its minutes source.

## 10. TODO_PIPELINE_VIEW_REQUIRED (nothing in gold yet — these gate the build)
- `v_la_councillors` — per-LEA roster (name, party, LA, LEA, seats, status, since_date, source_url).
- `lea ↔ local_authority ↔ constituency` crosswalk (for cross-linking + the County→LEA picker).
- `v_la_councillor_pay` — national pay/allowance schedule (display reference; not per-person).
- `v_la_councillor_votes` — per-member named votes (council, meeting_date, motion, member, vote,
  result, source_url) — **roll-call councils only**; carries a `vote_style` flag per council.
- `v_la_councillor_activity` — motions proposed/seconded + attendance per member (where minutes clean).
- `v_la_council_minutes_coverage` — per-council tier flag (clean / scanned-pending / cmis-pending /
  proposer-seconder) so the UI can pick the right empty state deterministically.
- `v_la_councillor_expenses` — S142 actuals where published (~5 councils), `has_open_data` flag.
All currently sandbox (`pipeline_sandbox/council_minutes/`): roster not built; votes = Carlow only.

## 11. Implementation plan (when promoted — not now)
- `utility/pages_code/your_councillors.py` (thin renderer, card-based, index→roster→dossier).
- `utility/data_access/your_councillors_data.py` (cache wrappers; reuse `get_constituency_conn`).
- `dail_tracker_core/queries/your_councillors.py` (display-only queries over the v_la_* views).
- `sql_views/constituency/constituency_la_councillors*.sql` (+ pay/votes/activity/coverage views;
  register AFTER any view they JOIN — dependency-order tripwire).
- `utility/app.py` — nav entry under Your Area (`url_path="your-councillors"`).
- `utility/ui/entity_links.py` — add `councillor_profile_url`, `your_councillors_url(la/lea)`; wire the
  reciprocal links into `local_government.py` + `constituency.py`.
- CSS: reuse `con-*` / `lg-*` + `party_stripe_html` / `ranked_member_card`; add a small `cl-vote-row`
  class (date · motion · For/Against/Abstain chip · result) in `shared_css.py`. `#ffffff` card bg.
- Phase 1 ships the ~15 clean councils (roster + pay + responsibilities + meeting activity; voting for
  Carlow). Phase 2 = off-box OCR (Galway/Louth) + ModernGov scraper (Dublin City/DLR) + corrected seeds
  (~11 councils) + voting expansion (Kilkenny/Laois/South Dublin/Fingal) — each lights up its tier as it lands.
