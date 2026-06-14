# Council Spending lens — design brief

_Shape brief, 2026-06-14. Placement decision (updated during build): a **standalone
"Council Spending" page** under "The Money" nav group — promoted from the original
"Procurement 5th tab" plan once we confirmed the per-council dossier surfaces AFS
(whole-council audited budget), which is local-government finance, broader than
procurement. The index + dossier renderers stay in `procurement.py` (co-located with their
views/helpers); the new page `pages_code/council_spending.py` is a thin shell that sets the
hero and dispatches the `?paid_publisher=` drill-down to the shared dossier renderer._

_Status: **BUILT** 2026-06-14 — view + query + page wired; lint, logic-firewall, 313 tests
green. Live visual check still outstanding._

## 1. User question this answers

_"How much does my county/city council spend, who does it pay, and how does that
compare to its audited accounts?"_ — a single, local, civic-accountability entry
point, in the spirit of theyworkforyou's "your area."

## 2. Current UI problems (not greenfield)

The data and render logic all exist in `utility/pages_code/procurement.py`, but council
spend is fragmented across three disconnected places:

- **Buried behind a toggle.** Councils are only surfaced _as councils_ via the
  `Local authorities only` toggle inside "Who actually gets paid?" → "Top public
  bodies" (`procurement.py:638`). Three deliberate steps to find it.
- **Dumped into a generic list.** "Who wins contracts?" → "By authority"
  (`procurement.py:347`) mixes councils into one free-text "Contracting Authority"
  ranking with departments, HSE and universities — no council flag
  (`v_procurement_authority_summary` has no `publisher_type`).
- **The richest council lens is invisible until you arrive.** The per-council dossier
  (both spend tiers side by side, spend-by-year, full AFS audited-accounts context:
  revenue by year, by service division, traceability %) already exists at
  `procurement.py:669-803` but is only reachable by clicking a council _after_ finding
  the toggle.

The asset is good; the **wayfinding** is the failure.

## 3. Bold redesigned layout

A new **"Your council"** tab (5th, beside the four question-phrased tabs). Two states:

**A — Council index (default)**
- One-line factual lede (counts from the view; no inference).
- Councils ranked, **grouped by region** (Connacht / Leinster / Munster / Ulster /
  Dublin bands). Card = council name + meta (`N suppliers · 2016–2026`) + **two value
  pills side by side**: `€X ordered` and `€Y paid` (whichever tiers it publishes —
  never one summed figure). Whole card links into the existing dossier.
- Region grouping is the bold move: turns a flat ranking into a map of the country and
  lets a reader find _their_ council fast.

**B — Council dossier (drill-down)** — reuses `_render_payments_publisher_profile`
(`procurement.py:737`) verbatim: `LOCAL AUTHORITY` kicker, both tier pills, spend-by-year
spine, AFS accounts block, supplier list. No change needed beyond linking to it.

## 4. Interaction model

- **Primary view:** region-grouped council index.
- **Detail view:** existing dossier via `?paid_publisher=<name>&paid_tier=<tier>` — route
  already exists (`procurement.py:2253`). Zero new routing.
- **Tier:** index shows both tier pills per council (no toggle — both stages at a
  glance). Dossier keeps its own tier control for the supplier list.
- **Flow:** Procurement → "Your council" → region/council → dossier → back. One hop.

## 5. Temporal behaviour

- **Index: all-time, no year pills.** Councils publish over different windows; a year
  filter on the ranking would imply cross-council comparability the data doesn't support.
- **Dossier: per-year spine retained** (`procurement.py:795-798` + AFS by-year chart) —
  temporal lives at the council level where it's honest.

## 6. Source-link behaviour

Reuse the existing payments footer rails (`_PAY_FOOT_HTML`, `procurement.py:828`):
Circular 07/2012 / FOI over-€20,000 disclosures, CRO-matched, "ordered and paid are
different stages and never summed; never summed across bodies; never added to award
values." AFS block carries its own "audited accounts — a separate measure, never added"
provenance (`procurement.py:686-693`). No new provenance section.

## 7. Chart and table strategy

- **Index:** ranked cards, no chart (a bar chart across councils with different
  publication windows would mislead — deliberately omitted).
- **Dossier (reused as-is):** brown bar = PO/payment spend per year; teal `#3a6b7e` bar
  = audited AFS revenue per year (distinct colour = distinct grain); by-division as
  cards. Keep.
- No `st.dataframe` anywhere (cards-only convention).

## 8. Empty-state copy

- **Source failure:** "Council spending data isn't available right now — the public-body
  payment views couldn't be loaded. A source/pipeline issue, not an empty result."
- **Region with no publishing councils:** render no band (omit silently).
- **Council dossier with no AFS:** already handled — `_render_council_accounts_context`
  returns silently (`procurement.py:680`).
- _Out of scope per the brief: the 10 non-publishing LAs get no "missing" treatment._

## 9. Visual differentiators (reconciled with the impeccable pass)

The framing decision that drives the rest: this is a **civic directory ("find my
council and see its scale"), not a national league table.** That reframing produces
three concrete moves:

- **Province bands ordered North→South** — Ulster → Connacht → Leinster → Munster.
  The **4 historic provinces**, not 5 bands: of the 21 publishing LAs, South Dublin is
  the only Dublin council present (Dublin City / Fingal / DLR are in the missing set), so
  it folds into Leinster (geographically correct — Dublin is in Leinster). Geography is
  encoded by _fixed reading order_, not colour (a per-region tint would imply
  categorical/political meaning the data doesn't carry). The band header is a hairline
  typographic stratum, full grid width, so even Ulster's 2-card band reads as
  intentional. **Rendered as a semantic `<h2 class="pr-region-head">`** (direct section
  heading under the page `<h1>` hero — no h2→h3 skip) so screen-reader users navigate bands
  by heading. Header copy is factual only:
  `MUNSTER · 5 councils publishing`. The N→S journey is the "map without a map."
- **No rank chips inside bands.** A global rank grouped by region produces
  non-contiguous numbers (Munster #1, #4, #9) and _is_ the league-table cliché. Bands
  are 3–7 cards, trivially scannable. Order by value DESC within a band as a soft
  signal, but the **council name is the hero** (call `_card(rank=None)`).
- **Dual-tier pills that cannot read as a sum.** ⚠️ Bug the first draft missed:
  `_paid_pill` renders _both_ tiers with the same `pr-pill-val` accent fill
  (`shared_css.py:5307`), which is the textbook visual for two slices of one total.
  Fix: map _visual finality to fiscal finality_ —
  - **paid** (money out the door) = **solid filled** accent pill — the firmest fact.
  - **ordered** (a PO commitment) = **dashed, hollow** pill — visibly provisional.

  Dashed-vs-solid + the verb baked into each chip + **no `+`/`·`/`=` ever between
  them** (whitespace only) make summing them obviously wrong. **Data reality (verified):
  each card carries exactly ONE pill** — only Meath and Offaly publish "paid"; the other
  19 publish "ordered" only. So the never-sum risk is not _within_ a card but _across_
  the ranking: a solid "paid" card (Meath) sitting above a dashed "ordered" card (South
  Dublin). The dashed/solid encoding earns its keep precisely there. Never pad a card
  with a fake `€0 paid`. The verb is also the **accessibility carrier** — colour-blind
  and screen-reader users get the stage from the word, not the border style.
- Everything else inherits `pr-card` / `pr-pill` / `pr-grid`, so it reads as part of
  Procurement, not a bolt-on.

  ```css
  /* Region band = typographic stratum; fixed N→S order encodes geography. */
  .pr-region-head { display:flex; align-items:baseline; justify-content:space-between;
      gap:.75rem; margin:1.6rem 0 .5rem; padding-bottom:.3rem;
      border-bottom:1px solid var(--border-strong); }
  .pr-region-head:first-of-type { margin-top:.4rem; }
  .pr-region-name { font-size:.82rem; font-weight:700; letter-spacing:.08em;
      text-transform:uppercase; color:var(--ink-strong); }
  .pr-region-count { font-size:.78rem; color:var(--text-meta);
      font-variant-numeric:tabular-nums; }
  /* Two STAGES of public money, never a sum. solid = realised, dashed = committed. */
  .pr-tier-pills { display:flex; flex-wrap:wrap; align-items:center; gap:.4rem;
      margin-top:auto; padding-top:.2rem; }
  .pr-pill-paid    { background:var(--accent-subtle); color:var(--accent);
      border:1px solid var(--accent-dim); }      /* realised — strongest ink   */
  .pr-pill-ordered { background:#ffffff; color:var(--ink-700);
      border:1px dashed var(--border-strong); }  /* committed — provisional    */
  ```

## 10. TODO_PIPELINE_VIEW_REQUIRED

1. **`v_procurement_council_summary`** _(recommended, small)_ — one row per council:
   both tier totals as separate columns (`ordered_safe_eur`, `paid_safe_eur`), `region`,
   a `province_order` INT (1=Ulster, 2=Connacht, 3=Leinster, 4=Munster, so the view sorts
   geographically, not alphabetically — the band order is data, not UI logic),
   `n_suppliers`, `min/max_year`. There is **no `region`/`entity_type` column in the
   fact** (verified) — province is derived by a static CASE map on `publisher_name` (Irish
   LA→province is fixed geography, not data inference). Lets the index render one card per
   council with the correct single pill **without the UI pivoting or summing rows**. The
   separate `ordered_safe_eur` / `paid_safe_eur` columns feed the dashed/solid pills in §9.
   Built **directly on `read_parquet`** (not on `v_procurement_payments`): the
   `procurement_*.sql` glob loads alphabetically with `swallow_errors=True`, and
   `procurement_council_summary.sql` sorts _before_ `procurement_payments.sql`, so a view
   dependency would silently drop (see `feedback_sql_view_dependency_order`). It replicates
   the exact `v_procurement_payments` filter, so totals match the existing council toggle.
   Ships-without-it fallback: read the existing per-`(body×tier)`
   `v_procurement_payments_publisher_summary` and group client-side — but a dedicated
   view keeps the logic firewall pristine. `region`/`entity_type` already live in
   `procurement_payments_fact`, so this is a pure GROUP BY, no new joins.
2. **Awards→council crosswalk** _(deferred, larger)_ — to add a "contracts this council
   awarded" section to the dossier (the eTenders award-ceiling stage), the free-text
   `"Contracting Authority"` string in `procurement_awards.parquet` must map to the
   canonical `publisher_id`. That key doesn't exist today, so the dossier ships with
   **payments + AFS only**; the awards section is a fast-follow once the crosswalk lands.

## 11. Implementation plan

**Files (as built)**
- `sql_views/procurement/procurement_council_summary.sql` — new `v_procurement_council_summary`
  (TODO #1; parquet-direct, province CASE, both tier columns).
- `dail_tracker_core/queries/procurement.py` — `council_summary()` query fn.
- `utility/data_access/procurement_data.py` — `fetch_council_summary_result()` wrapper.
- `utility/shared_css.py` — `.pr-region-head` + `.pr-pill-paid` / `.pr-pill-ordered`.
- `utility/pages_code/procurement.py` — `_render_councils()` + `_council_tier_pills()`
  (province-grouped clickable cards → `_paid_publisher_href`); the dossier
  `_render_payments_publisher_profile` + AFS context are reused unchanged.
- `utility/pages_code/council_spending.py` — NEW thin page: hero + `?paid_publisher=`
  dossier dispatch + `_render_councils()`.
- `utility/app.py` — registered `council_spending_page` under "The Money"
  (`url_path="rankings-council-spending"`, icon `location_city`), between Procurement and
  Public Payments.

**Reuse (no new logic):** `_card` (with `rank=None`), `clickable_card_link`,
`_paid_publisher_href`, `_eur`, `empty_state`, `_render_payments_publisher_profile`,
`_render_council_accounts_context`, `_PAY_FOOT_HTML`. Note: do **not** reuse `_paid_pill`
for the index cards — it emits `pr-pill-val` for both tiers, which breaks the never-sum
rail; the index needs the dashed/solid `pr-pill-ordered` / `pr-pill-paid` pair instead.

**New CSS (`shared_css.py`):** two groups — `.pr-region-head` (+ `.pr-region-name` /
`.pr-region-count`) and the tier pair `.pr-pill-ordered` / `.pr-pill-paid` (+
`.pr-tier-pills` wrapper). The tier split is mandatory, not cosmetic.

**Effort:** small — one render function, one SQL view, one fetch wrapper, ~5 CSS classes.
~90% re-surfacing logic that already exists; the new craft is the dashed/solid tier
pills and the N→S band grouping.

## 12. Real-world references & validation

A scan of how other civic-transparency tools present municipal spending (sources below).
The headline finding: the locked v1 is **well-aligned with established practice** — the
patterns mostly _validate_ the design rather than redirect it.

**Validates the v1 design:**
- **ProPublica Nonprofit Explorer** — region roll-up → entity-profile drill, with an
  empty search browsing the full set. Exactly our region bands → council dossier, and we
  already have empty-search browse via `_entity_search_hero`.
- **Tussell / Checkbook NYC** — both keep _award/committed_ value and _invoiced/paid_
  value in physically separate sections with separate timelines, never added. This is
  external confirmation of our two-lane, never-summed dossier (and the existing
  one-tier-only by-year chart at `procurement.py:795`). Don't stack the tiers.

**Data reality (confirms the AFS-only dossier was the right call):** NOAC performance
indicators and the amalgamated Annual Financial Statements are both **PDF-only, no API /
no open data** ([NOAC report 77](https://www.noac.ie/noac_publications/report-77-noac-performance-indicator-report-2024/),
[AFS catalogue](https://datacatalogue.gov.ie/dataset/local-authority-annual-financial-statements)).
Our per-line procurement-payments fact is genuinely additive over what is published
publicly — the value-add is real.

**Deferred enhancement backlog (NOT v1 — recorded so they aren't lost):**
- **Per-capita denominator** — [localauthorityfinances.com](http://localauthorityfinances.com/)
  (Univ. of Galway) shows council figures as € total, € per person, _and_ % of budget,
  with "compare up to 4 councils." A per-capita toggle is the fair cross-council ranking
  and reinforces "directory not leaderboard." Blocked on a pipeline-provided LA→population
  denominator (memory: `project_constituency_population_boundaries` — LA per-capita usage
  still pending). Pipeline metric, not UI inference.
- **Committed→paid reconciliation rate** — ProZorro-style "share of committed value with a
  matching payment" per council ([OCP Ukraine BI](https://www.open-contracting.org/2024/12/04/how-to-make-better-public-procurement-decisions-with-business-intelligence-insights-from-ukraine/)).
  Conceptually overlaps the AFS traceability % already on the dossier; revisit only after
  the awards→council crosswalk (TODO #2) lands.
- **Coverage matrix** — CouncilWatch's "Import Status" page (councils × what-they-publish ×
  last-refresh), rendering absent data as "not published," never €0
  ([councilcheck.co.uk](https://councilcheck.co.uk/)). This _is_ the missing-council
  coverage framing the user deprioritised for now; park until coverage is in scope.
- **Dual supplier ranking (by € and by count)** — Tussell ranks a buyer's suppliers both
  ways; a dossier-level polish, not index work.

_All four are scope additions beyond the locked v1 and are intentionally excluded from the
first build._

## 13. Accessibility & mobile pass

Focused micro-pass over the card/pill/band layer (the layer the impeccable critique
under-scoped). Findings folded into §9:

- **Never-sum cue is accessible** ✅ — the verb (`ordered`/`paid`) is the carrier:
  colour-independent, screen-reader-legible. Dashed-vs-solid is border-_style_, so
  colour-blind-safe. Pill contrast reuses the production `pr-pill-val` pairing
  (`accent` on `accent-subtle`) and `ink-700` on `#ffffff` — both already vetted.
- **Province band = semantic `<h2>`** (`.pr-region-head`), direct child of the page
  `<h1>` hero (no skipped level), so the index is navigable by heading. One concrete a11y
  win the original brief missed; live audit confirmed the hero is `<h1>`.
- **Mobile** ✅ — `pr-grid` already collapses to a single column at ≤640px
  (`shared_css.py:5375`); cards stack full-width, the whole card is the tap target
  (stretched-link), and the band header's `flex` / `justify-between` holds at 360px
  (short name left, count right). Dashed 1px borders render cleanly at phone scale; the
  verb is the fallback regardless.
- ⚠️ **Pre-existing focus-ring debt (NOT introduced here, NOT fixed here).**
  `.dt-card-link` — the shared stretched-link behind _every_ clickable card in the app
  (`shared_css.py:2287`) — has no `:focus-visible` outline, so keyboard focus is
  invisible. App-wide issue across ~9 pages. A one-rule fix
  (`.dt-card-link:focus-visible { outline:2px solid var(--accent); outline-offset:2px; }`)
  would benefit the whole app but shifts shared visuals, so it belongs in its own small
  PR, not buried in this tab. Flagged, not silently patched.
