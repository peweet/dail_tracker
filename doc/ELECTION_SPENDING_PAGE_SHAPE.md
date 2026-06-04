# Election Spending page — design brief (shape, not code)

Backing data: `data/gold/parquet/sipo_expenses_fact.parquet` → `v_sipo_expenses_base`
(401 candidates, 9 parties, GE2024). Test-guarded by `test/test_sipo_expenses.py`
(19 green). Provenance: `data/_meta/sipo_ge2024_expenses_sources.md`.

**Greenfield** — no existing page.

---

## 0. Comparator research — what other projects do (and what it changes)

Surveyed: OpenSecrets (US), UK Electoral Commission candidate-spending tool, and —
most relevant — **The Journal's 2025 investigation of this exact SIPO dataset**.

**Patterns worth copying:**
- **Leaderboard defaults.** Both the UK EC tool and The Journal lead with
  *"who spent the most / the least"* — confirms a **Top-candidates** lens is a strong
  (maybe the strongest) entry view. EC pre-loads most/least; we should too.
- **Foreground data quality, don't hide it.** The Journal explicitly led on *"broken
  links, missing candidates, bad math, candidates failing to fill the form in"* and
  built a manual DB anyway. The leading journalism on this data treats quality
  caveats as front-page material — our **verified/flagged honesty model is exactly
  right** and is a genuine differentiator, not an apology.
- **Expenditure categorisation** (OpenSecrets' 9 categories; SIPO's A–H headings) is
  the natural shape for the future Part-4 itemised view — but the UK EC's own verdict
  is that the *legislated* categories "do not provide useful information to voters",
  so present them plainly, not as analysis.

**The finding that changes the framing (critical):**
- The Journal's totals are **candidate's-own-return + party combined** (€9.2m;
  Ruth Coppinger **€46,186**; FG €2.6m, FF €2.2m). **Our dataset is ONLY the party
  *national-agent* spend attributed per candidate** — a *subset*. Our Coppinger is
  **€25,455**, our FF verified total €362k — these are NOT the same numbers and a user
  who has seen The Journal will assume they are. So:
  - The hero/glossary must say, precisely, **"what each party's national agent spent
    on its candidates"** — one component of total campaign cost, **not** "what it cost
    to run" and **not** comparable to The Journal's per-candidate totals.
  - Note the bigger picture exists and is **separate & not held here**: each
    candidate's *own* election-agent return (the ~528 forms The Journal used; ~150
    still unpublished by SIPO). Link out rather than imply we have it.
- **Avoid The Journal's editorial frames** ("barrier to running", "exclusionary",
  high-spenders-who-lost). Those are good journalism but they're *inference*. The app
  states the figure + source; the reader draws conclusions (no-inference rule).

Sources: [The Journal investigation](https://www.thejournal.ie/investigates-dail-election-expenses-6845119-Oct2025/),
[UK Electoral Commission — candidate spending](https://www.electoralcommission.org.uk/political-registration-and-regulation/financial-reporting/campaign-spending-candidates),
[OpenSecrets expenditures](https://www.opensecrets.org/campaign-expenditures).

## 1. User question this page answers

"At the 2024 general election, **how much did each party spend campaigning on its
candidates**, and **on which candidates was the most spent** — and can I check it
against the official record?"

Civic-accountability framing (theyworkforyou), not a finance dashboard.

## 2. Current UI problems

Greenfield — none. The risk to avoid: **conflation**. Three different money things
live near each other and must never blur:
- **this page** = election *expenses* (what a party SPENT campaigning), GE2024
- *Payments* page = TDs' salaries/allowances (ongoing pay) — different
- *Donations* = money RECEIVED (separate SIPO register, not built) — different

## 3. Bold layout (section order)

1. **Hero** — kicker `SIPO · General Election 2024`, title *"What the parties spent
   campaigning"*, dek that fences off donations & pay (copy in §below).
2. **Glossary strip** — 3 terms: *Assigned*, *Expenditure (by national agent)*,
   *Verified*. (Pre-empts the two-number confusion + the €0 trap.)
3. **Totals strip** — `€1.23m total verified spend` · `401 candidates` · `9 parties`
   · `380 verified (95%)`. Verified-only; one honest sub-line for the rest (§4).
4. **Lens control** (`st.segmented_control`): **By party** (default) · **Top
   candidates** · **By constituency**. No house/year toggle — single election.
5. **Content** (card-based, two-column where it ranks):
   - *By party*: 9 **party cards**, ranked by verified spend, each with a
     proportion bar (share of the largest party's spend) and `spent / assigned /
     candidates`. Whole card → party detail.
   - *Top candidates*: ranked **candidate cards** across all parties (Noel Rock
     €26,006 · Ruth Coppinger €25,455 · Mary Lou McDonald €18,372 …), party pill +
     constituency + source link.
   - *By constituency*: a constituency picker (`member_jump_panel` pattern) → every
     party's candidate spend in that constituency ("what was spent in my area").
6. **Provenance expander** (source-first; §6).

## 4. The honesty model (verified vs flagged) — the spine of the page

380/401 rows are `flag='ok'`. The other 21 are OCR-flagged
(`over_limit` / `spend_gt_assigned` / `assigned_over_limit` / `low_confidence` /
`no_amount`). Rule, enforced in the SQL and the UI:

- **Headline totals, party bars, and rankings draw from `is_verified` ONLY.**
- One quiet honest line under the totals: *"21 figures are OCR-flagged and held
  back from totals — view them under each party as 'needs checking'."*
- Flagged candidates appear in a collapsed **"Figures to verify"** group inside the
  party detail, rendered as **"amount unclear — see SIPO PDF p.N"**, never as a
  clean euro number. A flagged value is never laundered into a ranking.
- Every card (verified or not) shows a **source link** (party PDF + page) and, for
  verified rows below the confidence band, a small `verify` chip.

## 5. Temporal behaviour

**None.** Single election (GE2024). No year pills, no date range — state it in the
hero dek ("November 2024 general election") so absence reads as intent, not a gap.

## 6. Source-link behaviour

Source-first, mandatory (OCR-derived data).
- Per card: `Source: <Party> SIPO return, p.N →` linking the official
  `assets.sipo.ie/...` PDF (URL per party in the sources `_meta` doc; `source_page`
  gives the page).
- Provenance expander: what this is (national-agent expenses), the statutory limits
  (€38,900/€48,600/€58,350), the assigned-vs-spent distinction, the OCR caveat, the
  flag legend, the **no-inference note**, and a link to the SIPO GE2024 collection.

## 7. Chart & table strategy

- **No `st.dataframe` on any primary view** — cards only (drill-down export is the
  only place a table is even considered, and not in v1).
- **One viz, earned**: the party proportion bar *inside* each party card (CSS
  width = party_verified_spend / max_party_spend). Answers "who spent most" at a
  glance without a separate chart axis. (Mirrors the praised committee-composition
  bars.) No Altair needed for v1.

## 8. Empty-state copy (per section)

- Constituency with nothing on file: *"No SIPO election-expense figures on file for
  {constituency}."*
- Party detail, all-flagged (won't happen at 95% but guard it): *"Every figure for
  {party} needs checking against the source PDF — open it below."*
- Candidate search no match: *"No candidate matches '{q}'. Names are as printed on
  the SIPO return."*
- Zero verified spend on a candidate: *"€0 attributed by the national agent"* with a
  glossary tooltip — **not** "didn't campaign" (see §no-inference).

## 9. Design differentiators (greenfield)

- The **verified/flagged honesty system** (totals exclude flagged; flagged shown as
  "verify", never as numbers) — a reusable trust pattern.
- The **dual-number** treatment (spent vs assigned) made legible, not conflated.
- **Source-PDF-per-card** at candidate grain.
- Party **proportion bars** as the single, honest comparison viz.

### no-inference guardrails (hard)
- A high spend is **not** evidence of anything; a €0 attributed spend is **not**
  "didn't campaign" (the national agent may have spent on the *national* campaign,
  not attributed per-candidate). Copy states facts + source only; no adjectives, no
  rankings-as-judgement, no "biggest spender" loaded framing — just "most spent on".

## 10. TODO_PIPELINE_VIEW_REQUIRED (data needed, build as pipeline-owned views)

- `v_sipo_expenses_party_summary` — per-party verified totals, candidate counts,
  share-of-max (for the party cards + bars).
- `v_sipo_expenses_candidate_ranking` — verified candidates ordered by expenditure
  (for Top candidates).
- `v_sipo_expenses_by_constituency` — candidates grouped by constituency.
- **(deferred enrichment, not a v1 blocker)** candidate → `unique_member_code` fuzzy
  link, to make candidate cards click through to `/member-overview`. v1 shows names
  as printed (no member link) — flagged here so it's a known gap, not a silent one.
- **(deferred)** display-name normalisation: some parties print `SURNAME, First`
  (Aontú/SocDems) vs `First Surname` — canonicalise for display once member-linked.

## 11. Implementation plan (for the build session — NOT done here)

Files to create:
- `sql_views/sipo_expenses_party_summary.sql` → `v_sipo_expenses_party_summary`
- `sql_views/sipo_expenses_candidate_ranking.sql` → `v_sipo_expenses_candidate_ranking`
- `sql_views/sipo_expenses_by_constituency.sql` → `v_sipo_expenses_by_constituency`
  (all read `v_sipo_expenses_base`; verified-only headline aggregates live HERE, not
  in the page — logic firewall)
- `utility/data_access/sipo_data.py` — `get_sipo_conn()` + `fetch_party_summary()`,
  `fetch_candidate_ranking()`, `fetch_constituency(name)`, `fetch_filter_options()`
  (SELECT-only; no joins/aggregation/parquet reads in this module)
- `utility/pages_code/election_spending.py` — `election_spending_page()` with
  `@page_error_boundary`, `inject_css()`, `hide_sidebar()`

Files to modify:
- `utility/app.py` — register **one** page `st.Page(election_finance_page,
  title="Election Finance", icon=":material/savings:", url_path="election-finance")`.
  **NAV DECISION (the nav is a flat 12-item top bar — already crowded):**
  - It is NOT a `rankings-` page: those are league tables of *sitting TDs* that
    funnel into Member Overview; this is about election *candidates* (incl. losers
    like Noel Rock / Ruth Coppinger), so it stands on its own slug.
  - Use **"Election Finance"** (not "Election Spending") so the SAME page later gains
    a *Donations* tab — one nav item for the whole election-money domain instead of
    two. Top-level view tab: **Spending** (v1) · *Donations* (deferred).
  - Distinct from **Payments** (ongoing TD salaries/allowances) — different dataset,
    different grain, must not blur.
  - **Separate future task (not a blocker):** the 12-item flat top nav is at its
    limit; group `st.navigation` into sections — *The TD* / *Accountability* /
    *Money & influence* (Payments, Lobbying, Interests, Election Finance) / *Official
    record* / Glossary. That tidies all pages and gives this one a natural home.
- `utility/shared_css.py` — add `.es-party-bar` (proportion bar), `.es-verify-chip`,
  `.es-flagged` (muted "unclear" style); reuse `.dt-hero/.dt-totals-strip/.dt-card-*`.
- `test/test_sql_views.py` — add a `sipo_*` registration + column-assertion group.

Reused helpers: `hero_banner`, `glossary_strip`, `totals_strip`,
`ranked_member_card` (+ domain pills/badge), `clickable_card_link`,
`member_jump_panel` (for the constituency picker), `provenance_expander`,
`empty_state`, `page_error_boundary`, `clean_meta`.

---

### Open decisions for the user (before build)
1. **Default lens** — *By party* (recommended: honest, the filing entity) vs *Top
   candidates* (punchier). 
2. **Card click target** — party card → party detail (in-page) is clear; candidate
   card → nothing in v1 (no member link yet) vs → source PDF. Recommend: candidate
   card opens the **source PDF** until member-linking lands.
3. **Page title / url** — "Election Spending" / `election-spending` (recommended) vs
   folding under a broader "Election Finance" umbrella that could later hold donations.
4. **Naming** — show names as printed (fast, v1) vs wait for member-link
   normalisation (slower, cleaner). Recommend ship as-printed with the caveat.
