# BI Category Market Map — Analyst Fill-In Template

**Date:** {{REPORT_DATE}}
**Status:** TEMPLATE — bespoke, hand-built market map for a single CPV category. Fill every `{{PLACEHOLDER}}`. Sections tagged **[BUILT]** are wired to existing data-access functions; sections tagged **[PHASE-2-TODO]** are *not yet built* — do not fabricate their figures, leave the placeholder and the build note in place until the pipeline view lands.
**Grain scope:** This report's **report grain is AWARD** (national eTenders/OGP awards). It carries three read-only enrichment lanes — **TED (EU) 2024+** (single-bid competition, counts only), **PLANNED** (open-tender counts, counts only), and **CRO** (supplier identity) — none of which is ever summed or unioned into the award market size. It carries **no payments, no budget, no TED totals**. Never sum across grains; never union TED with national. See rails below.
**Related:** [BI_SPINOUT_ARCHITECTURE.md](../BI_SPINOUT_ARCHITECTURE.md) (§4 ethics firewall / §6 licensing / §10 no-list / §15 resolved decisions) · [PROCUREMENT_INTELLIGENCE_ROADMAP.md](../PROCUREMENT_INTELLIGENCE_ROADMAP.md) (Phase 2 market maps; Phase 0 primitives)
**Subject:** CPV `{{CPV_CODE}}` — `{{CPV_LABEL}}` (division `{{CPV_DIVISION}}`)
**Analyst:** {{ANALYST_NAME}} · **Corpus refresh:** {{REFRESH_DATE}} · **Coverage window:** {{COVERAGE_WINDOW}}

---

## How to generate this report

Run these `utility/data_access/` calls **in order** (the report generator reads *only* through the data-access layer — never `pd.read_parquet`, never a join/groupby in report logic; see rails §B). Each call returns a `QueryResult`; select the target row(s) by `cpv_code` / `cpv_division` as a display-only row selection (not an aggregation).

1. **Market size, value distribution, supplier count** — `fetch_cpv_summary_result()` → core [`cpv_summary(conn, *, limit=50, order_by="awards", year=None)`](../../dail_tracker_core/queries/procurement.py); returns all CPVs, select the `{{CPV_CODE}}` row. Yields `n_awards`, `n_awards_valued`, `awarded_value_safe_eur`, `n_suppliers`, `median_award_eur`, `p25_award_eur`, `p75_award_eur`. **[BUILT · AWARD grain]**
2. **Single-bid rate + TED buyer count** — `fetch_competition_by_cpv_result(min_lots=…)` → core [`competition_by_cpv(conn, *, min_lots=100)`](../../dail_tracker_core/queries/procurement.py); select the `{{CPV_DIVISION}}` row. Yields `single_bid_lot_pct`, `n_lots_with_bidcount`, `n_single_bid_lots`, `n_buyers`, `first_year`, `last_year`. The wired default is `min_lots=100`. **[BUILT · enrichment, TED 2024+, DIVISION grain]**
3. **Open-tender scope chip (counts only)** — `fetch_ted_tenders_result(only_open=True, sector=…)` and/or `fetch_live_tenders_result(sector=…)` → core [`ted_tenders`](../../dail_tracker_core/queries/procurement.py) / [`live_tenders`](../../dail_tracker_core/queries/procurement.py). Count open notices matching the CPV. **[BUILT · PLANNED grain — counts, never summed]** *(Note: both filter by `sector`, not raw CPV code; `ted_tenders` carries `cpv_division`. Precise full-code CPV filtering is a **[PHASE-2-TODO]** view param.)*
4. **Top suppliers / top buyers for the CPV (ranked, with share)** — **[PHASE-2-TODO]** no per-CPV ranking function exists. Do **not** substitute `fetch_supplier_summary_result` / `fetch_authority_summary_result` — those rank across *all* CPVs, not within one. Requires new pipeline views (`top_suppliers_for_cpv` / `top_buyers_for_cpv`, analogous to the also-missing `top_suppliers_for_authority`) that **emit `share_pct` + its denominator computed inside the view** (so the report renders a stored share and performs no cross-view division). Leave §C4/§C5 figures blank.
5. **Number of distinct buyers (award grain, all-time)** — **[PHASE-2-TODO]** `cpv_summary` exposes `n_suppliers` but **not** `n_buyers`. The only `n_buyers` available today is TED-2024+ competition scope (step 2, `competition_by_cpv.n_buyers`) — a different lane, not the all-time award-grain count.
6. **Year trend (per-CPV time series)** — **[PHASE-2-TODO]** `cpv_year_trend` does not exist. `fetch_cpv_summary_result(year=Y)` (reads `v_procurement_cpv_year_summary`) is a *cross-sectional per-year ranking*, not a per-CPV series. Stopgap: loop `fetch_cpv_summary_result(year=Y)` per year and stitch the `{{CPV_CODE}}` row — flag it as a stitched cross-section, not a native trend.
7. **Full award list for the CPV** — **[PHASE-2-TODO]** no `awards_for_cpv` function. The row-level precedents are [`awards_for_supplier`](../../dail_tracker_core/queries/procurement.py) (row-level; **sole-traders excluded**) and [`awards_for_authority`](../../dail_tracker_core/queries/procurement.py) (row-level; **retains `supplier_class` + `name_truncated` — it does NOT exclude individuals, it carries truncated names instead**). Both are AWARD grain and carry `value_kind` + `value_safe_to_sum` per row. The new `awards_for_cpv` must apply the company-class/PII gate **at source** (like `awards_for_supplier`) — it must **not** inherit `awards_for_authority`'s individual-row handling, or it will leak individual rows.
8. **SME participation** — **OWNER-DECISION GATE** (see §C7). No wired function; the definition and its coverage-gap label are an owner call before any view is built.

---

## 0. Doctrine — never-break rails (read before drafting)

- **AWARD grain only.** Every money figure here is an **awarded ceiling/estimate at notice, not spend.** Sum **only** rows where `value_safe_to_sum = TRUE`; even then it is awarded value, never expenditure.
- **Never sum or net across grains.** AWARDED, PAID, COMMITTED, PLANNED, BUDGET are five separate ledgers. This report's **report grain is AWARDED**; it carries three read-only enrichment lanes — **TED** (single-bid competition, counts only), **PLANNED** (open-tender counts, counts only), and **CRO** (supplier identity) — none ever folded into the award market size. No stacked bars, no running totals, no side-by-side arithmetic that implies one number. Each grain is a **separately labelled cell**.
- **Never union TED with national.** The single-bid enrichment is TED-2024+; keep it a labelled enrichment lane, never folded into the national award market size.
- **Concentration = award COUNTS, denominator shown** — never a share-of-value pie, never "concentration risk."
- **No scores, no verdicts, no framing** (see no-score rails at the foot). Single-bid, concentration, and SME shares are **facts with denominators**, never assessments.
- **Company-class only.** Every subject is an organisation. The generator must be structurally incapable of naming an individual; sole-trader/individual rows are excluded at source (`supplier_class = 'sole_trader_or_individual'`, `public_display = FALSE`, `privacy_status = 'review_personal_data'` all excluded).
- **Every figure ships with a grain tag, a coverage window, and a denominator** rendered inline (subtitle, not buried footnote).

---

## 0a. Money-grain badge / chip wording — OWNER-DECISION GATE (provisional, do not finalize)

The **grain identity** of every cell (which ledger: AWARD / PLANNED / TED / enrichment) is a settled never-break rail. But the exact **badge / chip display strings** are an **unresolved owner-decision gate** (GT-1 Primitive 1: *the owner signs the string table; do not invent it*), and the owner is away. Every bracketed chip in the fill-in cells below (`[AWARD]`, `[PLANNED]`, `[TED 2024+ …]`, and any decorative sub-string such as "ceiling", "NOT spend", "never summed", "sum-safe subset only") is therefore **PROVISIONAL wording**, shown only to convey the grain — not a finalized string. Do not present it as decided.

**Present these as OPTIONS to the owner (do not pick one):**

| Cell slot | Option A | Option B | Also to be signed |
|---|---|---|---|
| National award, summable subset | `Awarded` | `Award value` | — |
| Framework / DPS ceiling row | `Framework ceiling` | `Ceiling (not spend)` | whether to add an explicit **`Not safe to sum`** chip on CEILING |
| TED (EU) award value | `EU award value` | `TED award (EU)` | whether to add an explicit **`Not safe to sum`** chip on TED |
| Planned / open tender | `Planned` | `Open tender — count only` | — |

> Until the owner signs the string table, treat every chip string in the cells below as a placeholder for the grain, not a shipped label — exactly as the SME wording in §C7 is gated.

---

## C1 · Category definition & scope (lead card) — [BUILT]

**So-what (SME):** "What exactly is in this market, and how much public activity is there?"
**So-what (consultant):** "Fix the CPV boundary and the corpus window before analysis."

**Sourcing**

| Fact | Function (core) | Wrapper (data_access) | View | Grain | Status |
|---|---|---|---|---|---|
| n_awards, n_suppliers, awarded_value_safe_eur | `cpv_summary` | `fetch_cpv_summary_result` | `v_procurement_cpv_summary` | AWARD | BUILT |
| open-tender count | `ted_tenders` / `live_tenders` | `fetch_ted_tenders_result` / `fetch_live_tenders_result` | TED / eTenders live | PLANNED (counts) | BUILT (sector filter) |

**Fill-in card** *(chip strings are provisional — see §0a badge gate)*

- CPV code(s): `{{CPV_CODE}}` — `{{CPV_LABEL}}` · division `{{CPV_DIVISION}}`
- Award notices in window: **{{N_AWARDS}}** `[AWARD]`
- Distinct suppliers: **{{N_SUPPLIERS}}** `[AWARD]`
- Currently open tenders in scope: **{{N_OPEN_TENDERS}}** `[PLANNED]`
- Activity chips: `Public procurement ✓ · TED (EU) presence {{TED_PRESENCE}} · Payment registers — out of scope for this report`

**Caveats (carry inline):** *CPV coverage* (see library) — CPV coding is buyer-assigned and inconsistent, so this boundary captures only the coded subset; a large share of awards carry no real CPV code (fill `{{CPV_NULL_PCT}}` from a live query — do not hard-code). Sibling CPVs listed in §C10.

---

## C2 · Market size — AWARD grain — [BUILT]

**So-what (SME):** "How big is this market in public tenders — is it worth entering?"
**So-what (consultant):** "Size the category before advising bid/no-bid."

**Sourcing:** `cpv_summary` → `fetch_cpv_summary_result` → `v_procurement_cpv_summary` · **grain: AWARD**.

*(Chip strings below are provisional — see §0a badge gate.)*

| Metric | Value | Grain / caveat |
|---|---|---|
| Award notices | {{N_AWARDS}} | `[AWARD]` — counts of *notices*; one framework can spawn many call-off notices |
| Awards with a real valued figure | {{N_AWARDS_VALUED}} | `[AWARD]` — denominator for the value stats |
| Sum-safe awarded value | €{{AWARDED_VALUE_SAFE_EUR}} | `[AWARD]` — `value_safe_to_sum = TRUE` subset only; awarded value, never spend |
| Distinct suppliers | {{N_SUPPLIERS}} | `[AWARD]` |

> Lead with **counts and the valued-denominator**, not the € total. The € figure is an awarded value (never expenditure); framework/DPS ceiling rows are excluded from this sum-safe subset (they are `value_safe_to_sum = FALSE`).

---

## C3 · Award value distribution — median & IQR — [BUILT]

**So-what (SME):** "What's a typical contract worth here — and how wide is the spread?"
**So-what (consultant):** "Anchor the client's pricing to the market's central tendency, not the outliers."

**Sourcing:** `cpv_summary` (median/IQR **already exist on this view — do not re-derive**) → `fetch_cpv_summary_result` → `v_procurement_cpv_summary` · **grain: AWARD**.

| Statistic | Value | Note |
|---|---|---|
| Median award | €{{MEDIAN_AWARD_EUR}} | `median_award_eur` |
| Lower quartile (P25) | €{{P25_AWARD_EUR}} | `p25_award_eur` |
| Upper quartile (P75) | €{{P75_AWARD_EUR}} | `p75_award_eur` |
| Valued denominator | {{N_AWARDS_VALUED}} | median/IQR computed over valued awards only |

> Median + IQR are the honest centre-and-spread for a skewed award distribution — prefer them to a mean. *(An inflation-adjusted variant exists — `cpv_summary_real` / `fetch_cpv_summary_real_result` — but it is **EXPERIMENTAL / local-only**; do not ship its figures in a paid report without owner sign-off.)*

---

## C4 · Supplier landscape — top suppliers & concentration — [PHASE-2-TODO]

**So-what (SME):** "Who holds this market — is it winnable, and who would I be up against or teaming with?"
**So-what (consultant):** "Name the incumbents and their category share."

**Status: NOT BUILT.** There is **no per-CPV supplier ranking function.** `supplier_summary` ranks suppliers across *all* CPVs and cannot be filtered to one category. `cpv_summary.n_suppliers` gives the **count** ({{N_SUPPLIERS}}) but not the ranked list or share.

**Build note:** add a pipeline view + `top_suppliers_for_cpv(conn, cpv_code, *, limit=N)` returning `supplier, supplier_norm, n_awards, share_pct, share_denominator, most_recent_award, company_num` (company-class only, sole-traders excluded). **The view computes `share_pct` and its `share_denominator` inside the view, against the same sole-trader-excluded award population it ranks** — the report renders the stored `share_pct` and performs **no cross-view division**. **Do not** divide `n_awards` by `cpv_summary.N_AWARDS`: those denominators are on different bases (`cpv_summary.N_AWARDS` counts *all* awards, including the excluded sole-trader/individual rows), so the computed shares would be understated and would not reconcile against the "all others" bucket. Then fill:

| Rank | Supplier | # awards | Share of CPV award count | Most recent award |
|---|---|---|---|---|
| 1 | {{TOP_SUPPLIER_1}} | {{N_1}} | {{SHARE_1_PCT}}% | {{DATE_1}} |
| … | … | … | … | … |
| — | *all others* | {{N_OTHERS}} | {{SHARE_OTHERS_PCT}}% | — |

> **Share is read from the view's stored `share_pct`; denominator = {{SHARE_DENOMINATOR}}** (the view's own denominator, over the sole-trader-excluded population — *not* `cpv_summary.N_AWARDS`). The "all others" row is the complement within that same population (100% − ranked shares), a display-only subtraction on the view's own stored shares (mark `# logic_firewall: display_only`), never a second cross-view division. Concentration on award COUNTS, never share-of-value, never a pie mixing grains. Descriptive market-structure fact — *not* a barrier-to-entry or competitiveness verdict.

---

## C5 · Buyer landscape — top buyers, number of buyers & concentration — [PHASE-2-TODO]

**So-what (SME):** "Who are the buyers in this category, and which are most active — where do I focus BD?"
**So-what (consultant):** "Target-buyer list for the category."

**Status: NOT BUILT (two gaps).**
- **Ranked top buyers with share** — no per-CPV buyer ranking function (`authority_summary` ranks across all CPVs, not within one). Requires `top_buyers_for_cpv(conn, cpv_code, *, limit=N)` returning per-buyer `n_awards`, `share_pct`, `share_denominator` — **`share_pct` and its denominator computed inside the view against the same award population it ranks**, so the report renders a stored share and never divides across views. Do **not** compute buyer share from `cpv_summary.N_AWARDS`.
- **Number of distinct buyers (award grain, all-time)** — `cpv_summary` exposes `n_suppliers` but **not** `n_buyers`. The only `n_buyers` available today is `competition_by_cpv.n_buyers` = **{{N_BUYERS_TED}}**, which is **TED-2024+ competition scope**, a different lane — do **not** present it as the all-time award-grain buyer count.

**Build note fill-in (once the view lands):**

| Rank | Contracting authority | # awards | Share of CPV award count | Publishes payment register? |
|---|---|---|---|---|
| 1 | {{TOP_BUYER_1}} | {{B_N_1}} | {{B_SHARE_1_PCT}}% | {{B_REG_1}} |
| … | … | … | … | … |

- Distinct buyers (award grain, all-time): **{{N_BUYERS_AWARD}}** `[PHASE-2-TODO]`
- Distinct buyers (TED 2024+, competition scope): **{{N_BUYERS_TED}}** `[enrichment — different scope, do not conflate]`

> Share is read from the view's stored `share_pct`; denominator = {{B_SHARE_DENOMINATOR}} (the view's own denominator, not `cpv_summary.N_AWARDS`). Award COUNTS only; descriptive.

---

## C6 · Competition intensity — single-bid rate (CPV division) — [BUILT]

**So-what (SME):** "How contested is this category — do incumbents rarely face a field, or is it wide open?"
**So-what (consultant):** "Quantify contestability to advise bid/no-bid."

**Sourcing:** `competition_by_cpv` → `fetch_competition_by_cpv_result` → TED-2024+ competition view · **grain: enrichment, TED 2024+, DIVISION-level (2-digit `cpv_division`), lot-level rate.**

*(Chip strings below are provisional — see §0a badge gate.)*

| Metric | Value | Coverage |
|---|---|---|
| Single-bid lot rate | {{SINGLE_BID_LOT_PCT}}% | `[TED 2024+ · division {{CPV_DIVISION}}]` |
| Single-bid lots | {{N_SINGLE_BID_LOTS}} | of {{N_LOTS_WITH_BIDCOUNT}} lots with a disclosed bid count |
| Buyers in this division | {{N_BUYERS_TED}} | TED 2024+ scope |
| Window | {{TED_FIRST_YEAR}}–{{TED_LAST_YEAR}} | eForms era only |

> Rank only where `n_lots_with_bidcount` is healthy. **The wired function's default is `competition_by_cpv(min_lots=100)`** (`fetch_competition_by_cpv_result`) — that is the number a report generator will actually pass. The **"min_lots default 40"** figure in the verbatim COMPETITION caveat below is the default of a *different, per-buyer* function (`competition()`, currently unwired) and reads as a minimum-sample rule of thumb — **not** this CPV-division function's default. Do not conflate the two; small samples are noisy either way. **Finer per-full-CPV-code competition (below the 2-digit division) is [PHASE-2-TODO].** Carry the single-bid caveat verbatim (library) — it is a signal, never a verdict.

---

## C7 · SME participation — OWNER-DECISION GATE (do not pick)

**So-what (SME):** "Do firms like mine actually win here, or is it locked up by large incumbents?"

**This section is blocked on an owner decision.** "SME win-rate" has **at least three defensible definitions** that give **different numbers**, and the ~22% bid-count coverage gap must be labelled a specific way. Present the options to the owner; **do not select one, and do not ship an SME figure until the owner signs the definition and the coverage-gap label.**

**Definition options (pick exactly one — owner decides):**

| Option | Definition | Numerator / denominator | Measures | Caveat it inherits |
|---|---|---|---|---|
| **A · Award-side (win)** | SME-won awards ÷ all awards in the CPV | winning supplier flagged SME / {{N_AWARDS}} | who *wins* the work | award-grain; needs an agreed SME classifier on `supplier_class` |
| **B · Bid-side (participation)** | SME bidders ÷ all bidders on CPV lots | bidder-level SME flag / disclosed-bid lots | who *shows up* to compete | **only ~78% of awards disclose bid counts → ~22% gap**; TED-2024+ only |
| **C · Supplier-side (base composition)** | distinct SME suppliers ÷ distinct suppliers active | SME suppliers / {{N_SUPPLIERS}} | the *shape of the supplier base* | unweighted by award volume |

**Coverage-gap label options (owner also signs this):**

| Option | How the ~22% bid-count coverage gap is shown |
|---|---|
| **G1** | A visible **"not disclosed"** bucket alongside the SME share (hiding it would be a verdict by omission) |
| **G2** | A **footnote-only** rate stated on the disclosed subset, with the disclosed-denominator printed |
| **G3** | **Suppress** the bid-side rate entirely until coverage improves; ship only Option A or C |

> Rails: whichever definition is chosen, the figure is a **fact with a denominator**, never an "SME-friendliness score." The ~22% gap must be **shown, not hidden**. No wired function exists — a pipeline view is required after the owner decides.

---

## C8 · Market activity over time — year trend — [PHASE-2-TODO]

**So-what (SME):** "Is this market growing, flat, or shrinking — worth entering now?"
**So-what (consultant):** "Trend the category."

**Status: NOT BUILT.** No `cpv_year_trend` (per-CPV time series). `fetch_cpv_summary_result(year=Y)` (via `v_procurement_cpv_year_summary`) is a **cross-sectional per-year ranking**, not a native series.

**Stopgap (label it as such):** loop `fetch_cpv_summary_result(year=Y)` across {{YEAR_RANGE}}, extract the `{{CPV_CODE}}` row per year, and present as a **stitched cross-section** — award-notice **count by year** (lead), with awarded value as a **grain-labelled secondary panel** ("Awarded value — not money paid").

| Year | # awards | Awarded value (€, sum-safe) |
|---|---|---|
| {{Y1}} | {{Y1_N}} | €{{Y1_VAL}} |
| … | … | … |

> **No stacked bars.** Counts lead; value is a separately labelled panel under a hard divider. TED cross-border notices are shown as counts, never summed.

---

## C9 · Full award list — [PHASE-2-TODO]

**So-what (both):** "Show me the actual contracts behind the numbers."

**Status: NOT BUILT.** No `awards_for_cpv` function. Row-level precedents: `awards_for_supplier` (**sole-traders excluded**) and `awards_for_authority` (**retains `supplier_class` + `name_truncated` — it includes individuals with truncated names, it does NOT exclude them**). Both AWARD grain; each row carries `value_kind` + `value_safe_to_sum`. The CPV-keyed sibling **must apply the company-class/PII gate at source** (`supplier_class`, `public_display`, `privacy_status`) like `awards_for_supplier` — it must **not** inherit `awards_for_authority`'s individual-row handling, or individual rows will leak.

**Build note fill-in (row-level, company-class only):**

| tender_id | contracting_authority | supplier | award_date | value_eur | value_kind | value_safe_to_sum |
|---|---|---|---|---|---|---|
| {{…}} | {{…}} | {{…}} | {{…}} | €{{…}} | {{contract_award_value / framework_or_dps_ceiling}} | {{TRUE/FALSE}} |

> Every row keeps its `value_kind` label; **only** `value_safe_to_sum = TRUE` rows may be summed, and even then it is awarded value. Framework/DPS ceiling rows are flagged, never blended into a spend total. *(Whether the ceiling rows also carry an explicit "Not safe to sum" chip is an owner-gated wording decision — see §0a.)*

---

## C10 · Adjacent categories & data lineage — [BUILT / partial]

**So-what (both):** "What sibling CPVs bleed into this market, and how complete is this map?"

- Sibling/parent CPVs with activity (from `cpv_summary`, filter to division `{{CPV_DIVISION}}`): {{SIBLING_CPVS}}
- Coverage window: {{COVERAGE_WINDOW}} · Corpus refresh: {{REFRESH_DATE}}
- Unmatched / no-real-CPV note: {{CPV_NULL_NOTE}} — fill `{{CPV_NULL_PCT}}` / `{{CPV_NULL_VALUE_EUR}}` from a live query, never hard-code (see CPV-coverage caveat)

> State coverage windows and the CPV-coding gap once, in full. Unmatched records are **counted and shown, never silently dropped.**

---

## Caveat library — paste the relevant block verbatim (do not paraphrase, do not soften)

**[CPV COVERAGE]** (sourced phrasing, GT-5 §C1 / §C8 — the only sourced CPV-coverage wording available)
> CPV coding is buyer-assigned and inconsistent; adjacent codes may hold related activity — sibling CPVs listed in §C10.

*Fill-in (not part of the verbatim caveat; every numeric coverage claim needs a live-query anchor):* a large share of national award notices carry no real CPV code, so this boundary captures only the coded subset. State the coverage figures **only as placeholders filled from the data-access layer at generation time** — `{{CPV_NULL_PCT}}` of award notices are CPV-NULL; `{{CPV_NULL_VALUE_EUR}}` of sum-safe value sits in the NULL bucket — never as fixed constants inside the caveat text.

**[SINGLE-BID — recorded fact, never a verdict]** (verbatim, `dail_tracker_core/caveats.py` `COMPETITION`)
> single_bid_lot_pct = single-bid LOTS / lots-with-a-bid-count, from TED 2024+ award notices — each contract PART counted once (the honest lot-level rate; an earlier notice-level reading over-stated multi-lot buyers). A FACTUAL competition signal, NEVER a verdict: a single bidder is often legitimate — a niche/specialist supplier, bespoke research equipment, genuine urgency (research universities legitimately single-source a lot). It is the EU Single Market Scoreboard's procurement-integrity indicator: a prompt to look, not evidence of wrongdoing. Rank only buyers with a healthy n_lots_with_bidcount (min_lots default 40); small samples are noisy. Coverage is 2024+ only (the eForms era carries bid counts).

**[AWARD / FRAMEWORK-CEILING]** (verbatim, `api/routers/exports.py` `procurement_awards` caveat)
> AWARD values are ceilings/estimates, NOT money paid. Frameworks/DPS repeat one ceiling across supplier rows. Sum ONLY rows where value_safe_to_sum — and even that is awarded value, never expenditure. Upstream publishes quarterly with an inherent ~6-month lag (see data_currency).

**[THREE MONEY GRAINS — never sum]** (verbatim, `dail_tracker_core/caveats.py` `MONEY_GRAINS`)
> procurement AWARDS, public-body PAYMENTS, and T&A allowances are three different value grains — NEVER sum across them

**[TED / EU]** (verbatim, `api/routers/exports.py` `ted_awards` caveat)
> Pan-EU framework outliers (is_pan_eu_outlier) carry ceilings that dwarf the Irish market — exclude them from totals. Single-bid is a factual signal, never a verdict. Sum only value_safe_to_sum rows; never add to payments (different grain).

**[ENTITY MATCHING]** (verbatim, `utility/pages_code/procurement.py:2383-2385`)
> Grouped by name only; these may be different legal entities — confirm via the CRO number on each notice before treating them as one firm.

*Composed note (not verbatim — do not present as a canonical caveat):* an ambiguous match (`n_cro ≥ 2`) is a probable lead, never verified identity; unmatched records are counted and shown, not silently dropped.

---

## Never-break rails — pre-publish checklist

- [ ] Every money figure carries an **AWARD** grain tag; no figure summed across grains; no stacked bars mixing grains.
- [ ] Sum-safe totals use **only** `value_safe_to_sum = TRUE`; framework/DPS ceilings flagged, never blended into spend.
- [ ] TED single-bid kept as a **labelled enrichment lane**, never unioned with national awards; TED values never summed.
- [ ] Concentration/shares are read from the ranking view's own stored `share_pct` (computed against the sole-trader-excluded population); **never** divided in report logic against `cpv_summary.N_AWARDS` (different base) — no share-of-value pie.
- [ ] SME participation **not shipped** until the owner signs the definition (§C7) and the ~22% coverage-gap label; the "not disclosed" bucket is shown, not hidden.
- [ ] Money-grain **badge/chip wording is treated as provisional** (§0a) — the exact strings are not shipped until the owner signs the string table.
- [ ] Every subject is an **organisation**; sole-trader/individual rows excluded (`supplier_class`, `public_display`, `privacy_status` gates); no natural person named — including in any `awards_for_cpv` row list.
- [ ] All aggregation/joins live in **registered pipeline views + the query layer** — none in report/page logic (logic firewall).
- [ ] Every figure prints a **grain tag + coverage window + denominator** inline.
- [ ] `[PHASE-2-TODO]` sections left blank with their build notes — no fabricated figures.

### No-score rails (hard)

- **No influence / risk / access / competitiveness / concentration scores, rankings, or indices** — for any company, buyer, or person.
- **No verdict framing.** Single-bid, concentration, incumbency, and SME shares are **separate labelled facts with denominators and their caveats**, never fused into a composite number.
- **Forbidden as claims in any copy or chart:** *influence, corruption, waste, rigged, captured, influence-bought, uncompetitive, concentration risk, red flag*. State the fact and its denominator; let the reader's analyst draw the conclusion.
- **Presence ≠ causation** for any cross-register mention; report co-existence of public records only.

---

## SOURCES & ATTRIBUTION (reproduce verbatim on every published copy)

- **eTenders / OGP (national awards, market size, value distribution):** Contains Irish Public Sector Data (Office of Government Procurement) licensed under CC-BY 4.0.
- **TED / EU-OJ (single-bid competition enrichment):** Contains information from TED (© European Union), reused under Decision 2011/833/EU.
- **CRO — Companies Registration Office (supplier identity / entity crosswalk):** Contains Irish Public Sector Data licensed under a Creative Commons Attribution 4.0 International (CC BY 4.0) licence.

*Licence terms travel with the report: a re-publishing customer must honour attribution, the never-sum caveat, no re-identification of any person row, and no resale of raw exports. CC-BY does not cover data protection — any feature that would name a natural person triggers separate controller obligations and is out of scope for this report. We sell the curation and software, not the underlying public data.*

*Report generated {{REPORT_DATE}} · corpus refresh {{REFRESH_DATE}} · coverage window {{COVERAGE_WINDOW}} · analyst {{ANALYST_NAME}}.*