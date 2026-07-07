# Procurement Intelligence Roadmap

**Date:** 2026-06-28
**Scope:** Turn the procurement module from a strong public-data *explorer* into a *procurement-intelligence product* (supplier dossiers, buyer dossiers, market maps, confidence-labelled matching, explain-this-figure, opportunity workflows).
**Status:** PLAN — not yet implemented. Phase 0 is boundary-safe and ready to greenlight; later phases carry user-domain decisions (flagged) that must not be made autonomously.
**Related:** [PROCUREMENT_COMPETITOR_ANALYSIS.md](PROCUREMENT_COMPETITOR_ANALYSIS.md) — investigation + enrichment for supplier-anchored competitor maps (trade tagger 161→2,124 electrical/M&E awards; buyer reconciliation ~85%→~92% of TED volume via NFKD fold + curated rebrand/alias map — note ~half the *unmatched* residual is genuinely TED-only, not crosswalkable; CRO-anchored entity key). Confirms the same load-bearing gaps this roadmap flags (CPV-NULL hazard, TED↔eTenders buyer reconciliation, match-confidence/entity resolution) and is the supplier-side counterpart to Phase 2 market maps / Phase 3 competition.

## How this plan was produced

A 15-theme audit (one validation agent per point of the product feedback) plus an adversarial completeness review. Every "exists / partial / missing" claim was verified against **current** code, and the load-bearing claims were re-validated by hand (evidence inline below). This is a **surfacing** plan far more than a data-engineering one: most ingredients already exist in gold/views and simply aren't wired to the page.

### Load-bearing claims — independently validated
| Claim | Verdict | Evidence |
|---|---|---|
| Per-supplier match confidence is computed but never shown | ✅ confirmed | `data/gold/parquet/procurement_supplier_cro_match.parquet` has `match_method` + `match_confidence` ({0.0, 0.5, 0.9}); counts **6,047 exact_unique / 400 exact_ambiguous / 3,532 no_match**. The view selects only `match_method` ([procurement_supplier_summary.sql:53](../sql_views/procurement/procurement_supplier_summary.sql#L53)); the UI pill is binary ([procurement.py:414](../utility/pages_code/procurement.py#L414)). |
| Per-buyer single-bid view exists but is unwired | ✅ confirmed | `v_procurement_competition` is referenced **nowhere** in `utility/`. |
| Procurement has zero in-app export despite a shared helper | ✅ confirmed | `export_button` used in ~13 page/ui files; **0** download/export occurrences in `procurement.py`. Helper: [utility/ui/export_controls.py](../utility/ui/export_controls.py). |
| A bulk-export API is deployed but undiscoverable | ✅ confirmed | `/v1/data` + `/v1/data/{resource}` in [api/main.py:80,139](../api/main.py#L80); exports router included. |
| No per-user persistence (watchlists/alerts infeasible today) | ✅ confirmed | Only cookie/user code is anonymous [page_analytics.py](../utility/ui/page_analytics.py); `st.popover` unused anywhere (explain-panel is genuinely new). |

---

## TL;DR

The module is already excellent: search-first, fully URL-driven/shareable, **job-phrased** sections ("Who wins contracts? / Who actually gets paid? / Open right now / Patterns"), rigorous three-money-grain never-sum discipline, and a mature no-inference vocabulary. The gap to "intelligence product" is mostly **surfacing signals that already exist**.

The single most important fix: **surface the match-confidence signal.** 400 ambiguous CRO matches (where the displayed company number is an arbitrary pick) currently look identical to 6,047 firm matches. This is a trust risk for a commercial user and the fix is additive (the data already exists).

**Recommended first batch (Phase 0):** five reusable foundations — money-grain badges, the confidence-aware match pill, an explain-this-figure popover, in-page CSV export + API discoverability, and a centralised safe-vocabulary module. All boundary-safe, no pipeline rebuilds, each unlocks several later themes.

---

## Empirical data-test — does it work out? (2026-06-28)

Every concrete claim and proposed build below was then **tested against the real registered views and gold facts** (six query clusters + a compiled verdict; the make-or-break numbers were re-run by hand). **Verdict: holds-with-corrections** — 18 of 20 tested claims PASS or PASS-with-a-caveat; the flagship features (match-confidence surfacing, AWARD/CEILING + SPENT/COMMITTED badges, per-CPV market map, buyer/incumbency dossier, appears-in-both differentiator, search corpus) are all backed by clean, non-degenerate data. But several **numbers in the first draft were wrong** and are corrected here; two items hit hard blockers.

| Claim as first written | Empirical reality (queried) | Action |
|---|---|---|
| "~22% bid coverage; SME-win-rate uncertain" | **Bid fill 78.4%** (49,225/62,763), `n_awarded_smes>0` 83.2%, median bids 5 — **SME-win-rate & median-bids ARE computable** | ✅ *Risk inverted.* The real hazard is **CPV: 67.2% of awards carry no real CPV code** (OGP 81% NULL, €3.95bn safe in the NULL bucket) |
| Name-collision false positives "are real" | **Inverted.** The 603 name collisions are correct same-entity merges (RPS Ltd/Limited; Deloitte &/and). The normaliser works. | Retarget at the **400 CRO `n_cro≥2` picks** (one arbitrary `company_num`; one key → **112** companies) |
| Renewal cadence "~193 clean pairs" | **276–278 pairs** (≥3 distinct award-years, framework/call-off/central-purchasing excluded) — re-confirmed 278 | Use **~276**, not 193. Highest-streak pairs are ~annual; surface multi-year-gap pairs as the "next opportunity" |
| must-fix #1: "two conflicting paid figures per firm" | **No conflict** — 1,508/1,509 agree to €1 (`cro_company_num` is 1:1 with `supplier_normalised`). | Rewrite as **"pick one keying + disclose coverage loss"** (59.7% name-only, 37.6% of overlap have no CRO) |
| value_kind=award implies summable | **Only 26.1% of award rows sum-safe**; safe **€15.6bn vs naive €649.7bn (~42×)**; all 17,964 ceilings + 28,395 award rows NOT safe | Badge/explain totals **must key off `value_safe_to_sum`, never `value_kind`** |
| TED→CRO tiers 7,079/1,860/4,805 | That's the **2024+ eForms lane only**; full registered feed = **9,664/10,940/16,402** (COUNT grain) | State the grain; never sum |
| National sector-watch CPV | **Dead in the live-tender lane** — `v_procurement_live_tenders` projects **no CPV column** and the source is 100% NULL (0/2,363) | **Drop from the live-tender lane**; national CPV only via awards/TED lanes (needs extractor work) |
| Data-state strip "payment date range" | Payments have **no day-level date** — period/year/quarter only | Render a **quarter string** ("2026-Q4"); freshest award 2026-03-31; grains 62,763 awards / 420,612 payments / 13,744 TED — never summed |
| Don't-rebuild median/IQR | ✅ Confirmed `v_procurement_cpv_summary` already ships median/p25/p75 | Add only **`n_buyers`** (one line) |
| must-fix #7: CRO-number search | ✅ Confirmed — `cro_company_num` NULL on all paid/authority/cpv branches, **64.4%** on supplier branch | A CRO-number search resolves **award-side only** |

### 🔒 New privacy blocker found by the test (must be in the export spec)
There is **no `supplier_class='person'`** — the privacy gate is **`public_display`**, and the name-keyed `v_procurement_payments` view **does not project it**. A name-keyed paid figure/export built off that view would leak **20,314 individual rows** (`sole_trader_or_individual` + `id_code`), of which **9,371 are SPENT+safe = €1.04bn**. *Any* in-app export or name-keyed paid panel **must go through a view that applies `WHERE public_display`** (the CRO-keyed `entity_chain` path already gates correctly). This makes "in-app CSV export" slightly less of a pure quick-win than first stated: the export builder must enforce the gate.

### Feasibility by phase (post-test)
🟢 **Phase 0 matching** · 🟢 **Phase 0/1 money badges** · 🟡 **Phase 2 buyer/market** (CPV-NULL hazard, fix the SME copy) · 🟡 **Phase 3 competition/renewal** (68.9% TED-buyer↔eTenders-authority key overlap → needs fuzzy/alias join + hard-fail empty state + filter foreign authorities like GEANT; renewal=276) · 🟡 **Phase 4 awards↔payments** (rewrite must-fix #1; enforce `public_display` on export) · 🔴 **national-CPV watch** (blocked on extractor work).

---

## What's already strong (do NOT rebuild)

- **Search-first hero** over a 40,848-row unified corpus (5 entity kinds), both money grains, with a hard never-sum guard ([procurement.py:3278](../utility/pages_code/procurement.py#L3278)).
- **Job-phrased, shareable IA**: `_SECTION_LABELS` reads as reader questions, `?tab=`-synced. 4 of 6 target jobs are already first-class sections.
- **Three-grain discipline** enforced in copy *and* structure: separate axes, separate labelled columns, "never added together" caveats; paid vs ordered never stacked.
- **No-inference vocabulary** already pervasive (~80% done): "appears in both registers", "framework ceiling", "a single bid is a recorded fact, not a verdict", "may be different legal entities — confirm via the CRO number".
- **Rich supplier panels already exist**: awards, year-trend, paid-supplier, TED, competition, incumbency/dependency relationships, EPA, corporate-distress, register-footprint.
- **Sum-safety is a queryable column**, not just prose: `coverage_stats` computes `n_award_rows` vs `n_safe_rows` via `SUM(... FILTER WHERE value_safe_to_sum)` — exactly what an explain-panel needs.
- **A deployed, privacy-filtered JSON API** (17 routers incl. procurement + `/v1/data` bulk export with embedded licence/attribution/never-sum caveats).
- **CPV median/IQR already computed** ([procurement_cpv_summary.sql:21-23](../sql_views/procurement/procurement_cpv_summary.sql#L21)) — do not rebuild (audit correction).

---

## Phase 0 — Cross-cutting foundations (boundary-safe, ready now)

Five reusable UI/copy primitives. Zero new figures, no pipeline rebuilds. Each is imported cross-page, so building them first prevents drift across the dossier work that follows.

1. **Money-grain badge vocabulary** (`money_badge(value, grain, value_kind)`), effort **L**.
   `pr-pill-val` is overloaded across award / paid / ordered / TED / and 7 non-money pills, so no colour carries a consistent meaning. Give distinct treatments to **AWARD / FRAMEWORK CEILING / PO COMMITTED / CASH PAID / EU (TED) AWARD / SOURCE AMOUNT**, make `_value_pill` consult `value_kind` (AWARD vs CEILING — already in gold: 17,964 ceiling vs 44,799 award rows), repoint `_paid_pill` to the existing solid/dashed `pr-pill-paid`/`pr-pill-ordered` classes the council cards already use, and move non-money pills to a neutral `pr-pill-stat`. Keep the verb/label as the non-colour accessibility carrier. **Touches `company.py` and `follow_the_money.py` too** (helpers are imported cross-page — *audit must-fix #5*). Clear `__pycache__` after CSS edits.

2. **Match-confidence taxonomy + confidence-aware CRO pill**, effort **M** *(the headline fix)*.
   Additive: add `c.match_confidence` to the SELECT in `procurement_supplier_summary.sql` (the view already LEFT-JOINs the match parquet; the column is already persisted). Thread `match_confidence`/`cro_match_method` through `_SUPPLIER_COLS` + the thin data_access wrappers; replace the binary `_cro_pill`/`_cro_pill_from` with a tier-aware pill (safe fallback to binary when the column is absent). **Update `company.py:220-221` in the same change** or the canonical dossier keeps the binary pill (*must-fix #5*). No rebuild of the match itself.

3. **Explain-this-figure panel** (`explain_figure(label, value, FigureProvenance)` via `st.popover`), effort **L**.
   No per-number provenance affordance exists anywhere (`st.popover` unused). Add an inert `FigureProvenance` dataclass beside `QueryResult`; the popover **reports** (never derives) source view, filters applied, sum-safety verdict, excluded-row count, caveat, source link. Populate the 4 highest-value figures first (lede sum-safe total; supplier awarded; paid-supplier SPENT/COMMITTED; TED EU value with "ceiling-not-spend / never-sum-with-national"). **Guardrail:** `filter_label` must mirror the live SQL `WHERE` verbatim or it becomes a false claim.

4. **In-page CSV export + API discoverability**, effort **M** *(highest value-to-effort in the audit)*.
   Build `*_export` QueryResult builders in **data_access** (not the page) that select value-safe columns + a literal caveat column + source/licence + `value_kind`, and explicitly forbid a pre-summed sum-unsafe total. **Privacy gate (mandatory, found by the data-test):** any export touching the name-keyed payments path must run through a view that applies **`WHERE public_display`** — `v_procurement_payments` does **not** project it, and an unguarded name-keyed paid export leaks **20,314 individual rows / €1.04bn SPENT+safe**. Page only calls the existing `export_button`. Reuse the exact caveat strings from `api/routers/exports.py`. Splice the env-gated `api_json_link` ("This supplier as JSON", "Bulk data & API") into profiles/footer — it renders nothing until `DAIL_API_BASE_URL` is set, so it's safe to add unconditionally.

5. **Safe-vocabulary constants module** (`utility/ui/safe_vocab.py`), effort **M**.
   The approved phrasing is re-typed as inline f-strings across ~10 panels, so it drifts and can't be audited. Centralise approved phrases + a FORBIDDEN list (influence / corruption / waste / rigged). Copy refactor only — **must never soften an existing disclaimer below current strength.**

---

## Phase 1 — Supplier dossier + awards-vs-payments

**Goal:** collapse the duplicate supplier page onto one canonical dossier and add the at-a-glance awarded-vs-paid comparison.

- **Unify the two supplier pages.** The same `?supplier=` URL renders differently by entry point: `procurement.py _render_supplier_profile` has register-footprint but no EPA/corporate-distress; `company.py _dossier` has the reverse. Pick one canonical page (`/company _dossier` recommended), **redirect** the legacy router (never 404 — preserve deep links), and **carry the gate** when porting register-footprint (it's CRO-matched + ≥2 registers — porting changes *which* suppliers see it; *audit correction*). Depends on Phase 0 badges + confidence pill.
- **Per-supplier charity/public-body panel** = a **pandas filter** of the existing whole-register `fetch_charity_overlap_result()` on `supplier_norm`. **No new view/query** (*must-fix #6* — downscope from the agent's original M estimate).
- **Side-by-side award-vs-paid card** with a neutral coverage-language divergence line — **no derived ratio, no euro "gap"** (a cross-grain ratio implies the two totals are comparable). Shared between `procurement.py` and `company.py`.
- **Pick one paid-figure keying and disclose coverage** (*must-fix #1, reframed by the data-test*): the two sources do **not** conflict — where both yield a figure they agree 1,508/1,509 to €1 (`cro_company_num` is 1:1 with `supplier_normalised`). The real issue is **coverage loss**: the CRO-keyed path (`v_procurement_entity_chain`) silently drops the **59.7% of award suppliers with no usable `company_num`** and 37.6% of name-overlap suppliers with no CRO match. **Recommend the name-keyed paid figure** (higher coverage), label the 400 ambiguous CRO matches low-confidence, and document the key. *(Privacy gate below is mandatory for any name-keyed paid figure.)*
- **Batch the view edit** (*must-fix #3*): adding `match_confidence` (Phase 0) and `in_payments`/`paid_safe_eur`/`committed_safe_eur` (here) both edit `procurement_supplier_summary.sql` + `_SUPPLIER_COLS`. Do them as **one coordinated view change** with a single fixture/test run. Three separate labelled columns, **no summed column**.

---

## Phase 2 — Buyer dossier + market maps

**Goal:** bring the thinnest profile (authority does only 4 things today) to parity with the supplier profile; turn the thin CPV ranking into a real market dossier.

- **Buyer-keyed query siblings** (`incumbency_for_authority`, `authority_year_trend`, `top_suppliers_for_authority`) + thin cached wrappers; rebuild `_render_authority_profile` to mirror the supplier panel set (top suppliers, category mix, recurring suppliers, single-bid rate, framework use, value trend, live tenders for this buyer, expiring contracts).
- **New registered GROUP BY views** (firewall requires aggregation in views, not the page): `v_procurement_authority_category_mix`, `v_procurement_authority_procedure_mix` — value-safe gated, awarded-not-paid framing, with test fixtures (use the `pipeline-view` skill).
- **CPV market dossier** — *audit correction (must-fix #4):* **keep the existing median/IQR**; only add `n_buyers` + avg/median **bids** (where-reported caveat) + `v_procurement_cpv_top_parties` (top suppliers/buyers + share = incumbent concentration) + per-CPV trend. Rebuild `_render_cpv_profile` into top buyers/suppliers (clickable), concentration line, SME participation **with a coverage caveat**, year sparkline, full award list — all within the AWARD grain.

---

## Phase 3 — Competition + incumbent/renewal intelligence

**Goal:** make single-bid concentration self-explaining, wire the built-but-unused per-buyer view, add a renewal-cadence lens.

- **Self-explaining single-bid cards**: add market-median, `comparison_label` ("above/around/below typical"), and `is_thin_sample` to the per-CPV competition output; copy the supplier-panel's sample-size guard + inline national baseline verbatim to the Patterns CPV cards.
- **Wire the per-buyer single-bid view** (`v_procurement_competition` — fully built, unused): add a fetch wrapper + a buyer competition panel. **BLOCKING prerequisite** (*must-fix #2 — promoted from soft caveat; quantified by the data-test*): only **68.9% of TED competition buyers (626/908) exactly match** an eTenders `contracting_authority` (casing is not the issue — it's name variants/paren-suffixes), so ~31% silently drop. Resolve via a normalised/fuzzy join or curated alias map first; until resolved the panel must **hard-fail to "no competition data for this buyer"** rather than show a misleading empty/zero panel. Also **filter non-Irish authorities** (the #1 competition buyer, GEANT, is Dutch — TED carries foreign bodies). Note: competition = TED eForms 2024+ vs authority_summary = eTenders 2013+ (**different grains, never compare/sum**).
- **Renewal cadence** (`v_procurement_renewal_cycle`): buyer × CPV cadence band + `next_opportunity_year_est`, reusing the existing framework/call-off/central-purchasing exclusions, ≥3 award-years, CPV-coverage caveat (**~276 clean pairs validated** — re-confirmed 278, not the earlier 193). UI framed as a **past-cadence estimate, not a schedule**; no money attached. Surface multi-year-gap pairs as the "next opportunity" — highest-streak pairs are ~annual (so "next year" is trivial, not a prediction).

---

## Phase 4 — Opportunity workflows, search, IA polish (no accounts needed)

The highest-value items achievable **without** per-user persistence.

- **URL-saveable search & filters** (`?q=` for the hero; write open-tender facet state to `query_params`; "Copy this view" affordance). "Saved search = shareable URL" is the honest near-term substitute for accounts.
- **Richer search**: make `cro_company_num` (exact-match branch) and the CPV `url_key` searchable; add a separate tender-title/keyword corpus branch (own grain, own ceiling caveat); group results by kind; attach the confidence pill. *Audit correction (must-fix #7):* `cro_company_num` is NULL on the `paid_supplier`/`paid_body` corpus branches, so a CRO-number query only resolves **award-side** suppliers — set that expectation in copy.
- **"Opportunities similar to past awards"**: CPV-overlap view + supplier-profile panel (TED open set only, division-level, labelled a **navigational suggestion, not a recommendation**).
- **Surface the two buried jobs** ("Check competition", "Analyse a market") one level up, and build the **"Verify a money claim"** job using `_lifecycle_strip` + `explain_figure`. **No section-bar re-cut, preserve `?tab=` keys** (they're shareable bookmarks).
- **Always-visible data-state strip**: last run, newest award date, payments period, per-grain row/entity counts, green/amber source chip from `fetch_failures.json` (read via a data_access payload reader; extend `check_freshness` with payments/TED entries).

---

## Phase 5 — Per-user persistence (the architectural lift) — GATED on owner sign-off

The single biggest constraint. The app is fully stateless/cookieless with no identity layer (`page_analytics` is explicitly anonymous). Watchlists / alerts / saved searches are **not merely unbuilt — they are infeasible** without new identity + persistence + PII (emails) + GDPR/consent + scheduler infrastructure. **Do not start autonomously.**

- Pipeline prerequisite: add CPV enrichment to the national eTenders live snapshot (currently 0/2,363 CPV-filled) to unblock national sector watch.
- Identity + watchlist/saved-search store + a scheduled digest job that diffs open/expiring tenders against saved CPVs/buyers and emails a digest.

---

## Quick wins (do these first within Phase 0)

1. **In-page CSV export** on value-safe frames — highest value-to-effort ratio in the whole audit.
2. **Add `match_confidence` to the summary view SELECT** — one additive line, unblocks the entire confidence-pill foundation, no rebuild.
3. **Splice the env-gated `api_json_link`** into profiles/footer — a deployed bulk API has zero discoverability today.
4. **Make hero search `?q=`-shareable** — search state survives refresh / is bookmarkable like the drilldowns already are.
5. **Repoint `_paid_pill`** to the existing solid/dashed paid/ordered classes — paid vs committed becomes visually distinct everywhere, reusing existing CSS.
6. **Make `cro_company_num` / CPV code searchable** (already corpus columns, just excluded) — a CRO-number/CPV query stops silently returning zero.
7. **Wire the buyer single-bid panel** — a complete, correct view (`v_procurement_competition`) is sitting unused (gated on the TED↔eTenders name reconciliation above).

---

## ⚠️ User-domain decisions (require your explicit sign-off — not autonomous)

1. **Match-confidence taxonomy & wording**: the tier labels (HARD / exact-name-unique / exact-name-ambiguous / name-only-cross-register); show the raw 0.9/0.5 number or only a categorical tier; and whether ambiguous supplier→CRO matches keep showing an arbitrary `company_num` **with a caveat** or are **suppressed** (as `corporate_cro_match.sql` does). An ambiguous match must read as a "probable lead", never a verified identity.
2. **Exact money-badge wording**: "Award value" vs "Awarded", "Framework ceiling", "Cash paid", "PO committed", "EU award value", "Source amount only"; whether to add an explicit "Not safe to sum" chip on CEILING/TED.
3. **SME win-rate definition** for the market map (award-side vs bid-side vs supplier-side). *(Data-test correction: bid-count fill is **78.4%**, not ~22% — SME-win-rate and median-bids are computable; scope them to competitive procedures and disclose the **67.2% NULL-CPV** hazard that hollows out per-CPV/category lenses for national buyers like OGP.)* Different denominators → different numbers.
4. **Whether to show ANY derived divergence metric** on the awards-vs-payments card (recommendation: two figures side-by-side, neutral copy, **no ratio**).
5. **Whether the static completeness figures** (~3%/~7%/90%+, ~7%-of-national-spend) become **live recomputed %** or stay cited point-in-time prose. (Provenance/estimate claims — no auto-promotion.)
6. **Competition thresholds & renewal regularity**: the "above/around/below typical" cut-points, the thin-sample floor (and whether to lower `min_lots` and disclose vs keep the silent 100-lot filter), and whether to emit a specific `next_opportunity_year` vs only a cadence band.
7. **Whether to build accounts/persistence/alerting at all** (Phase 5) + the national eTenders CPV enrichment promotion — new PII/consent/GDPR infra + a pipeline/promotion call.
8. **View-only vs gold promotion** for the new authority/CPV/renewal aggregates (no new gold parquet is needed for the core plan), and whether the FORBIDDEN-vocabulary list is CI-enforced or advisory.

---

## Boundaries & risks (the never-break rails)

- **Three-grain never-sum** is the dominant risk across badges, exports, the data-state strip, the awards-vs-payments card, and the explain panel. AWARD/ceiling (eTenders + TED), PAYMENT (public/LA payments; SPENT vs COMMITTED also never blended), and BUDGET (AFS) always render as **separate labelled cells/columns** — never a combined "total records" or euro figure, never a stacked bar (stacking reads as a sum), never union TED with national.
- **Logic firewall**: all aggregation/JOIN/transform lives in registered SQL views or the query layer, never the page. New GROUP BY views need registration + test fixtures; run `tools/check_streamlit_logic_firewall.py` after every page touch.
- **Provenance / no-inference**: every new label REPORTS existing computed values, never invents one. Single-bid/incumbency/dependency/renewal are structure facts that keep the "never a verdict / often legitimate" caveat verbatim.
- **Match ambiguity is the real false-positive risk — but it's in the CRO match, not the name normaliser** (corrected by the data-test). The `name_norm` collisions (603 keys → ≥2 raw names) are *correct* same-entity merges (RPS Ltd/Limited). The genuine ambiguity is the **400 supplier→CRO matches with `n_cro≥2`** where the shown `company_num` is one arbitrary pick (one key → 112 companies). Surfacing `match_confidence`/`n_cro` is the mitigation.
- **Privacy**: `paid_supplier` is gated to `supplier_class='company'`; any in-app export or new payment-line search must inherit the same person-row quarantine.
- **Coverage honesty**: renewal cadence and "similar opportunities" are structurally bounded (CPV fill ~32.8%; national eTenders 0% CPV today) and ship with the partial-coverage caveat. Absence from the payment register is **coverage** (~7% of spend visible), NOT hidden money.
- **Churn risk**: do **not** re-cut the job-phrased section bar or rename `?tab=` keys; improvements are additive. Collapse the two supplier pages by **redirect, not 404**.

---

## Appendix — per-theme status (validated)

| # | Theme | Pri | Δ-effort | Current state (validated) |
|---|---|---|---|---|
| 01 | Supplier dossier | P1 | M | Mostly exists but split across two divergent pages on the same `?supplier=` key; neither has the full ingredient set. |
| 02 | Buyer dossier | P1 | L | Thinnest profile — `_render_authority_profile` does only 4 things; no authority-keyed incumbency/competition siblings. |
| 03 | Money-language badges | P1 | L | Partial — data supports it; one good distinction exists but `pr-pill-val` is overloaded; no framework-ceiling pill variant. |
| 04 | Match confidence | P1 | M | **Signal exists end-to-end; surfaced nowhere.** 400 ambiguous = firm in the UI. *The headline fix.* |
| 05 | Opportunity intel | P2 | L | Open-tender browse exists; all personalisation (watchlist/alert/saved) blocked on no per-user persistence. |
| 06 | Market maps | P2 | L | Thin CPV ranking; median/IQR already built; missing top-suppliers/top-buyers per CPV + n_buyers. |
| 07 | Competition explainability | P1 | L | Largely exists; supplier panel is the model citizen; CPV cards lack per-card baseline; buyer side absent. |
| 08 | Incumbent / renewal | P2 | M | ~70% built (incumbency/dependency/expiring); missing forward renewal-cadence + per-buyer "current incumbent". |
| 09 | Awards vs payments | P1 | M | Both grains surfaced but as sequential panels — no single at-a-glance side-by-side comparison. |
| 10 | Freshness / coverage | P1 | M | Partial/static; no UI for failed sources (`fetch_failures.json` read only by MCP/pipeline). |
| 11 | Export / API | P1 | M | Ingredients ~complete (shared `export_button`, deployed API) but procurement uses **none**. |
| 12 | Unified search | P1 | L | Real search-first hero over 5 kinds; number-led (CRO/CPV) queries silently fail; no grouping/confidence. |
| 13 | Explain panels | P1 | L | None anywhere (`st.popover` unused) — build from scratch as a shared helper. |
| 14 | User-jobs IA | P2 | M | ~70% shipped (job-phrased sections); missing the new "Verify a money claim" job. |
| 15 | No-inference vocab | P2 | M | ~80% done; two too-timid pills (incl. the CRO confidence one) + copy not centralised. |

*Generated from a 15-theme validation audit + adversarial review, 2026-06-28. Phase 0 is implementable on greenlight; everything touching match-confidence wording, money-grain labels, SME denominators, live-vs-cited figures, accounts, and gold promotion needs your sign-off.*
