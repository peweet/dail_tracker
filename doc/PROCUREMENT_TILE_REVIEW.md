# Procurement Tile Review — `dail_tracker_local_housing_procurement_judiciary_plan.md`

**Reviewer pass:** 2026-06-05. Scope: the **Public Money & Procurement** tile only (sec 0–3 Tile 1, sec 6 procurement contracts, sec 7 procurement views, sec 8 procurement tests, sec 10–14 procurement parts). Housing/Judiciary covered by sibling reviews. Every repo claim ground-truthed against real code; cites are `path:line`.

---

## Claims Ledger

| claim | doc says | repo reality (path:line) | verdict |
|---|---|---|---|
| procurement chains in pipeline | `afs`,`procurement`,`procurement_lobbying`,`ted` | `pipeline.py:64,76,81,85` — exactly those four | confirmed |
| `procurement_etenders_extract.py` builds awards + CRO match | yes | `extractors/procurement_etenders_extract.py:55-57` writes `procurement_awards.parquet` + `procurement_supplier_cro_match.parquet` + coverage | confirmed |
| extractor flags award≠spend | yes | `procurement_etenders_extract.py:182-231` full value-semantics block; `value_safe_to_sum:223-230` | confirmed |
| sole traders quarantined from CRO | yes | `procurement_etenders_extract.py:175-180` (`supplier_class`), `246-249` (company-only join), `276-280` | confirmed |
| frameworks/DPS/multi-supplier/`value_safe_to_sum` flagged | yes | `procurement_etenders_extract.py:193,207-231` | confirmed |
| `procurement_la_seed.py` seeds all 31 LAs, documents format drift | yes | `extractors/procurement_la_seed.py:46-114` (SEEDS list), kinds note XLSX/CSV/PDF/JS/stale | confirmed |
| `procurement_la_payments_extract.py` builds 31-council fact, writes silver, **not** in pipeline | yes | `extractors/procurement_la_payments_extract.py:62` `OUT_FACT=SILVER_PARQUET_DIR/la_payments_fact.parquet`; absent from `pipeline.py` CHAINS | confirmed |
| LA fact aligns to public-payments schema, distinguishes po_committed/payment_actual, applies privacy | yes | `procurement_la_payments_extract.py:15-19` (union note), `222` TIER map, `581-617` `classify_and_flag` quarantine **applied** | confirmed |
| `procurement_public_body_extract.py` writes gold-candidate sandbox, defers quarantine | yes | `procurement_public_body_extract.py:57` sandbox path, `728` `public_display=True`, cov `840` `privacy_quarantine_applied=False` | confirmed |
| `ted_ireland_extract.py` is silver, cleaned, not UI-exposed | yes | `extractors/ted_ireland_extract.py:53` `OUT_SILVER=…/ted_ie_awards.parquet`; `pipeline.py:85,112` "silver, not yet UI" | confirmed |
| five `sql_views/procurement_*.sql` exist (awards/supplier/authority/cpv/lobbying_overlap) | yes | `sql_views/` glob returns exactly those five | confirmed |
| `sql_views/lobbying_org_procurement.sql` exists | listed in §1 | NOT in `sql_views/procurement_*` glob; doc's own §7 calls it `v_lobbying_org_procurement` (lobbying-side, separate file). Minor mis-grouping | stale |
| `dail_tracker_core/queries/procurement.py` + `utility/data_access/procurement_data.py` exist | yes | both present and read | confirmed |
| **data-access unwraps QueryResult to empty DataFrame on failure** | yes (§P1) | `utility/data_access/procurement_data.py:39,45,51,57,64` all `.data`; **but** core already returns 3-state `QueryResult` (`queries/procurement.py:33-37`, `dail_tracker_core/results.py`) | confirmed-but-understated |
| no top-nav Procurement page | yes | `utility/pages_code/procurement*.py` glob → none | confirmed |
| `award_spend_link` produces candidate links not definitive | §P3 | `extractors/procurement_award_spend_link.py` exists, writes sandbox; it is an **entity-level JOIN/aggregate**, not row-level candidate matching | partially-wrong (see below) |

---

## Architectural Assessment

**The procurement backbone is real and shippable.** All three gold parquets exist on disk (`data/gold/parquet/procurement_awards.parquet` 2.1 MB, `…_cro_match.parquet`, `…_lobbying_overlap.parquet`, built 2026-06-05), the five views read them, the core query layer returns typed `QueryResult`, and the Streamlit wrapper is a thin `.data` adapter. The doc's central claim — "backend mature enough for a first UI skeleton, page is the missing piece" — is **correct**.

**The doc materially understates how much of "P1 — Core query contract improvement" is already done.** It says `procurement_data.py` "unwraps `QueryResult` to an empty dataframe on source failure. For a public page, that is not enough." Ground truth: the *core* already implements the exact 3-state model the doc proposes (`results.py:33-67` ok / no-rows / unavailable; `queries/procurement.py:33-37` turns any DuckDB error into `QueryResult.unavailable`). What's missing is only that the wrapper discards `ok`/`unavailable_reason` by calling `.data`. So the recommended `fetch_supplier_summary_result() -> QueryResult` is a **~10-line additive wrapper change**, not a contract redesign — and it's the correct minimal change. The doc's framing would lead a builder to over-scope this.

**`value_kind_legend` as a new `utility/ui/value_kind_legend.py` module duplicates existing primitives and the uncoupling intent.** `utility/ui/components.py` already has `glossary_strip(terms)` (`:299`), `totals_strip` (`:318`), `hero_banner` (`:281`), `empty_state` (`:451`), and a `page_error_boundary` (`:76`). A value-kind legend is a `glossary_strip` call with a fixed term list — it belongs as a small helper *in components.py* (or simply called inline), not a new top-level module. All CSS must live in `shared_css.py` per project convention; a standalone component risks drifting from that. **Reject the new module; reuse `glossary_strip`.**

**The proposed legend term list leaks vocabulary the awards feed does not emit.** The doc's legend lists `payment_actual`, `po_committed`, `budget_allocated`, `afs_expenditure`, `grant_or_subvention` alongside the award kinds. But `v_procurement_awards` only carries `value_kind ∈ {framework_or_dps_ceiling, framework_call_off, contract_award_value}` (`procurement_etenders_extract.py:220-222`). Showing payment/budget/grant rows in a legend on an awards-only page is misleading — it implies the page covers spend it doesn't. The legend on the **first** page must be scoped to the three award kinds only; the fuller taxonomy belongs on a later payments tab, gated by what's actually rendered.

**Value-taxonomy enforceability is genuinely strong, and is *in the data*, not the view.** `value_safe_to_sum` is computed once in the extractor (`procurement_etenders_extract.py:223-230`) and every summary view sums `… FILTER (WHERE value_safe_to_sum)` (`procurement_supplier_summary.sql:20`, `authority_summary.sql:10`, `cpv_summary.sql:11`). A UI page that reads `awarded_value_safe_eur` literally *cannot* surface a naive total — the unsafe rows are excluded upstream. This is the firewall working as designed. The page should never compute its own sum; it reads the pre-gated column.

**Minor staleness:** view headers cite `pipeline_sandbox/procurement_etenders_extract.py` (`procurement_awards.sql:2-3`) but the file now lives in `extractors/`. Cosmetic, but worth a sweep when the page lands.

**Award→payment "candidate matching" (§P3) does not match what `award_spend_link.py` does.** The doc's §P3 schema (`amount_similarity`, `date_window_days`, `description_similarity`, `match_confidence`, `source_award_id`, `source_payment_id`, `review_status`) describes a **row-level fuzzy candidate matcher**. The actual `extractors/procurement_award_spend_link.py` is an **entity-level aggregate join** keyed on CRO number / normalised name (`:99-131`), producing one row per supplier-entity with `realised_spend_eur` vs `total_award_eur` and a `spend_to_award_ratio` (`:154-161`). It is deliberately a JOIN, never a sum (`:6-7`, `:198-202`), and it depends on **sandbox** spend facts (`public_payments_fact` etc.) that are not promoted. So §P3 isn't "build this later" — a *different, simpler-grained* version already exists in sandbox. It should not ship to UI: it joins un-promoted, quarantine-deferred sandbox spend (`public_body_extract.py:728`) and would put an award-vs-spend ratio in front of users before the spend side has had a privacy pass.

---

## Devil's Advocate

- **Award-as-spend.** Mitigated *in the data layer* — `awarded_value_safe_eur` already excludes ceilings/repeats/sub-€1. Residual risk is **copy**, not numbers: any headline must read "awarded value, not actual spend" (extractor caveat `:308-315`), and the page should lead with count metrics (`n_awards`) which the views correctly call "the trustworthy metric" (`procurement_supplier_summary.sql:11`). Do not show a single euro figure without the inline caveat.
- **Lobbying-overlap-as-causation.** The view header is explicit: co-occurrence by entity, "NOT evidence lobbying influenced any award — there is no key linking a specific lobby to a specific contract" (`procurement_lobbying_overlap.sql:10-13`). The risk is entirely a UI-copy risk. The doc's banned-words list (§Required UI copy: no "Influenced/Bought/Conflict/Corruption") is correct and must be enforced in `civic-ui-review`. Acceptable copy: "appears on both the procurement and lobbying registers."
- **Sole-trader PII.** Awards path is safe — `sole_trader_or_individual` is quarantined from CRO match and the supplier-summary view filters `supplier_class = 'company'` (`procurement_supplier_summary.sql:22`), so individuals never reach the ranking. **But** the awards *detail* view `v_procurement_awards` carries every class (`procurement_awards.sql` has no class filter). If the page renders `awards_for_supplier`, it's keyed by `supplier_norm` from the company-class summary, so it's fine — *as long as the page never lets a user reach an individual's `supplier_norm`*. Worth a smoke test asserting no `sole_trader_or_individual` row is reachable.
- **False candidate links.** Real risk in `award_spend_link.py`: name-keyed entities (no CRO match) "may still be the same firm under a variant spelling" (`:200-202`), and the spend side pulls **quarantine-deferred** sandbox facts. Shipping a ratio that says "won €X, paid €Y" off probabilistic joins is exactly the kind of inference the project forbids in UI copy. **Defer hard.**
- **Shipping a tile before pages are hardened.** Real concern per `feedback_refactor_timing` / the audit backlog — existing pages (attendance/payments/lobbying) still carry open P0/P1 audit residue. A *read-only* procurement page over existing gold views adds little maintenance surface (no new extractor, no new parquet, no pipeline change) and is the cheapest possible addition. The risk is **scope creep** (payments tab, LA promotion, award-spend ratio) bolted on in the same sprint. Hold the first slice to read-only over the five existing views.
- **31-LA format drift.** The seed registry honestly documents it (`procurement_la_seed.py:46-114`: PLAYWRIGHT/NEEDS-RENDER/NON-PUBLISHER/stale-aggregate states) and the payments extractor has per-council `status` gating + an `aggregate_guard` + a content-validity gate (`procurement_la_payments_extract.py:570-573`). But this is a *silver* prototype with `parser_version="0.1.0"` and no schema/coverage tests in CI. Promoting it now would import 31 independent drift surfaces into a public page. **Not for the first slice.**

---

## Data Quality & Enrichments

- **eTenders awards (gold):** mature. Value semantics, name-truncation repair, CRO match, lobbying overlap all present and tested-in-extractor. Ready to surface.
- **LA payments (silver, on disk, not wired):** strong prototype; quarantine *is* applied here (unlike public-body). Needs: schema test, 31-council coverage JSON consumed by UI, freshness, and a `v_publicmoney_la_payments` view before any display. The doc's §P2 promotion checklist is sound and the recommended view name (`v_publicmoney_la_payments`, avoiding "spend" because some councils publish POs) is the right call (`la_payments_extract.py:222` TIER map proves the committed/spent split is real).
- **Public-body payments (sandbox):** quarantine **deferred** (`public_body_extract.py:728`, cov `privacy_quarantine_applied=False`). **Must not** reach UI until a quarantine pass runs. The doc correctly keeps this in "Future: payments/spend tab."
- **TED (silver):** value gated (`ted_ireland_extract.py:303`), winner→CRO, not UI-exposed. Fine as enrichment/cross-reference; no first-slice need.
- **award_spend_link (sandbox):** entity-level join, depends on un-promoted facts. Useful analytic artefact; not UI-ready.

---

## Build / Defer / Reject

| item | verdict | value/effort | reason |
|---|---|---:|---|
| `utility/pages_code/procurement.py` read-only page over the 5 existing views (supplier/authority/CPV/lobbying-overlap + caveat panel) | **BUILD** | High / Low | All gold + views + core queries exist today; no pipeline/extractor/parquet change. This is the correct minimal first slice. |
| `fetch_*_result() -> QueryResult` additive wrappers in `procurement_data.py` | **BUILD** | Med / Low | Core is already 3-state; only the wrapper discards it. ~10 lines, enables a real "source unavailable" state. Keep existing `.data` wrappers for back-compat. |
| Value-kind legend via existing `glossary_strip`, scoped to the 3 award kinds | **BUILD** | Med / Low | Reuses `components.py:299`; do **not** create `utility/ui/value_kind_legend.py`. |
| `test/test_procurement_page_smoke.py` (imports / missing-parquet→unavailable / zero-rows / no individual reachable) | **BUILD** | Med / Low | Cheap guardrail; the privacy reachability assert is the load-bearing one. |
| `value_kind_legend.py` as a **new top-level module** | **REJECT** | — | Duplicates `glossary_strip`/`totals_strip`; CSS-in-component violates `shared_css.py` convention. |
| Full payments/spend tab on first page (public-body + LA + HSE/Tusla) | **DEFER** | High / High | Sandbox facts, quarantine deferred for public-body; mixes grains; do after promotion. |
| LA-payments promotion (`v_publicmoney_la_payments`, gold, tests) | **DEFER** | High / High | Correct path in §P2; needs schema+coverage+privacy tests + freshness first. 31 drift surfaces. Not the first slice. |
| `procurement_award_spend_link` → UI (ratio/candidate links) | **DEFER (and re-spec)** | Med / High | Existing file is entity-join, not §P3 row-matcher; depends on un-promoted, quarantine-deferred sandbox spend; ratio = inference risk. Keep as analytic-only. |
| TED surfaced to UI | **DEFER** | Low / Med | Silver enrichment; no first-slice need; keep as cross-reference. |
| Legend term list as written (payment/budget/grant/afs kinds on an awards page) | **REJECT (scope to data)** | — | Awards feed emits only 3 `value_kind` values; showing spend kinds misleads. |
| Sweep stale `pipeline_sandbox/` paths in view headers | **BUILD (trivial)** | Low / Low | `procurement_awards.sql:2-3` etc. now point at moved files. |

---

## Bottom Line

The procurement backend is the most UI-ready surface in this brief: gold awards, five registered views, a typed `QueryResult` core, and a thin data-access layer all exist and the parquets are on disk. The minimal correct first slice is exactly what the doc's Sprint 1 proposes — a **read-only `procurement.py` over the five existing views with a caveat/legend panel** — and nothing more: no new extractor, no pipeline change, no parquet write. Two doc framings need correcting before a builder picks this up: (1) the "QueryResult-aware" change is already 90% done in core and is a ~10-line additive wrapper, not a contract redesign; and (2) `value_kind_legend` should reuse the existing `glossary_strip` primitive (scoped to the three award value-kinds the feed actually emits) rather than become a new module. Defer the entire payments/spend half — LA promotion, public-body sandbox, and the award→spend link — because they ride on un-promoted, partly quarantine-deferred sandbox facts and would import inference risk and 31 LA drift surfaces into a public page before those have had schema, coverage, and privacy hardening. Build the read-only awards explorer; hold everything money-grained-beyond-awards.
