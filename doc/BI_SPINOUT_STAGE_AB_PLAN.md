# BI Spinout — Stage A/B Build-Ready Execution Plan

| | |
|---|---|
| **Date** | 2026-07-08 |
| **Status** | RESOLVED (reports-first, 2026-06-28) → build-ready execution plan |
| **Owner** | Away — three OPTION-gated decisions below are staged for owner sign-off, not pre-decided |
| **Scope** | Stage A = build the Phase-0 trust-UI primitives; Stage B = reports-first operations for 3–5 paying design partners |
| **Cross-links** | [doc/BI_SPINOUT_ARCHITECTURE.md](doc/BI_SPINOUT_ARCHITECTURE.md) (§4 ethics firewall, §6 licensing, §10 no-list, §15 resolved decisions), [doc/PROCUREMENT_INTELLIGENCE_ROADMAP.md](doc/PROCUREMENT_INTELLIGENCE_ROADMAP.md) (Phase 0, lines 71–89) |
| **Report templates (companion deliverables)** | [doc/templates/BI_SUPPLIER_REPORT_TEMPLATE.md](doc/templates/BI_SUPPLIER_REPORT_TEMPLATE.md), [doc/templates/BI_BUYER_REPORT_TEMPLATE.md](doc/templates/BI_BUYER_REPORT_TEMPLATE.md), [doc/templates/BI_CATEGORY_MARKET_MAP_TEMPLATE.md](doc/templates/BI_CATEGORY_MARKET_MAP_TEMPLATE.md) |
| **Ground-truth basis** | Verified against `main` @ `4027d6e`. Roadmap line refs have drifted since 2026-06-28 — trust symbol names, re-locate by grep. Two stale roadmap claims corrected inline (`st.popover` now used; match-confidence is a two-view edit). |

> **Note on file references below.** Line numbers are the verified positions at the time of ground-truthing. They drift. Every reference names the **symbol** as well — re-locate by symbol, not by line.

---

# PART 1 — STAGE A: the Phase-0 trust-UI primitives

These five primitives are the trust UI the whole paid product leans on: they are what makes a number defensible when a design partner challenges it. Build them **before** the first report ships, because the reports render through them.

## Primitive 1 — Money-grain badge vocabulary: `money_badge(value, grain, value_kind)`

**What it is.** A single helper that centralises the scattered per-shape pill logic so every money figure carries an explicit grain label and never reads as summable when it is not. Today `pr-pill-val` is overloaded across award / paid / ordered / TED / est-value / date / percent contexts, and the ceiling-enum branch (`value_kind == "framework_or_dps_ceiling"`) is duplicated inline in ~5 places.

**Exact files + symbols to touch**

| File | Symbol / location | Action |
|---|---|---|
| [utility/pages_code/procurement.py](utility/pages_code/procurement.py) | `_value_pill` (:392) — hardcodes `pr-pill-val` + `" awarded"` | route through `money_badge` |
| | `_paid_pill` (:767) + `_paid_verb` (:763) — currently emits `pr-pill-val` | repoint to existing `pr-pill-paid` / `pr-pill-ordered` |
| | `_ted_value_pill` (:1945) — `pr-pill-val` + `" awarded (EU)"` | route through `money_badge` |
| | inline `pr-pill-val` spans: 2107, 2684, 2770, 2948, 3026, 3384, 3414, 3492, 3617, 3674, 3698, 3726, 3751; council-card pattern to COPY at 1007–1009 (already uses paid/ordered) | reclassify |
| | inline ceiling branch: 2339, 2589 (`parent_value_kind`), 2773, 3051, 3560 | centralise into `money_badge` |
| [utility/shared_css.py](utility/shared_css.py) | `.pr-pill-val` (:5730); `.pr-pill-paid` (:5939), `.pr-pill-ordered` (:5941) already exist | **`.pr-pill-stat` does NOT exist — net-new CSS** |

**Blast radius (rebuild in the same change or they visually drift):**
- [utility/pages_code/company.py](utility/pages_code/company.py) — imports `_value_pill` (:54), uses at 249, 364.
- [utility/pages_code/follow_the_money.py](utility/pages_code/follow_the_money.py) — imports `_paid_pill` (:49), uses at 285, 287, 343, 405.
- **Undocumented 4th consumer** (roadmap names only the two above): [utility/pages_code/public_payments.py](utility/pages_code/public_payments.py) has its **own** local `_value_pill(val, sem)` (:184) + inline `pr-pill-val` spans (223, 286, 316, 677, 711). It shares the CSS class and will diverge if `.pr-pill-val` is restyled.

**`value_kind` literal enum in gold** (`data/gold/parquet/procurement_awards.parquet`) — the badge branches on these, not on "AWARD"/"CEILING":

| value_kind | value_safe_to_sum | rows |
|---|---|---|
| `contract_award_value` | False | 28,395 |
| `contract_award_value` | True | 16,404 |
| `framework_or_dps_ceiling` | False | 17,964 |

"AWARD" = `contract_award_value` (44,799); "CEILING" = `framework_or_dps_ceiling` (17,964). **The summable subset is only the 16,404 `value_safe_to_sum=True` rows — `value_kind` alone is not sum-safety.**

**Effort: L** (copy/CSS + repoint; no query change). **Caveat:** `.pr-pill-paid` is currently visually *identical* to `.pr-pill-val`, so repointing `_paid_pill` changes semantics but not appearance until the class is restyled.

**Dependencies:** none — pure UI/CSS. Should consume Primitive 5's vocabulary (build order below).

**OWNER-DECISION GATE — badge wording (present, do not decide):**

| Option | Award | Paid | Committed | TED | Ceiling |
|---|---|---|---|---|---|
| **A — verb-led** | "Awarded" | "Cash paid" | "PO committed" | "EU award value" | "Framework ceiling" |
| **B — noun-led** | "Award value" | "Payment" | "Commitment" | "EU award value" | "Framework ceiling" |
| **Sub-gate** | Add an explicit **"Not safe to sum"** chip on CEILING and TED badges? (yes / no) | | | | |

Owner signs the string table and the chip decision. Do not invent it.

**Boundary rails:** three-grain never-sum (AWARD/ceiling vs PAID vs COMMITTED as separate labelled cells); the **verb/label is the non-colour accessibility carrier**; ceiling and TED never summed with national. **Clear `__pycache__` after the CSS edit** (memory: 1.58 widget theming).

---

## Primitive 2 — Match-confidence pill

**What it is.** Surface the CRO match-confidence signal that already exists end-to-end in gold but is rendered nowhere: every CRO pill today is **binary** (matched / not). An ambiguous match must read as "probable lead", never verified identity.

**This is a TWO-VIEW edit (roadmap names one — correction):**

| File | Location | Action |
|---|---|---|
| [sql_views/procurement/procurement_supplier_summary.sql](sql_views/procurement/procurement_supplier_summary.sql) | LEFT-JOINs match parquet (:64–65), selects `c.match_method AS cro_match_method` (:53); does **not** select `match_confidence` / `n_cro` | one-line additive SELECT |
| [sql_views/procurement/procurement_supplier_year_summary.sql](sql_views/procurement/procurement_supplier_year_summary.sql) | independently LEFT-JOINs same parquet (:57), selects `cro_match_method` (:51) | **same additive SELECT — required** |
| [dail_tracker_core/queries/procurement.py](dail_tracker_core/queries/procurement.py) | `_SUPPLIER_COLS` (:48–53) — consumed against **both** views (v_..._summary at :73 **and** v_..._year_summary at :75) | add `match_confidence` (+ ideally `n_cro`) |
| [utility/data_access/procurement_data.py](utility/data_access/procurement_data.py) | `fetch_supplier_summary_result` (:153–156) thin pass-through | **no change** |

> **Load-bearing correction:** adding `match_confidence` to `_SUPPLIER_COLS` **breaks the year path** unless `procurement_supplier_year_summary.sql` also projects it — hence the two-file edit.

**Pills to repoint:**
- `_cro_pill(row)` (procurement.py:417) — binary; and the canonical dossier pill at [company.py](utility/pages_code/company.py):250 calls it (imported :41). *(Roadmap's "company.py:220-221" is now `_lobby_pill_for`; the real CRO pill is :250.)*
- `_cro_pill_from(company_num, status)` (procurement.py:2221) — binary; used by follow_the_money.py:346 and procurement.py 914/1525/1609/2200/2423.

**Confidence values in gold** (`procurement_supplier_cro_match.parquet`; cols: `supplier, supplier_norm, n_cro, company_num, company_status, match_method, match_confidence`):

| match_method | match_confidence | rows |
|---|---|---|
| `exact_unique` | 0.9 | 6,067 |
| `no_match` | 0.0 | 3,523 |
| `exact_ambiguous` | 0.5 | 402 |

`n_cro` is present in the parquet but, like `match_confidence`, is **not** in either view SELECT — surfacing the ambiguity signal needs the same additive edit.

**Effort: M.** **Dependencies:** the two-view SELECT edit + `_SUPPLIER_COLS` + a fixture regeneration run ([test/fixtures/sql_views/_generate.py](test/fixtures/sql_views/_generate.py):1074 already stubs `match_confidence: [0.9,...]`). **Batching opportunity:** the roadmap (Phase 1, must-fix #3) says to batch this with the `in_payments` / `paid_safe_eur` column adds — same two view files + `_SUPPLIER_COLS`, one coordinated change, single fixture run.

**OWNER-DECISION GATE — tier labels + ambiguity policy (present, do not decide):**

| Option | Label scheme | Raw number? |
|---|---|---|
| **A — categorical only** | "Verified match" / "Probable match" / "Ambiguous — unconfirmed" / "No CRO match" | hidden |
| **B — categorical + raw** | as A, plus the `0.9` / `0.5` / `0.0` shown in tooltip/popover | shown on hover |
| **C — method-named** | mirrors gold: "exact-name-unique" / "exact-name-ambiguous" / "no-match" | optional |
| **Ambiguity sub-gate** | (i) **suppress** `company_num` when `n_cro ≥ 2` (as `corporate_cro_match.sql` does), **or** (ii) **show** one `company_num` with a "probable lead — not a verified identity" caveat | |

Owner signs the wording **and** the suppress-vs-caveat policy.

**Boundary rails:** provenance / no-inference — the pill **reports the stored `match_confidence`, never derives one**; ambiguity is the real false-positive risk (one normalised key → up to 112 companies).

---

## Primitive 3 — Explain-this-figure popover: `explain_figure(label, value, FigureProvenance)`

**What it is.** A popover that, for a headline money figure, reports source view · live filters · sum-safety verdict · excluded-row count · caveat · source link. The popover **reports, never derives**.

**Stale roadmap claim corrected:** the roadmap asserts `st.popover` is "unused anywhere." It is **now used once**, at [procurement.py](utility/pages_code/procurement.py):558 (`with st.popover("ⓘ How is this adjusted?")`), inside the experimental inflation note — gated local-only (line 568 renders "Experimental · local only — not shown in the published app"). So in the **published** app it is still effectively unused, and this now serves as a **working precedent** for the pattern (metadata dict → popover). No `explain_figure` helper and no `FigureProvenance` exist.

**Exact files + symbols to touch**

| File | Symbol | Action |
|---|---|---|
| [dail_tracker_core/results.py](dail_tracker_core/results.py) | `QueryResult` frozen dataclass (:33–67) | add `FigureProvenance` beside it — same frozen / inert / cacheable pattern |
| [dail_tracker_core/caveats.py](dail_tracker_core/caveats.py) | existing caveat constants | reuse for the caveat field — do not write new prose |
| new UI module under [utility/ui/](utility/ui/) | `explain_figure(label, value, FigureProvenance)` | consumed by procurement.py and company.py |

**Four highest-value target figures** (all in procurement.py): `_page_lede` sum-safe total; `_value_pill` supplier awarded pill; `_render_paid_supplier_panel` SPENT/COMMITTED panel; `_render_ted_supplier_panel` TED EU value (value at 2240–2246).

**Effort: L** (a dataclass + a popover renderer; precedent exists at :558). **Dependencies:** none pipeline-side; reuse `caveats.py` strings + `QueryResult` source metadata.

**OWNER-DECISION GATE — mostly a guardrail, one real choice.** The hard rail is that the popover's `filter_label` **must mirror the live SQL `WHERE` verbatim** or it becomes a false claim. The one genuine owner choice couples to roadmap user-decision #5: if the panel surfaces the **static completeness figures (~3% / ~7% / 90%)**, do they become **live-recomputed %** or stay **cited prose**? (provenance/estimate = no auto-promotion). Owner signs off that every provenance string reports an existing computed value only.

**Boundary rails:** no-inference (every field REPORTS an existing value); structure-facts keep their "never a verdict" caveat verbatim; three-grain labels on any figure the panel explains.

---

## Primitive 4 — In-app CSV export + API discoverability

**What it is.** A value-safe, privacy-gated CSV export button plus an env-gated "View as JSON" API link on the supplier/company surfaces. Ingredients exist; procurement uses none of them.

**Confirmed building blocks**

| Component | Location | Signature / behaviour |
|---|---|---|
| `export_button` | [utility/ui/export_controls.py](utility/ui/export_controls.py):9 | `export_button(df, label, filename, key) -> None`; CSV deferred via zero-arg `lambda: df.to_csv(...)`; disabled when empty |
| `api_json_link` | [utility/ui/entity_links.py](utility/ui/entity_links.py):404 | `api_json_link(path, label="View as JSON") -> str`; **env-gated** on `DAIL_API_BASE_URL` (:416) — returns `""` when unset, safe to splice unconditionally |
| API route | [api/routers/procurement.py](api/routers/procurement.py):53–63 | per-supplier `/v1/procurement/suppliers/{supplier_norm}/dossier`; bulk `/v1/data` |
| Caveat / attribution constants | [api/routers/exports.py](api/routers/exports.py) | `_ETENDERS_ATTRIBUTION` (:43), `_TED_ATTRIBUTION` (:44), `_NO_PERSONS_NOTE` (:45–48); `procurement_awards` caveat (:80–85), `procurement_payments_fact` caveat (:106–110), `procurement_lobbying_overlap` (:126–129) — the canonical strings to reuse in-app |

The `api_json_link` splice pattern is already established at votes.py:404, member_overview.py:2362, legislation.py:323 — **not yet** in procurement.py / company.py.

**Privacy gate (the real gate here — mandatory).** `exports.py` (:114–117) already bakes the person-row filter for payments: `public_display = TRUE AND supplier_class <> 'sole_trader_or_individual' AND privacy_status <> 'review_personal_data'`. **`v_procurement_payments` does NOT project `public_display`** — an unguarded name-keyed paid export leaks **20,314 individual rows / €1.04bn**. The in-app `*_export` builder must route through a `WHERE public_display`-applying view, **not** reuse `v_procurement_payments` raw, and must forbid a pre-summed sum-unsafe total.

**Effort: M.** **Dependencies:** new `*_export` `QueryResult` builders in `data_access` (select value-safe cols + literal caveat/licence/`value_kind` cols — built in **data_access, not the page**); the privacy-gated payments view; firewall check after any page touch.

**OWNER-DECISION GATE — a privacy/policy gate, not a wording vote.** Adding an export "is a privacy decision, not a config tweak" (exports.py:69). Owner signs **which caveat string rides on each frame** and that the name-keyed paid path **inherits the `public_display` + person-row quarantine**. The `api_json_link` splice itself is boundary-safe (no gate — renders nothing until `DAIL_API_BASE_URL` is set).

**Boundary rails:** privacy quarantine (`supplier_class = 'company'`, `public_display`); three-grain never-sum (separate labelled columns, **no** pre-summed total); licence/attribution must travel with the data (CC-BY-4.0 / TED reuse). Run the firewall check after the page touch.

---

## Primitive 5 — Safe-vocabulary constants module: `utility/ui/safe_vocab.py`

**What it is.** A single home for the **short** badge/pill vocabulary ("awarded" / "paid" / "ordered" / "committed" / "sum-safe" / short labels) plus a **FORBIDDEN-word** list — the copy that today is inlined across ~10 panels. **Confirmed does NOT exist** (glob clean; no `safe_vocab` symbol).

**Where approved phrasing lives today (two homes):**
1. **Long-form caveats — already centralised in core:** [dail_tracker_core/caveats.py](dail_tracker_core/caveats.py) holds `PROC_LOBBY`, `COMPETITION`, `PROCUREMENT_AWARDS`, `PUBPAY`, `MONEY_GRAINS`, `ENTITY_COOCCURRENCE`, `DIARY`, etc., moved **verbatim** from pages and imported by `dossiers.py` / `serialize.envelope`. Its docstring already states the "each interface RENDERS it but none OWNS it" philosophy. **But it does NOT hold the short badge vocab and has no FORBIDDEN list.**
2. **Short vocab — inline f-strings:** `_value_pill` "awarded" (procurement.py:393), `_paid_verb` "ordered"/"paid" (:764), `_ted_value_pill` "awarded (EU)" (:1950), `public_payments._semantics_label` / `_tier_label` (:184–316).

**FORBIDDEN convention — prose only, no code.** The terms live in [doc/TENDER_ALERT_SYSTEM_DESIGN.md](doc/TENDER_ALERT_SYSTEM_DESIGN.md):333–336 ("rigged", "corrupt", "waste", "cronyism", "influence-peddling"; "win probability", "chance of winning", "recommended/target price"), echoed in [doc/BI_SPINOUT_ARCHITECTURE.md](doc/BI_SPINOUT_ARCHITECTURE.md):158 and [mcp_server/qs_valuation.py](mcp_server/qs_valuation.py):6. **There is no code-level FORBIDDEN list and no CI check over rendered copy.** *(The `_FORBIDDEN_CALLS` / `_FORBIDDEN_METHODS` in [tools/check_streamlit_logic_firewall.py](tools/check_streamlit_logic_firewall.py):56,69 are unrelated — they forbid `st.dataframe`/raw SQL in pages, not vocabulary.)*

**Effort: M** (copy refactor across ~10 panels; net-new module + optional CI hook).

**OWNER-DECISION GATES (two):**
- **Enforcement (roadmap user-decision #8):** is the FORBIDDEN list **CI-enforced or advisory**? Owner signs.
- **Placement (architectural, not flagged by roadmap):** the module lives at `utility/ui/safe_vocab.py` (UI-only) **or** in core beside `caveats.py` (so API/dossier surfaces share it). The existing precedent is in core — the two must complement, not duplicate. Owner picks.

**Boundary rails (hard):** the refactor **must never soften an existing disclaimer below current strength** — it is copy-consolidation only; the approved phrasing is the user's domain, **moved verbatim, never re-phrased** (the rule `caveats.py` already follows).

---

## Recommended build ORDER

| # | Primitive | One-line rationale |
|---|---|---|
| 1 | **P5 — safe_vocab** | Establish the vocabulary source-of-truth first so P1/P3/P4 pull strings from it instead of being re-edited twice (owner must pick placement first — scaffold pending the gate). |
| 2 | **P1 — money_badge** | Pure UI/CSS, effort L; consumes P5; unblocks correct grain labelling for every report figure. |
| 3 | **P3 — explain_figure** | Effort L, reuses `caveats.py` + `QueryResult`; layers provenance onto the badges from P1. |
| 4 | **P2 — match-confidence** | Batch the two-view edit with the Phase-1 `in_payments`/`paid_safe_eur` column adds — one coordinated view change, single fixture run. |
| 5 | **P4 — export** | Last: largest data-layer surface (privacy-gated view + `data_access` builders + firewall check); depends on the grain labels and caveats settled in P1/P5. |

## Shared boundary rails (every Stage A primitive)

- **Logic firewall:** any new aggregate/join goes in a registered view (P2), never in page/report code; export frames are built in `data_access`, not the page (P4). Run `check_streamlit_logic_firewall.py` after touching pages.
- **Never-sum:** each grain is a separately labelled cell; ceiling and TED never summed with national.
- **Clear `__pycache__` after CSS edits** (P1) — 1.58 widget-theming trap.
- **Verbatim discipline:** approved phrasing is moved, never re-phrased or softened (P3, P5).

---

# PART 2 — STAGE B: reports-first operations

The resolved entry point is **hand-built supplier / buyer / category reports for 3–5 paying design partners before any accounts, alerts, or persistence** (accounts = Phase 5, owner-sign-off-gated — out of scope here).

## Recruiting 3–5 design partners

Target one of each archetype so feedback spans the three report types and both buyer personas (SME-competitor and consultant-advisor):

| Archetype | Who | Why them | Lead report |
|---|---|---|---|
| **Bid consultancy** | a firm that writes tenders for SME clients | tests incumbency / renewal / single-bid rigour; will stress every denominator | Buyer dossier + Category market map |
| **Mid-size supplier** | an active public-sector supplier below the top tier | tests the supplier dossier against a firm that knows its own award/payment history | Supplier dossier |
| **Agency / journalist desk** | a newsroom or public-affairs desk | tests the no-verdict / co-presence rails hardest — highest reputational scrutiny | Category market map + cross-register footprint |

**Recruitment mechanics:** offer a **single bespoke report at a design-partner rate** in exchange for structured feedback and a reference; no subscription, no login, no data retention on their behalf. Deliver as a static PDF/HTML artefact. Success signal for graduating past the entry phase = a partner **pays again** for a second report unprompted.

## The three report products

Each report reuses **existing registered views/functions** (GT-2) + **analyst time** for narrative, layout, unmatched-record handling and QA. Section-by-section design lives in the companion templates:

| Product | Subject | Template | Centrepiece (the paid hook) |
|---|---|---|---|
| **Supplier dossier** | one supplier org (CRO-anchored) | [doc/templates/BI_SUPPLIER_REPORT_TEMPLATE.md](doc/templates/BI_SUPPLIER_REPORT_TEMPLATE.md) | A4 awards-vs-payments reconciliation (two labelled ledgers) |
| **Buyer dossier** | one contracting authority | [doc/templates/BI_BUYER_REPORT_TEMPLATE.md](doc/templates/BI_BUYER_REPORT_TEMPLATE.md) | B4 awards-vs-payments reconciliation |
| **Category market map** | one CPV division/group | [doc/templates/BI_CATEGORY_MARKET_MAP_TEMPLATE.md](doc/templates/BI_CATEGORY_MARKET_MAP_TEMPLATE.md) | C5 awards-vs-payments in category |

### BUILT vs ANALYST-TIME (data sourcing per section)

Legend: **BUILT** = a verified `dail_tracker_core.queries` function exists (GT-2); **ANALYST-TIME** = assemble/narrate from BUILT outputs (only display-only aggregation on an already-filtered single-entity subset), no new engine; **PHASE-2-TODO** = the named function does not exist anywhere in the repo (do not assert it does — needs a pipeline-owned view before it can be reached).

| Section | Supplier | Buyer | Category |
|---|---|---|---|
| **Identity / profile** | BUILT — `xref_summary`, `entity_chain_for_company` | BUILT — `authority_summary` | BUILT — `cpv_summary` |
| **Activity over time** | BUILT — `supplier_year_trend` (true single-entity series) | ANALYST-TIME — only `authority_summary(year=…)` cross-sectional; `authority_year_trend` = **PHASE-2-TODO** | ANALYST-TIME — only `cpv_summary(year=…)` cross-sectional; `cpv_year_trend` = **PHASE-2-TODO** |
| **Concentration / incumbency** | BUILT — `incumbency_for_supplier`, `dependency_for_supplier` | **PHASE-2-TODO** — needs a pipeline-owned `top_suppliers_for_authority` / authority-concentration view; a top-5-supplier share must **not** be rolled up from row-level `awards_for_authority` in the report (that `groupby(supplier)` is a firewall breach, rail (b)) | ANALYST-TIME from `cpv_summary` (`n_suppliers`); top-supplier-within-CPV = **PHASE-2-TODO** |
| **Awards-vs-payments (centrepiece)** | BUILT — `supplier_year_trend` (AWARDED) + `payments_supplier_summary` / `payment_lines_for_pair` / `entity_chain_for_company` (`paid_safe_eur`, `committed_safe_eur`) | BUILT — awards side `authority_summary`/`awards_for_authority` + payments side `payments_publisher_summary` (`payments_for_publisher` = nearest top-payee source) | BUILT — `cpv_summary` (AWARDED) + public-payments `categories` / `category_suppliers` (**note:** `spend_category` ≠ CPV — taxonomy caveat) |
| **Competition / single-bid** | ANALYST-TIME — `awards_for_supplier` carries row-level `n_bids_received`; a supplier-level share needs a view (firewall) — supplier-level function = **PHASE-2-TODO**; `competition_by_cpv` gives category context | **`competition()` per-buyer exists in core but is UNWIRED (no data_access wrapper)** — needs wiring; `competition_by_cpv` (BUILT/wired) as fallback | BUILT — `competition_by_cpv` (TED 2024+) |
| **Renewal cadence + open pipeline** | BUILT — `expiring_contracts_etenders`, `expiring_contracts` (TED, `renewal_max`), `live_tenders`, `ted_tenders`; `renewal_cycle` = **PHASE-2-TODO** | BUILT — same set | BUILT — same set (sector filter) |
| **Category / buyer mix** | BUILT/ANALYST-TIME — authority split via `incumbency_for_supplier`; CPV split via `awards_for_supplier` (row-level `cpv_code`/`cpv_description`; ANALYST-TIME rollup over the supplier's own already-filtered awards) | ANALYST-TIME; `authority_category_mix` = **PHASE-2-TODO** | ANALYST-TIME from `cpv_summary`; buyer-within-CPV = **PHASE-2-TODO** |
| **Cross-register footprint** | BUILT — `lobbying_overlap`, `charity_overlap`, `epa_compliance_for_supplier`, `xref_summary`, MCP `ministerial_diary_organisation` | PARTIAL — buyer-as-lobby-target via MCP `who_ministers_meet` / diary; `procurement_lobbying_overlap` is supplier-keyed. **Subject = the public body/department, never the office-holder**: raw co-presence counts only, linked out to the free register; no minister ranked / scored / profiled; carry `PROC_LOBBY` + `DPO` verbatim (rail (d)(ii)) | **PHASE-2-TODO** for any per-category aggregation (needs a category-level cross-register view); otherwise restrict to the per-supplier co-presence rows already produced by the BUILT supplier overlaps — **no** per-category rollup in the report (firewall) |
| **Lineage / coverage** | BUILT — MCP `data_coverage` | BUILT — MCP `data_coverage` | BUILT — MCP `data_coverage` |

**Accuracy landmines to hold in the analyst's head (GT-2):**
- `company_num` type differs: `entity_chain_for_company(company_num: str)` vs `epa_compliance_for_supplier(company_num: int)`.
- **Two distinct "payments" registers** — procurement-side `payments_*` (`v_procurement_payments`, SPENT/COMMITTED) vs public-payments-side `supplier_summary`/`categories` (`v_public_payments`, `amount_semantics`). Different views, different grains — never merge.
- **"year trend" trap** — only `supplier_year_trend` is a true single-entity time series; authority and CPV have only cross-sectional per-year rankings.
- **`incumbency_for_supplier` has no CPV column** — its output is authority-keyed only (`contracting_authority`, `authority_is_central_purchasing`, `n_awards`, `n_distinct_years`, `first_year`, `last_year`, `awarded_value_safe_eur`). Per-supplier CPV mix comes from `awards_for_supplier` (`cpv_code`/`cpv_description`), rolled up over that one supplier's already-filtered rows — never from `incumbency_for_supplier`.

## Pricing anchor

Consistent with [doc/BI_SPINOUT_ARCHITECTURE.md](doc/BI_SPINOUT_ARCHITECTURE.md): **bespoke reports EUR 1.5k–10k**. Anchor within the band by depth/breadth:

| Tier | Scope | Indicative |
|---|---|---|
| Single-subject dossier (one supplier or one buyer), standard sections | narrowest | ~EUR 1.5k–3k |
| Category market map (multi-supplier, multi-buyer, reconciliation) | mid | ~EUR 4k–7k |
| Multi-subject / bespoke cut (e.g. a category + its top-5 incumbents) | broadest | up to EUR 10k |

Design-partner rate sits at the low end in exchange for structured feedback + a reference. **Never** price on a per-figure or per-lead basis (implies a lead-value claim the no-inference rails forbid).

## Definition of Done — the first paid report

A report is shippable only when **all five** hold:

| Gate | Criterion |
|---|---|
| **Accuracy** | Every figure traces to a named registered view/function (GT-2). No page-side aggregation. Numbers reproduce from the query layer. `value_kind` and lifecycle tier carried on every money figure. |
| **Caveats** | Each figure renders grain tag + coverage window + denominator as a subtitle (not a buried footnote). The relevant canned blocks — `[GRAIN]`, `[TED]`, `[PAYCOV]`, `[COUNTS]`, `[SINGLEBID]`, `[ENDDATE]`, `[REGISTER]`, `[ENTITY]` — are present. Any co-occurrence / competition / payments figure carries the **verbatim** `caveats.py` string (`PROC_LOBBY`, `COMPETITION`, `PUBPAY`, `MONEY_GRAINS`, `DPO`). |
| **Attribution** | Per-source licence/attribution present **verbatim** from [api/routers/exports.py](api/routers/exports.py) / [NOTICE.md](NOTICE.md): eTenders/OGP CC-BY-4.0, CRO CC-BY-4.0, Charities CC-BY-4.0, TED — "Contains information from TED (© European Union), reused under Decision 2011/833/EU." (`_TED_ATTRIBUTION`, exports.py:44 — reproduce exactly; never abbreviate "European Union" to "EU"), lobbying.ie PSI, and — if Iris-distress facts appear — the Iris Oifigiúil acknowledgement + source URL. |
| **No cross-grain sum** | No figure sums or nets across AWARDED / PAID / COMMITTED / PLANNED / BUDGET. TED shown as counts/notices, never summed. Reconciliation = **two labelled ledgers, paired panels, hard divider — never a stacked bar**. |
| **Company-class gate** | Every subject is an organisation (CRO number / normalised org name). Rows where `supplier_class = 'sole_trader_or_individual'` **or** `public_display = FALSE` **or** `privacy_status = 'review_personal_data'` are excluded. Zero named individuals / directors / beneficial-owner narrative. |

## Lightweight pre-ship QA checklist

- [ ] Every figure shows grain tag + coverage window + denominator (as chart subtitle).
- [ ] No stacked bar mixes grains; each reconciliation section is two labelled ledgers.
- [ ] TED figures are counts/individual notices, never a total.
- [ ] Single-bid sections show the "not disclosed" bucket (no verdict-by-omission).
- [ ] Cross-register section is a co-presence table — **no network graph** — with the `[REGISTER]` rider. On buyer/category surfaces, the body/department is the subject; no office-holder is ranked, scored, or profiled; `PROC_LOBBY` + `DPO` ride verbatim.
- [ ] Company-class scan: grep the source rows for any `sole_trader_or_individual` / `public_display = FALSE` / `review_personal_data`; result must be zero. No natural person named anywhere.
- [ ] Per-source attribution/licence footer present verbatim.
- [ ] Unmatched / un-crosswalked records are **counted and shown**, not silently dropped.
- [ ] Forbidden-vocabulary scan of the prose: none of *influence*, *corruption*, *waste*, *rigged*, *captured*, *influence-bought*, *cronyism*, *influence-peddling*, *win probability*, *chance of winning*, *recommended/target price*.
- [ ] Data-currency / last-refresh date stated per feed.
- [ ] Second-analyst spot-check of ≥3 headline figures against the query layer before sign-off.

---

# Never-break rails (binding on Stage A and Stage B)

Distilled from GT-4 ([doc/BI_SPINOUT_ARCHITECTURE.md](doc/BI_SPINOUT_ARCHITECTURE.md) §4/§6/§10/§15, [tools/check_streamlit_logic_firewall.py](tools/check_streamlit_logic_firewall.py), the three-money-grain rule).

**(a) Never-sum the five value grains.** AWARDED · PAID · COMMITTED · PLANNED · BUDGET are distinct grains — never added, unioned, netted, or stacked into an implied total. Only sum **within** a grain and only where `value_safe_to_sum` permits. **Never union TED (EU) with national.** No stacked bars / single totals / running sums across grains — each grain is a separately labelled cell. The never-sum caveat travels in-surface, in exports and reports.

**(b) Logic firewall.** All aggregation / JOIN / reshape / rollup lives in registered views + the query layer — never in page or report logic. No raw `read_parquet`/`read_csv`/`scan_*`, no `merge`/`pivot`/`groupby.agg`, no `duckdb.connect(":memory:")`/`register`, no `JOIN`/`GROUP BY`/`OVER (` in SQL literals in the presentation-adjacent layers. If a report needs a new aggregate, **add or extend a pipeline-owned view** — do not compute it in the report. The only escape hatch is a sanctioned `# logic_firewall: display_only` on an already-filtered subset; never a workaround for a missing view.

**(c) No-inference / no-score.** No influence / risk / access / conflict scores or rankings — for anyone. No corruption or verdict framing. Structure facts (single-bid, incumbency, renewal, co-occurrence) are **facts with their caveats, never accusations**. No personal dossiers or named-individual scoring. Forbidden as claims (build-breaking terms — this binding list is the superset of the design-doc rail and the Stage B QA checklist): *influence*, *corruption*, *waste*, *rigged*, *captured*, *influence-bought*, *cronyism*, *influence-peddling*, *win probability*, *chance of winning*, *recommended/target price*.

**(d) The three resolved decisions (2026-06-28).** (i) **Reports-first** — hand-built reports for 3–5 paying partners before any accounts/alerts/persistence (Phase 5, owner-gated). (ii) **Lobbying-overlap + ministerial-diary access may be paid — CO-OCCURRENCE ONLY:** raw counts as separate labelled facts, never a score; **the company is the subject, never the politician**; carry `PROC_LOBBY` and `DPO` caveats verbatim + the `matched_supplier` human-verification field; link out to the free public registers; company-class/PII double-gate applies; flagged highest reputational risk.
   - **(ii-extended) Buyer & category cross-register surfaces.** The same subject/co-presence rider binds wherever a minister-centric register (`who_ministers_meet` / ministerial diary) appears on a buyer dossier or category market map: the **public body / department is the subject**, never the office-holder. Diary / lobbying-target rows are **raw co-presence counts only, linked out to the free register**; **no office-holder is ranked, scored, or profiled** — a diary count is the body's footprint, never a profile of the minister. Carry `PROC_LOBBY` and `DPO` verbatim on those buyer-side and category-side panels too, and apply the company-class/PII double-gate identically.
   - (iii) **Iris-derived corporate distress is in the paid product — FACT-ONLY + ATTRIBUTED:** facts only (notice type, date, entity, status), never verbatim gazette text or PDF layout; carry the Iris acknowledgement + URL on every use; remains a solicitor-checklist item (§14.2).

**(e) Attribution / licence.** **Sell the software/service/curation, never the data** — assert rights only in the compilation + software (sui generis DB right), not the source facts. Per-source attribution rides on every report/export/API response: eTenders/OGP CC-BY-4.0; CRO CC-BY-4.0; Charities CC-BY-4.0; lobbying.ie PSI (ethics guardrails apply); public-body payments PSI/per-publisher (verify per publisher); TED/EU-OJ EU reuse (verify exact terms); **Iris Oifigiúil = Government copyright, NOT open** (fact-only + acknowledgement + URL). Pass-through ToS binds re-publishers to attribution, the never-sum caveat, no re-identification, no raw-export resale. CC-BY does not cover GDPR — any named natural person triggers controller obligations.

**(f) Company-class / PII double-gate** (inherited by every paid surface). Exclude at snapshot/export time any row where `supplier_class = 'sole_trader_or_individual'`, `public_display = FALSE`, or `privacy_status = 'review_personal_data'`. No re-identification of individual payment rows. CRO-director / charity-trustee cross-referencing stays sandboxed. Personal insolvency / individual bankruptcy excluded by policy. Every new paid surface must be verified to inherit the gate (solicitor-checklist §14.2 item 8).

---

# Decisions still needed from owner

| # | Decision | Where | Options staged |
|---|---|---|---|
| 1 | **Match-confidence tier labels** + raw-number visibility + ambiguous-match policy | Primitive 2 | A categorical-only / B categorical+raw / C method-named; sub-gate: **suppress** vs **caveat** on `n_cro ≥ 2` |
| 2 | **Money-badge wording** string table + "Not safe to sum" chip | Primitive 1 | A verb-led / B noun-led; chip yes/no |
| 3 | **SME denominator convention** for concentration & single-bid facts in reports | Stage B report design | A award-notice count / B value-safe award count (`n_value_safe_awards`) / C distinct-buyer or distinct-CPV basis |
| 4 | **Completeness figures** in `explain_figure` — live-recomputed % or cited prose | Primitive 3 (user-decision #5) | live % / cited prose |
| 5 | **Export caveat-per-frame** assignment + confirm paid-path privacy quarantine | Primitive 4 | which `exports.py` caveat string rides each frame |
| 6 | **safe_vocab enforcement + placement** | Primitive 5 (user-decision #8 + arch gate) | CI-enforced vs advisory; `utility/ui` vs core beside `caveats.py` |
| 7 | **Report pricing tier** per product within the EUR 1.5k–10k band | Stage B | dossier low / category mid / bespoke high |

Items 1–3 are the three the owner explicitly reserved (do not decide) — presented as neutral options above, not chosen.

> **Engineering note (non-prescriptive — for reference only, not a recommendation, and attached to no single option).** For decision #3, each option's existing-code footprint, stated symmetrically so the owner sees the same information for all three: **Option A** (award-notice count) is the basis the existing `incumbency_for_supplier` / `dependency_for_supplier` functions already compute on; **Option B** (value-safe award count) maps to the existing `n_value_safe_awards` column on `supplier_summary`; **Option C** (distinct-buyer / distinct-CPV) would draw on `n_authorities` / `n_suppliers`-style distinct counts. None of these footprints is a reason to prefer one option — the choice is the owner's.