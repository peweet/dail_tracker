# Procurement (eTenders) — Build & Streamlit Integration Plan

Status: **Phases 0–2 + lobbying enrichment SHIPPED (2026-06-03); Procurement page
(Phase 3) deferred by request.** Done: extractor → committed gold; overlap probe
promoted to `pipeline_sandbox/procurement_lobbying_xref.py` (gold); both wired as
`procurement` + `procurement_lobbying` pipeline chains; five `procurement_*.sql`
views + `utility/data_access/procurement_data.py`; lobbying enrichment live
(`sql_views/lobbying_org_procurement.sql` + `fetch_org_procurement` +
"Also a State supplier" disclosure on the org profile); `procurement` group added
to the `test_view_group_registers` smoke test (passes). NOT done: the dedicated
Procurement page + nav (Phase 3), bespoke fixture column-tests (Phase 4).

Owner context: see [[project_procurement_etenders]],
[[feedback_pipeline_view_inspection]] (read the SQL views before writing UI),
[[feedback_no_inference_in_app]] (verifiable data only in the UI),
[[project_sql_view_test_fixtures]], [[feedback_gold_layer_quarantine]].

---

## 1. Goal & scope

Ship a **Procurement** page that surfaces, from the OGP eTenders open data:

- which firms win the most state contracts (count — the trustworthy metric),
- the honest spend picture (the "awarded value ≠ spend" correction),
- companies that appear on **both** the procurement and lobbying registers
  (co-occurrence disclosure, never causation).

Non-goals for v1: per-TD conflict surfacing (needs Interests↔supplier matching),
SIPO-donor overlap (blocked on [[project_sipo_ocr]]), contract→lobby causal
linkage (no shared key — structurally impossible from this data).

---

## 2. Architecture decision — promote in place (the cbi/cro pattern)

`cbi` and `cro` already live in `pipeline_sandbox/` yet run as first-class
`pipeline.py` chains writing committed gold. Procurement follows the same shape —
**no file move**, just:

1. repoint the extractor's outputs from `data/sandbox/parquet/` → `data/gold/parquet/`,
2. register it as a `pipeline.py` chain,
3. move every join/aggregation into `sql_views/procurement_*.sql` (the
   firewall-checked layer), so the page stays display-only,
4. add a `data_access` module + a `pages_code` page + nav wiring.

The Python **lobbying-overlap probe becomes throwaway** — its join is re-expressed
as a SQL view (logic firewall: joins belong in views, not page/python ad-hoc).

---

## 3. Phase 0 — gold promotion

### 3.1 Extractor output paths
`pipeline_sandbox/procurement_etenders_extract.py`:
- `OUT_AWARDS` → `data/gold/parquet/procurement_awards.parquet` (row grain:
  one row per award×supplier; carries `supplier_class`, `name_truncated`,
  `value_eur`, `value_kind`, `is_framework_or_dps`, `value_shared_across_suppliers`,
  `value_safe_to_sum`, `Contracting Authority`, `Main Cpv Code`, date).
- `OUT_MATCH` → `data/gold/parquet/procurement_supplier_cro_match.parquet`
  (supplier grain → CRO `company_num`/`status`/`match_method`).
- `OUT_COV` stays `data/_meta/procurement_coverage.json` (provenance +
  value-semantics counts; already verified against the data.gov.ie API).

⚠️ Gitignore: `*.parquet` is globally ignored — add a negation rule for the two
new gold files (same trap documented in [[project_curated_meta_reference_files]]
and [[project_sql_view_test_fixtures]]). The `data/_meta/*.json` coverage files
are already tracked.

Confirm all writers keep `compression="zstd", compression_level=3,
statistics=True` ([[feedback_parquet_write_convention]]). They do.

### 3.2 pipeline.py chain
Add to `CHAINS` after `cro` (it depends on the CRO silver register, same as cro):
```python
("procurement", "pipeline_sandbox/procurement_etenders_extract.py"),
```
Add a `_CHAIN_BLURBS["procurement"]` line. The extractor already self-fetches +
caches the CSV (`ensure_csv()`), so it is headless-safe.

---

## 4. Phase 1 — SQL views (the firewall layer)

All in `sql_views/`, prefix `procurement_`, registered by the glob loader. Read
the existing `corporate_*.sql` views first ([[feedback_pipeline_view_inspection]]) —
match their header-comment + provenance style.

| view | grain | purpose |
|---|---|---|
| `v_procurement_awards` | award×supplier | display columns, framework flags; the feed |
| `v_procurement_supplier_summary` | supplier | **main ranking** — n_awards, n_authorities, safe-to-sum total, CRO match, lobby flags |
| `v_procurement_authority_summary` | contracting authority | who buys most (by count) |
| `v_procurement_cpv_summary` | CPV category | category breakdown |
| `v_procurement_lobbying_overlap` | supplier | the 123 — JOIN awards↔lobbying silver, role + return counts |
| `v_procurement_coverage` | 1 row | hoist the `_meta` JSON stats for the methodology expander (or page reads JSON directly) |

Firewall-critical rules baked into the views (NOT the page):
- **`value_safe_to_sum` gating**: any `SUM(value_eur)` is `SUM(value_eur)
  FILTER (WHERE value_safe_to_sum)`. The naive total is never summable in a view.
- **CRO join**: `v_procurement_supplier_summary` LEFT JOINs
  `procurement_supplier_cro_match.parquet` (company-class only — individuals
  quarantined upstream).
- **Lobbying overlap**: `v_procurement_lobbying_overlap` does the
  `name_norm_expr`-equivalent normalised join against
  `data/silver/lobbying/parquet/returns_master.parquet` +
  `client_company_returns_detail.parquet`. Exact-name match (conservative);
  excludes `name_truncated` + non-company classes.

`name_truncated` rows are excluded from supplier rankings/matching but kept in the
raw feed flagged "source name incomplete" ([[feedback_gold_layer_quarantine]]).

---

## 4b. VALUE TAXONOMY & classification pattern (cross-source — adopt before consolidation)

> **Where we are:** still the **ingestion phase** — taking sources in piecemeal, parsing
> and testing each (eTenders gold ✓, LA PO probes ✓, TED pulled ✓), seeing what turns up.
> **Wholesale consolidation has NOT begun.** This taxonomy is the contract to lock in
> *now*, before merging anything, so the consolidation doesn't corrupt the data by
> conflating money that means different things — and so a PDF's logical row structure is
> replicated faithfully in tabular form (one PO line = one typed row, with its tier).

Every source measures a **different point in the spend lifecycle**; figures are NOT
interchangeable or summable across points. Tag every row on two axes.

**Axis 1 — realisation tier:** `PLANNED → AWARDED → COMMITTED → SPENT` (+ BUDGET aggregate).
**Axis 2 — `value_kind`** (controlled vocab; extends the 2 already in gold):

| `value_kind` | tier | UI verb | summable? | source |
|---|---|---|---|---|
| `estimate_advertised` | PLANNED | "expected ~€X" | no | eTenders/TED notice estimate |
| `budget_allocated` | PLANNED(agg) | "budgeted €X" | within LA/year only | AFS / NOAC |
| `contract_award_value` | AWARDED | "awarded €X" | caution | eTenders/TED single award *(in gold)* |
| `framework_or_dps_ceiling` | AWARDED | "up to €X, shared" | **NO** | frameworks/DPS, pan-EU (GÉANT) *(in gold)* |
| `po_committed` | COMMITTED | "ordered €X" | yes | LA Purchase-Orders-over-€20k |
| `payment_actual` | SPENT | "paid €X" | **yes (true spend)** | LA "Paid" lists / Dept payments |

Rules to bake into the model + views (not the page):
1. **Every value row carries `value_kind` + `realisation_tier`** — extend the gold's
   existing `value_kind` to this full vocab when the spend/TED tiers land.
2. **`value_safe_to_sum` is derived from `value_kind`** (true only for `po_committed` /
   `payment_actual`, and `contract_award_value` with caution) — the firewall already does
   this for awards; generalise it, don't reinvent per source.
3. **One tier per view / per page section** — never a blended list or cross-tier total.
4. **The verb is the disambiguation** — render "paid/ordered/awarded/expected €X", never bare €.
5. **No cross-tier arithmetic** (no "awarded − paid = outstanding"): the tiers have no shared
   key (a notice→award→PO→payment chain is unlinked), so reconciliation is a fiction by default.
6. **PDF→tabular fidelity:** when ingesting a PO PDF, one printed line becomes one row tagged
   `po_committed` or `payment_actual` (per that council's Paid flag) — preserve the source's
   own grain; do not aggregate or re-interpret at ingestion. Test each council's parse against
   the PDF's visible row count + total before trusting it.

Full rationale + the anti-overwhelm UX pattern: `doc/PROCUREMENT_INVESTIGATION.md`
("VALUE TAXONOMY & classification pattern").

## 5. Phase 2 — data access

New `utility/data_access/procurement_data.py`, copying `corporate_data.py`:
- `@st.cache_resource get_procurement_conn()` → `register_views(conn,
  ["procurement_*.sql"], swallow_errors=True)`.
- `_safe(sql, params)` wrapper (try/except → empty frame).
- `@st.cache_data(ttl=300)` fetchers, **`SELECT * FROM v_...` only** — no JOIN /
  GROUP BY / window / read_parquet in this module (same forbidden list as the
  page). One fetcher per view:
  `fetch_supplier_summary`, `fetch_awards`, `fetch_authority_summary`,
  `fetch_cpv_summary`, `fetch_lobbying_overlap`, `fetch_coverage`.

---

## 6. Phase 3 — the Streamlit page (the specific integration)

> ⚠️ **SUPERSEDED for the multi-source picture (2026-06-04).** This §6 describes an
> *eTenders-awards-only* page. The agreed IA once awards + payments + AFS are modelled together
> is **entity-first (company / public body), lifecycle-structured leaves**, with the
> extraction-derived caveat as a UI primitive — see `PUBLIC_PAYMENTS_FACT_SCHEMA.md` **Part C**.
> Keep §6 below only as the eTenders-slice reference.

### 6.1 Where it goes in the nav
Procurement is an **institutional/transparency dataset**, org-centric not
TD-centric — the same shape as Corporate Notices, Statutory Instruments,
Appointments. Those all sit in the top-nav with a `rankings-*` slug, so for
sibling consistency:

- `utility/app.py`: add `from pages_code.procurement import procurement_page` and
  an `st.Page(procurement_page, title="Procurement",
  icon=":material/receipt_long:", url_path="rankings-procurement")`. Place it next
  to **Corporate Notices** (its closest cousin — both CRO-matched org datasets).
- `utility/ui/entity_links.py`: add `"procurement": "rankings-procurement"` to
  `PAGES` (keep app.py + PAGES in lockstep — the file comment mandates it).

(`receipt_long` is the natural Material glyph for contracts/invoices; confirm it
isn't already used — Payments uses `payments`, so it's free.)

### 6.2 Page IA (top → bottom)
Build with the shared component kit (`ui/components.py`), no bespoke widgets
([[feedback_iteration_process.md]], [[feedback_dataframes_secondary_only]] — no
`st.dataframe` on primary views):

1. **Hero** (`hero_banner`) + **glossary_strip**
   (Framework / DPS / CPV / Contracting Authority) + a prominent **value caveat
   callout**: "Figures are *awarded contract value*, not money paid. Framework &
   DPS notices are multi-year ceilings." This is the page's signature honesty move.
2. **Top suppliers by contracts won** — `ranked_member_card`/`rank_card_row`
   style cards from `v_procurement_supplier_summary` ordered by `n_awards`
   (the trustworthy count metric). CRO-status chip + lobbying-overlap badge.
3. **"The €570bn that isn't"** — a `stat_strip`/`totals_strip` panel contrasting
   the naive sum vs `value_safe_to_sum` total, with the 24× explainer. Reads
   `v_procurement_coverage`. This *is* a story (open-data literacy) and doubles
   as the methodology.
4. **Per-supplier search** → supplier detail (`?supplier=`): their awards,
   distinct authorities, CRO link, lobbying-overlap disclosure block.
5. **On the lobbying register too** — `v_procurement_lobbying_overlap` rendered as
   neutral disclosure cards ("Won N contracts • Filed M lobbying returns"), each
   cross-linking to the **Lobbying** page (`/rankings-lobbying`) for that org.
6. **Browse** — by contracting authority and by CPV category
   (`clickable_card_link` tiles).
7. **Sources & methodology** expander (copy the `.corp-methodology` pattern):
   render `source.attribution` + `landing_page` link + `retrieved_utc` from
   `procurement_coverage.json`; restate the value/ truncation/ privacy caveats.

### 6.3 Page boilerplate
- `page_error_boundary` wrap, `hide_sidebar()`, `inject_css()`.
- All CSS into `shared_css.py` (page-scoped `.proc-*` block), nothing inline
  ([[feedback_iteration_process]]).
- Provenance footer via the established `sidebar_provenance`/methodology pattern.
- Civic-voice empty states (`empty_state`), friendly dates (`fmt_civic_date`).

---

## 7. Phase 4 — tests

Per [[project_sql_view_test_fixtures]] (`test/test_sql_views.py`):
- Add a tiny `procurement_awards.parquet` + `procurement_supplier_cro_match.parquet`
  fixture under `test/fixtures/sql_views/data/gold/parquet/` (+ a minimal lobbying
  fixture for the overlap view). Mind the `*.parquet` gitignore negation.
- Registration smoke test: every `procurement_*.sql` registers without error.
- `v_`-prefix lint already covers the new views.
- Assert the firewall invariant: a naive `SUM(value_eur)` over the fixture differs
  from the view's safe-to-sum total (guards the gating from regressing).

CI jobs (lint / firewall / typecheck / test / sql-contracts,
[[feedback_firewall_marker_placement]]) must stay green. Page reads only via
`data_access`; no `read_parquet`/aggregation in the page (firewall reviewer).

---

## 8. Phase 5 — cross-page integration (deferred)

- **Lobbying page → Procurement**: on an org that also wins contracts, show a
  "State contracts" badge linking to `/rankings-procurement?supplier=`. Symmetric
  to the overlap block on the procurement side.
- **Interests / Member Overview**: a TD's declared directorship/shareholding in a
  supplier → conflict-surface disclosure. Needs Interests-company↔supplier name
  matching; own kickoff later.

---

## 8b. Actual-spend tier (per-council PO-over-€20k) — deferred, decision-gated

The shipped feature is the eTenders **awards** layer (framework ceilings, not spend).
A second tier — the per-council **"Purchase Orders over €20,000"** publications — is
the *actual money paid* (supplier-named, CRO-joinable). It is **additive and optional**:
grow it one council at a time, stop whenever the marginal council isn't worth it. The
project is already ambitious, so this tier stays **deferred behind a one-probe decision
gate**, not committed wholesale.

**What the probes already established** (`pipeline_sandbox/probe_procurement_pdf*.py`,
`probe_procurement_excel.py`, `probe_procurement_city_vs_county.py`):

- **Extraction is solved, no OCR.** 7 reachable councils sampled → **all digital**;
  PDFs via fitz word-geometry + largest-x-gap column split + leading-PO#/ID strip;
  Excel via openpyxl. CRO joins land in the same 32–56% band as eTenders. PaddleOCR is
  **not** needed for procurement (reserved for SIPO).
- **Excel is the lowest-debt format.** Only ~2 distinct schemas seen
  (`OrderNo·Supplier·Period·EURO·Description` / `PO·SUPPLIER·TOTAL·DESCRIPTION·PAID`),
  because **Circular Fin 07/2012 mandates the fields** (supplier, cost, description) so
  councils converge. Landmines are mild and uniform (header at row 2, 1–3 total rows,
  amounts are real numbers, single-sheet).
- **City ≠ county.** 31 LAs = 26 county + 3 city + 2 merged; where both exist they
  publish separate lists (Galway City vs Cork County share only 2.7% of suppliers, all
  national utilities). The macro budget-by-division layer is a *different* question,
  best sourced from the official **amalgamated AFS (datacatalogue.gov.ie) + NOAC**, not
  by scraping POs.

**RECOMMENDATION — size the schema count before building, not the file count.**

1. **Decision gate (one probe, ~½ day):** run a header-signature census across all CKAN
   spend publishers + ~10 off-catalog councils. The debt scales with the number of
   **distinct schemas**, *not* the number of files or councils.
   - ~3–5 schemas → **GO**: the tier is a small finite `schema_map` table. Build it.
   - 15+ schemas → **DEFER**: not worth the maintenance surface yet.
2. **Debt-capping design (when GO):** a `schema_map` table keyed by publisher →
   `{host, format (pdf/xlsx), supplier_col, amount_col, column_order, po_id_prefix}` +
   **one** generic reader (find-header / drop-totals / coerce-amount / fitz-or-openpyxl).
   Adding a council = adding one row, never a new parser.
3. **Known finite gaps to budget for (all minor, none blocking):**
   - Legacy `.xls` (~9% of corpus) needs `xlrd` — add the dep (pipeline-only extra) or
     convert; never core deps (Cloud syncs core only).
   - 2 of ~9 councils are access-blocked (Galway County WAF/TLS, Cork City Umbraco
     `/media/{guid}/` behind JS) → need a small **Playwright** listing-scrape, not new
     extraction logic.
   - Per-council column ORDER and PO#/ID prefixes vary → already handled by the
     largest-x-gap split + numeric-strip; record the quirk in `schema_map`, don't
     special-case in code.
4. **Provenance / privacy carry over unchanged:** `value_safe_to_sum` discipline,
   `name_truncated` flag, sole-trader/individual quarantine (keep company-suffix
   suppliers), CC-BY attribution + `retrieved_utc`, no-inference UI copy.

Net: the spend tier is **bounded, known-pattern work** — safe to commit *per council*
once the schema census says GO, and cheap to stop. Do **not** treat it as a single
all-or-nothing national scrape.

---

## 8c. Outstanding ingestion backlog (stocktake 2026-06-03)

**In production:** eTenders awards (gold), **TED IE awards (silver `ted_ie_awards`, 13,126)**,
**amalgamated AFS (silver `afs_amalgamated_divisions`, 64, BUDGET tier)**.
**Built, not promoted:** `public_payments_fact` (sandbox, 8,021 rows / 17 central+semi-state
publishers, SPENT tier) — ⚠ still on the drifted `amount_semantics` col; **converge to
`value_kind`+`realisation_tier` before promoting** (§4b).

Not yet ingested, in rough priority:

1. **⭐ LA Purchase-Orders over €20k — 31 councils (the per-transaction layer).** Fully probed
   (22/31 parsed live, ~250–320k row estimate), merged registry `procurement_la_registry.py`,
   shared reader proven (fitz+largest-x-gap+numeric-strip / xlsx-csv direct) — but **never
   materialised into a fact**, and NOT in `public_payments_fact` (which holds only central/
   semi-state publishers, zero councils). **Being kicked off in its own context window** —
   see the kickoff prompt in `doc/PROCUREMENT_INVESTIGATION.md` ("LA PO corpus — kickoff").
2. **Per-LA AFS** — the 31 *individual* council financial statements (per-council by-division
   BUDGET; the per-constituency prize). Only the amalgamated/national AFS is in. Heavier PDF
   extract (statutory tables, scanned older years possible). Reuse `afs_amalgamated_extract.py`.
3. **Rest of the AFS document** — only I&E-by-division is ingested. **Note-16 budget-vs-actual**
   (stacked sub-table parser needed), capital expenditure by division, balance sheet.
4. **Hard LA tail** — Carlow/Cavan/Mayo/Roscommon (JS-rendered → Playwright), Louth/Tipperary
   (target the right file), pre-2016 AFS (old programme-group wording).
5. **CSO GFA budget** (`probe_la_finance_budget.py`) — probed only (general-gov by economic
   category, 2000–2025); promote to silver if the BUDGET tier wants the economic-category cut.
6. **Mini-competitions / standalone awards** — probed; ~88% supplier overlap with eTenders →
   marginal, **deliberate skip** unless a specific need appears.

Candidate sources **not yet considered** (in-scope-ish, log before deciding):
- **NDP / capital-projects tracker** (gov.ie investment projects / MyProjectIreland) — major
  capital procurement with values; never looked at.
- **Revised Estimates Volume (central appropriations by programme)** — deprioritised earlier
  as "too far" (central-gov budget, not procurement per se); revisit only on request.

## 8d. ⭐ FINAL CRITICAL STEP — cross-source consolidation (the union), gated on FULL ingestion

**Do NOT start until every payment-grain source above is materialised.** Attempting the union
mid-ingestion just churns the contract as councils / HSE-Tusla land their real shapes (decided
2026-06-03 — premature otherwise). This is the last step before any gold promotion + views + page.

**Feasibility was checked empirically (2026-06-03) — "union everything" is NOT real.** The
sources are three different grains; only one grain unions:

- **Payment grain → the union** (`public_payments_fact`): central/semi-state `public_payments_fact`
  (built, sandbox) + **HSE/Tusla** (`procurement_hse_tusla_parser.py`) + **LA POs** (per-council,
  not built). These concat — *once they share a schema* (see precondition).
- **Award grain → SEPARATE sibling fact**: eTenders `procurement_awards` (gold) + TED
  `ted_ie_awards` (silver). Award *ceilings*, not paid transactions — never concat into payments.
- **Budget grain → SEPARATE sibling fact**: amalgamated AFS + per-LA AFS + CSO. Aggregate
  by-division, not per-transaction — never concat into payments.

**The blocker is a schema contract, not a `pl.concat`.** Three vocabularies already drift:
`public_payments_fact` uses `amount_semantics`∈{po_committed,payment_actual}; HSE/Tusla emits
`{payment_incl_vat,invoice_payment}` (and a DQ JSON, *not a parquet*, with `supplier_norm`/
`doc_ref` column-name mismatches + missing provenance/privacy cols); TED uses
`{contract_award_value,framework_or_dps_ceiling}`. Per §4b, lock the **2-axis taxonomy
(`realisation_tier` + controlled `value_kind`)** as the canonical `public_payments_fact` schema
FIRST; every producer then EMITS that schema (HSE/Tusla needs renames + vocab map + the missing
columns + to actually write a parquet — a cross-context coordination item), THEN the concat is trivial.

Sequence when ingestion is complete:
1. Author the canonical `public_payments_fact` schema + taxonomy contract (one source of truth;
   `doc/PUBLIC_PAYMENTS_FACT_SCHEMA.md` must adopt `value_kind`+`realisation_tier`, not fork).
2. Conform each payment-grain producer to it (public-body extractor, HSE/Tusla parser, LA parser).
3. Run the privacy quarantine pass (public_display currently True for all — deferred) + a CRO join
   on the public-body rows (it has none yet; reuse the eTenders/TED matcher).
4. `pl.concat` the conformed producers → one `public_payments_fact`; keep award + budget as siblings.
5. Promote to gold + `sql_views/public_payments_*.sql` + tests/fixtures + (optional) page (§6/§7).

## 9. Logic-firewall checklist (gate before merge)

- [ ] No `read_parquet` / JOIN / GROUP BY / window / pandas merge in
      `procurement.py` or `procurement_data.py`.
- [ ] Every business metric (counts, safe-to-sum totals, CRO match, overlap)
      defined in a `sql_views/procurement_*.sql` view.
- [ ] `value_safe_to_sum` gating lives in the view; the page can only display it.
- [ ] Every value row tagged `value_kind` + `realisation_tier` (§4b); one tier per view,
      no cross-tier totals, verb-on-every-figure.
- [ ] `name_truncated` / individual quarantine enforced in the view.
- [ ] CC-BY attribution + landing page + `retrieved_utc` shown in the UI.
- [ ] No inference / causal language in UI copy (overlap = co-occurrence only).

---

## 10. Open decisions

1. **Slug**: `rankings-procurement` (sibling-consistent) vs clean `procurement`.
   Plan assumes the former; flip both app.py + PAGES if we prefer clean.
2. **CRO join location**: row-level in the view each load (simple, DuckDB-fast) vs
   the pre-baked `procurement_supplier_cro_match.parquet` (current). Plan keeps the
   pre-baked supplier match + view LEFT JOIN.
3. **Coverage surface**: `v_procurement_coverage` view vs page reading the JSON
   directly. A view keeps the firewall pure; JSON is simpler. Lean view.
4. **Refactor timing**: this is feature work, fine to do now; the broader
   src/ reorg stays deferred ([[project_reorg_plan]], [[feedback_refactor_timing]]).
