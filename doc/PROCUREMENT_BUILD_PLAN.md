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

## 9. Logic-firewall checklist (gate before merge)

- [ ] No `read_parquet` / JOIN / GROUP BY / window / pandas merge in
      `procurement.py` or `procurement_data.py`.
- [ ] Every business metric (counts, safe-to-sum totals, CRO match, overlap)
      defined in a `sql_views/procurement_*.sql` view.
- [ ] `value_safe_to_sum` gating lives in the view; the page can only display it.
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
