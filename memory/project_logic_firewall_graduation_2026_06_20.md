---
name: project_logic_firewall_graduation_2026_06_20
description: Logic-firewall audit outcome — corporate.py panels graduated to gold; SQL→Polars "precompute for lazy execution" premise is FALSE because the app caches every query (@st.cache_data)
metadata:
  type: project
---

Audit "business logic in display layer → ETL" + "SQL views → Polars lazy execution", actioned 2026-06-20.

**DONE (Part A — real firewall wins):**
- corporate.py receiver-appointer ranking + operator-firm concentration + CBI badge GRADUATED to gold via `extractors/corporate_receiver_enrich.py` (production location, wired into pipeline.py after the `cro` step, NOT pipeline_sandbox — user rule: lift to production when it writes gold consumed by the live app). Writes `corporate_notices_enriched.parquet` (notices superset: is_receivership/is_spv/has_parent_mention/receiver_firms/has_receiver_firm/cbi_register/cbi_ref_no/year) + `corporate_receiver_appointers.parquet` + `corporate_receiver_firms.parquet`. Views in `sql_views/corporate/corporate_receiver.sql`; `v_corporate_notices` repointed to the superset. The page `_render_featured`/`_render_operator_strip` now select-and-render only. Test: `test/extractors/test_corporate_receiver_enrich.py`. The enrichment has an in-script parity guard (gold == verbatim page logic).
- **CBI badge method = B2** (validated empirically): exact entity-norm match ∪ ≥2-token longest-substring. The OLD page (B1, len≥6 only) shipped FALSE POSITIVES — `donnybrook` matched a street address, `allianz` matched a different legal entity. B2 drops the 31 single-token FPs, keeps 62 genuine prefixed-entity recoveries (300 badges total vs 238 exact). Never re-introduce single-token substring matching.
- company.py: `value_counts` on one firm's notices = legit display_only (marker added); EPA membership join graduated to `has_epa_licence` flag on `v_procurement_supplier_summary` (LEFT JOIN epa_supplier_compliance.parquet WHERE n_licences>0). Needed a new fixture `test/fixtures/sql_views/data/gold/parquet/epa_supplier_compliance.parquet` (company_num 123456→licensed).

**DE-SCOPED (Part B — premise was false):** The audit said SQL views are "recomputed every page load → precompute in Polars." **NOT TRUE: 24/25 data-access fetchers are `@st.cache_data` (ttl 300–600s)**, so views recompute at most once per cache window per param, NOT per load. entity_search measured 578ms cold / 117ms warm, cached 10min. The procurement summary views are architecturally CORRECT (joins belong in registered views per CLAUDE.md). Precompute would trade data freshness + pipeline complexity for ~0.5s once/session. Before any "precompute a hot view" work: measure server_ms AND check the @st.cache_data layer first (see [[project_scaling_plans_2026_06_18]]). PR 0 (legal-diary OOM) also dropped — the view's `current_date-7` cap already bounds it to 4.2MB via predicate pushdown.

**Reusable lesson:** audit subagents overstate — the legal-diary agent said "790k-row OOM" (true row count, but the cap already fixes it); the SQL agent said "recomputed every load" (false — cached). Verify each flagged clause against the cache layer + predicate pushdown before building precompute. Links: [[feedback_pipeline_changes_data_anchored_promotion]], [[feedback_parquet_write_convention]].
