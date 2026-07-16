# pipeline_sandbox — what's load-bearing, what's dead, how to touch it safely

`pipeline_sandbox/` is **not** a graveyard. It is half throwaway probes and half
**deliberately-sandboxed live scrapers**. Treat "it's in sandbox" as *no signal*
about whether it's safe to delete or move — check the wiring (below) first.

## The deliberate pattern (why live code lives here)

Fragile, network-heavy scrapers **stay** in `pipeline_sandbox/` on purpose: run by
hand, **not** wired into `pipeline.py` or the GitHub Action. A transform-only
`extractors/*_promote_to_gold.py` reads their already-vetted `data/sandbox/…parquet`
and writes the **committed** gold projection → SQL view → test. The committed gold
is the safety net: the scraper can break or change and the shipped data survives.

Reference: `extractors/enrichment_promote_to_gold.py` header; `extractors/epa_promote_to_gold.py`.

## ⚠️ LIVE extractors that live in `pipeline_sandbox/` — DO NOT DELETE

Each is invoked by **path string** (not import), so a grep-for-imports check misses
them. Each is called by a promote script, backs a registered SQL view, and/or is
imported by a test:

| Sandbox file | Wired into | Test |
|---|---|---|
| `isif_portfolio_extract.py` | `enrichment_promote_to_gold.py`, `corporate_isif_portfolio.sql` | ✅ `test_enrichment_parsers` + `test_enrichment_promote_privacy` |
| `eu_tam_ireland_extract.py` | `enrichment_promote_to_gold.py`, `procurement_eu_tam_state_aid.sql` | ✅ same |
| `cbi_enforcement_extract.py` | `enrichment_promote_to_gold.py`, `corporate_cbi_enforcement.sql` | ✅ same |
| `epa_accountability_view.py` (+ `epa_capability_register`, `epa_enforcement_pull`, `_capability_join`) | `epa_promote_to_gold.py`, `procurement_epa_compliance.sql` | ✅ `test_epa_promote_privacy` (added 2026-06-27) |
| `nsai_capability_register.py`, `nsai_certified_companies_scrape.py` | promoted feature → `data/sandbox/parquet/nsai_capability_register.parquet` | — |
| `legal_diary_openview_probe.py` | origin of `extractors/legal_diary_openview_extract.py` | ✅ `test_legal_diary_openview` |
| `news_mentions/` | `extractors/news_mentions_extract.py` | ✅ `test_news_mentions_extract`, `test_per_member_search` |
| `housing/` (`noac_housing_wide…`, `ssha_appendix_wide…`) | `derelict_sites_levy_extract.py`, `noac_collection_rates_extract.py`, constituency SQL views | ✅ `test_derelict_sites_levy`, `test_noac_collection_rates` |
| `council_minutes/` | `extractors/councillors_promote_to_gold.py` | ⚠️ via `test_la_councillors` (active WIP) |
| `committee_evidence/` | `extractors/committee_witnesses_extract.py` (→ silver) | ⚠️ view-registration only |
| `disclosed_po_spend/` | `extractors/disclosed_bq_po_extract.py` + `DISCLOSED_PO_INTEGRATION_PLAN.md` | — (TIER1-B, not promoted yet) |
| `courts_reader/` | validated in `procurement_public_body_extract.py` | (covered by public-body tests) |
| `_planning_output/` (data dir) | read by `extractors/planning_decision_profiles.py` etc. | ✅ `test/siting/` |
| `procurement_payee_cro_anchor_probe.py`, `iris_planning_notices_audit.py`, `planning_areaofsite_normalise.py`, `planning_scale_gated_triggers.py` | cited as provenance / registered audit tools in `extractors/`, `doc/`, `planning_corpus_seed.csv` | — |

**Before retiring anything from sandbox**, confirm it isn't referenced by path:
```bash
git grep -l -- "<basename>" -- extractors services sql_views test doc planning_rules
```

## Test-anchor status of the committed-gold promotions

Almost everything that reaches committed gold is test-anchored. The one gap found in
the 2026-06-27 audit — **EPA** (`epa_promote_to_gold.py`: committed gold + natural-person
privacy mask, but only view-level smoke coverage) — was closed by extracting a pure
`project_supplier_compliance()` + `gold_pii_columns()` and adding
`test/extractors/test_epa_promote_privacy.py` (8 tests, incl. an integration check on the
committed parquet). Behaviour unchanged.

## Obsolete prototypes — feature ALREADY promoted to a self-contained extractor (RETIRED 2026-06-27)

A third category the original map missed: prototypes whose feature is **fully promoted** to a
production extractor that fetches **and** transforms itself (reads NO sandbox output) — unlike the
live scrapers above, nothing depends on the sandbox copy, so it is pure dead weight. Verified
self-contained + no code/test references (only narrative doc mentions). Removed; restore tracked code
with `git checkout 936650a -- <path>` (untracked prototype parquets/csvs were not in history).

| Retired sandbox dir | Superseded by (self-contained prod extractor) |
|---|---|
| `noac_accountability/` | `extractors/noac_indicators_long_extract.py` + `noac_scorecard_extract.py` (live on local_government page, tested) |
| `participation/` | `extractors/participation_extract.py` (gold + attendance page) |
| `member_contact/` | `extractors/member_contact_extract.py` (gold + member-overview) |
| `dept_children_payments/` | `extractors/procurement_dept_readingorder_parser.py` (gold + accommodation-spend page) |

Still-WIP prototypes (NOT promoted, kept): `disclosed_po_spend/` (TIER1-B genuine gap, verified
2026-06-27), `spend_service_bridge/`, `pq_disclosures/`, `historic_members/`.

## Dead probes — ARCHIVED to `pipeline_sandbox/_archive/` (2026-07-16, from HEAD `18dd551`)

Zero references in live code or tests; cold 3+ weeks. Moved (not deleted) to
`pipeline_sandbox/_archive/` — restore with `git mv pipeline_sandbox/_archive/<f> pipeline_sandbox/<f>`
or `git checkout 18dd551 -- pipeline_sandbox/<f>`. See that folder's `README.md`.

`etenders_live_probe.py`, `etenders_itt_pull_probe.py` (ITT login/JS-gated dead end),
`inspect_hse_tusla.py` (throwaway), `cpo_planning_prospect_probe.py` (CPO feature parked),
`procurement_unlinked_payees_probe.py` (superseded by `services/coverage_qa.py`),
`si_department_backfill.py` (one-off backfill, already applied),
`procurement_la_registry.py` (routes merged into `extractors/procurement_la_payments_extract.py`).

### Still under review (NOT archived — confirm before moving)
- **Siting probes** `siting_api_bench.py`, `siting_api_prototype.py`,
  `siting_grid_precompute_experimental.py`, `siting_layers_simplify_experimental.py`
  (`_finalize` sibling shipped) — no live refs, but may be intentional hand-run
  benchmarks; awaiting owner call.
- **`pipeline_sandbox/housing/` experimental cluster** (~19 probes) — the Housing PAGE is
  LIVE and integrated; a handful of these feed it (see below), the rest are exploratory.
  Needs a per-file producer check before any move.
