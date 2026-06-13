# Dáil Tracker — Review Prompt Pack (2026-06-05 briefs)

Eight independent review sessions. Each prompt is paste-ready into a fresh Claude Code session opened in the repo (or one per git worktree). Every prompt assumes `doc/REVIEW_CONTEXT.md` carries the shared invariants, landmark map, verification protocol, action mode, CLAIMS-LEDGER format, and output schema — so the prompts stay short.

Run order: 1–7 in parallel (disjoint outputs); 8 last, after the others land.

---

## 1 — CBI Second-Pass

```text
Review session for Dáil Tracker. First read doc/REVIEW_CONTEXT.md in full and follow it (invariants, landmark map, verification protocol, action mode, ledger + output schema).

Focus doc: doc/dail_tracker_cbi_second_pass_reconciliation.md. Save to doc/CBI_SECOND_PASS_REVIEW.md.

Task: ground-truth the doc's claims about current CBI ingestion (extractors/cbi_registers_extract.py, the "cbi" pipeline chain, sql_views/corporate_cbi_distress.sql, cbi_* parquet writers/consumers) — which outputs are gold vs sandbox, whether the corporate-notices xref is load-bearing, whether CIT/Designated Entities truly fail, whether member-interest/lobbying xrefs are unused. Then assess the proposed expansion from "register enrichment" → "regulated-entity intelligence" (warning notices, enforcement, prohibition, revocation, Dear CEO, AML bulletins, outlook reports): per family judge architectural fit, new-extractor-vs-extension, parse tractability (HTML vs ASP.NET postback vs PDF/OCR), cadence/maintenance, join key. Devil's advocate hard on: scope creep off the parliamentary mission, PII/defamation of naming individuals (personal-insolvency precedent), no-inference when surfacing "sanctioned", scraper decay, register name-match false positives. Recommend ≤3 highest-ROI additions + explicit reject/defer list, with value_kind/provenance discipline for each.
```

---

## 2a — Procurement / Public Money

```text
Review session for Dáil Tracker. First read doc/REVIEW_CONTEXT.md in full and follow it.

Focus doc: doc/dail_tracker_local_housing_procurement_judiciary_plan.md — ONLY the Procurement / "Public Money & Procurement" tile (sec 0–3 Tile 1, 6, 7 procurement views, 8 procurement tests, 10–14 procurement parts). Save to doc/PROCUREMENT_TILE_REVIEW.md.

Task: ground-truth procurement_etenders_extract.py, procurement_la_seed.py, procurement_la_payments_extract.py (confirm it's silver-only, not in pipeline.py), procurement_public_body_extract.py, procurement_award_spend_link.py, ted_ireland_extract.py, sql_views/procurement_*.sql, dail_tracker_core/queries/procurement.py, utility/data_access/procurement_data.py (does it unwrap QueryResult to an empty DataFrame on failure?). Assess the proposed page skeleton, QueryResult-aware data-access, value_kind_legend component, LA-payments promotion path, and award→payment candidate matching — fit with existing layering, duplication of utility/ui/components.py or the uncoupling plan, enforceability of the value taxonomy. Devil's advocate: award-as-spend, lobbying-overlap-as-causation, sole-trader PII, false candidate links, shipping a new tile before pages are hardened, 31-LA format drift. Recommend the minimal correct first slice (likely read-only page over existing views + caveat panel, no LA-payments promotion yet), defer/reject the rest.
```

---

## 2b — Local Authority & Housing

```text
Review session for Dáil Tracker. First read doc/REVIEW_CONTEXT.md in full and follow it.

Focus doc: doc/dail_tracker_local_housing_procurement_judiciary_plan.md — ONLY the Local Authority & Housing tile (sec 3 Tile 2, geography model, 6 housing/AFS contracts, 7 housing views, 8 housing tests, sec-13 "why not just Housing"). Also read doc/SSHA_social_housing_summary.md. Save to doc/LOCAL_AUTHORITY_HOUSING_REVIEW.md.

Task: ground-truth what LA/housing data actually exists (la_afs_extract.py / afs_amalgamated_extract.py and their parquets; confirm SSHA is docs-only; confirm no housing SQL views). Recall two known traps: the unresolved LA→constituency crosswalk, and AFS = operating-expenditure-by-division ≠ total spend. Assess the proposed source order (SSHA→grants→delivery→homelessness→census), the housing_ssha_la_fact schema/views, and especially the geography bridge (dimension_geography / bridge_la_constituency) — sound or hand-waving? Devil's advocate: SSHA net-need misread as total need (HAP/RAS/SHCEP exclusion), LA data shown as constituency/TD "performance" (no-inference), AFS mislabelled "total spend", forced LA mapping for region-grain homelessness, drift into a national-stats dashboard, building before procurement/hardening. Recommend the minimal slice (validate SSHA as clean silver + one housing-money source, LA-only geography with explicit approximate/unknown labels) before any page.
```

---

## 2c — Courts & Judiciary

```text
Review session for Dáil Tracker. First read doc/REVIEW_CONTEXT.md in full and follow it. Privacy is the dominant axis.

Focus doc: doc/dail_tracker_local_housing_procurement_judiciary_plan.md — ONLY the Judiciary/Courts lane (sec 4 full, 6 judiciary contract, 7 judiciary+CPO views, 8 judiciary tests, 9 acceptance, Sprint-7 CPO probe). Save to doc/JUDICIARY_LANE_REVIEW.md.

Task: ground-truth utility/app.py routing of Courts & Judiciary, pdf_infra legal-diary poller, extractors/legal_diary_extract.py, the three judiciary sql_views, dail_tracker_core/queries/judiciary.py, utility/data_access/judiciary_data.py, utility/pages_code/judiciary.py. Confirm legal-diary is NOT a pipeline.py chain (separate poller). Check whether privacy invariants use asserts (stripped under -O) vs runtime exceptions. Audit the case-row contract for any field that could leak a natural person / case reference / solicitor annotation; judge whether anonymisation is test-verifiable or merely asserted. Devil's advocate: re-identification via court+date+list even when anonymised, "listed sessions" still implying judge performance, raw_case reaching gold, CPO probe leaking addresses/persons, civic value vs privacy/legal risk. Confirm CPO belongs to Infrastructure/LA context, not merged into Judiciary. Recommend ship-guarded / beta-hidden / hold + the exact golden privacy tests that must precede any public exposure.
```

---

## 3 — Public-Record Sources (Brief #1)

```text
Review session for Dáil Tracker. First read doc/REVIEW_CONTEXT.md in full and follow it. Remember: surfacing, not ingesting, is the bottleneck.

Focus doc: doc/dail_tracker_public_record_intelligence_sources_for_claude.md (source-discovery brief: board minutes, FOI/AIE logs, quarterly trackers, audit, planning/housing, EU funds). Save to doc/PUBLIC_RECORD_SOURCES_REVIEW.md.

Task: de-duplicate every cluster against what's already ingested/scoped (reference_irish_gov_data_sources, project_procurement_semistate, project_new_sources_scoping_2026_06_04, project_source_health_registry; eTenders/LA-AFS/TED already done) — mark ALREADY DONE / ALREADY SCOPED / GENUINELY NEW with the covering file/doc. For new clusters score format tractability (structured HTML/CSV/XLSX/ArcGIS = good; PDF minutes / FOI logs = hard), join key to existing entities, cadence, maintenance fragility (31-LA drift), and dataset-vs-"records-exist" signal (FOI logs prove existence but carry no extractable content). Devil's advocate: is board-minute / risk-register / disclosure-count ingestion mission-serving or scope sprawl into general "due diligence" the project can't maintain? PII/defamation? vanity sources that never join? Be willing to say "most of this is out of scope; here are the few that aren't." Shortlist ≤5 that complete an EXISTING half-built surface (C&AG/NOAC/OPR→LA-AFS+procurement; PI2040→infrastructure; State Boards→public-body universe), each with value_kind/provenance and the page it feeds.
```

---

## 4 — Tangible Sources (Brief #2)

```text
Review session for Dáil Tracker. First read doc/REVIEW_CONTEXT.md in full and follow it. "Has a URL" ≠ "worth building".

Focus doc: doc/dail_tracker_second_pass_tangible_sources.md (concrete URLs + formats). Save to doc/TANGIBLE_SOURCES_REVIEW.md.

Task: de-duplicate sec 3 (eTenders/OGP) and 3.2 (LA budgets/AFS) against procurement_etenders_extract.py and the AFS extractors — what's genuinely additive (data.gov.ie eTenders open-data as a STABLE alternative to scraping; quarterly mini-comp CSVs; OGP framework expiry/drawdown metadata) vs redundant; produce an overlap ledger. Independently rank ALL families by build-tractability (machine-readable: DHLGH ArcGIS/CSV/GeoJSON, Social Housing Construction Status XLSX, DPC fines table, ERDF XLSX, CSO public-body register = high; PDF minutes/annual-reports/audit chapters = low, dual-parser + reconciliation gate). For each high-tractability source give likely extractor shape, join key, surface it feeds. Devil's advocate: tangible-but-useless (a lone board-minutes PDF), PII (maritime usage-licence submissions, FOI released records), fragile per-body scrapers, where dual-parser+reconciliation makes a PDF uneconomic, what's better as manual reference than ETL. Build/defer/reject per family weighted to machine-readable + completes-existing-surface + low decay; each "build" gets value_kind, provenance/source-hash, parquet-zstd, a reconciliation check.
```

---

## 5 — Ship vs Rebuild

```text
Review session for Dáil Tracker. First read doc/REVIEW_CONTEXT.md in full and follow it.

Focus doc: doc/dail_tracker_ship_vs_rebuild_strategy.md (argues: ship current Streamlit as hardened public beta, don't rebuild UI yet). Also read doc/fastapi_query_core_uncoupling_plan.md. Save to doc/SHIP_VS_REBUILD_REVIEW.md.

Task: ground-truth the readiness table (sec 2) and coupling claims (sec 7) — do pages already have provenance footers / freshness banners / empty-state + sidebar_shell helpers? is freshness.json generated at pipeline end (tools/check_freshness.py)? does data-access already return empty DataFrames on failure rather than QueryResult? how coupled are pages really (CSS in shared_css.py, logic-firewall markers)? Much of the doc's "minimum hardening" may already be DONE — find out via the per-page audit memories. Assess the proposed new UI components + QueryResult pattern vs existing helpers and the uncoupling plan (additive or reinventing?), and the four framework options vs the committed DuckDB-core-seam/registry/FastAPI direction. Devil's advocate both ways: is the data trustworthy enough for a PUBLIC beta given open logic-firewall violations (value_counts, theme classification), audit DQ gaps, and PII domains (judiciary/insolvency)? is "harden Streamlit first" sunk-cost into a layer you're decoupling from? what's under-weighted: legal exposure of publishing enforcement/judiciary data, pipeline not yet proven headless on Ubuntu, public-launch moderation/feedback load. Deliver ship/partial-ship/hold with a page-by-page allowlist tied to each page's open P0/P1 audit items, and a hardening list scoped to what's actually undone.
```

---

## 8 — Synthesis (run last)

```text
Synthesis session for Dáil Tracker. First read doc/REVIEW_CONTEXT.md in full and follow its invariants.

Read all seven review outputs: doc/CBI_SECOND_PASS_REVIEW.md, PROCUREMENT_TILE_REVIEW.md, LOCAL_AUTHORITY_HOUSING_REVIEW.md, JUDICIARY_LANE_REVIEW.md, PUBLIC_RECORD_SOURCES_REVIEW.md, TANGIBLE_SOURCES_REVIEW.md, SHIP_VS_REBUILD_REVIEW.md. Save to doc/REVIEW_SYNTHESIS.md.

Task: produce ONE consolidated roadmap. (1) Merge and DEDUPE every "build" recommendation across reviews — the two source briefs and the procurement tile overlap heavily (eTenders open-data, LA-AFS, C&AG/NOAC), so collapse duplicates into single line-items with the strongest rationale. (2) Reconcile conflicts between reviews (e.g. ship-now vs build-new-tiles) and state the resolved position. (3) Rank the deduped backlog by value/effort, honouring "surfacing > ingesting" and the never-union/no-inference/privacy invariants. (4) Produce a single Build-Next shortlist (≤8), a Defer list, and an explicit Out-of-Scope list, each item tagged to the surface it serves and the review(s) it came from. (5) One-paragraph executive bottom line: what to ship, what to build first, what to reject. Cite the source review for every line-item.
```
