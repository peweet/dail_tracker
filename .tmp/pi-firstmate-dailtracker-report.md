# Executive summary

I synthesized the four independent reconnaissance reports and cross-checked the most consequential claims against the current public repository at HEAD `87b766a9`. I found no confirmed Critical issue. The dominant risks are High/Medium-High: user-facing money-grain contract violations in the TED procurement surface, red clean-checkout verification gates, and scheduled-refresh/publish paths that can ship or keep mutating after failures. A second cluster is contract drift: public docs, generated contracts, source registries, and monitoring metadata have lagged behind the app/API/data surfaces they are meant to govern.

The repository also has real strengths: explicit no-wrongdoing framing, a Streamlit-free core shared by API/MCP/UI, useful three-state query error semantics, atomic parquet and runtime data-contract helpers, privacy-aware bulk exports, and multiple security/secret guards. Those controls make the gaps tractable, but several controls are stale, too narrow, or not wired into the current public checkout.

## Ranked top-risk table

| Rank | Finding | Severity | Impact summary | Confidence / corroboration |
|---:|---|---|---|---|
| 1 | TED procurement values are summed and TED row grain is mislabeled | High | Can display misleading euro totals and inflated “notice” counts, contradicting the repo’s own never-sum TED rule. | High; data scout, independently verified. |
| 2 | Clean public checkout verification is red in multiple places | High | CI/local gates can fail before product logic is tested; contributors cannot trust the advertised check surface. | High; architecture + reliability scouts, independently reproduced for doc-index/MCP/conventions and verified `.claude` test gap. |
| 3 | Daily money-flow refresh can publish after `pipeline.py` returns non-zero | High | Partial/mixed data refreshes can be committed after chain failures. | High; reliability scout, independently verified. |
| 4 | POSIX chain timeout kills only the parent process | High | Timed-out Linux refreshes can leave child/grandchild ETL processes running and mutating data after failure. | High; reliability scout, independently verified. |
| 5 | Source registry, output monitoring, coverage sidecars, and source-cadence checks are incomplete/stale | Medium-High | Health dashboards and publish gates can understate parser coverage or miss silver/coverage regressions. | High; data + architecture scouts, independently verified. |
| 6 | API/UI/public documentation contracts are stale or internally inconsistent | Medium-High | Public users and API consumers are told the wrong resource/page/route surface; migration contracts become unreliable. | High; product + architecture scouts, independently verified. |
| 7 | API readiness and delivery smokes are too shallow; curl fallback disables TLS verification | Medium | Deployments can be “healthy” while most resources are broken; fallback downloads can accept unverified TLS responses. | High; reliability scout, independently verified. |
| 8 | Streamlit trust/security defects: missing VAT caveat, unescaped legacy source links, and a visible label bug | Medium | Public-payment totals may be compared on mixed VAT bases; source links are an HTML injection surface; Dáil counts can be mislabeled. | High for code facts; product scout independently verified here. |
| 9 | Core query layer and CSV affordances diverge from their documented/product contracts | Medium | Joins/aggregations can bypass SQL-view review, and high-value pages do not meet the “CSV export for every view” product promise. | High for static code; product scout independently verified here. |
| 10 | Runtime/deployment/security hardening gaps remain | Medium/Low-Medium | Some direct extractors bypass runtime thread caps; privileged CLA workflow and mutable Docker base weaken supply-chain posture; public errors expose internals. | Medium-High; reliability scout independently verified representative spans. |

## Cross-cutting themes and dependencies

- **Contract drift is systemic, not isolated.** Generated/public docs (`doc/INDEX.md`, `doc/URL_CONTRACT.md`, `doc/CLASS_CONTRACT.md`, `doc/CLOUD_READINESS.md`, `doc/SOURCES.md`, README) are often older than the source they describe. Some checks pass because they verify the generator’s own stale representation rather than the committed document or live route semantics.
- **Money-grain/provenance guardrails exist, but newer surfaces exceed their coverage.** The repo states never-sum rules and has fact-card/lint tooling, yet TED same-grain sums, planned live-tender estimates, payment VAT caveats, source-registry metadata, and silver-output monitoring sit outside or beyond current ratchets.
- **Soft degradation needs matching readiness/monitoring.** `swallow_errors=True` and `QueryResult` improve user-facing semantics, but shallow `/health` and gold-only output baselines can mask broad API/view breakage.
- **Public/private planning boundaries need explicit public-tree handling.** The public repo intentionally lacks `planning/product/**`, yet docs and MCP surfaces still reference Siting/Planning features. This is documentation-boundary evidence only; I did not inspect any private worktree.
- **Several high-impact remediations depend on policy choices.** TED monetary presentation and public Planning/Siting exposure are verified defects/contract conflicts, but the target behavior requires later captain/product approval before implementation.

## Strengths confirmed

### S1 — Trust framing and no-wrongdoing language are strong
- **Severity:** Positive.
- **Impact:** Reduces risk that accountability records are read as allegations.
- **Confidence:** High.
- **Evidence:** `README.MD:23` says the project is not an official record and does not infer wrongdoing/influence/causation; `README.MD:84` describes Streamlit and FastAPI reading through a Streamlit-free core; `PRODUCT.md:40-41` makes “Data is the evidence” and “Accessible by default” explicit design principles.

### S2 — Core query/API semantics distinguish outage from true empty data
- **Severity:** Positive.
- **Impact:** UI/API/MCP can avoid silently presenting missing sources as “no results.”
- **Confidence:** High.
- **Evidence:** `dail_tracker_core/results.py:1-16` documents the three-state `QueryResult`; `dail_tracker_core/results.py:33-40` defines `SourceUnavailable`; `dail_tracker_core/results.py:86-94` raises it at required gates; `api/main.py:164-170` maps it to a 503 JSON response.

### S3 — Layering and UI/database firewalls are actively guarded
- **Severity:** Positive.
- **Impact:** Keeps Streamlit presentation, reusable query code, and SQL contracts more separable.
- **Confidence:** High.
- **Evidence:** `test/dail_tracker_core/test_core_no_streamlit_imports.py:46-72` statically/dynamically blocks Streamlit in core; `test/utility/test_firewall_no_raw_db_in_ui.py:5-13` states UI SQL/parquet access is a leak; `test/utility/test_firewall_no_raw_db_in_ui.py:69` fails on raw UI DB access. I also ran `PYTHONDONTWRITEBYTECODE=1 python3 tools/check_streamlit_logic_firewall.py`, which reported `OK — scanned 85 files`.

### S4 — Data-safety primitives are mature
- **Severity:** Positive.
- **Impact:** Atomic writes, row floors, and runtime contracts reduce silent data loss.
- **Confidence:** High.
- **Evidence:** `services/parquet_io.py:1-34` documents atomic `.part`/replace writes, compression conventions, and optional row floors; `services/data_contracts.py:1-37` explains runtime contract enforcement for payment facts; `extractors/procurement_etenders_extract.py:531-535` calls `guard_award_fact()` before writing awards; `extractors/procurement_payments_consolidate.py:932-973` calls `guard_payment_fact()` and then `save_parquet(..., min_rows=...)`.

### S5 — Security/privacy controls exist in important places
- **Severity:** Positive.
- **Impact:** Good baseline for a public civic-data repository.
- **Confidence:** High.
- **Evidence:** Weekly `pip-audit --strict` is configured at `.github/workflows/audit.yml:25-33`; gitleaks full-history CI is configured at `.github/workflows/ci.yml:69-81`; the API container runs as non-root at `Dockerfile:54`; exports are default-deny and filter natural persons at `api/routers/exports.py:1-15`; MCP documents stdio-only/no-remote posture at `mcp_server/server.py:14-17` and read-only annotations at `mcp_server/server.py:332`.

## Prioritized confirmed defects / material gaps

### F1 — TED procurement values are summed despite the repo’s never-sum TED invariant
- **Severity:** High.
- **Domain:** Extraction/data contracts, SQL views, core queries, product UI.
- **Impact:** Users can see TED euro totals described as “summable awarded value,” even though the project rule says TED notice values must not be summed. This can overstate or mischaracterize EU Official Journal awards.
- **Confidence:** High.
- **Corroboration:** Data scout; independently verified at HEAD.
- **Evidence:** The invariant appears in `AGENTS.md:50`, `sql_views/AGENTS.md:3`, and `doc/DATA_GRAINS.md:73`. Current code sums TED values in `dail_tracker_core/queries/procurement/ted.py:39-40`, `sql_views/procurement/procurement_ted_supplier_summary.sql:17-20`, and `sql_views/procurement/procurement_entity_chain.sql:36`. The UI presents this as “summable awarded value” at `utility/pages_code/procurement/ted.py:123-124` and again at `utility/pages_code/procurement/ted.py:202`.
- **Dependency / approval note:** The defect is verified; the preferred replacement display (counts/median/distribution vs. retained diagnostics) is a product/data-policy choice.

### F2 — TED row grain is inconsistent, and headline “notice” counts can count winner rows
- **Severity:** High.
- **Domain:** Extraction/data contracts, core queries, UI copy.
- **Impact:** A `notice × winner` feed can be treated as one row per notice, inflating headline counts where notices have multiple winners and misleading downstream metadata consumers.
- **Confidence:** High.
- **Corroboration:** Data scout; independently verified.
- **Evidence:** The extractor declares `Grain: ONE ROW PER (notice x winner)` at `extractors/ted_ireland_extract.py:16`; the history view repeats `notice × winner` at `sql_views/procurement/procurement_ted_awards_history.sql:7`. Metadata instead says `ted_ie_awards` is “one row per TED award notice” at `data/_meta/fact_contracts.yaml:39` and `data/_meta/fact_grain.csv:4`. The corpus stats query labels `COUNT(*)` as `n_notices` at `dail_tracker_core/queries/procurement/ted.py:33`; the UI labels it “EU Official Journal award notices” at `utility/pages_code/procurement/ted.py:123`. A nearby trend query correctly uses `COUNT(DISTINCT publication_number)` at `dail_tracker_core/queries/procurement/ted.py:78`, confirming the distinction is known.

### F3 — Clean public checkout verification is currently not portable/trustworthy
- **Severity:** High.
- **Domain:** Reliability, CI, maintainability, architecture docs.
- **Impact:** Contributors and CI can hit failures unrelated to their changes, blocking or obscuring real regressions.
- **Confidence:** High.
- **Corroboration:** Architecture scout found red doc/MCP/conventions gates; reliability scout found the `.claude` portability failure; I independently reproduced/check-read both clusters.
- **Evidence:**
  - CI runs doc-index and MCP-catalog checks at `.github/workflows/ci.yml:23-24`; `tools/dev.py:67-89` defines the same convention/MCP/doc-index tasks and `tools/dev.py:119-123` includes them in the check surface.
  - `PYTHONDONTWRITEBYTECODE=1 python3 tools/build_doc_index.py --check` returned `doc/INDEX.md is STALE`; the committed index links missing `doc/PLAN_ACP_GEOMETRY_AND_PRECEDENT.md` at `doc/INDEX.md:83`, while `tools/check_no_private_ip.py:89` intentionally denies that path as private/sensitive.
  - `PYTHONDONTWRITEBYTECODE=1 python3 tools/check_mcp_catalog.py` failed with `FileNotFoundError` for `tools/evals/tool_retrieval_queries.json`; the checker hardcodes that manifest at `tools/check_mcp_catalog.py:20` and reads it at `tools/check_mcp_catalog.py:175`. `git ls-files 'tools/evals/tool_retrieval_queries.json'` returned no tracked path.
  - `PYTHONDONTWRITEBYTECODE=1 python3 tools/check_conventions.py` failed on `tools/build_delivery_smoke_fixture.py:50` (`relation.write_parquet(str(output))`), while the root invariant requires `services.parquet_io.save_parquet` at `AGENTS.md:53`; `test/tools/test_conventions.py:20` runs this ratchet in tests.
  - `test/test_runtime_env.py:48-50` requires `.claude/settings.json`, and `test/test_runtime_env.py:154-159` reads it unconditionally. `.gitignore:282` ignores `.claude/`, `.gitignore:290-292` says it stays local, and `git ls-files '.claude/**'` returned no tracked file; pytest is the default test path at `pyproject.toml:361-362` and CI runs non-integration tests at `.github/workflows/ci.yml:132`.

### F4 — Money-flow workflow can publish after `pipeline.py` exits non-zero
- **Severity:** High.
- **Domain:** Reliability, deployment, data publishing.
- **Impact:** A scheduled refresh can commit allow-listed data from a partial failed run. The publish gate catches row/schema regressions but is not tied to overall pipeline success.
- **Confidence:** High.
- **Corroboration:** Reliability scout; independently verified.
- **Evidence:** `pipeline.py:772-777` returns `1` when any chain is broken. `.github/workflows/money_flow_refresh.yml:85-100` captures that exit under `set +e` and stores `pipeline_exit`. The heartbeat step correctly requires `steps.run.outputs.pipeline_exit == '0'` at `.github/workflows/money_flow_refresh.yml:102-108`, but the publish step at `.github/workflows/money_flow_refresh.yml:110-117` is gated only on `skip_publish`, not on the captured exit code.

### F5 — POSIX chain timeouts do not kill descendant processes
- **Severity:** High.
- **Domain:** Reliability, pipeline operations.
- **Impact:** On Linux runners, a timed-out chain can leave child/grandchild Python processes alive and still mutating data/logs after the orchestrator records failure.
- **Confidence:** High.
- **Corroboration:** Reliability scout; independently verified.
- **Evidence:** `pipeline.py:424-429` states the timeout must kill the process tree; `_kill_process_tree()` repeats that descendant contract at `pipeline.py:515-523`. The Windows branch uses `taskkill /T` at `pipeline.py:527-532`, but the POSIX branch only calls `proc.kill()` at `pipeline.py:534`. Chain wrappers spawn children with `subprocess.run`, e.g. `lobbying_refresh.py:42` and `lobbying_refresh.py:52`, `payments_refresh.py:35` and `payments_refresh.py:44`. Scheduled money-flow runs on Ubuntu at `.github/workflows/money_flow_refresh.yml:49-50`.

### F6 — Source registry and monitoring metadata understate live coverage and miss high-value outputs
- **Severity:** Medium-High.
- **Domain:** Extraction, monitoring, provenance, architecture.
- **Impact:** Health/cadence dashboards can say live parsed sources are unwired; publish gates can miss regressions in silver facts that SQL views expose directly; most coverage sidecars are outside the extraction-quality guard.
- **Confidence:** High.
- **Corroboration:** Data + architecture scouts; independently verified representative spans.
- **Evidence:**
  - Pipeline wires public-body payments at `pipeline.py:214-220`, local-authority payments at `pipeline.py:242-247`, and consolidation at `pipeline.py:248-254`.
  - The registry adapter still says public-body targets are “not yet wired into pipeline.py” at `tools/build_source_registry.py:164-167`; the public-body test locks `parser_wired is False` at `test/tools/test_source_registry.py:43-66`. LA adapter records pollability but does not set `parser_wired=True` at `tools/build_source_registry.py:191-216`.
  - Generated examples show live pollable sources as unwired: Waterford LA at `data/_meta/source_registry.generated.json:1782-1794`, Dept Climate at `data/_meta/source_registry.generated.json:2002-2016`, OPW at `data/_meta/source_registry.generated.json:2620-2634`.
  - `tools/check_output_regressions.py:1-10` scopes the completeness guard to gold parquet; it imports `GOLD_PARQUET_DIR` at `tools/check_output_regressions.py:37` and emits current baselines for committed gold only at `tools/check_output_regressions.py:69-84`. Yet live SQL views read silver directly, including `sql_views/procurement/procurement_ted_awards.sql:45`, `sql_views/procurement/procurement_ted_awards_history.sql:34`, `sql_views/procurement/procurement_ted_tenders.sql:27`, `sql_views/procurement/procurement_afs_national.sql:23`, and `sql_views/procurement/procurement_live_tenders.sql:34`.
  - `tools/check_extraction_quality.py:11-17` documents a two-extractor pilot; adapters cover only `judiciary_diary_link_coverage.json` and `supplier_entity_xref_coverage.json` at `tools/check_extraction_quality.py:49-62`, while a bounded `git ls-files '*coverage*.json' | wc -l` found 42 tracked coverage JSONs.

### F7 — Coverage/provenance JSON sidecars remain non-atomic in many live extractors
- **Severity:** Medium.
- **Domain:** Extraction, provenance, monitoring.
- **Impact:** A crash can corrupt or truncate coverage evidence even when parquet facts remain atomic and healthy.
- **Confidence:** High.
- **Corroboration:** Data scout; independently verified.
- **Evidence:** `services/coverage_io.py:1-20` explains why `write_text(json.dumps(...))` is not atomic and provides `save_coverage()`. `extractors/AGENTS.md:3` and `planning/civic/extractors/AGENTS.md:4` require the helper. Representative live offenders still write raw JSON: `extractors/procurement_etenders_extract.py:635`, `extractors/procurement_payments_consolidate.py:1015`, `extractors/public_body_payments/main.py:259`, `extractors/ted_ireland_extract.py:546`, and `planning/civic/extractors/planning_appeal_outcomes.py:514`. A bounded `git grep -l 'write_text(json.dumps' -- 'extractors/**' 'planning/civic/extractors/**' | wc -l` returned 42 files.

### F8 — API/UI/public docs and generated URL contracts have drifted from the current app/API
- **Severity:** Medium-High.
- **Domain:** Product, Streamlit UX, public API, documentation/architecture.
- **Impact:** Users, API consumers, and migration work can rely on wrong public counts, missing/extra pages, and a route shape that differs from app helpers.
- **Confidence:** High.
- **Corroboration:** Product + architecture scouts; independently verified.
- **Evidence:**
  - README says the Streamlit app has 27 pages across 8 sections including Planning at `README.MD:238-240`; `doc/SOURCES.md:222-232` repeats 27 pages and a Planning/Siting section. Current `utility/app.py` groups are at `utility/app.py:112`, `utility/app.py:124`, `utility/app.py:180`, `utility/app.py:223`, `utility/app.py:286`, `utility/app.py:312`, and `utility/app.py:327` — no Planning group. A read-only AST count found `routes=30 visible=24 hidden=6`; `git ls-files 'planning/product/**' 'utility/pages_code/siting_check.py' | wc -l` returned 0.
  - README says “16 published resources” at `README.MD:272`; `doc/SOURCES.md:189-211` documents 16 resources plus meta. Current `_RESOURCES` in `api/routers/catalog.py:21` contains 22 entries by AST count, including newer resources at `api/routers/catalog.py:170-221`; `test/api/test_api_new_domains.py:154-162` pins those newer catalogue names.
  - API top-level source attribution at `api/routers/catalog.py:262` names only `api.oireachtas.ie + lobbying.ie + SIPO + Charities Regulator`, despite the same catalogue exposing procurement, public payments, housing, public finance, local government, constituencies, and councillors.
  - README still puts bulk data exports on the roadmap at `README.MD:301`, while `api/main.py:217` includes the exports router and `api/routers/exports.py:1` documents live `/v1/data` downloads.
  - `doc/URL_CONTRACT.md:7-13` calls deep links an external contract but lists routes as `?page=<slug>` (e.g. `doc/URL_CONTRACT.md:18`, `doc/URL_CONTRACT.md:40`). The generator emits that display at `tools/migration/extract_url_contract.py:273`. Live helper docs and code use path slugs: `utility/ui/entity_links.py:59-67` defines canonical `url_path` slugs, and `utility/ui/entity_links.py:112-128` emits `/member-overview?...`, `/rankings-votes?...`, etc. `test/utility/test_internal_link_slugs.py:3-7` describes the live dead-link check as literal `href="/<slug>"` against `st.Page(url_path=...)`.

### F9 — API/container readiness and delivery smoke tests are too shallow
- **Severity:** Medium.
- **Domain:** API, deployment, CI.
- **Impact:** `/v1/health`, Docker healthcheck, and delivery CI can be green while most resources/views are missing or broken.
- **Confidence:** High.
- **Corroboration:** Reliability scout; independently verified.
- **Evidence:** `dail_tracker_core/connections.py:400-403` and `dail_tracker_core/connections.py:409-420` register broad API view sets with `swallow_errors=True`. Health requires only `v_payments_base` at `api/routers/health.py:15` and returns OK after that single required-view check at `api/routers/health.py:28-37`. Docker healthcheck calls only `/v1/health` at `Dockerfile:57-58`. CI delivery smokes wait only for `/v1/health` at `.github/workflows/ci.yml:155-163` and `.github/workflows/ci.yml:177-181`. The delivery fixture intentionally creates only schema-empty payments parquets at `tools/build_delivery_smoke_fixture.py:1-6` and writes them at `tools/build_delivery_smoke_fixture.py:36-50`.

### F10 — Curl fallback disables TLS verification for scraper downloads
- **Severity:** Medium.
- **Domain:** Security, extraction reliability.
- **Impact:** Any fallback download can accept a wrong-host or man-in-the-middle response; callers without validators then trust unverified bytes.
- **Confidence:** High for code behavior; exploitability depends on network/source context.
- **Corroboration:** Reliability scout; independently verified.
- **Evidence:** `_curl_bytes()` documents and uses `-k` at `services/http_engine.py:454-472`. `fetch_bytes()` enables `curl_fallback=True` by default at `services/http_engine.py:485-492` and falls through to `_curl_bytes()` at `services/http_engine.py:541-548`. Representative caller `extractors/census_saps_2022_fetch.py:58` passes no validator; `extractors/procurement_nta_parser.py:220-222` checks `%PDF` only after fallback bytes return.

### F11 — Public Payments page omits the canonical VAT-basis caveat
- **Severity:** Medium.
- **Domain:** Product UX, public payments, provenance/money grains.
- **Impact:** Users can compare cross-publisher public-payment totals as if VAT bases were uniform, despite canonical caveats saying they are mixed/mostly unknown.
- **Confidence:** High.
- **Corroboration:** Product scout; independently verified.
- **Evidence:** `dail_tracker_core/caveats.py:67-71` says public-body payments have varied/unconfirmed VAT basis and points to `procurement_payments_vat_matrix.json`; exports repeat this at `api/routers/exports.py:121-122` and expose the reference at `api/routers/exports.py:352`. The Streamlit provenance footer at `utility/pages_code/public_payments.py:607-615` and headline caveat at `utility/pages_code/public_payments.py:870-875` mention lifecycle/grain/no-wrongdoing caveats but not VAT. `rg -n 'VAT|vat' utility/pages_code/public_payments.py` returned no hits.

### F12 — Legacy source-link renderer interpolates unescaped data into unsafe HTML
- **Severity:** Medium.
- **Domain:** Streamlit UX, security, provenance.
- **Impact:** A source label or URL containing quotes/markup can corrupt rendered HTML; in the worst case it is an injection surface in source/provenance rows.
- **Confidence:** High for code defect; medium for exploitability because source rows are mostly pipeline-controlled.
- **Corroboration:** Product scout; independently verified.
- **Evidence:** `utility/ui/source_links.py:43-50` inserts `url` and `label_text` directly into an HTML string; `utility/ui/source_links.py:54-56` renders it with `unsafe_allow_html=True`. The safer canonical helper escapes href/label/ARIA at `utility/ui/entity_links.py:508-536`. Current callers include `utility/ui/vote_explorer.py:17` / `utility/ui/vote_explorer.py:321` and `utility/pages_code/lobbying_3.py:170`, `utility/pages_code/lobbying_3.py:1247`, `utility/pages_code/lobbying_3.py:2212`.

### F13 — “What They Own” mislabels Dáil-only results as “TDs and senators”
- **Severity:** Low-Medium.
- **Domain:** Streamlit UX/product copy.
- **Impact:** A Dáil-only count can render as if it included both TDs and senators.
- **Confidence:** High.
- **Corroboration:** Product scout; independently verified.
- **Evidence:** The chamber selector offers `Dáil`/`Seanad` at `utility/pages_code/what_they_own.py:192`; the selected chamber scopes `fetch_member_index_alltime()` / `fetch_member_index()` at `utility/pages_code/what_they_own.py:292`; the label is set to `"TDs and senators" if house == "Dáil" else "senators"` at `utility/pages_code/what_they_own.py:317` and rendered in the heading at `utility/pages_code/what_they_own.py:332`.

### F14 — Core query layer no longer matches its documented retrieval-only contract
- **Severity:** Medium.
- **Domain:** Core queries, SQL view contracts, API/MCP reuse.
- **Impact:** Aggregations/joins can bypass SQL-view review and contract tests, making semantics harder to keep stable across UI/API/MCP.
- **Confidence:** High.
- **Corroboration:** Product scout; independently verified.
- **Evidence:** `dail_tracker_core/queries/__init__.py:1-7` says modules contain only retrieval SQL and that joins/aggregation/value-gating live in registered `sql_views/*.sql`. Current query code does a per-quarter `GROUP BY` in `dail_tracker_core/queries/public_payments.py:221-231`, a pandas merge/join in `dail_tracker_core/queries/constituency.py:140-149`, and multiple procurement payment `GROUP BY`s including `dail_tracker_core/queries/procurement/payments.py:81`, `dail_tracker_core/queries/procurement/payments.py:149`, `dail_tracker_core/queries/procurement/payments.py:166`, `dail_tracker_core/queries/procurement/payments.py:218`, `dail_tracker_core/queries/procurement/payments.py:243`, `dail_tracker_core/queries/procurement/payments.py:261`, and `dail_tracker_core/queries/procurement/payments.py:377`.

### F15 — CSV/export affordances do not meet the stated product contract on high-value pages
- **Severity:** Medium.
- **Domain:** Product, Streamlit UX.
- **Impact:** Journalists/researchers cannot reliably take “every view” away from the UI, despite that being an explicit user need.
- **Confidence:** High for static coverage gap; medium for live user impact until browser review.
- **Corroboration:** Product scout; independently verified.
- **Evidence:** `PRODUCT.md:11` says journalists/researchers need “filterable tables and CSV export for every view”; `PRODUCT.md:41` says CSV export should be available but not in the way. The shared helper exists at `utility/ui/export_controls.py:9-24`. Static searches found no `export_button` or `download_button` in `utility/pages_code/what_they_own.py` or `utility/pages_code/public_payments.py`; `what_they_own.py:105-152` renders the leaderboard without an export affordance.

### F16 — Runtime/deployment/security hardening gaps remain in direct entry points and workflows
- **Severity:** Medium / Low-Medium.
- **Domain:** Reliability, security, deployment.
- **Impact:** Directly scheduled extractors can bypass thread caps; privileged workflow/supply-chain surfaces are broader than necessary; public API errors can disclose implementation details.
- **Confidence:** Medium-High.
- **Corroboration:** Reliability scout; independently verified representative spans.
- **Evidence:**
  - `services/runtime_env.py:17-20` says the cap must be imported before numpy/pandas/streamlit; `test/test_runtime_env.py:28-39` ratchets only selected entry points. Direct workflows run `extractors/etenders_live_tenders_extract.py` at `.github/workflows/live_tenders_refresh.yml:53-54` and `extractors/legal_diary_openview_extract.py` at `.github/workflows/legal_diary_openview_refresh.yml:86-90`; those files import Polars at `extractors/etenders_live_tenders_extract.py:40` and `extractors/legal_diary_openview_extract.py:43` without a preceding runtime-env fence.
  - The CLA workflow uses `pull_request_target` at `.github/workflows/cla.yml:22`, broad write permissions at `.github/workflows/cla.yml:25-29`, and an unpinned third-party action tag at `.github/workflows/cla.yml:43-44`.
  - `Dockerfile:18` uses a mutable base tag (`ghcr.io/astral-sh/uv:python3.12-bookworm-slim`).
  - Query wrappers include raw exception text in `unavailable_reason` at `dail_tracker_core/queries/__init__.py:56-61`; the global API handler returns `str(exc)` at `api/main.py:164-170`, and routers also propagate `data["error"]`, e.g. `api/routers/judiciary.py:30-32`.
  - No tracked `SECURITY.md`, `.github/SECURITY.md`, or CodeQL workflow was found with `git ls-files 'SECURITY.md' '.github/SECURITY.md' '.github/workflows/*codeql*' '.github/codeql*'`.

### F17 — Generated architecture/source-cadence docs and planning boundaries are public-checkout inconsistent
- **Severity:** Medium.
- **Domain:** Documentation, architecture consistency, public/private boundary.
- **Impact:** Migration/readiness/source-health work can chase obsolete blockers, fail public-only checks, or imply private Siting coverage that the public tree does not contain.
- **Confidence:** High.
- **Corroboration:** Architecture scout; independently verified.
- **Evidence:**
  - `doc/CLASS_CONTRACT.md:3-5` says it is generated/checked, but the committed doc still lists deleted/missing modules at `doc/CLASS_CONTRACT.md:76` (`utility/pages_code/procurement.py`) and `doc/CLASS_CONTRACT.md:90` (`utility/pages_code/siting_check.py`). Running `PYTHONDONTWRITEBYTECODE=1 python3 tools/migration/extract_class_contract.py` to stdout showed current counts of 1,009 emitted / 1,324 selectors / 993 contract / 304 dead CSS, while the committed summary differs at `doc/CLASS_CONTRACT.md:13-18`. The `--check` option is described as “fail on new unstyled classes” at `tools/migration/extract_class_contract.py:295`, not a committed-doc freshness check.
  - `doc/CLOUD_READINESS.md:3` says it is generated; committed headlines say 14 hardcoded local-path blockers, 21 bare-requests modules, and 123/129 pollable sources at `doc/CLOUD_READINESS.md:9-11`. Running `PYTHONDONTWRITEBYTECODE=1 python3 tools/migration/scan_cloud_readiness.py` showed current headlines of 0 hardcoded local-path blockers, 0 unreviewed HTTP-engine bypasses, and 128/134 pollable sources.
  - `doc/SOURCE_CADENCE_PROCEDURE.md:20-21` treats `planning_rules/_corpus_registry/planning_corpus_seed.csv` as a live registry, but `git ls-files` shows only `data/_meta/source_registry.generated.json` and no tracked planning-corpus CSV. `PYTHONDONTWRITEBYTECODE=1 python3 tools/migration/build_source_cadence.py --check` reported 36 `planning_corpus:PC*` ledger rows whose source left every registry; the failure message points to outdated `python tools/build_source_cadence.py` at `tools/migration/build_source_cadence.py:254`.
  - `planning/civic/extractors/AGENTS.md:3` says civic planning extractors are live, while `planning/civic/extractors/planning_applications_ingest.py:1` says “Phase 0 (sandbox)” and writes silver at `planning/civic/extractors/planning_applications_ingest.py:38`; `planning/civic/extractors/planning_decision_profiles.py:10-12` documents sandbox inputs/outputs, but current constants write `data/silver/parquet/...` at `planning/civic/extractors/planning_decision_profiles.py:42-43`. `doc/SANDBOX_MAP.md:47` references `_planning_output/` and `test/siting/`, and `doc/SANDBOX_MAP.md:95-96` lists siting probes absent from this public tree. MCP exposes optional `siting_check` and returns unavailable when `planning.product` imports fail at `mcp_server/server.py:2329-2410`.

### F18 — AFS amalgamated extractor is live but still self-documents as sandbox/not wired and lacks its own coverage sidecar
- **Severity:** Medium.
- **Domain:** Extraction, provenance, documentation/architecture.
- **Impact:** Maintainers can misclassify a live audited-accounts source as disposable sandbox code; monitoring cannot track a dedicated coverage sidecar; row provenance is less specific than available per-year URLs.
- **Confidence:** High.
- **Corroboration:** Data scout; independently verified.
- **Evidence:** The docstring says “sandbox” and “NOT gold, NOT wired to pipeline.py” at `extractors/afs_amalgamated_extract.py:1-7`. Pipeline wires it as `afs` at `pipeline.py:146-149`, and SQL reads `data/silver/parquet/afs_amalgamated_divisions.parquet` at `sql_views/procurement/procurement_afs_national.sql:23` and `sql_views/procurement/procurement_afs_national.sql:36`. The extractor writes that silver parquet at `extractors/afs_amalgamated_extract.py:40-42` and `extractors/afs_amalgamated_extract.py:208-209`; rows carry a generic source string at `extractors/afs_amalgamated_extract.py:198-205`, while per-year `URLS` exist at `extractors/afs_amalgamated_extract.py:45-56`. `git ls-files '*afs*coverage*'` shows LA AFS sidecars but no `afs_amalgamated` coverage sidecar.

### F19 — Live-tender planned estimates are summed in a registered SQL summary despite “NEVER summed” comments
- **Severity:** Medium.
- **Domain:** SQL views, money-grain safety, data contracts.
- **Impact:** A loaded registered view can expose a planned-estimate total/floor; current core stats avoid it, but future consumers can use the summary without the intended caveat.
- **Confidence:** Medium-High.
- **Corroboration:** Data scout; independently verified.
- **Evidence:** `sql_views/procurement/procurement_live_tenders.sql:11-13` says `estimated_value_eur` is a planned buyer estimate and “NEVER summed”; the selected column repeats this at `sql_views/procurement/procurement_live_tenders.sql:28`. The summary sums it at `sql_views/procurement/procurement_live_tenders.sql:40-49`. Current core stats avoid a euro total and describe no total beyond an indicative planned-tier floor at `dail_tracker_core/queries/procurement/tenders_live.py:84-99`. `data/_meta/fact_contracts.yaml:21-23` documents money-grain fields, but no `etenders_live_tenders` entry appeared in the scoped grep.

## Hypotheses / high-leverage opportunities (not confirmed defects)

### H1 — TED monetary presentation needs a captain-approved target policy
- **Severity:** High opportunity / policy dependency.
- **Impact:** F1/F2 are verified, but the product choice is whether to remove euro totals entirely, replace them with median/distribution/counts, or retain only explicitly labelled diagnostics.
- **Confidence:** High that a policy choice is needed; no row-level magnitude quantified here.
- **Evidence:** Same as F1/F2, especially `AGENTS.md:50`, `doc/DATA_GRAINS.md:73`, `dail_tracker_core/queries/procurement/ted.py:33-40`, and `utility/pages_code/procurement/ted.py:123-124`.

### H2 — Public Planning/Siting boundary needs a captain-approved public-contract decision
- **Severity:** Medium opportunity / policy dependency.
- **Impact:** Docs/MCP mention Siting and private-boundary concepts, while the public app lacks a Planning section and no public `planning/product/**` is tracked. The next step is a boundary/product decision, not an agent inference.
- **Confidence:** High for public-tree inconsistency; no private worktree inspected.
- **Evidence:** `README.MD:238-240`, `doc/SOURCES.md:157-163`, `doc/SOURCES.md:222-232`, `utility/app.py:112-327`, `mcp_server/server.py:2329-2410`, and `git ls-files 'planning/product/**'` returning 0.

### H3 — Paid-vs-ordered public-payment blending may be an accepted product exception, but it is not fully reconciled
- **Severity:** Medium opportunity.
- **Impact:** Public-payment UI copy is careful, but source docs say purchase orders and payments are different lifecycle tiers. A follow-up should determine whether current “sum-safe” totals are the approved exception or need different presentation.
- **Confidence:** Medium; no row-level query performed.
- **Evidence:** `doc/DATA_LIMITATIONS.md:650-651` says paid/ordered tiers and VAT bases differ; public-payments UI states “Ordered or paid — not a single spend figure” at `utility/pages_code/public_payments.py:870-875`; `sql_views/procurement/procurement_public_payments.sql:14-21` was reported as blending paid + ordered in supplier summary, but I did not fully inspect/quantify that view here.

### H4 — Default landing and CSV/API export division of labor should be validated against product intent
- **Severity:** Medium opportunity.
- **Impact:** `What They Own` is product-prioritized, but `/` renders member overview; CSV affordances are absent on some high-value pages while API bulk exports now exist.
- **Confidence:** Medium.
- **Evidence:** Product priority appears at `PRODUCT.md:11-13` and `PRODUCT.md:40-43`; `_home_page()` renders `member_overview_page()` at `utility/app.py:42-64`; `api/routers/exports.py:1-20` makes bulk files a programmatic-consumer product; F15 documents missing page-level CSV controls.

### H5 — Framework-neutral data-access cache shim migration is a likely high-leverage cleanup
- **Severity:** Medium opportunity.
- **Impact:** Reduces Streamlit coupling in data-access modules beyond the already-clean core.
- **Confidence:** Medium; not exhaustively reviewed.
- **Evidence:** `utility/data_access/_cache.py:1-33` implements a shim; architecture scout reported many remaining `utility/data_access/*.py` direct Streamlit imports. I did not rescan every data-access module for this synthesis.

### H6 — Accessibility/freshness evidence is mostly static
- **Severity:** Medium opportunity.
- **Impact:** ARIA/focus/static freshness helpers exist, but no live browser/assistive-tech or universal freshness pass was run here.
- **Confidence:** Medium.
- **Evidence:** Static examples include `utility/ui/entity_links.py:9-12`, `utility/ui/cartogram.py:157-158`, and `utility/data_access/freshness_data.py:43-72`; I did not run Streamlit/browser automation.

## Unknowns and limitations

- I did not load parquet, raw JSON/JSONL, PDFs, raw corpora, generated datasets, or large artifacts into context. I used tracked-file greps, selective reads, AST counts, and read-only/check commands with bounded output.
- I did not run ETL, Streamlit, live browser tests, TestClient, export endpoints, or the full `uv run ... tools/dev.py check`; some normal project paths write caches/logs/generated artifacts.
- I did not inspect any separate private `planning/product` Siting worktree. Public-tree references to Siting/Planning are documentation-boundary evidence only.
- I did not quantify TED row-count/value magnitude with DuckDB/Polars row queries; F1/F2 are code/contract confirmations, not measured user-visible deltas.
- Repository secrets are not visible; money-flow R2/probe readiness findings are based on tracked workflow comments and conditions.
- The scout reports were not all from the same HEAD, so I treated their claims as leads and verified consequential ones against current HEAD `87b766a9`.

## Recommended next checks, in priority order

1. **TED money-grain triage:** run bounded DuckDB/Polars counts for `COUNT(*)`, `COUNT(DISTINCT publication_number)`, multi-winner distributions, and current TED euro displays; prepare product options for captain approval before changing UI/API semantics.
2. **Clean-checkout gate triage:** in a disposable lane with the project uv profile, confirm the `.claude` test failure and red doc-index/MCP/conventions gates, then decide whether each is a missing tracked input, stale generated document, or over-broad ratchet.
3. **Refresh publish safety:** verify money-flow workflow behavior with a failing dummy chain or static workflow test; separately check POSIX process-group timeout handling with a child/grandchild sleeper.
4. **Monitoring/metadata coverage:** audit `source_registry.parser_wired` semantics, silver facts served by SQL views, extraction-quality sidecars, and non-atomic coverage writes; prioritize TED/payments/AFS/planning sidecars.
5. **API readiness/security:** design a readiness-vs-liveness split or endpoint smoke matrix; review curl TLS fallback policy and callers that lack validators.
6. **Public docs/contracts reconciliation:** derive current API resource/page counts from source, decide the public Planning/Siting boundary, and reconcile `doc/URL_CONTRACT.md` route shape with live path-slug helpers/tests.
7. **Streamlit trust/UX review:** check public-payments VAT caveat, source-link escaping, `What They Own` copy, and CSV/export affordance coverage in a live browser pass.
8. **Generated architecture docs/source-cadence:** decide which generated docs are snapshot-only versus freshness-checked; make source-cadence public-overlay behavior explicit before wiring it into fast/CI.
9. **Deployment/security posture:** review direct extractor runtime-env fences, CLA workflow permissions/pinning, Docker base digest pinning, generic public API error bodies, and public `SECURITY.md`/SAST expectations.

## Bounded commands consulted

Representative commands run with bounded output; all project files remained unmodified (`git status --short` was clean after checks):

- `git status --short`; `git rev-parse --short HEAD`; `env | grep '^PI_' | sort`.
- Read all four supplied scout reports from `/root/firstmate/data/.../report.md`.
- Read root `AGENTS.md` plus nested `dail_tracker_core/AGENTS.md`, `extractors/AGENTS.md`, `planning/civic/extractors/AGENTS.md`, `sql_views/AGENTS.md`, `mcp_server/AGENTS.md`, and `utility/pages_code/AGENTS.md`.
- `git ls-files '*AGENTS.md'`, `git ls-files 'planning/product/**' 'utility/pages_code/siting_check.py'`, and targeted `git grep -n`/`rg -n` over the paths cited above.
- Read-only/check commands with `PYTHONDONTWRITEBYTECODE=1`: `python3 tools/build_doc_index.py --check`; `python3 tools/check_mcp_catalog.py`; `python3 tools/check_conventions.py`; `python3 tools/check_streamlit_logic_firewall.py`; `python3 tools/check_no_private_ip.py`; `python3 tools/migration/extract_url_contract.py --check`; `python3 tools/migration/extract_class_contract.py` (stdout only); `python3 tools/migration/scan_cloud_readiness.py` (stdout only); `python3 tools/migration/build_source_cadence.py --check`.
- Small stdlib AST/count scripts over `api/routers/catalog.py`, `utility/app.py`, and tracked-file lists; no parquet/raw corpus reads.

No repository files were edited, generated, committed, pushed, or posted externally during this synthesis. The only write performed was this report under `/root/firstmate/data/dail-recon-synthesis/report.md`.
