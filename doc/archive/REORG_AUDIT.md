# Repo reorganisation audit

**Date:** 2026-05-27 (refreshed 2026-05-27 PM after Iris incremental shard cache wire-up)
**Re-scouted: 2026-06-02** — see the "ARCHITECTURE CHANGE" section immediately below. The pipeline moved from a flat `STEPS` list to per-domain refresh *chains*, ~14 new production files landed, and two sandbox files became load-bearing. The original audit body below is preserved for rationale; corrections are flagged inline with **[2026-06-02]**.
**Status:** Stage 0 — read-only scout. No files moved or deleted yet.
**Decision needed before Stage 1:** review the `unsure` and `decide` sections and confirm/override the tags — plus the new DECIDE items at the end (chain destination, sandbox promotion, root `__init__.py`).

**Change since morning baseline:** `iris_oifigiuil_etl_polars.py` is now incremental by default; a new `iris_incremental_shards.py` lives at project root and is imported by the ETL. The sandbox version (`pipeline_sandbox/iris_incremental_shards.py`) was deleted in the same session. See the IRIS section under KEEP for the destination, and the "Path-resolution caveat" note further down.

---

## ⚠️ ARCHITECTURE CHANGE since the original scout (added 2026-06-02)

The original audit modelled the pipeline as a flat `pipeline.STEPS` list, with **one** subprocess site at `pipeline.py:121`. **That model no longer holds.** `pipeline.py` is now a thin dispatcher over **9 per-domain refresh chains** (`CHAINS` in [pipeline.py:51-61](pipeline.py#L51-L61)):

```
bootstrap → members → payments → attendance → seanad → interests → lobbying → iris → legislation
```

Each chain is a self-contained `<domain>_refresh.py` orchestrator at repo root that:
- computes `_ROOT = Path(__file__).resolve().parent`, and
- runs its step scripts via `subprocess.run([sys.executable, "<script>.py"], cwd=_ROOT)` (or imports + calls `.run()` directly, as `iris_refresh.py` does for `si_entity_enrichment` / `iris_si_bill_enrichment`).

**Consequences for the reorg (these supersede the matching sections below):**

1. **Subprocess impact is 10× larger.** It's not one site at `pipeline.py:121` — it's `pipeline.py` plus all 9 refresh scripts. Every relative `"<script>.py"` name + every `cwd=_ROOT` breaks once files move under `src/dail_tracker/`.
2. **The Iris path-resolution caveat generalises.** Every refresh script (not just the 2 Iris ETL files) derives paths from `Path(__file__).parent`. After the move, `_ROOT` is the new package dir, not repo root.
3. **Load-bearing sandbox dependency** — ✅ RESOLVED 2026-06-02 (see DECIDE — promote load-bearing sandbox files):
   - ~~`pipeline_sandbox/public_appointments_enrichment.py`~~ **promoted to repo root** as `public_appointments_enrichment.py` (required Iris gold step, [iris_refresh.py:95](iris_refresh.py#L95)). C5 violation cleared.
   - The Seanad chain (still fine, no sandbox): `seanad_refresh.py` imports the Dáil domain modules directly (`attendance`, `enrich`, `payments_full_psa_etl`, `transform_votes`, `oireachtas_pdf_poller`, `services.votes`) — a deliberate cross-domain reuse, see [seanad_refresh.py:28-42](seanad_refresh.py#L28-L42).
4. **~14 new production files** are unclassified by the original KEEP table — listed in the new "KEEP — added 2026-06-02" subsection.

**Frontloading opportunity (the point of this re-scout):** collapse the repeated `_ROOT = Path(__file__).parent` + relative-`cwd` subprocess pattern across all 10 orchestrators into **one shared `PROJECT_ROOT` import** *now, on main*. After that, the structural move is find/replace instead of per-file path surgery.

---

## ⚠️ RE-AUDIT 2026-06-04 — stale claims corrected (READ THIS FIRST)

A four-front evidence sweep on 2026-06-04 found this plan's **executed-work ledger is accurate**, but its **codebase description and status claims have drifted**. The 2026-06-02 body below was written against a 9-chain model that no longer holds. Corrections, with code evidence:

1. **NOT 9 chains — there are 16.** `CHAINS` ([pipeline.py:51-90](pipeline.py#L51-L90)) now runs: bootstrap, members, payments, attendance, seanad, interests, lobbying, iris, legislation, **afs, cbi, cro, procurement, procurement_lobbying, ted, freshness**. Every "9 chains" / "10 orchestrators" statement below is stale; the 7 new chains are unclassified by the KEEP tables.

   **↳ 2026-06-05:** now **18 chains** (added `source_health` + `cso`); all 18 chain script paths exist. The `extractors/` graduation is complete and a new **`corporate/`** package now holds `cro_poller` + `cro_normalise` (with `shared/name_norm.py` extracted) — both post-date this block's tree.

2. **C5 (sandbox→production) — ✅ NOW RESOLVED 2026-06-04 (see CURRENT TARGET STRUCTURE above).** *The text below is the original finding, retained for rationale; it described the problem before the `extractors/` graduation closed it.* It grew ~7× before resolution: The "✅ RESOLVED 2026-06-02" notes (which promoted ONE file) are misleading. **7–8 `pipeline_sandbox/` scripts are now load-bearing committed pipeline steps**: `afs_amalgamated_extract.py` (pipeline.py:64), `cbi_registers_extract.py` (:68), `cro_corporate_xref_enrichment.py` (:72), `procurement_etenders_extract.py` (:76), `procurement_lobbying_xref.py` (:81), `ted_ireland_extract.py` (:85), and `si_legislation_directory_extract.py` (nested in iris_refresh.py:125). Several write committed **gold** consumed by the UI. **This is the worst coupling in the repo and the real decision the reorg must make:** promote these to `domains/` or gate them out of `CHAINS`. It matters more than the `src/` move itself.

3. **`__main__`-guard claim is REFUTED.** "Most top-level scripts execute on import (no guard)" (and the matching `pyproject.toml` TODO) is stale: 13/15 sampled production files HAVE guards. Only `legislation.py` and `questions.py` lack one (both write parquet at module level). The `-m` dispatch precondition for Step 5 is therefore mostly already met.

4. **Subprocess topology mis-stated.** Not "~11 sites": 8 chains use `subprocess.run`, `pipeline.py` uses `Popen` (pipeline.py:145), `seanad_refresh.py` uses direct imports (no subprocess), plus ~30 inner dispatch sites.

5. **Test baseline corrected (supersedes the "358/0/24" edit, which was itself wrong).** Real state on this branch = **506 passed / 3 failed / 78 skipped**, and **CI's `test` job is RED** — 2 genuine, non-data-gated failures in `test/test_la_payments.py` (`strip_id_prefix` two-numeric-run case; broken xlsx `emit_file` path). A 3rd, `test_sipo_expenses.py::test_name_quality`, is data-gated (SIPO parquet untracked) and flaky on collection order. **Fix CI red before any reorg work — it is the gate the whole plan depends on.**

   **↳ 2026-06-05 update — the CI-red gate is now MET.** The `test_la_payments` failures are fixed; the full `test` lane is **615 passed / 0 failed / 9 skipped** (green). The `sql-contracts` job was *separately* red — `member_overview_data._DOMAIN_FILES` registered `legislation_si_index.sql` before its `v_si_current_state` dependency (silently swallowed in prod, caught by `test_member_overview_connection_builds`); **fixed 2026-06-05** (added `legislation_si_current_state.sql` ahead of the index view), now 75 passed / 0 failed. **`test` + `sql-contracts` + `firewall` + `typecheck` are all green.** The only red left is the `lint` job (≈45 `ruff check` + 50 `format` files) — but that is in-flight reorg churn (judiciary / legislation / payments / sipo / dead `split_attendance_hall`), not a logic break; it clears with a `ruff format .` + `--fix` once the slices settle. So the "fix CI red before reorg work" precondition no longer blocks.

**Net:** the executed frontload (paths.py, public_appointments promotion, `__init__.py` deletion, lobbying_fetch move, config-constant migration) is all verified real, and the strategy (frontload → mechanical worktree move) still holds. But every inventory/status section below is **circa-2026-06-02 and partially stale** until reconciled against 16 chains + the reopened C5.

---

## ✅ CURRENT TARGET STRUCTURE (refreshed 2026-06-04) — authoritative

**This section is the live folder plan. It supersedes the stale per-file KEEP tables (below) and the §2 tree** — both predate the `extractors/` layer and the 7 newer chains. Two things changed the picture since 2026-06-02:

1. **`extractors/` now exists** as the "graduated from sandbox" production layer. 17 load-bearing scripts were `git mv`'d out of `pipeline_sandbox/` into a flat top-level `extractors/` dir. In the package layout, `extractors/` **dissolves into `domains/`** — each script belongs to a data subject; the flat holding-pen has no reason to persist one level down.
2. **C5 (sandbox→production coupling) is FULLY RESOLVED (2026-06-04).** Zero production code imports from `pipeline_sandbox/` — verified by grep (the only `sys.path.insert(pipeline_sandbox)` calls left are sandbox-internal SIPO siblings). The last straggler, `cro_financial_statements_extract.py` (a stale untracked duplicate that lingered in `pipeline_sandbox/` after the graduation), was promoted to `extractors/` and its registry/test/doc callers repointed. `pipeline_sandbox/` is now prunable without breaking the pipeline. **The earlier "C5 NOT resolved — grew 7×" bullet and the REOPENED DECIDE item are obsolete.**

**Organizing principle (unchanged):** infra → shared → domains (by data subject) → orchestration → tools → ui. Flat domains (~18 sibling folders), each = one source/subject.

```
src/dail_tracker/
├── infra/              # bedrock: no domain logic, imported everywhere
│   config.py · paths.py · manifest.py · run_paths.py · logging_setup.py
│   http_engine.py · storage.py · urls.py · schema_validation.py · member_paginated.py
│   (DELETE services/dail_config.py — duplicate of config.py)
├── shared/             # pure cross-domain helpers
│   normalise_join_key.py · quarantine.py · analytics_loading.py
│   select_drop_rename_cols_mappings.py
├── orchestration/
│   pipeline.py         # the 16-chain dispatcher
│   chains/             # the *_refresh.py orchestrators:
│                       #   bootstrap members payments attendance seanad
│                       #   interests lobbying iris legislation
│                       # (afs/cbi/cro/procurement/ted/freshness/source_health are
│                       #  single-script chains → CHAINS points straight at the
│                       #  domain/tool module; no chains/ wrapper needed)
├── domains/            # one folder per data subject
│   members/        ← members_api_service, flatten_members_json_to_csv, member_interests,
│                     services/members, wikidata_socials_etl, ministerial_tenure_build, wiki_data
│   attendance/     ← attendance
│   votes/          ← transform_votes, enrich, services/votes
│   debates/        ← dbsect_listings_flatten, services/dbsect_harvest
│   questions/      ← questions
│   interests/      ← (interests ETL driven by interests_refresh)
│   committees/     ← committees_long_format_etl
│   payments/       ← payments_full_psa_etl, payments_member_enrichment
│   legislation/    ← legislation, bill_amendments_flatten, si_entity_enrichment,
│                     services/legislation_unscoped, extractors/si_legislation_directory_extract,
│                     extractors/si_lrc_classlist_extract, extractors/si_lrc_enrichment_build
│   iris/           ← iris_oifigiuil_poller, iris_oifigiuil_etl_polars, iris_incremental_shards,
│                     iris_silver_rebuild, iris_si_bill_enrichment, repair_future_iris_placeholders,
│                     public_appointments_enrichment, corporate_notices_enrichment
│   corporate/      ← cro_poller, cro_normalise, extractors/cro_corporate_xref_enrichment,
│                     extractors/cro_financial_statements_extract, extractors/cbi_registers_extract
│   charity/        ← charity_normalise, charity_resolved, charity_enriched
│   lobbying/       ← lobbying_poller, lobby_processing, lobbying_pdf_extract
│   procurement/    ← extractors/procurement_etenders_extract, procurement_public_body_extract,
│                     procurement_la_payments_extract, procurement_hse_tusla_parser,
│                     procurement_lobbying_xref, procurement_la_seed, procurement_publishers_seed,
│                     sample_extract_procurement_pdf, extractors/ted_ireland_extract
│   local_authority/← extractors/afs_amalgamated_extract, la_afs_extract, la_afs_camelot_ie
│   reference/      ← ec_constituency_pop_extract
│   oireachtas_api/ ← services/oireachtas_api_main   (cross-domain fetch layer)
│   pdf_infra/      ← oireachtas_pdf_poller, pdf_downloader, pdf_endpoint_check
├── tools/              # operational / CI / monitoring (stay grouped, not scattered)
│   build_source_registry.py · build_source_health.py · check_freshness.py
│   freshness_report.py · gold_rowcounts.py · check_streamlit_logic_firewall.py · publish_data.py
└── ui/                 ← all of utility/ (app.py · shared_css · config · constants
                          · pages_code→pages/ · ui→components/ · data_access/)

pipeline_sandbox/       # STAYS at root — research/probes, NOT packaged, now prunable
archive/                # dead/superseded (si_baseline.json, old audit .py, _old docs)
test/  →  unit/{infra,shared,domains/<d>} · integration/ · golden/ · ui/
```

**Judgment calls (signed off 2026-06-04):**
- **`extractors/` dissolves into `domains/`** (vs keeping it as a flat "pipeline-step scripts" layer — rejected: re-creates the flat problem).
- **`cro_*` → `corporate/` not `charity/`** — CRO is the company register; its real consumers are corporate-notices, procurement supplier-matching, and lobbying xref.
- **Single-script chains** (afs, cbi, cro, procurement, procurement_lobbying, ted, freshness, source_health) need no `chains/` wrapper — `CHAINS` references the domain/tool module directly.
- **`tools/` stays cohesive** rather than scattering monitoring scripts into domains.
- **3 deletions on the way in:** `future_enrichment_ideas.py`, `iris_memeber_interests.py` (typo, dead), `pdf_backfill_scraper.py` (broken import) — all already tagged `dead`.
- **Flat domains over thematic super-groups** — ~18 folders, each unambiguously one subject; thematic grouping adds debatable boundary calls (is PSA pay "parliament" or "money"?).

The B1–B6 boundary rationale in §2 below (paths.py single root, pure `transform.py` vs thin `etl.py`, hoisted pandera contracts, chains-as-data) still holds — this section only refreshes *where the files land*, not *why the boundaries exist*.

---

## Tag legend

| Tag | Meaning | Action in Stage 1 |
|---|---|---|
| `keep` | Production code, has imports / pipeline / test refs | Move to its `src/dail_tracker/...` destination |
| `dead` | No imports, no pipeline refs, no test refs, no recent edits, or known-broken | Delete |
| `archive` | Useful as history but out of the way | Move to `archive/` |
| `sandbox` | Belongs under `pipeline_sandbox/` per project rule | Stay in (or move into) sandbox |
| `unsure` | I flag, you decide | Confirm / override before move |
| `decide` | A higher-level question | Answer in this doc |

## Headline numbers

| | Count |
|---|---|
| Python files in scope | 155 (133 after excluding `audit_screenshots/`, `experimental/`, `dail_tracker_bold_ui_contract_pack_v5/`) |
| Markdown files in scope | 90 |
| Top-level `.py` to classify | 45 → **~59** **[2026-06-02]** (+9 `*_refresh.py` chains, +`iris_silver_rebuild.py`, +`committees_long_format_etl.py`, +`corporate_notices_enrichment.py`, +`payments_member_enrichment.py`, +root `__init__.py`) |
| `keep` (will be moved) | **~48** **[2026-06-02]** (was 34; +14 — see "KEEP — added 2026-06-02") |
| `dead` (will be deleted in Stage 1) | 3 |
| `unsure` (resolved during session) | 0 original — but 3 NEW `decide`/`unsure` items added 2026-06-02 (chain destination, sandbox promotion, root `__init__.py`) |
| `sandbox` (move into sandbox) | 1 — but 2 sandbox files are now load-bearing pipeline deps (see ARCHITECTURE CHANGE) |
| Streamlit UI files (utility/) | 36 — all `keep`, moving to `src/dail_tracker/ui/` |
| Pre-flight test baseline | **CURRENT = 615 passed · 0 failed · 9 skipped (CI `test` job GREEN)** — verified 2026-06-05. The 2026-06-04 "506·3·78 / CI RED" figure is retired: the `test_la_payments` failures are fixed and `sql-contracts` was repaired (member_overview SI-view ordering). `test`/`sql-contracts`/`firewall`/`typecheck` all green; only `lint` is red (in-flight reorg formatting churn). Earlier "358/0/24" and "294/11" also retired. **The CI-red precondition for Step 5 is met.** |

---

## KEEP — moves into the new structure

### infra / orchestration / shared

| Current path | Destination | Reason |
|---|---|---|
| `config.py` | `src/dail_tracker/infra/config.py` | Imported by 32 files. Bedrock. |
| `manifest.py` | `src/dail_tracker/infra/manifest.py` | Imported by pipeline.py |
| `services/run_paths.py` | `src/dail_tracker/infra/run_paths.py` | Just added; imported by manifest + pipeline + logging_setup |
| `services/logging_setup.py` | `src/dail_tracker/infra/logging_setup.py` | Imported by pipeline + 3 standalone scripts |
| `services/http_engine.py` | `src/dail_tracker/infra/http_engine.py` | Imported by services/members, services/votes, services/legislation_unscoped, services/oireachtas_api_main |
| `services/storage.py` | `src/dail_tracker/infra/storage.py` | Imported by services/oireachtas_api_main, services/members; tested by test/test_services_members.py |
| `services/urls.py` | `src/dail_tracker/infra/urls.py` | Imported by services/oireachtas_api_main, services/legislation_unscoped; tested by test/test_url_builders.py |
| `services/dail_config.py` | **DELETE — merge into config.py** | Duplicates config.py's `LOG_DIR` / `API_BASE` / etc. Was a stale parallel constant module. Already noted as duplicate during earlier work. |
| `pipeline.py` | `src/dail_tracker/orchestration/pipeline.py` | The orchestrator |
| ~~`tear_down.py`~~ | **DELETED 2026-05-27** | 60% of functions were no-ops on wrong paths. Useful cleanups migrated into producers (`member_interests.py`, `lobby_processing.py`). |
| `normalise_join_key.py` | `src/dail_tracker/shared/normalise_join_key.py` | Imported by enrich.py, member_interests.py + 2 tests |
| `quarantine.py` | `src/dail_tracker/shared/quarantine.py` | Imported by lobby_processing.py |
| `analytics_loading.py` | `src/dail_tracker/shared/analytics_loading.py` | DuckDB helper — registers parquets as views (Jupyter / interactive use) |

### domains/

| Current path | Destination | Reason |
|---|---|---|
| **Iris** | | |
| `iris_oifigiuil_poller.py` | `domains/iris/poller.py` | pipeline.STEPS + imported by run_iris_poll.py |
| `iris_oifigiuil_etl_polars.py` | `domains/iris/etl.py` | pipeline.STEPS; **imports `iris_incremental_shards` for the per-PDF shard cache** |
| `iris_incremental_shards.py` | `domains/iris/incremental_shards.py` | **Added 2026-05-27 PM.** Per-PDF parquet shard cache for the Iris ETL — fingerprints on (mtime_ns, size, EXTRACTOR_VERSION), atomic `.part`→`replace` writes, manifest-based skip. Imported by `iris_oifigiuil_etl_polars.py`; standalone CLI for cache inspection/`--rebuild`. |
| `iris_si_bill_enrichment.py` | `domains/iris/si_bill_enrichment.py` | pipeline.STEPS |
| `repair_future_iris_placeholders.py` | `domains/iris/repair_placeholders.py` | Per-memory ad-hoc repair utility for Iris parser gaps |
| **Lobbying** | | |
| `lobbying_poller.py` | `domains/lobbying/poller.py` | pipeline.STEPS |
| `lobby_processing.py` | `domains/lobbying/processing.py` | pipeline.STEPS; tested by test/test_lobby_processing.py |
| `lobbying_pdf_extract.py` | `domains/lobbying/pdf_extract.py` | pipeline.STEPS |
| **Members** | | |
| `members_api_service.py` | `domains/members/api_service.py` | Imported by flatten_members_json_to_csv.py |
| `flatten_members_json_to_csv.py` | `domains/members/flatten.py` | pipeline.STEPS |
| `member_interests.py` | `domains/members/interests.py` | pipeline.STEPS; tested by test/test_member_interests.py |
| `services/members.py` | `domains/members/api.py` (rename to avoid collision with `members/` dir name) | Imported by services/oireachtas_api_main; tested by test/test_services_members.py |
| **Attendance** | | |
| `attendance.py` | `domains/attendance/etl.py` | pipeline.STEPS |
| **Payments** | | |
| `payments_full_psa_etl.py` | `domains/payments/full_psa_etl.py` | pipeline.STEPS; tested by test/test_payments_golden.py + test/fixtures/payments/_generate_expected.py |
| **Charity** | | |
| `cro_normalise.py` | `domains/charity/cro_normalise.py` | pipeline.STEPS |
| `charity_normalise.py` | `domains/charity/normalise.py` | pipeline.STEPS |
| `charity_resolved.py` | `domains/charity/resolved.py` | pipeline.STEPS |
| `charity_enriched.py` | `domains/charity/enriched.py` | pipeline.STEPS |
| **Wikidata** | | |
| `wikidata_socials_etl.py` | `domains/wikidata/socials_etl.py` | pipeline.STEPS; tested by test/test_wikidata_socials_etl.py |
| `ministerial_tenure_build.py` | `domains/wikidata/ministerial_tenure.py` | pipeline.STEPS |
| `wiki_data.py` | `domains/wikidata/avatars_downloader.py` | Manual script that builds `avatar/wikidata/manifest.json` (consumed by ui/avatars.py). Rename to clarify intent (avoids confusion with wikidata_socials_etl). |
| **Legislation / SI** | | |
| `legislation.py` | `domains/legislation/etl.py` | pipeline.STEPS |
| `bill_amendments_flatten.py` | `domains/legislation/bill_amendments.py` | pipeline.STEPS |
| `si_entity_enrichment.py` | `domains/legislation/si_entity_enrichment.py` | pipeline.STEPS; imported by ministerial_tenure_build |
| **Votes** | | |
| `transform_votes.py` | `domains/votes/transform.py` | pipeline.STEPS |
| `enrich.py` | `domains/votes/enrich.py` | pipeline.STEPS; cross-domain join, but votes is biggest consumer; tested by test/test_enrich_join.py |
| `services/votes.py` | `domains/votes/api.py` | Imported by services/oireachtas_api_main; tested by test/test_services_votes.py |
| **Questions** (added 2026-05-27) | | |
| `questions.py` | `domains/questions/etl.py` | Wired into pipeline.STEPS this session ("Flatten parliamentary questions", after legislation step). Produces `silver/parquet/questions.parquet` consumed by `sql_views/member_debate_sections.sql`. |
| **Debates** | | |
| `dbsect_listings_flatten.py` | `domains/debates/listings_flatten.py` | pipeline.STEPS |
| `services/dbsect_harvest.py` | `domains/debates/harvest.py` | Imported by services/oireachtas_api_main |
| **PDF infra** | | |
| `pdf_downloader.py` | `domains/pdf_infra/downloader.py` | pipeline.STEPS |
| `pdf_endpoint_check.py` | `domains/pdf_infra/endpoint_check.py` | Imported by pdf_downloader; manifest.py imports lazily via `_check_endpoints()` |
| `oireachtas_pdf_poller.py` | `domains/pdf_infra/poller.py` | pipeline.STEPS; imported by all 4 `run_*_poll.py` runners |
| **Oireachtas API (cross-domain)** | | |
| `services/oireachtas_api_main.py` | `domains/oireachtas_api/main.py` | pipeline.STEPS (in-process step) |
| `services/legislation_unscoped.py` | `domains/oireachtas_api/legislation_unscoped.py` | Imported by services/oireachtas_api_main |

### KEEP — added 2026-06-02 (not in original scout)

**Refresh chain orchestrators** (the new pipeline layer). Default destination proposed: `src/dail_tracker/orchestration/chains/<name>.py` (keeps them beside `pipeline.py`). **See DECIDE — chain destination** for the alternative (domain-local `domains/<d>/refresh.py`).

| Current path | Proposed destination | Reason |
|---|---|---|
| `bootstrap_refresh.py` | `orchestration/chains/bootstrap.py` | CHAINS[0]; cross-domain (PDFs + Members API + flatten members/debates). Imports `services.oireachtas_api_main`. |
| `members_refresh.py` | `orchestration/chains/members.py` | CHAINS; Wikidata socials + ministerial tenure + committees long-format |
| `payments_refresh.py` | `orchestration/chains/payments.py` | CHAINS; PSA ETL + member enrichment |
| `attendance_refresh.py` | `orchestration/chains/attendance.py` | CHAINS |
| `seanad_refresh.py` | `orchestration/chains/seanad.py` | CHAINS; **imports Dáil domain modules directly** (`attendance`, `enrich`, `payments_full_psa_etl`, `transform_votes`, `oireachtas_pdf_poller`, `services.votes`) — cross-domain reuse, update these import paths carefully |
| `interests_refresh.py` | `orchestration/chains/interests.py` | CHAINS |
| `lobbying_refresh.py` | `orchestration/chains/lobbying.py` | CHAINS |
| `iris_refresh.py` | `orchestration/chains/iris.py` | CHAINS; imports `iris_silver_rebuild`, `si_entity_enrichment`, `iris_si_bill_enrichment`; subprocesses `public_appointments_enrichment.py` (promoted out of sandbox 2026-06-02) + `corporate_notices_enrichment.py` |
| `legislation_refresh.py` | `orchestration/chains/legislation.py` | CHAINS |

**New domain ETL / enrichment files:**

| Current path | Proposed destination | Reason |
|---|---|---|
| `iris_silver_rebuild.py` | `domains/iris/silver_rebuild.py` | Imported by `iris_refresh.py` (`rebuild_silver_from_bronze`) |
| `corporate_notices_enrichment.py` | `domains/iris/corporate_notices_enrichment.py` | Iris-derived gold step (step 6 of iris_refresh); consumed by `sql_views/corporate_corporate_notices.sql` + UI corporate page |
| `payments_member_enrichment.py` | `domains/payments/member_enrichment.py` | Payments chain step; consumed by `sql_views/payments_member_detail.sql` |
| `committees_long_format_etl.py` | `domains/committees/long_format_etl.py` (**NEW domain dir**) | Members chain step; consumed by `utility/data_access/committees_data.py` + committees UI |
| `services/schema_validation.py` | `infra/schema_validation.py` | jsonschema validate-at-fetch of API envelopes (per pyproject comment); API/JSON boundary → infra |
| `services/member_paginated.py` | `infra/member_paginated.py` *(RESOLVED 2026-06-02 — see DECIDE; only dep is `http_engine`, generic across questions+legislation → infra not members)* | API pagination helper (the questions-cap fix per memory); tested by `test/test_member_paginated.py` |

### runners/

| Current path | Destination | Reason |
|---|---|---|
| `run_attendance_poll.py` | `src/dail_tracker/runners/attendance_poll.py` | Thin wrapper around `oireachtas_pdf_poller.run_one("attendance")` |
| `run_interests_poll.py` | `src/dail_tracker/runners/interests_poll.py` | Thin wrapper |
| `run_payments_poll.py` | `src/dail_tracker/runners/payments_poll.py` | Thin wrapper |
| `run_iris_poll.py` | `src/dail_tracker/runners/iris_poll.py` | Thin wrapper around `iris_oifigiuil_poller.main()` |

### ui/

All 35 files under `utility/` move to `src/dail_tracker/ui/` preserving internal structure:
- `utility/app.py` → `ui/app.py` (Streamlit entrypoint)
- `utility/shared_css.py` → `ui/shared_css.py`
- `utility/config.py` → `ui/config.py` (UI-specific constants — distinct from infra/config.py)
- `utility/constants.py` → `ui/constants.py`
- `select_drop_rename_cols_mappings.py` (repo root) → `src/dail_tracker/shared/select_drop_rename_cols_mappings.py` — pure ETL column-mapping data; imported by domains/lobbying/processing + domains/members/flatten. Relocated out of `utility/` (the UI layer) to repo root on 2026-06-01 to fix a domain→UI import; it is **shared/**, not UI.
- `utility/pages_code/` → `ui/pages/`
- `utility/ui/` → `ui/components/` (rename to avoid `ui/ui/` nesting)
- `utility/data_access/` → `ui/data_access/`

**Streamlit gotcha**: every Streamlit page's imports (`from pages_code.X`, `from ui.Y`, `from data_access.Z`, `from shared_css`, `from config`) needs rewriting to the new namespace. ~50 import sites across 11 pages. Plus `utility/app.py` is the entrypoint.

---

## DEAD — delete in Stage 1

| Path | Reason for deletion |
|---|---|
| `future_enrichment_ideas.py` | Contains only commented-out URLs and notes; zero executable code; 0 imports; name signals scratch file. Move the URL list to a doc file (e.g. `doc/FUTURE_DATASETS.md`) if you want to keep the list. |
| `iris_memeber_interests.py` | Typo filename. Contains only imports + commented-out PDF text. No functions. No imports of it anywhere. Looks like a one-off exploration dump. |
| `pdf_backfill_scraper.py` | Imports `from pipeline_sandbox.payment_pdf_url_probe` — but that .py source has been deleted (only orphan .pyc remains). Script is broken on import. Has TODO header `"finish this feature"` — never finished. |
| `experimental/test_read_scan_pdf.py` | Imports `config.SCAN_PDF_DIR` which doesn't exist in current config.py. Pulls in heavy `ocrmypdf` dependency. Single file in an `experimental/` folder; the experiment is over. |
| `pipeline_sandbox/__pycache__/` (entire dir) | 10 orphan .pyc files for sources that no longer exist: `legislation_unscoped_fetch`, `legislation_unscoped_validate`, `legislation_unscoped_silver_views`, `payment_pdf_url_probe`, `si_entity_enrichment`, `iris_oifigiuil_etl_polars`, `cro_normalise`, `charity_normalise`, `quarantine`, `lobbying_fetch`. |
| ~~`pipeline_sandbox/iris_incremental_shards.py`~~ | **DELETED 2026-05-27 PM.** Promoted to project root with the typo-import fixed (now imports `iris_oifigiuil_etl_polars`). Live at root as `iris_incremental_shards.py` — see KEEP / Iris. |
| **Stray root logs** | `pipeline.log`, `pipeline_run.log`, `endpoint_check.log`, `dbsect_after_pipeline.log`, `attendance_run.log`, `logs/pipeline.log`, `logs/runs/`, `services/logs/`. **[2026-06-02] These are now all gitignored** (confirmed via `git status --ignored`) — they won't be committed, so this is no longer a Stage-1 blocker. Optionally delete the on-disk files for tidiness, but not required. |
| `.coverage` | pytest-cov artifact. **[2026-06-02] Confirmed gitignored.** No action needed. |
| `tear_down.py` | **DELETED 2026-05-27** — 60% of functions were no-ops on wrong paths or targeted nonexistent dirs. Useful cleanups migrated into producers: `member_interests.py` now self-cleans per-year CSVs; `lobby_processing.py` now self-cleans cleaned.csv/cleaned_output.csv. |

---

## ARCHIVE — move to `archive/` (keep for history, out of the way)

| Path | Destination | Reason |
|---|---|---|
| `doc/REARCHITECTURE_old.md` | `archive/doc/REARCHITECTURE_old.md` | Filename says "_old"; superseded by `doc/REACHITECTURE_NEW.MD` |
| `doc/dail_tracker_improvements_v4.md` | `archive/doc/dail_tracker_improvements_v4.md` | Version-suffixed draft (`_v4`); no v5 ever, but the audit/improvements story has clearly evolved (see SHORT_TERM_PLAN, the per-page audits) |
| `si.png` (repo root) | `archive/audit_screenshots/si.png` | Loose SI audit screenshot at root; should live with the rest of audit captures |
| `si_baseline.json` | `archive/audit_screenshots/si_baseline.json` | SI title-quality baseline from the audit; per memory the P0 fixes shipped, but baseline still useful for regression detection — archive rather than delete |
| `audit_screenshots/` (whole dir, 22 .py) | `archive/audit_screenshots/` | Per `reference_audit_toolkit.md` — these are per-page Playwright capture/verify scripts paired with the AUDIT.md docs. All audits in your memory have already shipped fixes. Keep as templates but out of the active codebase. |

---

## Markdown doc staleness audit (added 2026-06-02)

Full sweep of all **48 project-authored** `.md` files (the "90 in scope" headline includes vendored `.venv/`, `.claude/`, `.agents/`, and `dail_tracker_bold_ui_contract_pack_v5/` packs, which are excluded here — the contract pack is already tagged delete-at-project-conclusion under DECIDE). Assessed for staleness via per-file content + git dates + cross-check against shipped code. Verdict: **33 KEEP · 11 ARCHIVE · 2 DELETE · 1 dedupe.**

> **Reconciliation with the ARCHIVE section above:** that section already lists `doc/REARCHITECTURE_old.md` (confirmed ARCHIVE — author-marked `_old`, medallion redesign shipped) and `doc/dail_tracker_improvements_v4.md` as archive moves. **Correction on the latter:** the v4 doc is still the live, heavily cross-referenced improvement roadmap (CITATION plan, HANDS_OFF, ENRICHMENTS all point to it) — recommend **KEEP-but-refresh** (its April-2026 status snapshot is stale; line 2's "Supersedes v3" is now a dangling ref since v3 no longer exists), not archive. Override at your discretion.

### DELETE (2) — superseded, no residual value

| Path | Reason |
|---|---|
| `doc/company_register_notices_feature.md` | Self-marked PARKED + explicitly superseded by `corporate_feature.md`; Corporate page shipped instead. |
| `doc/receiver_appointers_feature.md` | Header says "will be absorbed into corporate" — and it was; receiver-appointer ranking is the lead of the shipped `utility/pages_code/corporate.py`. |

### ARCHIVE (11) — work shipped, keep as historical record (→ `archive/doc/`)

| Path | Reason |
|---|---|
| `doc/LOBBYING_POC_AUDIT.md` | SHIPPED banner: P0+all P1+all P2 done, P3-only remains. |
| `doc/PAYMENTS_AUDIT.md` | P0s/P1s fixed per memory; doc's open-items list is stale. |
| `doc/LEGISLATION_AUDIT.md` | All UI work shipped; only pipeline-blocked remnants. |
| `doc/MEMBER_OVERVIEW_AUDIT.md` | 2 P0+6 P1+6 P2+3 P3 all shipped; one pipeline TODO. |
| `doc/COMMITTEES_AUDIT.md` | 5/6 P1 shipped (per memory); doc reads as historical. |
| `doc/STATUTORY_INSTRUMENTS_AUDIT.md` | Majority shipped; remnants are deferred-design/low-yield. |
| `doc/view_bill_pdf_feature.md` | Shipped as `v_legislation_pdfs` / `_section_bill_pdfs`. |
| `doc/public_appointments_feature.md` | Shipped as `public_appointments.py`. |
| `doc/corporate_feature.md` | Shipped as `corporate.py`. |
| `doc/SANDBOX_CLEANUP_PLAN.md` | Subsumed by this REORG_AUDIT (sandbox-promotion + graduation set). |
| `doc/LOGIC_FIREWALL_AUDIT.md` + `doc/LOGIC_FIREWALL_PLAN.md` | Pass-1 violations remediated (V1 checker now passes clean); plan partly premised on the phantom `page_contracts/` YAML layer (see A1). Counts as 2 of the 11. |

(That's 12 rows for 11 docs because the two LOGIC_FIREWALL files share one row.)

### DEDUPE (1)

| Path | Action |
|---|---|
| `.impeccable.md` (repo root) | Stale subset-copy of `PRODUCT.md` (already diverged, missing 2 newest sections). Replace body with a one-line pointer to `PRODUCT.md` — **but first confirm the `impeccable` skill doesn't hard-read the filename** (memory says it reads `PRODUCT.md`, so likely safe). |

### KEEP — but flag for a status refresh (the docs that mislead)

- `doc/SHORT_TERM_PLAN.md` + `doc/TICKETS.md` — **every checkbox unticked** despite much being shipped; completion is tracked nowhere here. Real "what's done" signal lives in `CI_CD.md` + memory. Reframe TICKETS as a *spec library*, not a tracker.
- `doc/dail_tracker_improvements_v4.md` — stale April-2026 snapshot; dangling v3 reference (see reconciliation note above).
- `doc/PERFORMANCE.md` — Phase 1 (zstd-everywhere) now shipped/enforced per `feedback_parquet_write_convention`.
- `doc/oireachtas_explorer_full_comparison.md` — Questions section overtaken by the shipped questions silver layer.
- `doc/ENRICHMENTS.md` — parked URL-audit block (2026-04-30) needs closeout.

### KEEP as-is (active work / living references)

- **Live workstreams:** `REORG_AUDIT.md` (this file), `doc/SEANAD_PARITY_BUILD_PLAN.md`, `doc/CI_CD.md`, `doc/CICD_TODO.md`, `doc/LOGIC_FIREWALL_AUDIT_PASS2.md` (only firewall doc with open items), `test/HANDS_OFF_TEST_PLAN.md`.
- **Audits with genuine open items:** `doc/LOBBYING_AUDIT.md`, `doc/interests_audit.md`, `doc/votes_audit.md`, `doc/SIDEBAR_AUDIT.md`, `doc/LOBBYING_FILEDBY_AUDIT_2026_05_31.md`.
- **Unbuilt feature specs (live ideas):** `highlights_page_idea`, `minister_activity_feature`, `parliamentary_questions_feature` (Member-Overview half built, `/questions` page not), `policy_to_action_trace_scoping`, `lobbying_to_regulation_timeline`, `chart_export_branding`, `legislation_benchmark_oireachtas_explorer`.
- **Living references:** `PRODUCT.md`, `doc/DATA_LIMITATIONS.md`, `doc/COMPETITIVE_LANDSCAPE.md`, `doc/CITATION_AND_DATA_PLAN.md`, `doc/SSHA_social_housing_summary.md`, both `data/_meta/*.md` provenance files, `doc/lobbying_sql_learning.md` (lowest-confidence KEEP — personal study material).

### Incidental fixes worth doing while in here

1. **Case-mismatch:** `doc/interests_audit.md` is lowercase but its own uplift prompt points to `doc/INTERESTS_AUDIT.md`; most siblings are UPPERCASE — naming is inconsistent across the audit set.
2. **Dead path:** `STATUTORY_INSTRUMENTS_AUDIT.md` cites non-existent `tmp/audit_si/`; real captures are under `audit_screenshots/`.
3. **Overlap:** `policy_to_action_trace_scoping.md` and `lobbying_to_regulation_timeline.md` both cover the lobbying×SI temporal-overlap concept — consolidation candidate.
4. `archive/` does not exist yet — these moves create it (same dir the audit's ARCHIVE section uses).

---

## SANDBOX — move into pipeline_sandbox/

| Path | Destination | Reason |
|---|---|---|
| `lobbying_fetch.py` | `pipeline_sandbox/lobbying_fetch.py` | Its own docstring says `"STATUS: SANDBOX. Companion to lobbying_bootstrap.py."` — currently misfiled at root |

---

## UNSURE — needs your call

| Path | Default suggestion | Why I'm flagging it |
|---|---|---|
| `questions.py` | **RESOLVED: keep → wired into pipeline.STEPS** | Now placed after "Process legislation" as step "Flatten parliamentary questions". Will move to `domains/questions/etl.py` in Stage 1. |
| `pipeline_sandbox/lobbying_bootstrap.py` | **DELETED 2026-05-27** | User confirmed: job complete |
| `pipeline_sandbox/parquet_backfill.py` | **DELETED 2026-05-27** | User confirmed: job complete |
| `pipeline_sandbox/pydantic_manifest_example.py` + `_findings.md` | **DELETED 2026-05-27** | User confirmed: exploration concluded |
| `pipeline_sandbox/quarantine_plan.md` | `keep` (sandbox) | Planning doc for the quarantine logic that's now in `quarantine.py`. Still relevant context. |

---

## DECIDE — repo-level questions

### `dail_tracker_bold_ui_contract_pack_v5/` (60 files, 11 .py + 49 .md)

**RESOLVED 2026-05-27**: Skill/prompt pack used during UI design collaboration. **Keep in place during reorg; tagged for deletion at project conclusion.** Stage 1 leaves the directory untouched and the reorg's `src/dail_tracker/` skeleton ignores it.

### `pyproject.toml` already exists

I assumed in Stage 1 we'd add one. You already have a 5KB `pyproject.toml` at root — I haven't checked whether it's configured for editable install / `src/` layout. Stage 1 will need to update it to declare `src/dail_tracker/` as the package root.

**[2026-06-02] Checked. It is flat-layout hatchling, NOT src-ready.** Five separate blocks hardcode flat module names and all need rewriting for `src/dail_tracker/`:
1. `[tool.hatch.build.targets.wheel].packages` — lists 17 explicit modules/dirs (`utility`, `services`, `config.py`, `pipeline.py`, …). Becomes `packages = ["src/dail_tracker"]`.
2. `[tool.basedpyright].include` — lists `services`, `config.py`, `manifest.py`, `enrich.py`, `transform_votes.py`, `questions.py` + `venvPath`/`venv`. Repoint to the new module paths.
3. `[tool.ruff.lint.isort].known-first-party = ["utility", "normalise_join_key"]` → `["dail_tracker"]`.
4. `[tool.ruff.lint.per-file-ignores]` keyed on `"utility/pages_code/*.py"` → `"src/dail_tracker/ui/pages/*.py"`.
5. `[tool.pytest.ini_options].pythonpath = ["."]` → `["src"]` (plus `testpaths` if test dirs move).

Also note: `[project.scripts]` only defines `dail-pipeline = "pipeline:main"` (the rest are commented out) → becomes `dail-tracker.orchestration.pipeline:main`.

### DECIDE — chain destination (NEW 2026-06-02) — ✅ RESOLVED 2026-06-02: A

The 9 `*_refresh.py` chains can go either:
- **(A, CHOSEN)** `src/dail_tracker/orchestration/chains/<name>.py` — keeps the dispatcher (`pipeline.py`) and its chains together; clean separation of orchestration from domain logic.
- **(B)** `src/dail_tracker/domains/<domain>/refresh.py` — domain-local cohesion, but scatters orchestration and is awkward for `bootstrap` (cross-domain) and `seanad` (imports 6 Dáil modules).

**Resolution: A.** Grounded in reading the chains: `bootstrap_refresh` (PDFs + Members API + debates) and `seanad_refresh` (imports `attendance`/`enrich`/`payments_full_psa_etl`/`transform_votes`) are inherently cross-domain — B has no clean home for them. They are dispatched as a set by `pipeline.py`'s `CHAINS` list, so they ARE the orchestration layer.

**Does Step 3 (D2) make this moot? No — half-true only.** The chains are a *mix*: inside `iris_refresh`, steps 2–4 are import-and-call (`si_entity_enrichment.run()`), steps 1/5/6 are subprocess; `pipeline.py` then dispatches each whole chain as a subprocess (to tee per-chain stdout). Step 3 collapses the duplicated *runner* boilerplate (`_hr()`, timing, `failures.append`, subprocess wrapper) into one shared runner, but each chain's *definition* (which steps, order, skip-flags) remains per-chain data that still needs a home — and that home is `orchestration/chains/`. Step 3 shrinks each file from ~150 lines to a short declaration; it does not remove the destination question.

### DECIDE — promote load-bearing sandbox files (NEW 2026-06-02) — ✅ RESOLVED 2026-06-04 (all graduated to `extractors/`; see CURRENT TARGET STRUCTURE at top)

> **REOPENED 2026-06-04:** This was marked resolved after promoting ONE file, but the sweep found **7–8** sandbox scripts are now load-bearing pipeline steps (see the RE-AUDIT block at the top): `afs_amalgamated_extract.py`, `cbi_registers_extract.py`, `cro_corporate_xref_enrichment.py`, `procurement_etenders_extract.py`, `procurement_lobbying_xref.py`, `ted_ireland_extract.py`, `si_legislation_directory_extract.py`. Each needs the same promote-or-gate decision applied below. The text that follows resolved only `public_appointments_enrichment.py`.

`pipeline_sandbox/public_appointments_enrichment.py` was a **required** Iris gold step ([iris_refresh.py:95](iris_refresh.py#L95)) living in the throwaway sandbox (C5 violation).

**Resolution: promoted (Step 4 done on this branch).** It's the identical twin of `corporate_notices_enrichment.py` (same `main()`+argparse+`--write`+`__main__` shape, same required-gold-step role, same subprocess-with-`--write` invocation) — and corporate was already at root. Actions taken:
- `git mv pipeline_sandbox/public_appointments_enrichment.py → ./public_appointments_enrichment.py` (final home `domains/iris/` at Step 5).
- Repointed path setup to match the twin: `from config import GOLD_PARQUET_DIR, SILVER_DIR` + `from paths import PROJECT_ROOT as _ROOT` (the old `Path(__file__).parents[1]` would have resolved to the *parent of repo root* once moved up a level — a latent break this fixes).
- Updated `iris_refresh.py` step 5 to reference the root path (drops the `/ "pipeline_sandbox"` segment).
- Fixed 5 pre-existing `E741` lints (the file was previously exempt via ruff's `extend-exclude = ["pipeline_sandbox"]`; at root it's now linted — renamed ambiguous `l` → `line`).
- **Verified:** appointments parquet is data-identical pre/post (1060×13, `frames.equals()==True`; only parquet metadata bytes differ); ruff clean; `pytest test/` = **358 passed · 24 skipped**, identical to baseline.

Kept subprocess-driven (no `run()` library entry added) to stay consistent with the twin; an optional `run()` for both is a Step-6 nicety, not now. The Seanad chain's reuse of Dáil domain modules is fine (direct import, no sandbox); `seanad_refresh.py` lands in `orchestration/chains/` at Step 5 with imports repathed.

### DECIDE — root `__init__.py` (NEW 2026-06-02) — ✅ RESOLVED + EXECUTED 2026-06-02

**Resolution: stray — deleted on this branch.** It was a 0-byte empty file created incidentally in commit `b316d2a`. Under the current flat layout (`pytest.pythonpath=["."]`) every module imports top-level (`import config`, `import enrich`); confirmed **zero** `import dail_extractor` / `from dail_extractor` references anywhere. It does NOT "become" `src/dail_tracker/__init__.py` (that's a fresh file at Step 5). An empty package marker at repo root is a latent hazard (tools may treat the whole repo as one package). `git rm __init__.py` done; pytest unchanged.

### DECIDE — `services/member_paginated.py` destination — ✅ RESOLVED 2026-06-02: `infra/`

**Resolution: `infra/member_paginated.py`** (NOT `domains/members/`). Its only dependency is `from services.http_engine import fetch_json` (an adapter) — zero domain logic. It's a *generic* per-member API-pagination helper (takes a `url_builder` callable + df, returns raw payloads) used by **both** the questions and legislation flatteners via `oireachtas_api_main`. It's a peer of `http_engine`/`urls`/`storage`/`schema_validation`, all → `infra/`. Test → `test/unit/infra/test_member_paginated.py`.

### New SQL views & tests (NEW 2026-06-02)

`sql_views/corporate_corporate_notices.sql` + `sql_views/payments_member_detail.sql` stay in `sql_views/` (SQL views don't move, per original plan). New test `test/test_member_paginated.py` → `test/unit/infra/test_member_paginated.py` (resolved: `member_paginated.py` → `infra/`, see DECIDE above).

### `archive/` directory doesn't exist yet

Stage 1 creates it. The contents listed in the ARCHIVE section above all land there.

### `members/` / `avatar/` / other top-level dirs

I didn't fully enumerate top-level directories beyond the known ones (services, utility, test, pipeline_sandbox, sql_views, sql_queries, data, audit_screenshots, experimental, dail_tracker_bold_ui_contract_pack_v5, doc). If there's an `avatar/` or `members/` dir with content that's neither code nor in `data/`, flag it and I'll classify in a follow-up.

---

## Path-resolution caveat — Iris files (generalises to ALL refresh chains)

**[2026-06-02]** This caveat is no longer Iris-specific. Every one of the 9 `*_refresh.py` chains sets `_ROOT = Path(__file__).resolve().parent` and relies on it being repo root (for `cwd=` and relative paths). All of them break identically after the move. The fix below — swap `Path(__file__).parent` derivations for canonical `config.py` constants (or a shared `PROJECT_ROOT`) — should be applied across all 10 orchestrators, not just the 2 Iris ETL files. **This is the single highest-value frontload: do it on `main` first and the move becomes mechanical.**

Both Iris ETL files compute their default input/output dirs from `Path(__file__).resolve().parent`:

- [iris_oifigiuil_etl_polars.py:1748-1750](iris_oifigiuil_etl_polars.py#L1748-L1750) — `DEFAULT_INPUT_GLOB`, `DEFAULT_OUT_DIR`
- [iris_incremental_shards.py:71-73](iris_incremental_shards.py#L71-L73) — `DEFAULT_SHARD_ROOT`, `DEFAULT_INPUT_GLOB`

Today `__file__.parent` IS the project root because both files live at root. After Stage 1 they move to `src/dail_tracker/domains/iris/`, and `__file__.parent` becomes that directory — so the defaults would resolve to `src/dail_tracker/domains/iris/data/bronze/iris_oifigiuil/…` which doesn't exist.

**Stage 1 fix (small):** swap the `Path(__file__).resolve().parent` derivations for the canonical constants in [config.py](config.py) (`BRONZE_DIR`, `SILVER_DIR`). Both files already use `from config import …` patterns elsewhere in the codebase, so the import path is established. Roughly 6 lines across the two files.

**The Iris shard cache directory** (`data/silver/iris_oifigiuil_shards/`) is NOT moved by the reorg — data dirs stay put. The fix is purely about how the Python code FINDS that directory after the file moves.

Pipeline orchestration (`pipeline.py:71`) does NOT need updating for path resolution — it shells out via `[sys.executable, "iris_oifigiuil_etl_polars.py"]` today, will become `["-m", "dail_tracker.domains.iris.etl"]`, but in either form the child process inherits CWD = project root.

---

## Subprocess invocation impact

**[2026-06-02 — REWRITTEN. The original claim of "only one location" is obsolete.]**

`subprocess.run([sys.executable, "<script>.py"], cwd=_ROOT)` now appears in **`pipeline.py` plus all 9 `*_refresh.py` chains** — ~11 sites total:
- `pipeline.py:109` dispatches each chain by relative script name (`CHAINS` tuples).
- Each refresh chain dispatches its own step scripts the same way (e.g. `attendance_refresh.py:35`, `lobbying_refresh.py:40`, `iris_refresh.py:45/99/109`).

After the `src/` move, two things break at every site: the relative `"<script>.py"` filename (the file is no longer in CWD) and the `cwd=_ROOT` derivation (`_ROOT` becomes the package dir, not repo root).

**Recommended Stage-1 treatment:** convert dispatch to `[sys.executable, "-m", "dail_tracker.<...>"]` (module form, CWD-independent) AND/OR introduce one shared `PROJECT_ROOT` constant in `infra/` that all orchestrators import. Every dispatched script needs a working `if __name__ == "__main__":` guard — **verify all 9 chains' step scripts now**, since the chain set grew.

The poll runners (`run_*_poll.py`) still don't use subprocess — they call `oireachtas_pdf_poller.run_one()` directly. Their imports update straightforwardly.

---

## Test reorg

| Current path | Destination |
|---|---|
| `test/test_enrich_join.py` | `test/unit/domains/votes/test_enrich_join.py` |
| `test/test_lobby_processing.py` | `test/unit/domains/lobbying/test_processing.py` |
| `test/test_member_interests.py` | `test/unit/domains/members/test_interests.py` |
| `test/test_payments_golden.py` | `test/golden/test_payments.py` |
| `test/test_normaize_join_key.py` | `test/unit/shared/test_normalise_join_key.py` (also fix typo: `normaize` → `normalise`) |
| `test/test_services_members.py` | `test/unit/domains/members/test_api.py` |
| `test/test_services_votes.py` | `test/unit/domains/votes/test_api.py` |
| `test/test_http_engine.py` | `test/unit/infra/test_http_engine.py` |
| `test/test_url_builders.py` | `test/unit/infra/test_urls.py` |
| `test/test_wikidata_socials_etl.py` | `test/unit/domains/wikidata/test_socials_etl.py` |
| `test/test_gold_df.py` | `test/integration/test_gold_df.py` |
| `test/test_silver_layer.py` | `test/integration/test_silver_layer.py` |
| `test/test_silver_parquet.py` | `test/integration/test_silver_parquet.py` |
| `test/test_silver_lobbying_parquet.py` | `test/integration/test_silver_lobbying_parquet.py` |
| `test/test_sql_views.py` | `test/integration/test_sql_views.py` |
| `test/test_page_imports.py` | `test/ui/test_page_imports.py` |
| `test/test.py` | **DELETE if empty / commented-out** — quick check needed |
| `test/fixtures/` | `test/fixtures/` (stays) |
| `test/conftest.py` | `test/conftest.py` (stays) |

---

## Pre-flight test baseline

**CURRENT BASELINE (verified 2026-06-05): 615 passed · 0 failed · 9 skipped — GREEN.** Running CI's exact selector (`-m "not integration and not sql and not sources and not bronze"`) is clean. The `sql-contracts` job is also green (75 passed / 0) after the member_overview SI-view ordering fix. `firewall` + `typecheck` green. Only `lint` is red (in-flight reorg formatting churn — not a logic break).

- The earlier **2026-06-04 "506 · 3 · 78 / CI test job RED"** claim is **retired**: the two `test/test_la_payments.py` failures (`strip_id_prefix` two-numeric-run; xlsx `emit_file`) are fixed on this branch.
- The **9 skips are all "missing data" skips** (uncommitted gold/silver parquet) — they'd run if pipeline output were present; none are permanent.
- The earlier "358/0/24" (a bad edit) and "294/11" baselines are also **retired**. Re-run `.venv\Scripts\python.exe -m pytest -m "not integration and not sql and not sources and not bronze" -q` immediately before the Step-5 move to re-confirm green.

> The 2026-05-27 figures and failure clusters below are **HISTORICAL** — retained for rationale only.

---

### HISTORICAL — `pytest test/` on main, 2026-05-27, BEFORE any reorg moves:

- **294 passed, 11 failed, 10 skipped** (13.38s)
- All 11 failures are pre-existing bugs, NOT caused by anything in the reorg work. Three clusters:

**Cluster A — pandera/polars API drift (7 failures)**
`@pa.dataframe_check` decorator in newer pandera (≥0.20) passes a `PolarsData` NamedTuple to the check function instead of the DataFrame directly. Every `def check(cls, df): return df["col"]...` crashes with `TypeError: tuple indices must be integers or slices, not str`. Files: test_gold_df.py, test_silver_layer.py, test_silver_lobbying_parquet.py, test_silver_parquet.py.

**Cluster B — schema drift / data freshness (3 failures)**
- `test_silver_layer.py::test_attendance_days_within_bounds` — same `sitting_days_count` column-not-found that we fixed in `enrich.py` (the test reads `aggregated_td_tables.csv` which lacks count columns; same fix pattern needed).
- `test_silver_lobbying_parquet.py::test_lobbying_table_non_empty[grassroots_campaigns]` — `grassroots_campaigns.parquet is empty` — likely real data issue.
- `test_gold_df.py::test_master_td_count_in_range` — AssertionError on row count.

**Cluster C — pandas/polars confusion (1 failure)**
`test_normaize_join_key.py::test_join_keys_are_unique_in_members` — `AttributeError: 'DataFrame' object has no attribute 'alias'`. Test mixes a pandas DataFrame with a polars-only method.

**What this meant at the time**: 294/305 was the known-good baseline as of 2026-05-27. Superseded — see the current **615 · 0 · 9 (GREEN)** baseline at the top of this section. After the Step-5 move, the post-fix pass count must match the pre-move count exactly; any new failure = a reorg-induced regression to investigate.

## Next step

Once you've reviewed and confirmed/edited the remaining tags:
1. I commit this file as-is in a setup PR (so reviewers of the reorg PR can see the rationale).
2. Stage 1 opens a `git worktree` on a branch named `reorg/src-layout`.
3. Inside the worktree: do all the moves + deletes + import rewrites per this audit; update `pyproject.toml`; switch subprocess calls to `-m` form.
4. Run full test suite (expect 294 pass / 11 fail — same as baseline). Run full pipeline. Streamlit walkthrough.
5. If clean: open PR. If something breaks at hour 5: walk away from the worktree, no damage to main.

---
---

# Architecture review & contracts-first migration plan (2026-06-02)

*Written as a platform-architecture proposal. No code moved. Everything above this line is the file-relocation audit ("where files go"); this section is the rationale and coupling strategy ("why the boundaries exist, how stages talk, what order is safe").*

## 0. The one reframe that scopes everything

**The pipeline↔UI boundary is already excellent — leave it alone.** The "logic firewall" ([doc/LOGIC_FIREWALL_AUDIT_PASS2.md](doc/LOGIC_FIREWALL_AUDIT_PASS2.md), [contract pack PIPELINE_VIEW_BOUNDARY.md](dail_tracker_bold_ui_contract_pack_v5/docs/PIPELINE_VIEW_BOUNDARY.md)) already gives a clean, documented, mostly-enforced contract: pipeline + registered DuckDB SQL views own all modelling (joins, rollups, flags, rankings); Streamlit does retrieval-only against approved registered views; [`utility/data_access/_sql_registry.py`](utility/data_access/_sql_registry.py) centralises view registration. That is a mature data-contract boundary. **This reorg is entirely pipeline-internal.** Do not re-model anything UI-side; the UI move (audit Step 5) is mechanical path-only.

Everything below targets the *other* side: the 60-file flat repo root where stages are coupled by **filename, filesystem path, and import-time side effect** rather than by contract.

## 1. Assessment — concrete coupling points, layering violations, duplication

### Coupling points (each cites the pain, not a slogan)

- **C1 — Orchestration ↔ filename coupling.** `pipeline.py` + all 9 `*_refresh.py` invoke stages by *relative script filename* via `subprocess.run([sys.executable, "<file>.py"], cwd=_ROOT)` ([pipeline.py:109](pipeline.py#L109), [iris_refresh.py:45/99-100/109](iris_refresh.py#L45)). The "stage interface" is *"a .py file at a known path that does work on `__main__`"* — not a callable with a signature. Rename or move a stage → the orchestrator breaks. This is the #1 reason the audit's move is scary.
- **C2 — Path-by-`__file__` coupling, defined ≥4 times.** `config.PROJECT_ROOT = Path(__file__).parent` ([config.py:11-12](config.py#L11)); every `*_refresh.py` recomputes `_ROOT = Path(__file__).resolve().parent`; `_sql_registry.PROJECT_ROOT = Path(__file__).resolve().parents[2]` ([_sql_registry.py:29](utility/data_access/_sql_registry.py#L29)); the two Iris ETL files derive dirs the same way. Four independent "where is root" definitions, all positional on the file's location → all shift on a move.
- **C3 — Import-time side effects (the deepest coupling).** `config.py` runs `init_dirs()` at import ([config.py:87](config.py#L87)) — importing "pure config" writes to the filesystem. And per [pyproject.toml:100](pyproject.toml#L100), most top-level scripts "execute on import (no `__main__` guard)." **Consequence: you cannot import a stage to test one of its functions without triggering its work.** That single fact blocks "runnable/testable in isolation."
- **C4 — Cross-domain internal-symbol imports.** `seanad_refresh.py` imports `attendance`, `enrich`, `payments_full_psa_etl`, `transform_votes` directly ([seanad_refresh.py:28-31](seanad_refresh.py#L28)). This is *reuse* (good — don't duplicate the Dáil parsers) but expressed as "reach into another domain's module," not "call a shared transform with a stable signature."
- **C5 — Sandbox→production layering violation.** `pipeline_sandbox/` is documented throwaway, "never called by the pipeline" (contract-pack CLAUDE.md), yet `iris_refresh.py:99` runs `pipeline_sandbox/public_appointments_enrichment.py --write` as a *required* gold step. The invariant is silently broken.
- **C6 — UI input contract is a string literal path.** The pipeline→UI data handoff is `read_parquet('data/silver/...')` text inside each `.sql`, rewritten at runtime by `absolutize_data_paths` against `parents[2]` ([_sql_registry.py:35-39](utility/data_access/_sql_registry.py#L35)). It works, but the contract is "a relative path string + a positional root guess."

### Layering violations

- `config.py` mixes **pure constants** with **IO** (`init_dirs()` side effect) — violates its own docstring ("no side effects beyond init_dirs").
- **No transform/IO separation.** Each domain script reads bronze, transforms, and writes silver in one module with module-level execution. There is no pure-logic layer you can test without disk.
- **No code-level layer enforcement.** Medallion layers are physical dirs only; any script can read any layer (`enrich` reads silver+gold, writes gold). Fine functionally, but nothing makes the dependency direction explicit.

### Duplication (candidates to delete, not rewrite)

- **D1 — `PROJECT_ROOT` ×4** (C2). Collapse to one.
- **D2 — Two orchestration frameworks, inner one ×9.** `pipeline.py` has a chain-runner (`_run_subprocess`, timing, failure isolation); each `*_refresh.py` re-implements an identical step-runner (`_hr()`, `subprocess.run(... cwd=_ROOT)`, timing, `failures.append`). Same structure confirmed across all 9. Collapse to one runner; chains become data.
- **D3 — Parquet-write convention repeated at every writer** (`zstd`/`level=3`/`statistics=True`, per [feedback_parquet_write_convention]). One `infra.io.write_parquet` helper.
- **D4 — `_sql_registry.py` is the model already done right** — it deduped the view-registration boilerplate. Follow that pattern for D1/D2.

### Existing strengths to preserve

Logic firewall (§0); `_sql_registry` centralisation; `services/` is **already an adapters layer** (`http_engine`, `storage`, `urls`, `logging_setup`, `run_paths`, `schema_validation`) — just misnamed; `manifest.py` + `run_paths.py` already give an orchestration run-contract.

## 2. Target architecture — layout, boundaries, and the *why* of each

Simplest layout that buys loose coupling (no abstraction with one implementer):

```
src/dail_tracker/
  infra/                 # adapters + cross-cutting IO  (≈ today's services/ + the IO half of config)
    config.py            #   PURE constants only — init_dirs() removed
    paths.py             #   THE single PROJECT_ROOT + layer-path resolver (replaces the 4 copies)
    io.py                #   read/write helpers; zstd parquet convention baked in (kills D3)
    http_engine.py  storage.py  urls.py  logging_setup.py  run_paths.py  schema_validation.py
  contracts/
    schemas/             # pandera DataFrameModels HOISTED from test/ — the inter-stage contract
  domains/<d>/
    transform.py         # PURE: DataFrame -> DataFrame. No IO, no work on import.
    etl.py               # THIN shell: io.read -> transform -> validate(schema) -> io.write. Has main().
  orchestration/
    pipeline.py          # the dispatcher (unchanged role)
    chains.py            # ONE chain-runner; chains declared as data (kills D2)
  ui/                    # today's utility/ — governed by the logic firewall, move is path-only
  runners/               # thin poll wrappers
```

**Boundary → what change it lets you make without touching the other side:**

| Boundary | Why it exists (the concrete decoupling it buys) |
|---|---|
| **B1 `infra/paths.py`** | One place defines root + layer dirs. Move files or relocate `data/` → edit *one* module, not 4 `__file__` derivations. Stages `from dail_tracker.infra.paths import SILVER_PARQUET_DIR` and never compute paths. **Fixes C2/D1 — and is the precondition that makes the audit's file-move mechanical.** |
| **B2 `transform.py` (pure) vs `etl.py` (IO shell)** | The deepest fix. Splitting `DataFrame→DataFrame` logic from read/write lets you unit-test a stage with a 5-row in-memory fixture — no bronze PDFs, no prior pipeline run. Change storage format → touch `etl.py`+`io.py`; change a business rule → touch `transform.py` and test it alone. **Fixes C3 + "testable in isolation."** |
| **B3 `contracts/schemas/`** | Today the stage-N→stage-N+1 contract is implicit ("trust the columns"). The pandera silver/gold models **already exist in `test/`** — hoist them so they're the *runtime* boundary: `etl.py` validates output before writing; the next stage validates on read. A break is caught at the boundary, not 3 stages downstream. **This is the "schema/interface" the brief asks for — reuse, don't author new.** |
| **B4 `orchestration/chains.py`** | Chains as data + one runner. "Add a step" = add a tuple, not copy-paste a 150-line script. In-process steps become imported callables (no subprocess, no filename). **Fixes C1/D2.** Justified because there are already 10 implementers — not a speculative abstraction. |
| **B5 `infra/config.py` purity** | Drop `init_dirs()` at import; call it explicitly in each `main()`. Config becomes importable in a test/notebook with zero filesystem writes. **Fixes the config half of C3.** |
| **B6 promote `public_appointments_enrichment.py`** | A load-bearing dependency must not live in the throwaway tree. Restores the invariant "nothing in `pipeline_sandbox/` is called by the pipeline." **Fixes C5.** |

## 3. Coupling strategy — how stages communicate, with a real before/after

**Mechanism:** *file-based data contracts (medallion parquet/CSV at named paths) validated by pandera schemas at the IO boundary, with pure transforms and injected paths.* Stages never import each other's internals to get data — they read a contract-validated file written by an upstream stage. Cross-domain *logic* reuse (C4, the Seanad case) is allowed but expressed as a call into a shared transform with a stable signature, not a reach into a sibling script.

### Before / after — orchestration (verbatim from `iris_refresh.py`)

**Before** — orchestrator hardcodes filenames + paths; sandbox path leaks in:
```python
def step_poll() -> bool:
    r = subprocess.run([sys.executable, "iris_oifigiuil_poller.py"], cwd=_ROOT)   # C1: filename
    return r.returncode == 0

def step_appointments_gold() -> bool:
    script = _ROOT / "pipeline_sandbox" / "public_appointments_enrichment.py"     # C5: sandbox leak
    r = subprocess.run([sys.executable, str(script), "--write"], cwd=_ROOT)       # C2: cwd=_ROOT
    return r.returncode == 0
```

**After** — a chain is *data*; steps are callables with signatures; root/paths come from `infra`:
```python
# domains/iris/chain.py
from dail_tracker.orchestration.chains import Step, run_chain
from dail_tracker.domains.iris import poller, silver_rebuild, si_entity_enrichment, public_appointments

IRIS_CHAIN = [
    Step("poll",          poller.run,                  skip_flag="skip_poll"),
    Step("silver",        silver_rebuild.rebuild,      skip_flag="skip_silver"),
    Step("si_gold",       si_entity_enrichment.run,    skip_flag="skip_derived"),
    Step("appointments",  public_appointments.run),    # promoted out of sandbox (B6)
]
# run_chain handles timing, logging, manifest, failure isolation — once, not ×9 (D2)
```
No filename literal, no `cwd`, no sandbox path. Reordering or skipping is a data edit.

### Before / after — stage IO + contract (target shape for B2/B3)

**Before** — read + transform + write fused, no schema gate:
```python
# domains/payments/etl.py (today: one module, work at import)
df = pl.read_parquet(_ROOT / "data/silver/.../payments.parquet")   # IO + path coupling
df = df.with_columns(...) ...                                       # transform (untestable alone)
df.write_parquet(out, compression="zstd", compression_level=3, statistics=True)  # D3 repeated
```

**After** — pure transform + thin shell + schema validation at the boundary:
```python
# domains/payments/transform.py  — PURE, unit-testable with a 5-row frame
def build_member_payments(raw: pl.DataFrame) -> pl.DataFrame: ...

# domains/payments/etl.py        — thin; main() guards the work
from dail_tracker.infra import io, paths
from dail_tracker.contracts.schemas import PaymentsSilver
def run() -> None:
    raw  = io.read_parquet(paths.SILVER_PARQUET_DIR / "payments_raw.parquet")
    out  = build_member_payments(raw)
    PaymentsSilver.validate(out)          # B3: contract enforced before publish
    io.write_parquet(out, paths.SILVER_PARQUET_DIR / "payments.parquet")  # D3: convention in one place
```

## 4. Migration plan — PR-sized, mechanical vs behavioral separated, with safety + rollback

**Hard gate between every step: the code-level pytest pass count must not drop** (re-baseline first — see Risk R5; some integration failures are data-dependent, not code). The ordering is deliberate: do the *enabling de-coupling* (mechanical, on `main`) first, so the scary file-move becomes trivial, and defer the genuinely behavioral transform/IO split to the very end where it can stop anytime.

| # | Step | Kind | What changes | Why it's safe (proof) | Rollback |
|---|---|---|---|---|---|
| **0** | Re-baseline | — | Run `pytest test/` on `main`; classify each failure code-vs-data | Establishes the real gate (the doc's 294/11 is stale) | n/a |
| **1** | `infra/paths.py` single root | **mechanical** | Add one `PROJECT_ROOT`+layer module; point all 10 orchestrators + `_sql_registry` + 2 Iris ETLs at it. No file moves. | `PROJECT_ROOT` value is identical; `pytest` unchanged; `pipeline.py --list` + a `register_views` smoke | revert 1 file |
| **2** | Config purity | **behavioral (small)** | Remove `init_dirs()` at import; call it in `pipeline.main()` + each `etl.main()` | `pytest`; one full pipeline run still creates dirs | re-add import-time call |
| **3** | One chain-runner | **mechanical** | Collapse the 9 `*_refresh.py` step-runners into `orchestration/chains.py`; chains as data. Files stay at root. | `pipeline --select <chain>` yields identical stdout/manifest per chain; `pytest` | per-file revert |
| **4** | Promote sandbox dep (B6) | **behavioral (isolated)** | Move `public_appointments_enrichment.py` out of `pipeline_sandbox/`; fix the `iris_refresh` ref | Run iris chain; diff `public_appointments.parquet` byte-for-byte | move back |
| **5** | **The file move** (audit body) | **mechanical** | Execute relocation to `src/dail_tracker/`, import rewrites, `pyproject` 5-block update, `subprocess`→`-m`. *In a worktree.* | `pytest` matches Step-0 baseline exactly; full pipeline; Streamlit smoke; manifest compare | discard worktree |
| **6+** | Transform/IO split + schema hoist (B2/B3) | **behavioral** | **One domain per PR.** Split `etl.py`→`transform.py`+shell; hoist that domain's pandera model to `contracts/schemas/`; validate at boundary | `assert_frame_equal` old vs new output on a fixture; integration test still green; schema validates | revert one domain |

Steps 1–4 are the frontload: low-risk, on `main`, and they remove C1/C2/C3/C5 — the exact couplings that make Step 5 dangerous. After them, Step 5 is find/replace. Step 6 is optional and incremental; the project can stop after Step 5 with a clean layout and still have gained the decoupling.

**Steps that cannot be made non-breaking:** none are *unavoidably* breaking, but **Step 2** (import semantics) and **Step 4** (a real dependency's path) are behavioral — kept in their own PRs, never bundled with a mechanical move. **Step 5** is large; if a single PR is unreviewable, split by layer in this order: `infra/` → `contracts/` → `domains/` → `orchestration/` → `ui/` → `test/`, each green before the next.

## 5. Risk register — silent breaks + early detection

- **R1 Import side-effects** (C3): importing a module does work/writes files. *Detect:* add a test that imports every `dail_tracker.*` module with CWD set to a tmp dir and asserts no files were created. Forces the `main()`-guard discipline and catches regressions.
- **R2 Path constants** (C2/C6): after the UI move, `_sql_registry.parents[2]` and any residual `__file__` root shift silently — views still *register* but read the wrong/empty path, surfacing as empty UI, not an error. *Detect:* a test that registers all `sql_views/*.sql` against a temp conn and asserts each resolves to an existing data path; assert `paths.PROJECT_ROOT == repo root` in CI.
- **R3 Cached artifacts:** the Iris incremental shard cache keys on `(mtime_ns, size, EXTRACTOR_VERSION)` (content, not path) so a move is safe — but verify with one `--rebuild` post-move. Delete the dead `pipeline_sandbox/__pycache__/` orphans (audit DEAD) so stale `.pyc` can't shadow a moved source.
- **R4 CI/tooling assumptions:** `pytest.pythonpath=["."]`, `ruff.isort.known-first-party`, `basedpyright.include`, `hatch...wheel.packages` all hardcode flat names ([pyproject](pyproject.toml#L150)). *Detect:* CI fails loudly on the move PR — acceptable because it's pre-merge; the pyproject DECIDE block lists all five to change.
- **R5 Data not committed** (`git ls-files data/silver|gold` = 0): integration tests (`test_silver_parquet`, `test_gold_df`) read on-disk generated data → green only where the pipeline has run. This is a **pre-existing test↔local-data coupling**, not reorg-caused, and explains part of the 11 failures. *Mitigation:* the gate is the **code-level** pass count; pin data-dependent tests as a separate, environment-gated set so reorg PRs aren't blamed for data noise.
- **R6 Cross-domain imports** (C4): the Seanad chain's `import attendance, enrich, …` must be repathed; if missed it's an `ImportError` *at import* (module-level) → fails fast/loud in `pytest` collection. Low risk by virtue of being eager.
- **R7 Subprocess CWD:** children inherit `cwd=repo root` today; the `-m` switch keeps that, but `infra.paths` absolute resolution is what actually removes the dependence. *Detect:* a smoke test that runs a chain from a different CWD.

## 6. What NOT to do (tidy-looking churn with no payoff right now)

- **Don't rename `services/` → `infra/` as a standalone PR.** It already *is* the adapters layer; the rename touches 30+ import sites for zero new capability. Fold it into the Step-5 move if at all, or keep the name.
- **Don't split every domain's transform/IO up front.** Do B2/B3 lazily — only for domains you're actively touching or that have real test pain. A thin poller gains nothing from the split.
- **Don't build a plugin/entry-point discovery system for stages.** The chains-as-data list is sufficient; each stage has one caller.
- **Don't commit `data/` to git or build a feature store.** File-based medallion contracts are right at this scale; the upgrade is schema-validation at the boundary (B3), not new storage infra.
- **Don't touch the logic firewall / UI data-access modelling.** That boundary is mature; the UI move is path-only.
- **Don't "improve" `pipeline.py` / `enrich.py` / `normalise_join_key.py` mid-move.** Flagged fragile/frozen (contract-pack CLAUDE.md). Relocate mechanically; change no behavior.
- **Don't bundle a mechanical move with a behavioral refactor in the same PR** — the brief's explicit rule; it's also what keeps rollback cheap.

## 7. Assumptions to confirm (I'm guessing here — flagging per the brief's rules)

- **A1 — RESOLVED 2026-06-02: the page-contract YAML layer does not exist.** `utility/page_contracts/` is absent on disk, not gitignored, zero code references, no `*.yaml` anywhere under `utility/`. The "every page driven by `page_contracts/<page>.yaml`" in the contract-pack CLAUDE.md + firewall plan is **aspirational, never built**. The firewall is enforced by convention + the `_sql_registry`/data-access pattern. **Implication for this plan:** the inter-stage "schema/interface" must be the **pandera models (B3)** — do NOT take a dependency on, or try to revive, the page-YAML system as part of the reorg. If a contract layer is wanted later, that's a separate decision from this reorg.
- **A2** — silver/gold parquet is **regenerated, not committed** (confirmed 0 tracked). So unit-stage isolation needs `test/fixtures/`, and integration tests are inherently local/CI-data-gated. **Confirm that's acceptable** (it shapes R5 + B3).
- **A3 — UPDATED 2026-06-02: Seanad has ongoing work → treat as PROVISIONAL, not frozen production.** It's wired into `CHAINS` + `config.py`, but active development is in flight (see `project_seanad_parity` + `pipeline_sandbox/seanad_*_experimental.py`). **Treatment:** during the reorg, the seanad chain moves like the others, but its domain code is tagged provisional and is the LAST domain to get the Step-6 transform/IO split (don't refactor a moving target). Its cross-domain reuse of `attendance`/`enrich`/`payments_full_psa_etl`/`transform_votes` (C4) stays as-is — repath imports only, change no logic. If the in-flight work renames/reshapes seanad files, re-confirm its KEEP rows before Step 5.
- **A4 — see "Impact of starting Step 1 now" below.**

### A4 — Impact of starting Step 1 (single-root frontload) now, on `main`

**What it is:** designate ONE canonical project-root + layer-path source (`config.py` already has `PROJECT_ROOT`/layer constants — make it *the* source pre-move) and repoint the duplicate derivations at it. No files move; no behavior changes.

- **Blast radius:** ~12 files — the 9 `*_refresh.py` (`_ROOT = Path(__file__)...` → `from config import PROJECT_ROOT`), the 2 Iris ETLs, and `_sql_registry.py` (`parents[2]` → canonical). The refresh scripts already do `from config import ...`, so the import path is established.
- **Behavioral change:** none. Every derivation already resolves to repo root today; the value is identical before/after. Pure consolidation.
- **The one subtlety:** `_sql_registry.py` currently uses `parents[2]` specifically to AVOID importing `config` (and thus `config`'s import-time `init_dirs()` side effect) into the Streamlit process. Repointing it pulls that side effect into UI launch (harmless — just `mkdir -p`, already happens server-side). Cleanest is to **pair Step 1 with Step 2 (config purity / drop `init_dirs()` at import) for the `_sql_registry` change specifically**, so the UI import stays side-effect-free.
- **Verification:** full `pytest` (must match baseline), `python pipeline.py --list`, and one `register_views()` smoke (UI loads, a view returns rows). ~30 min of work.
- **What it unblocks:** this is the precondition that turns the big file-move (Step 5) from per-file `__file__`/path surgery into mechanical find/replace. It also removes the 4-way `PROJECT_ROOT` duplication and the "a move silently breaks paths" fragility *regardless of whether the full reorg ever happens*.
- **Reversibility / regret:** per-file revert; no data touched, no move. **Low-regret** — even if you abandon the reorg, you're left with a consolidated root, which is strictly better. The only "wasted" effort on abandonment is negligible.
- **Relation to `feedback_refactor_timing`:** Step 1 is *not* a big refactor — it's a localised de-duplication. It's exactly the kind of safe frontload that can land during active work without the "defer to plateau" concern that applies to Step 5 (the mass move).

### Step 1 — EXECUTION LOG (2026-06-02, orchestration-layer slice DONE)

Executed dry-run-first, incrementally, baseline-gated. **Result: green, no regressions.**

- **Refreshed test baseline first.** Had to reinstall `rpds-py` (0.30.0 → 2026.5.1) — its compiled extension was broken, blocking collection of 5 test modules via `jsonschema`→`referencing`→`rpds`. *Environment fix, not code.* Post-fix baseline = **334 passed · 1 failed · 24 skipped**. The 1 failure (`test_v_member_registry_executes`, `{SEANAD_MEMBER_PARQUET_PATH}` unsubstituted) is **pre-existing and tied to the live Seanad WIP** — the audit's stale "294/11" is retired.
- **Dry-run proof** (no edits): asserted `config.PROJECT_ROOT`, `config.BASE_DIR`, a refresh `_ROOT`, an Iris ETL `_ROOT`, and `_sql_registry parents[2]` ALL equal the repo root → consolidation is a provable no-op.
- **Changes shipped:**
  - NEW `paths.py` — side-effect-free single source of `PROJECT_ROOT` (becomes `infra/paths.py` at move). *Refinement vs the original A4 note:* a dedicated side-effect-free module (not `config.py`) so importers don't drag in `init_dirs()` — this decouples Step 1 from Step 2 entirely.
  - `config.py` now does `from paths import PROJECT_ROOT`; `BASE_DIR = PROJECT_ROOT` (dropped the dead `from pathlib import Path`).
  - 8 of 9 refresh chains repointed `_ROOT = Path(__file__)...` → `from paths import PROJECT_ROOT as _ROOT` (attendance, bootstrap, interests, legislation, lobbying, members, payments, iris).
- **`seanad_refresh.py` deliberately UNTOUCHED** — it has uncommitted in-flight Seanad work (adds `payments_member_enrichment` to the payments step). Its 2 pre-existing `E402`s are in that WIP, not introduced here.
- **Verification:** ruff clean on all changed files; all 8 chains import with zero side-effects and correct `_ROOT`; `pipeline.py --list` works; **final pytest = 334·1·24, identical to baseline.**

**Deferred to later increments:**
1. ✅ **DONE 2026-06-02 — Option B (see Option B execution log below).** The ~12 root-level domain ETLs were migrated to `config` layer constants.
2. **`_sql_registry.py` (`parents[2]`) + the rest of `utility/`** — UI layer; carries `sys.path` mechanics risk. Fold into the Step-5 move where packaging/sys.path is handled properly.
3. ✅ **DONE 2026-06-02.** `lobbying_fetch.py` used `parents[1]` from a root-level file → resolved to the *parent of the repo root* (would write bronze CSVs outside the project). Root cause: it was **misfiled at root** — its docstring usage (`python pipeline_sandbox/lobbying_fetch.py`), `STATUS: SANDBOX` header, and sibling convention all expect it in `pipeline_sandbox/`, where `parents[1]` correctly resolves to repo root. Fixed by `git mv lobbying_fetch.py pipeline_sandbox/` (no code change — `parents[1]` is now correct). Verified via `--dry-run` (`Dest: data/bronze/lobbying_csv_data`); nothing imports it (companion `lobbying_bootstrap` already deleted). Closes the audit's SANDBOX item.

### Option B — EXECUTION LOG (2026-06-02, domain-ETL data paths → config constants, DONE)

Same dry-run-first, baseline-gated discipline. **Result: 358 passed · 0 failed · 24 skipped — no regressions.**

- **Scope:** 12 root-level domain ETLs switched from `_ROOT / "data" / ...` string-walking to `config.BRONZE_DIR` / `SILVER_DIR` / `SILVER_PARQUET_DIR` / `GOLD_PARQUET_DIR` / `DATA_DIR` constants. Medallion *layers* now come from config (one owner); domain *leaf* dirs (`iris_oifigiuil`, `wikidata`, `_meta`, `committees`) stay as local appends. **No new config constants added** (lowest-risk, config untouched).
- **Per-file nuances handled:**
  - `corporate_notices_enrichment.py` & `payments_member_enrichment.py` keep their root var (repointed to `paths.PROJECT_ROOT`) because it's still needed for `sys.path.insert` / `.relative_to()`; only the data paths moved to config.
  - `wiki_data.py` builds `avatar/wikidata` (not a medallion layer, no config constant) → got the Step-1 treatment (`from paths import PROJECT_ROOT`) instead of Option B.
  - 6 files gained `from config import …`; 5 already imported config (extended); ruff auto-removed 6 now-dead `from pathlib import Path` imports.
- **Verification:** (1) dry-run proof asserted all **20** `config`-constant expressions resolve byte-identically to the old `_ROOT`-based paths *before* editing; (2) ruff clean on all 12; (3) import-smoke imported all 12 modules and re-confirmed all 20 constants byte-identical at runtime; (4) **pytest 358·0·24**.
- **Env note:** the venv was missing `idna`/`certifi` (and earlier `rpds-py`) mid-session — restored via targeted `pip install` to unblock the gate. Likely concurrent `uv` activity uninstalling the `pipeline` extra ([[feedback_uv_env_management]]). Recommend a clean `uv sync --extra pipeline` to reconcile pip/uv state.
- **What remains from the original `Path(__file__)` footprint (categorised audit 2026-06-02):** of ~115 raw hits, only one category is the real bug, and it's now closed in live code.
  - **(A) project-root derivation** — the actual bug. **Live pipeline/server code: DONE** (config, paths, 9 chains, 12 domain ETLs, `lobby_processing.py` sql_queries, `lobbying_fetch` moved). Remainder is **UI only** (`utility/config.py`, `data_access/_sql_registry.py`, `ui/avatars.py`, `data_access/member_overview_data.py`, `_UTIL` in votes/member_overview/glossary) → **deferred to Step 5** (entangled with packaging/sys.path); plus `services/dail_config.py` (dead — delete) and `tools/check_streamlit_logic_firewall.py` (trivial dev tool).
  - **(B) `sys.path.insert` bootstrap hacks (~31: 9 UI, ~21 tests, 1 sandbox)** — a separate smell, **removed wholesale by the Step-5 editable install**, not per-file.
  - **(C) legitimate file-relative resource access (~47)** — `schema_validation.SCHEMA_DIR`, test fixture dirs, `audit_screenshots/` outputs — **correct, must stay**.
  - **(D) `pipeline_sandbox/` `_ROOT` (~32)** — throwaway; out of scope.
  - **`paths.py`** is the one legitimate derivation by design; `wiki_data.py` keeps a benign `PROJECT_ROOT / "avatar"` append.

---
---

# 8. Logging hardening plan (added 2026-06-02)

*Remediation design for the logging assessment, parked here because it shares a code touch with **D2** (collapse the 9 duplicated chain step-runners into one) — do both in the same pass. Phase 3 (retention) belongs with `doc/CI_CD.md` / `doc/CICD_TODO.md` scheduled jobs.*

**Status:** Plan only — nothing implemented. Phases 1 & 2 are ready to build on `main`; Phase 3 deferred to go-live (log history is low-priority during alpha, important once unattended in production).

## 8.0 Why (plain summary)

There's a good **per-run logging system**: `python pipeline.py` creates `logs/runs/<run_id>/` with the orchestrator log, one file per chain, and a `manifest.json` (what ran, durations, exit codes, git SHA). Keep it. The problem: it's used **only by the full pipeline**. The 9 per-domain chain scripts (`attendance_refresh.py`, `iris_refresh.py`, …) don't plug in — run one on its own and nothing is saved. So output got captured by hand (`python attendance_refresh.py > attendance_run.log`), and those files piled up at repo root and in `tmp/`. Separately, the old catch-all `logs/pipeline.log` has no size limit and reached 92 MB. None are git-tracked (all ignored) → a tidiness/observability problem, not a leak risk.

## 8.1 The one idea that shapes the fix

Chains print progress with plain `print()` (the `──── [1/6] … ────` banners, "done in 4.2s", Seanad summaries). Python's logging **cannot capture `print()`** — so "turning on logging" in a chain would save a near-empty file while the useful output still vanishes. The full pipeline already solves this by **tee-ing the whole output stream** of each chain to a file ([pipeline.py:106-128](pipeline.py#L106-L128)). So the standalone-chain fix is the same: copy the whole stream to a file. Simpler than rewriting ~200 `print()` calls into logging, captures everything, and keeps the console clean.

## 8.2 Phase 1 — stop the bleeding *(now; ~30 min, low-risk)*

| # | Change | Plain terms |
|---|---|---|
| 1.1 | One shared log format | Pipeline and chains format lines differently today (separators + field order). Define the format once in `services/logging_setup.py`; everything imports it. |
| 1.2 | Size-cap the old catch-all | `logs/pipeline.log` grows forever (the 92 MB file). Swap its `FileHandler` for `RotatingFileHandler(maxBytes≈10 MB, backupCount=3)` → can't exceed ~40 MB. Affects only the 4 standalone scripts that hit the legacy path. See **Decision A**. |
| 1.3 | One-time cleanup | Delete ~120 MB of stray logs: root `pipeline.log`/`pipeline_run.log`/`attendance_run.log`/`dbsect_after_pipeline.log`/`endpoint_check.log`/`streamlit_test.log`, stale `services/logs/`, 11× `tmp/*.log`, the 92 MB `logs/pipeline.log`, and the aborted `logs/runs/2026-05-31…` dir. All git-ignored. |

## 8.3 Phase 2 — chains save their own logs *(now; the real fix, ~1–2 hrs; D2-aligned)*

Stops new stray logs at the source — there's no longer any reason to hand-redirect.

**8.3a New helper `chain_logging(chain_name)` in `services/logging_setup.py`:**

```python
@contextmanager
def chain_logging(chain_name: str):
    if os.environ.get(ENV_RUN_ID):
        # started BY the full pipeline — parent already tees our output
        setup_logging()                      # console only
        yield None
        return
    # run on its own — act as our own mini-pipeline
    run_id = make_run_id()
    os.environ[ENV_RUN_ID] = run_id          # so sub-scripts we launch cooperate
    setup_logging()                          # console only (tee owns the file)
    create_run_manifest(run_id)              # full-parity manifest — agreed
    log_path = run_dir(run_id) / "pipeline.log"
    with _tee_stdio(log_path):               # copy ALL output (print + logging) to file
        try:
            yield run_id
        finally:
            run_finished_at(run_id)
```

`_tee_stdio(path)` = small context manager replacing `sys.stdout`/`sys.stderr` with a writer fanning out to console + file (UTF-8, line-buffered — reuse the pipeline's encoding care for `→`/`é` on Windows).

Two points so a future reader doesn't "fix" them:
- **No duplicate lines:** in standalone mode we deliberately attach *no* logging file-handler. The tee owns the file; logging writes to console, which the tee copies. One writer = no double-write.
- **Sub-scripts captured free:** a launched step (e.g. `attendance.py`) inherits the tee'd stream, so its output lands in the same run-folder file — no per-chain plumbing.

**8.3b Manifest (agreed):** standalone runs write a full `manifest.json` + rollup row + `latest_run_id.txt`, same as a pipeline run. `manifest.py` already supports it; it's just not called from chains today.

**8.3c Apply to all 9 chains:** replace each `logging.basicConfig(...)` with wrapping `main()`'s body in `with chain_logging("<name>"):`.

```python
# before (attendance_refresh.py)
def main() -> int:
    argparse.ArgumentParser(...).parse_args()
    logging.basicConfig(level=logging.INFO, format="…")   # remove
    started = time.monotonic()
    ...
# after
def main() -> int:
    argparse.ArgumentParser(...).parse_args()
    with chain_logging("attendance"):
        started = time.monotonic()
        ...
```

Per-chain notes: `seanad_refresh.py` configures **no** logging today (its `_log.info` is silently dropped) — this wires it up for the first time. All 9 already import root from `paths.py`, so no path work.

**Outcome:** `python iris_refresh.py` alone now produces `logs/runs/<id>/pipeline.log` + manifest, like the full pipeline. Running under `pipeline.py` is unchanged (chain detects it's a child, lets parent capture).

## 8.4 Phase 3 — go-live grade *(deferred; decide now, enable at launch)*

- **3.1 Retention:** `prune_old_runs(days=…)` exists ([run_paths.py:87](services/run_paths.py#L87)) but is dead code ([pipeline.py:218-221](pipeline.py#L218-L221)). Enable in `main()` or (preferred) from the scheduled CI job. Window: suggest 90 days.
- **3.2** Revisit the Phase-1 size cap once real run volume is known.
- **3.3** Upload `logs/runs/<id>/` as a CI artifact on failed scheduled runs (layout already zip-friendly). Add a `doc/CICD_TODO.md` line.
- **3.4** `DAIL_LOG_LEVEL` env switch; optional JSON formatter for ingestion. Cheap once format is centralised (1.1).
- **3.5** Failure alerting — out of scope; the seam where this meets `test/HANDS_OFF_TEST_PLAN.md` notifications.

## 8.5 Decision A — the old catch-all file (plain English)

`logs/pipeline.log` is a shared file a few small scripts write to when run alone (`iris_si_bill_enrichment.py`, `si_entity_enrichment.py`, `services/dbsect_harvest.py`, `services/oireachtas_api_main.py`).
- **Option A (recommended, = Phase 1.2):** keep it, but cap its size ("never past ~10 MB; when full, start fresh, keep last 3"). One line; nothing else moves.
- **Option B:** drop it entirely — make even these small scripts create their own timestamped run folder. Tidier/uniform, but changes shared-function behaviour and touches the 4 scripts (more work, slightly more risk).

**Recommend A now** (solves the only real problem — runaway size — in one line); revisit B later for full uniformity. This is the only open question; everything else is agreed.

## 8.6 Risks & verification

- A chain run *under* the pipeline must not mint a second run folder — the `ENV_RUN_ID` check prevents it; confirm `python pipeline.py --select attendance` yields exactly one run dir.
- Tee must force UTF-8 (as the pipeline does) for arrows/accents on Windows.
- `setup_logging` already refuses to add handlers twice; verify chains don't import a step module that configures logging before `chain_logging` runs.
- **Acceptance:** (1) `python attendance_refresh.py` → exactly one `logs/runs/<id>/pipeline.log` with banners + a manifest row; (2) `python pipeline.py --select iris` → one run dir, no nesting, iris still captured; (3) repeated standalone `si_entity_enrichment.py` → `logs/pipeline.log` stays under the cap.

## 8.7 Effort & order

Phase 1 (~30 min) → Phase 2 (~1–2 hrs, the payoff) → Phase 3 (go-live). Phases 1–2 are low-risk, land on `main`, and aren't blocked by the `src/` reorg. When the reorg lands, `chain_logging` is the natural home for the consolidated chain-runner (**D2**).
