# Repo reorganisation audit

**Date:** 2026-05-27
**Status:** Stage 0 — read-only scout. No files moved or deleted yet.
**Decision needed before Stage 1:** review the `unsure` and `decide` sections and confirm/override the tags.

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
| Top-level `.py` to classify | 45 → **44** (tear_down.py deleted this session) |
| `keep` (will be moved) | 33 |
| `dead` (will be deleted in Stage 1) | 3 |
| `unsure` (resolved during session) | 0 — all 4 resolved (questions → keep; 3 sandbox files → deleted) |
| `sandbox` (move into sandbox) | 1 |
| Streamlit UI files (utility/) | 36 — all `keep`, moving to `src/dail_tracker/ui/` |
| Pre-flight test baseline | **258 pass / 11 fail / 10 skipped** (refreshed 2026-05-27 after producer cleanups) |

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
| `iris_oifigiuil_etl_polars.py` | `domains/iris/etl.py` | pipeline.STEPS |
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

### runners/

| Current path | Destination | Reason |
|---|---|---|
| `run_attendance_poll.py` | `src/dail_tracker/runners/attendance_poll.py` | Thin wrapper around `oireachtas_pdf_poller.run_one("attendance")` |
| `run_interests_poll.py` | `src/dail_tracker/runners/interests_poll.py` | Thin wrapper |
| `run_payments_poll.py` | `src/dail_tracker/runners/payments_poll.py` | Thin wrapper |
| `run_iris_poll.py` | `src/dail_tracker/runners/iris_poll.py` | Thin wrapper around `iris_oifigiuil_poller.main()` |

### ui/

All 36 files under `utility/` move to `src/dail_tracker/ui/` preserving internal structure:
- `utility/app.py` → `ui/app.py` (Streamlit entrypoint)
- `utility/shared_css.py` → `ui/shared_css.py`
- `utility/config.py` → `ui/config.py` (UI-specific constants — distinct from infra/config.py)
- `utility/constants.py` → `ui/constants.py`
- `utility/select_drop_rename_cols_mappings.py` → `ui/select_drop_rename_cols_mappings.py` (imported by flatten_members_json_to_csv + lobby_processing)
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
| `pipeline_sandbox/iris_incremental_shards.py` | Imports `iris_oifiguil_etl` (typo, doesn't exist anywhere). Broken on import. |
| **Stray root logs** | `pipeline.log`, `pipeline_run.log`, `endpoint_check.log`, `dbsect_after_pipeline.log`, `attendance_run.log`, `streamlit_test.log`, `logs/pipeline.log` (88 MB), `services/logs/` (dir). **Note 2026-05-27**: tear_down.py was deleted; sweep these manually in Stage 1. |
| `.coverage` | pytest-cov artifact; should be gitignored (check `.gitignore`) |
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

### `archive/` directory doesn't exist yet

Stage 1 creates it. The contents listed in the ARCHIVE section above all land there.

### `members/` / `avatar/` / other top-level dirs

I didn't fully enumerate top-level directories beyond the known ones (services, utility, test, pipeline_sandbox, sql_views, sql_queries, data, audit_screenshots, experimental, dail_tracker_bold_ui_contract_pack_v5, doc). If there's an `avatar/` or `members/` dir with content that's neither code nor in `data/`, flag it and I'll classify in a follow-up.

---

## Subprocess invocation impact

Only **one** location in the codebase uses `subprocess.run([sys.executable, script])` — [pipeline.py:121-122](pipeline.py#L121-L122). Every step name in `STEPS` becomes a module path:
- `"lobbying_poller.py"` → `["-m", "dail_tracker.domains.lobbying.poller"]`
- `"enrich.py"` → `["-m", "dail_tracker.domains.votes.enrich"]`

This is the bulk of the pipeline change. Every step script needs a working `if __name__ == "__main__":` guard; most already do.

The runners (`run_*_poll.py`) don't use subprocess — they call `oireachtas_pdf_poller.run_one()` directly. Their imports update straightforwardly.

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

## Pre-flight test baseline (2026-05-27)

`pytest test/` on main, BEFORE any reorg moves:

- **245 passed, 11 failed, 10 skipped** (12.75s)
- All 11 failures are pre-existing bugs, NOT caused by anything in the reorg work. Three clusters:

**Cluster A — pandera/polars API drift (7 failures)**
`@pa.dataframe_check` decorator in newer pandera (≥0.20) passes a `PolarsData` NamedTuple to the check function instead of the DataFrame directly. Every `def check(cls, df): return df["col"]...` crashes with `TypeError: tuple indices must be integers or slices, not str`. Files: test_gold_df.py, test_silver_layer.py, test_silver_lobbying_parquet.py, test_silver_parquet.py.

**Cluster B — schema drift / data freshness (3 failures)**
- `test_silver_layer.py::test_attendance_days_within_bounds` — same `sitting_days_count` column-not-found that we fixed in `enrich.py` (the test reads `aggregated_td_tables.csv` which lacks count columns; same fix pattern needed).
- `test_silver_lobbying_parquet.py::test_lobbying_table_non_empty[grassroots_campaigns]` — `grassroots_campaigns.parquet is empty` — likely real data issue.
- `test_gold_df.py::test_master_td_count_in_range` — AssertionError on row count.

**Cluster C — pandas/polars confusion (1 failure)**
`test_normaize_join_key.py::test_join_keys_are_unique_in_members` — `AttributeError: 'DataFrame' object has no attribute 'alias'`. Test mixes a pandas DataFrame with a polars-only method.

**What this means for the reorg**: 245/256 is our known-good baseline. After Stage 1, the same 245 should pass (in their new paths) and the same 11 should fail. Any new failures = reorg-induced regressions to investigate.

## Next step

Once you've reviewed and confirmed/edited the remaining tags:
1. I commit this file as-is in a setup PR (so reviewers of the reorg PR can see the rationale).
2. Stage 1 opens a `git worktree` on a branch named `reorg/src-layout`.
3. Inside the worktree: do all the moves + deletes + import rewrites per this audit; update `pyproject.toml`; switch subprocess calls to `-m` form.
4. Run full test suite (expect 245 pass / 11 fail — same as baseline). Run full pipeline. Streamlit walkthrough.
5. If clean: open PR. If something breaks at hour 5: walk away from the worktree, no damage to main.
