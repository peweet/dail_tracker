# Dáil Tracker — Shared Review Context (read first)

**Purpose:** single primer for the parallel review sessions assessing the 2026-06-05 source/architecture briefs in `doc/dail_tracker_*.md`. Read this in full before your task-specific prompt. It carries the project invariants, the verified repo landmark map, the verification protocol, and the required output format so all reviews are consistent and mergeable.

---

## 1. What the project is

Irish civic-accountability app. **Server-side ETL** (mostly Polars) → **DuckDB + Parquet** silver/gold → **SQL views** (`sql_views/*.sql`, the semantic contract) → **Streamlit** UI (pandas). An in-progress effort decouples Streamlit into a thin layer over a Streamlit-free core (`doc/fastapi_query_core_uncoupling_plan.md`). The stated bottleneck is **surfacing existing data, not ingesting more**.

Orientation docs to skim first: `doc/IDEAS.md` (master idea map), `doc/DATA_MAP.md` (all-domains status board + the 3-money-grain rule), `MEMORY.md` (index of project memories).

## 2. Non-negotiable invariants (flag every breach)

- **Logic firewall** — UI must not define business logic/classification. Classification, grouping, `value_counts`, dedup belong in the pipeline/SQL views, not Streamlit. Pages read views; they don't model.
- **No inference in app UI** — the app presents verifiable, source-linked data only. It never directs users to conclusions (welcome in *planning chat*, forbidden in *UI copy*).
- **Cite news/real-world claims** — any assertion about events/fines/scandals/named individuals needs a reputable inline source link.
- **Never union across money grains** — `value_kind` + `realisation_tier` (PLANNED→AWARDED→COMMITTED→SPENT) gate summation; only sum where `value_safe_to_sum`. Award ≠ spend; framework/DPS ceilings ≠ payments; budget ≠ actual; grant_allocated ≠ grant_paid.
- **Privacy** — never display personal/individual data naming private citizens (precedent: personal insolvency suppressed, corporate fine; judiciary must anonymise natural persons, strip raw case text/references). Use runtime exceptions, not `assert` (stripped under `-O`).
- **Parquet writes** — every writer passes `compression="zstd", compression_level=3, statistics=True`. No bare `to_parquet`/`write_parquet`.
- **Refactor timing** — defer large refactors; a reorg is already underway. Don't propose mass file moves.

## 3. Verified repo landmark map (confirmed 2026-06-05)

**pipeline.py chains that EXIST:** `("afs", extractors/afs_amalgamated_extract.py)`, `("cbi", extractors/cbi_registers_extract.py)`, `("procurement", extractors/procurement_etenders_extract.py)`, `("procurement_lobbying", extractors/procurement_lobbying_xref.py)`, `("ted", extractors/ted_ireland_extract.py)`.

**NOT wired into pipeline.py** (exist as files only — confirm in your review): `extractors/procurement_la_payments_extract.py`, `procurement_public_body_extract.py`, `procurement_award_spend_link.py`, `procurement_la_seed.py`, `procurement_publishers_seed.py`, `procurement_hse_tusla_parser.py`, `procurement_seai_parser.py`, `legal_diary_extract.py` (no `judiciary` chain at all).

**Extractors:** `extractors/cbi_registers_extract.py`; `la_afs_extract.py`, `la_afs_camelot_ie.py`, `afs_amalgamated_extract.py`; the `procurement_*` set above; `ted_ireland_extract.py`; `legal_diary_extract.py`.

**SQL views:** `sql_views/corporate_cbi_distress.sql`; `procurement_{awards,supplier_summary,authority_summary,cpv_summary,lobbying_overlap}.sql`; `judiciary_legal_diary_{schedule,counts,cases}.sql`.

**Pages:** `utility/pages_code/judiciary.py` exists. **No** `procurement.py` or `local_authority*` page exists yet.

**Query layer:** `dail_tracker_core/queries/{procurement,judiciary}.py`.

**Data-access layer:** `utility/data_access/{procurement,judiciary}_data.py`.

**UI helpers:** `utility/shared_css.py` (all CSS lives here); `utility/ui/components.py` (note: under `utility/ui/`, not `utility/`); plus `utility/ui/{source_links,entity_links,table_config,export_controls,avatars}.py` and panel modules. Verify what already exists before proposing new components.

**Pollers/infra:** `pdf_infra/` (e.g. `pdf_endpoint_check.py` API canaries). Freshness/source-health: `tools/check_freshness.py`, `build_source_health.py`, `build_source_registry.py`.

> Treat this map as a starting index, not gospel — still open the file and confirm behaviour before asserting it in your review.

## 4. Verification protocol

1. **Ground-truth first.** Every brief was written from external GitHub inspection (one author states they never cloned the repo). Verify each claim against real code before assessing it.
2. **Parallelize grounding.** Use read-only Explore subagents to confirm file claims concurrently.
3. **Timebox.** If a claim can't be confirmed in a couple of file reads, mark it `unverifiable` and move on — do not rabbit-hole.
4. **Cite.** Every statement about the repo gets a real `path:line` reference.

## 5. Action mode (all review sessions)

Analysis-first. You MAY: (a) save your review to the named `doc/<TOPIC>_REVIEW.md`; (b) write read-only probe scripts named `pipeline_sandbox/probe_review_<task>_*.py` to test source/parse tractability. You must NOT modify `pipeline.py`, existing extractors, gold/silver parquet, pages, or run `uv`/test churn (Windows ruff file-lock + multi-agent same-tree risk).

## 6. CLAIMS LEDGER format (first deliverable)

| claim | doc says | repo reality (path:line) | verdict |
|---|---|---|---|

verdict ∈ {confirmed, stale, wrong, unverifiable}.

## 7. Standard output schema (use these exact H2 headers, in order)

```
## Claims Ledger
## Architectural Assessment
## Devil's Advocate
## Data Quality & Enrichments
## Build / Defer / Reject
## Bottom Line
```

Keep `Build / Defer / Reject` as an explicit per-item table (item | verdict | value/effort | reason). End with a one-paragraph `Bottom Line`. Save the whole thing to your task's `doc/<TOPIC>_REVIEW.md` so a later synthesis pass can merge all reviews.
