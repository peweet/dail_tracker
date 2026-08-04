# CLAUDE.md — Dáil Tracker

Front-load conventions so sessions don't re-discover them by exploring (the biggest recurring token cost), and codify the data/UI boundaries that keep the project correct.

## Never-break rules

**When rules collide:** correctness and provenance beat token economy beat speed.

- **Never `Read` data files** — tracked parquet/CSV run to tens of MB and flood the context window. Query via the `dail-tracker` MCP (deferred; small structured results) or `duckdb`/`polars` with a `LIMIT`. The MCP is a stdio subprocess Claude Code starts and kills automatically — never launch it; use `/mcp` to restart.
- **Three money grains never union/sum together** (procurement awarded vs. payments vs. budget; never sum TED). See the data map in memory before touching money facts.
- **Provenance is the user's domain** — don't invent figures, don't promote to gold without data-anchored evidence, don't infer values in UI copy.
- **Polars for ETL, pandas only in the UI layer.** Don't mix.
- Parquet writes are **atomic, zstd + statistics**, with a **row-floor guard** — use the `save_parquet` helper, don't bypass it.
- Join key for members is the **normalised TD name** (NFKD accent-fold). Reuse the existing normaliser; don't invent matching.

## First move — route the question (reflex, not a mandate)

Reach for the cheap path **first**; escalate to reading files only when it genuinely can't answer.

| The question | First move | Instead of |
|---|---|---|
| columns / rows / grain / freshness of dataset X? | `describe_dataset("X")` | Reading a parquet or `fact_cards.json` |
| which dataset / view / doc covers topic T? | `search_project("T")` | repo-wide `Grep` |
| where is function/class F defined? | `code_outline(path)` → `Read(offset=,limit=)` that span | Reading the whole file |
| what does this >1,500-line file do? | its `# ── SECTION MAP ──` header (`limit:60`) | Reading the whole file |
| will renaming/reordering a SQL view break things? | `view_deps` | grepping `sql_views/` |
| will renaming a **column** break views? | `column_deps(view, col)` — sqlglot lineage; `mode:'regex'` hits need a read | grepping `sql_views/` |
| who **calls/uses** Python symbol F? (rename, signature change) | `py_refs(module, name)` — jedi call sites + import bindings; blind to `_LazyModule`/getattr | grepping the repo for the name |
| who said / who's asking about topic T? | `search_speeches` / `search_questions` (BM25-ranked, corpus-wide) | ILIKE trawls via member feeds |
| can I sum these money columns? | never-sum grain rule (above) | assuming |
| which doc covers X? | `doc/INDEX.md` | grepping `doc/` |
| is this LIVE or experimental? | `doc/SANDBOX_MAP.md` | inferring from the path |
| understand a whole module / cross-cutting behaviour | Read the file if it fits, else use a scoped exploration subagent | iterating small scoped tools |
| a sweep across many files | one scoped exploration subagent (dumps stay in its context) | many main-thread Reads |
| a well-briefed build (files+constraints+check known) | a lean implementation subagent; brief must name files, change, acceptance check | building in a long main thread |

**Escalate explicitly:** after 2–3 scoped calls without an answer, say the cheap path can't answer and Read the specific span. Don't tool-fragment a small file — if it fits in one bounded Read, read it.

## Token & context discipline

- **Delegate broad sweeps** only when output is much smaller than input (a conclusion from bulky reading). A subagent pays its own context cost, so batch related questions into one spawn and never spawn for a bounded lookup a cheap tool answers. Scope every `Grep`/`Glob`; throwaway scripts print counts/top-N, never raw dumps.
- **`/clear` between unrelated tasks.** Before re-deriving a known trap, run `uv run python tools/discoveries.py <topic>`. Optional local memory is supplemental and may be stale; verify every named file or flag in the current tree.
- **When compacting or summarizing a session,** preserve verbatim: the modified-file list, commands still to run, and the user's requirements and errors.

## Streamlit conventions

- **Logic firewall:** pages (`utility/pages_code/`) contain **no business logic** — queries/transforms go through `utility/data_access/`, and pages render from registered contracts. The firewall checker enforces this.
- Dataframes are secondary UI; prefer deliberate components and shared design tokens. Use the `civic-ui-review` / `impeccable` skills when they are installed.

## Environment & commands

- The environment is managed with `uv`; use `uv run python ...` so commands work across platforms. **UTF-8 is required** (`PYTHONUTF8=1` / `PYTHONIOENCODING=utf-8`) or console output can break on Irish accents.
- **Memory (added 2026-07-26 after two OOM session crashes):** `services/runtime_env.py` caps
  the BLAS thread count and must stay the **first** import in every entry point — uncapped,
  `import pandas` reserves ~650 MB of commit per process on this 20-core box. Each open Claude
  session also holds ~1.2 GB (client + its own MCP server), so close surplus windows before a
  siting run (`pytest test/siting` commits 5.4 GB). `tools/hooks/guard_memory.py` blocks heavy
  commands below 1.5 GB free. Measure **private bytes**, never working set — Windows trims the
  latter, which reads ~30 MB while the process commits 714 MB.
- Canonical tasks: `uv run python tools/dev.py list`; focused verification: `uv run python tools/dev.py verify`; deterministic local gates: `uv run python tools/dev.py check`. `pytest --testmon` remains opt-in because its database invalidates on wide refactors.
- App: `uv run streamlit run utility/app.py`. ETL: `uv run python pipeline.py`. The task runner exposes the firewall, convention, MCP-catalog, dependency, type, and test checks.
- **UI audit: `python tools/ui_capture.py {routes|capture|diff|a11y|probe}`** — THE screenshot/accessibility harness (replaced 88 ad-hoc `audit_screenshots/_*.py` probes). Routes are AST-parsed from `utility/app.py`, so never hardcode a route list. `--serve` starts its own Streamlit (~1 GB; siting routes excluded unless `--include-heavy`). `a11y` needs `npm install --no-save axe-core` and attributes each violation `ours`/`vendor` — **only `ours` is actionable**. Used by the `civic-ui-review` skill.

## Where to look first

- `doc/INDEX.md` — generated doc map; scan before grepping `doc/`. `doc/SANDBOX_MAP.md` — LIVE vs. experimental (`pipeline_sandbox/` = experiments; LIVE extractors are DO-NOT-DELETE).
- `uv run python tools/discoveries.py <topic>` and tracked `memory/` notes — hints for known decisions; verify them against current source.
- `dail_tracker_core/queries/`, `sql_queries/`, `utility/data_access/` — the data-access surface.
- Dataset schema/grain: `describe_dataset` / `list_datasets` (served from `data/_meta/fact_cards.json`) — never scan a parquet.
