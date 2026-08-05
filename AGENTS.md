# Dáil Tracker agent guide

This file is the portable, tool-neutral entry point for work in this repository. More specific `AGENTS.md` files override it inside their directories.

## Start here

1. Run `git status --short` and preserve unrelated changes.
2. Route the task with the table below; read only the files needed for that area.
3. Use `rg --files` and scoped `rg -n` searches. The tracked `.rgignore` hides bulky generated data and artifacts from default searches.
4. Use `uv run --locked --group dev --extra pipeline --extra api --extra mcp python tools/dev.py verify` for focused, changed-file-aware verification. Use the same command with `check` before a broad handoff. The runner repairs a bare invocation once, but specifying the profile avoids a bootstrap re-exec.

Before re-deriving a known project trap, run `uv run python tools/discoveries.py <topic>`. For a source file over roughly 1,500 lines, read its leading `SECTION MAP` first and then open only the relevant span.

## Durable project knowledge

- Put rules that must apply on every run in this file or the nearest nested `AGENTS.md`.
- Put a concise, trigger-keyed lesson in `tools/discoveries.jsonl` and supporting evidence in `memory/<slug>.md`. The configured Codex `UserPromptSubmit` hook may inject up to two matching one-liners; inspect and trust it once with `/hooks`.
- For a deeper workstation-local lookup, use `search_project(query, kind="external_memory")` explicitly. `kind="memory"` searches checked-in public cards only. External memory is excluded from ordinary project search and may be stale; verify every path, number, and implementation claim against the current tree.
- Local Codex Memories and imported Claude memories are supplemental personal context. Never make them the only copy of a repository invariant, decision, or verification command.

## Subagent policy

- The primary agent is the captain: it owns requirements, decisions, integration, and the final response.
- Delegate only genuinely independent, bounded exploration, review, verification, or log-analysis tracks. Keep tightly coupled work in the primary thread.
- Use the project `scout` role for read-only codebase mapping, the project `reviewer` role for independent correctness, regression, provenance, and test review, and the project `worker` role for bounded implementation after the approach is decided.
- Every spawn must select one of those project roles, start with fresh context, and receive a self-contained five-part brief: Objective, Scope, Invariants, Acceptance, and Result contract. The project `PreToolUse` hook rejects non-conforming spawns.
- Scouts and reviewers stay read-only, return concise evidence with file references, and never post externally or make product, legal, or commercial decisions.
- A worker owns only the files named in its brief, preserves concurrent edits, and returns changed files plus verification evidence to the captain; it never commits, pushes, or posts externally.
- Wait for every requested result, adjudicate disagreements against repository evidence, and report unresolved uncertainty explicitly.
- Permit at most one write-capable agent in a checkout. Parallel implementation requires separate Git worktrees and explicit integration ownership; do not improvise worktrees for `planning/product`, which uses the separate `.git-siting` worktree.

## Routing

| Work | Start with | Local guidance |
| --- | --- | --- |
| API and reusable query logic | `api/`, `dail_tracker_core/queries/` | `dail_tracker_core/AGENTS.md` |
| Streamlit pages | `utility/pages_code/`, then `utility/data_access/` | `utility/pages_code/AGENTS.md` |
| Extraction and enrichment | `extractors/` or `planning/civic/extractors/` | the nearest `AGENTS.md` |
| SQL views and data contracts | `sql_views/`, `test/sql_views/` | `sql_views/AGENTS.md` |
| MCP tools and repository navigation | `mcp_server/`, `test/mcp_server/` | `mcp_server/AGENTS.md` |
| Private Siting product | `planning/product/` | `planning/product/AGENTS.md`; tracked `CLAUDE.md` is the configured migration fallback |
| Project documentation | `doc/INDEX.md` | `doc/SANDBOX_MAP.md` distinguishes live and experimental code |
| Canonical development commands | `tools/dev.py` | run `uv run --locked --group dev --extra pipeline --extra api --extra mcp python tools/dev.py list` |

When the configured `dail-tracker` MCP server is available, prefer `search_project`, `code_outline`, `py_refs`, `view_deps`, `column_deps`, and `describe_dataset` for broad discovery. Otherwise use narrowly scoped `rg`. After two or three unsuccessful index calls, inspect the specific source span directly.

## Repository invariants

- Never load parquet, large JSON/JSONL, PDFs, or raw corpora into an agent context. Query data with DuckDB/Polars plus a `LIMIT`, or use the MCP metadata tools. Override `.rgignore` only for a known path with `rg --no-ignore`.
- Procurement awards, public-body payments, and budgets are different money grains. Never union or sum them together, and never sum TED notice values.
- Preserve provenance. Do not invent values, promote inferred facts to gold, or move data transformations into UI copy.
- Use Polars in ETL and pandas only in the presentation layer.
- Write parquet through `services.parquet_io.save_parquet`; writes must stay atomic, compressed, and guarded against accidental row loss.
- Reuse the existing member-name normalisers in `shared/`; do not introduce another accent-fold or matching rule.
- `pipeline_sandbox/` is experimental. Do not replace or delete a live extractor based only on a similarly named sandbox script.
- `services/runtime_env.py` must remain the first project import in memory-heavy entry points so native thread caps are applied before pandas/NumPy load.

## Verification

- Focused: `uv run --locked --group dev --extra pipeline --extra api --extra mcp python tools/dev.py verify`
- Inspect the selected checks without running them: `uv run --locked --group dev --extra pipeline --extra api --extra mcp python tools/dev.py verify --plan`
- Fast suite: `uv run --locked --group dev --extra pipeline --extra api --extra mcp python tools/dev.py test-fast`
- Full local merge-gate approximation: `uv run --locked --group dev --extra pipeline --extra api --extra mcp python tools/dev.py check`
- Agent prompt changes: `python tools/dev.py agent-context` (stdlib-only; no environment bootstrap)
- SQL contract changes also need `uv run --locked --group dev --extra pipeline --extra api --extra mcp python tools/dev.py sql-contracts` with committed gold data present.

Successful focused verification is cached against the exact Git/worktree fingerprint. A changed file, commit, interpreter, or verification policy invalidates the receipt. Failed runs are never cached.
