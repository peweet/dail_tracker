# Dáil Tracker agent guide

This file is the portable, tool-neutral entry point for work in this repository. More specific `AGENTS.md` files override it inside their directories.

## Start here

1. Run `git status --short` and preserve unrelated changes.
2. Route the task with the table below; read only the files needed for that area.
3. Use `rg --files` and scoped `rg -n` searches. The tracked `.rgignore` hides bulky generated data and artifacts from default searches.
4. Use `uv run python tools/dev.py verify` for focused, changed-file-aware verification. Use `uv run python tools/dev.py check` before a broad handoff.

Before re-deriving a known project trap, run `uv run python tools/discoveries.py <topic>`. For a source file over roughly 1,500 lines, read its leading `SECTION MAP` first and then open only the relevant span.

## Routing

| Work | Start with | Local guidance |
| --- | --- | --- |
| API and reusable query logic | `api/`, `dail_tracker_core/queries/` | `dail_tracker_core/AGENTS.md` |
| Streamlit pages | `utility/pages_code/`, then `utility/data_access/` | `utility/pages_code/AGENTS.md` |
| Extraction and enrichment | `extractors/` or `planning/civic/extractors/` | the nearest `AGENTS.md` |
| SQL views and data contracts | `sql_views/`, `test/sql_views/` | `sql_views/AGENTS.md` |
| MCP tools and repository navigation | `mcp_server/`, `test/mcp_server/` | `mcp_server/AGENTS.md` |
| Project documentation | `doc/INDEX.md` | `doc/SANDBOX_MAP.md` distinguishes live and experimental code |
| Canonical development commands | `tools/dev.py` | run `uv run python tools/dev.py list` |

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

- Focused: `uv run python tools/dev.py verify`
- Inspect the selected checks without running them: `uv run python tools/dev.py verify --plan`
- Fast suite: `uv run python tools/dev.py test-fast`
- Full local merge-gate approximation: `uv run python tools/dev.py check`
- SQL contract changes also need `DAIL_INTEGRATION_TESTS=1 uv run pytest -m sql -q` with committed gold data present.

Successful focused verification is cached against the exact Git/worktree fingerprint. A changed file, commit, interpreter, or verification policy invalidates the receipt. Failed runs are never cached.
