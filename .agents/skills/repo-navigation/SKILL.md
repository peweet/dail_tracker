---
name: repo-navigation
description: Locate Dail Tracker code, datasets, SQL dependencies, and documentation with bounded MCP navigation. Use for unfamiliar repository questions or change-impact discovery; skip when the exact source span is already known. Excludes private Siting product navigation.
---

# Repository navigation

Use the configured `dail-tracker` MCP server. Resolve the tool names exposed by
the current client; the names below are the server's canonical names. A skill
does not connect a missing server or change its permissions.

Choose the entry tool from the question; do not run every route:

| Evidence needed | Entry and next step |
|---|---|
| Unknown module, dataset, view, or document | `search_project(query, kind, limit=5)`; follow the most relevant hit by kind below. Omit `kind` for mixed questions. |
| Python implementation | `code_outline(path, limit=20, response_format="concise")`, then read only the relevant symbol's line span. Skip the outline if that span is already known. |
| Python change impact | `py_refs(path, name, limit=20)` before a symbol rename/signature change; `py_deps(path)` for module import impact. Static results do not cover dynamic dispatch. |
| SQL view dependencies | `view_deps(view)` for the named view; `column_deps(view, column)` when the question concerns one column's lineage. |
| Dataset contract | `describe_dataset(name)` using the returned dataset name. Read grain, provenance, columns, and availability before choosing a query. |
| Documentation or public lesson | Read the returned heading/span. Use `kind="memory"` for checked-in public evidence cards. |

For directory outlines, start with `limit=5`: this bounds parsed/returned modules,
not definitions. Prefer a named view to the whole SQL graph.

Inspect errors and omission markers. `metadata_total`, `metadata_returned`, and
`metadata_truncated` cover lexical metadata only; `content_spans` are separate FTS
hits. Refine the query/kind when the relevant evidence was omitted. Counts and
cached indexes do not prove current or exhaustive coverage.

If MCP is unavailable or a call stalls, use scoped `rg --files` / `rg -n` and
bounded source reads. After two or three unsuccessful index calls, inspect the
specific source directly. Check the leading SECTION MAP before reading a large
source file. Batch only independent lookups with known arguments and retain
errors, paths, line spans, and completeness markers in the selected output.

Stop retrieval once the question has source-backed evidence. Reuse those findings
until the source or task changes. Return exact paths/symbols/spans plus unresolved
coverage or freshness limits. External memory requires explicit
`kind="external_memory"` and current-tree verification. Private Siting tasks must
follow `planning/product/AGENTS.md` and its private navigation surface.
