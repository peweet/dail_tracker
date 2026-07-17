---
status: SHIPPED
domain: infra
updated: 2026-07-17
read_when: Implementing VS Code / cross-agent customization, hooks, or MCP search-context work
key: agent-customization
---

# Agent Customization & Search-Context Plan

> **SHIPPED 2026-07-17.** Phases 1–5 implemented. Landed: `.vscode/settings.json`
> search/files exclusions + `chat.useClaudeMdFile`; `.vscode/mcp.json` + `.mcp.json`
> MCP registration; `search_project` tool + 5 new MCP resources + 2 prompts in
> `mcp_server/server.py`; three hooks (`tools/hooks/{guard_data_reads,session_context,
> firewall_check}.py`) wired in `.claude/settings.json`; four `.claude/rules/*.md`;
> four `.github/prompts/*.prompt.md`. Measured wins: search.exclude removes 659 tracked
> files / ~193 MB (incl. 218 parquet) from the agent search surface; `search_project`
> answers "where does X live" in ~500 tokens vs a 131-file / 1,120-line grep. Phase 6
> (usage policy) is practice, not code. Deferred items unchanged. Config under `.claude/`
> + `.vscode/settings.json` stays local by repo convention (like CLAUDE.md); only the
> portable MCP/prompt configs are git-tracked.

Sources: VS Code agent-customization docs (mcp-servers, custom-instructions, hooks,
prompt-files, agent-plugins, language-models, optimize-usage, workspace-context,
mcp-configuration) + modelcontextprotocol.io intro. Read 2026-07-17. Implementer: Opus.

## The two levers

"More powerful search context" decomposes into:

1. **Subtract noise** — the repo deliberately tracks ~100 MB of runtime parquet/CSV/avatars
   (re-included at the bottom of `.gitignore` for Streamlit Cloud). `.gitignore` exclusion is
   what keeps files out of VS Code agent search — so these *tracked* binaries sit inside the
   text-search/grep/semantic-index surface of any VS Code agent. Claude Code is protected by
   the `deny Read` rules in `.claude/settings.json`; VS Code agents are not.
2. **Add signal** — the project already has the right retrieval assets (`fact_cards.json`,
   `doc/INDEX.md`, the join map, 60+ MCP tools). They are reachable by Claude Code only, via
   user-level MCP config and CLAUDE.md conventions. Register them in-repo, expose them as MCP
   resources/prompts, and enforce the query-not-scan rule with hooks so it works for every
   agent deterministically, not by instruction-following.

## Current state (verified 2026-07-17)

| Asset | State |
|---|---|
| `CLAUDE.md`, `.claude/` (settings, 5 skills, 5 agents, 3 commands, SessionEnd hook) | Rich, Claude-Code-only, **gitignored** (local-only by choice) |
| `mcp_server/server.py` | stdio, ~60 tools, 7 prompts, **1 resource** (`data://coverage`); registered only in user-level Claude config |
| `.vscode/settings.json` | sqltools only — **no `search.exclude` / `files.exclude`**, no `chat.*` settings |
| `.vscode/mcp.json`, `.mcp.json`, `.github/instructions|prompts|hooks` | none |
| Data-read guard | Advisory (CLAUDE.md prose) + Claude-only permission denies |

---

## Phase 1 — Search-context hardening (config only, do first)

**1a. `.vscode/settings.json` — exclusion settings** (docs: workspace-context#exclusion-settings).
Semantics: `files.exclude` hides from Explorer *and* removes from text search, grep, and the
semantic index; `search.exclude` removes from text search/grep but keeps files visible.
Data should stay visible → use `search.exclude` for data, `files.exclude` for pure noise:

```jsonc
{
  "search.exclude": {
    "**/data/bronze/**": true,
    "**/data/silver/**": true,
    "**/data/gold/**": true,
    "**/*.parquet": true,
    "avatar/**": true,
    "test/fixtures/**/*.parquet": true,
    "doc/archive/**": true,
    "doc/source_pdfs/**": true,
    "planning_rules/**/raw/**": true,
    "pipeline_sandbox/**/samples/**": true,
    "pipeline_sandbox/**/corpus/**": true,
    "logs/**": true
  },
  "files.exclude": {
    "**/__pycache__": true,
    "**/.ruff_cache": true,
    "**/.pytest_cache": true
  }
}
```

Keep `data/_meta/**` searchable — the curated CSVs/fact cards are exactly what agents *should*
find. Per the docs this "improves search relevance, speeds up searches, and reduces tokens
consumed by search results" — it is the single cheapest win in this plan.

**1b. Single source of instructions.** Add `"chat.useClaudeMdFile": true` so VS Code agents
read the existing `CLAUDE.md` instead of duplicating it into `.github/copilot-instructions.md`.
Do NOT create a parallel copilot-instructions file — two sources will drift.

## Phase 2 — Register the MCP server in-repo

**2a. `.vscode/mcp.json`** (docs: mcp-configuration). Makes the query-not-scan toolset
available to VS Code agents, with dev-mode auto-restart while editing the server:

```jsonc
{
  "servers": {
    "dail-tracker": {
      "type": "stdio",
      "command": "${workspaceFolder}/.venv/Scripts/python.exe",
      "args": ["${workspaceFolder}/mcp_server/server.py"],
      "env": { "PYTHONUTF8": "1" },
      "dev": { "watch": "mcp_server/**/*.py", "debug": true }
    }
  }
}
```

**2b. `.mcp.json` (repo root, Claude Code project scope)** — same server, so the config is
versioned instead of living only in user-level config. Note `.vscode/` is tracked in this repo
while `.claude/` is gitignored; anything meant to survive the machine goes on the VS Code side.

**2c. Secrets pattern (future-proofing).** When the server ever needs credentials (R2, API
keys), use the `inputs` array with `promptString` + `password: true` and `${input:...}`
substitution, or `envFile` — never hardcoded env values. No secrets are needed today; this is
the documented pattern to follow, not work to do now.

## Phase 3 — Grow the MCP surface: resources, prompts, and a retrieval meta-tool

This is the biggest "powerful search context" item. Today only `data://coverage` exists as a
resource; everything else costs a tool round-trip.

**3a. Resources** (attachable as context via "MCP: Browse Resources" / `@` in chat, no
round-trip): add to `mcp_server/server.py`:
- `data://fact-cards` — full `data/_meta/fact_cards.json`
- `data://fact-card/{dataset}` — templated per-dataset card
- `data://join-map` — same payload as the `join_map()` tool
- `doc://index` — `doc/INDEX.md` verbatim
- `doc://sandbox-map` — `doc/SANDBOX_MAP.md` verbatim

**3b. `search_project(query)` meta-tool** — one repo-aware retrieval tool that ranks matches
across the *metadata* layer only: fact-card names/descriptions/columns, `doc/INDEX.md`
front-matter (title/domain/read_when), `sql_views/**/*.sql` header comments, and registered
view names from `utility/data_access/_sql_registry.py`. Returns top-N `{kind, name, path,
why-matched}` rows. This converts "where does X live?" from a tree-grep (hundreds of file
reads for any agent) into one cheap structured call. Keep it metadata-only — never scan
parquet or page source. Plain substring/token scoring is fine; no embeddings.

**3c. Prompts.** Extend the existing 7 with two task-shaped ones: `data-question` (walks the
describe→query→cite flow) and `scope-check` (coverage + never-sum guardrails before a money
question). VS Code invokes as `/dail-tracker.<prompt>`, Claude Code as `/mcp.dail-tracker.<prompt>`.

## Phase 4 — Hooks: make the data-discipline rules deterministic

VS Code reads hooks from `.claude/settings.json` too (docs: hooks), so hooks written once run
in **both** Claude Code and VS Code. Caveats that must shape the scripts: VS Code is Preview;
it **ignores matcher syntax** (every hook under an event fires — the script itself must filter
by tool name from stdin JSON); tool property naming differs (camelCase vs snake_case — handle
both); 30 s default timeout; must run on Windows (invoke via `python`, not bash-isms).

**4a. PreToolUse data-read guard** (`tools/hooks/guard_data_reads.py`): parse stdin JSON; if
the tool is a file-read and the path matches `*.parquet` / `data/bronze|silver|gold` /
`doc/source_pdfs` / sandbox corpora → exit 2 with a message pointing at
`describe_dataset`/`list_datasets`. Turns the CLAUDE.md "never Read data files" rule from
advisory prose into enforcement for every agent and subagent, in both tools.

**4b. SessionStart context injection** (`tools/hooks/session_context.py`): emit
`additionalContext` with git branch, `build_doc_index.py --check` staleness result, and a
one-line freshness summary from `data/_meta/heartbeats/`. Small (<15 lines), fresh, per-session.

**4c. PostToolUse firewall check**: after Edit/Write where the path is under
`utility/pages_code/`, run `tools/check_streamlit_logic_firewall.py <file>`; on violation emit
`systemMessage` (warn, don't block — the checker also runs in review).

Keep the existing SessionEnd pkill hook as-is (Claude Code only; VS Code has no SessionEnd
event, so it is inert there — harmless).

## Phase 5 — Path-scoped instructions + prompt files

**5a. Rules files.** Both tools read `.claude/rules/*.md` (Claude uses `paths:` front-matter;
VS Code custom-instructions doc lists `.claude/rules/` as a supported location). Create four
short files: `streamlit-pages` (paths `utility/pages_code/**` — firewall, contracts, no
business logic), `etl` (`extractors/**`, `pipeline*.py` — Polars-only, atomic zstd parquet,
row floor), `sql-views` (`sql_views/**` — dependency order, read views first), `data-access`
(`utility/data_access/**`). Each ≤20 lines, rule + one-line *why*. **Do not slim CLAUDE.md
yet** — prove the scoped files load in both tools first (defer-refactor rule), then move the
duplicated sections out of CLAUDE.md in a later pass.

**5b. Prompt files** `.github/prompts/*.prompt.md`: port the three `.claude/commands`
(`build-page`, `review-page`, `bold-redesign-page`) so VS Code gets the same slash commands —
front-matter `agent`, optional `model`, and `tools: ['dail-tracker/*']` where the task is
data-shaped. Add `data-question.prompt.md` using `${input:question}`.

## Phase 6 — Usage/model policy (no config, just practice)

Matches docs/optimize-usage; mostly already this repo's practice: keep thinking effort at
adaptive defaults (raise only for architecture); plan with a Plan agent then hand to a cheaper
implementer; new session per task, `/compact` within long ones; disable unused tools via
Configure Tools; monitor per-request cost via response hover + Agent Debug Logs. Leave
`chat.utilityModel`/`chat.utilitySmallModel` at defaults.

## Explicitly deferred

- **Agent plugin packaging** (bundle skills+agents+hooks+MCP behind a `plugin.json`): pays off
  only when sharing across machines/repos or publishing — revisit at BI-spinout time. If done,
  use Claude format (`.claude-plugin/plugin.json`) for the `${CLAUDE_PLUGIN_ROOT}` token, and
  note skills must be plain kebab-case (namespaced names fail silently).
- **MCP sandbox config** — macOS/Linux only; this box is Windows.
- **HTTP/remote MCP transport** — blocked on the commercial-uplift auth work by design.
- **MCP Apps / sampling / elicitation** — no use case yet.

## Order & effort

1. Phase 1 + 2 — pure config, ~1 h, immediate token savings.
2. Phase 3b (`search_project`) then 3a/3c — ~half day; the core retrieval win.
3. Phase 4 hooks — ~half day incl. cross-tool stdin quirks; test in both tools.
4. Phase 5 — incremental, one rules file at a time.

Verification: for exclusions, grep for a known parquet-only string in VS Code search (should
be absent) and check the chat Diagnostics view; for hooks, attempt a `Read` of a silver
parquet in each tool and confirm the block message; for MCP, `MCP: List Servers` +
`MCP: Browse Resources` show the new surface.
