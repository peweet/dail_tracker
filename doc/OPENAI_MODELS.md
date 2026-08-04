---
tier: REFERENCE
status: LIVE
domain: infra
updated: 2026-08-04
supersedes: []
read_when: configuring OpenAI or Codex for Dail Tracker, Siting, or the coding-agent evaluation harness
key: REFERENCE|LIVE|infra
---

# OpenAI and Codex operation

The repository contains two applications with different model boundaries.

| Surface | OpenAI requirement | Contract |
| --- | --- | --- |
| Dáil Tracker Streamlit/API/core | None | Deterministic and model-neutral; do not add an SDK to the core runtime. |
| Dáil Tracker MCP | Codex-compatible MCP client | Read-only local tools registered by `.codex/config.toml`. |
| Siting engine and deterministic API | None | Same inputs and layer snapshot produce the same assessment regardless of model availability. |
| Siting free-text intake and appraisal | Optional OpenAI SDK | OpenAI Responses API or the retained local Claude CLI, only at the edges of the deterministic engine. |
| Coding-agent evals | Codex CLI or Claude Agent SDK | Provider selected at runtime; offline unit tests do not invoke either model. |

## Siting language tier

Install the private engine and optional OpenAI transport without adding either to Dáil Tracker's
deployed runtime:

```powershell
uv sync --frozen --extra siting --extra siting-ai --group dev
```

Set the standard OpenAI API key outside source control, then choose the provider:

```powershell
$env:OPENAI_API_KEY = "..."
$env:SITING_LLM_PROVIDER = "openai"
```

Supported provider values are `openai`, `claude`, and `auto`. When resolving a provider, `auto` selects
configured OpenAI first and selects the local Claude CLI only when OpenAI is not configured; it does not
retry a failed OpenAI request through Claude. Provider-specific defaults are `gpt-5.6-sol` for OpenAI and
`sonnet` for Claude. Override them only when evaluating a deliberate model change:

```powershell
$env:SITING_OPENAI_MODEL = "gpt-5.6-sol"
$env:SITING_OPENAI_REASONING_EFFORT = "medium"
```

`SITING_LLM_MODEL` and `SITING_LLM_REASONING_EFFORT` are provider-neutral aliases. A direct function
or CLI argument takes precedence over environment configuration. If an application serves distinct
end users, set `SITING_OPENAI_SAFETY_IDENTIFIER` to a stable internal identifier; the adapter hashes
it before sending it to OpenAI.

The OpenAI path uses the Responses API, sends `store=False`, sets reasoning effort explicitly, and
uses strict JSON Schema output for intake and consultant-report structures. Every object schema
rejects additional properties and requires all declared fields. Refusals, incomplete responses,
missing content, timeouts, and malformed output fail as `AssistantError`; SDK exceptions are not
echoed because they may contain request data.

The model receives a project description and deterministic engine output. A consultant appraisal
can additionally receive verbatim authority case evidence. Treat those inputs as confidential and
untrusted: they cannot override system instructions, and model prose remains non-citable. The
deterministic report is the evidence artefact. The private HTTP API intentionally remains the
deterministic tier; enabling the SDK does not create a model-backed network endpoint.

Current API choices follow the official [GPT-5.6 model guidance](https://developers.openai.com/api/docs/guides/latest-model)
and [Structured Outputs guide](https://developers.openai.com/api/docs/guides/structured-outputs).

## Durable lessons and memory

Codex has two complementary persistence layers. Required shared guidance belongs in tracked
`AGENTS.md` files; local Memories are a personal recall aid and must not be the only copy of a
repository invariant. The project configuration enables memory use but leaves automatic generation
off. This avoids silently retaining sensitive content read from ordinary local files or shell output;
`memories.disable_on_external_context = true` adds a second guard for chats that used MCP, web, or
tool-search context. Use `/memories` to make a deliberate per-chat choice, or opt into generation in
personal configuration after reviewing the privacy boundary.

The provider-neutral shared lesson path is:

1. `tools/discoveries.jsonl` holds short, trigger-keyed lessons.
2. `memory/<slug>.md` holds public supporting detail.
3. `planning/product/claude/memory/INDEX.md` and its linked notes hold private Siting decisions;
   `claude/` is a legacy path name, not a provider requirement.
4. The configured `UserPromptSubmit` hook checks the compact discovery index and injects at most
   two matching one-liners. A compatible `Stop` hook asks once for a durable-learning assessment
   after a substantive session. On first use, review and trust both project hooks with `/hooks`.

The MCP namespace keeps the privacy boundary explicit: `search_project(query, kind="memory")`
searches checked-in public `memory/` cards only. The legacy workstation store is excluded unless an
operator deliberately requests `kind="external_memory"`; its snippets use a non-repository
`memory://external/` path and must not be treated as shared or current project truth.

Where the installed client exposes import, bring existing workstation-local Claude Code history
into Codex with desktop **Settings > Import**, or run `/import` in an interactive CLI session,
choose **Claude Code**, and select the project memories and recent chats. The currently installed
CLI reports `external_agent_memory_import` as an under-development feature and disabled, so do not
make that path a repository dependency. Use `/memories` to review whether a chat may use existing
local memories or contribute to future ones. Imported and generated Codex state lives under
`~/.codex/memories/`; treat it as generated personal state, not as a file-editing API or a
repository knowledge base. ChatGPT web memory is separate from the local Codex store.

The Siting Responses API adapter does **not** receive Codex or Claude development memory. It sees
only its system policy, the current user input, deterministic engine output, and explicitly supplied
case evidence. Adding repository memory to that runtime would create an unreviewed factual and
prompt-injection channel, so any future runtime knowledge feature must use an explicit, scoped,
provenance-bearing retrieval contract rather than implicit agent memory.

See the official [AGENTS.md guidance](https://learn.chatgpt.com/docs/agent-configuration/agents-md),
[Codex Memories guidance](https://learn.chatgpt.com/docs/customization/memories), and
[Claude Code import guidance](https://learn.chatgpt.com/docs/import).

## Coding-agent evaluations

Prepare the shared MCP environment first. Add the legacy `evals` dependency group only when using
the Claude backend:

```powershell
uv sync --frozen --extra mcp --group dev
# Claude backend as well:
uv sync --frozen --extra mcp --group dev --group evals
```

The runners under `tools/evals/` accept:

```powershell
$env:DAIL_EVAL_PROVIDER = "codex"       # codex | claude | auto
$env:DAIL_EVAL_MODEL = "gpt-5.6-sol"
$env:DAIL_EVAL_REASONING_EFFORT = "medium"
```

`auto` prefers the installed Codex CLI and otherwise uses `claude-agent-sdk`. The Codex backend runs
`codex exec --json` without a shell, scopes it to the requested worktree, parses its JSONL event
stream, and normalises final text, tool calls, token usage, timeouts, and errors into the existing
benchmark result shape. It uses the user's Codex authentication; it does not read `OPENAI_API_KEY`.

Unit tests inject fake subprocess and SDK results. Running a benchmark or smoke script itself can
consume model quota and must be an explicit operator action.

## Codex project setup

Codex discovers the tracked root and nested `AGENTS.md` files automatically. Project MCP settings
live in `.codex/config.toml`, not `.mcp.json`:

- `CLAUDE.md` is configured as a fallback filename only when a directory has no `AGENTS.md`, so
  tracked Siting guidance remains available during migration without overriding native Codex files.
- `dail-tracker` is enabled and excludes private Siting tools.
- `siting` installs both MCP and geospatial extras, filters to Siting tools, and is disabled by
  default so a clean public clone remains lean. Enable it only in a workspace that has the private
  overlay and external layer data.

The existing Claude configuration remains available for operators who use it. Historical Claude
benchmarks, provider-specific analytics, and the retained Claude backend are not migration targets.
