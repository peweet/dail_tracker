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

Supported provider values are `openai`, `claude`, and `auto`. `auto` uses configured OpenAI first
and falls back to the local Claude CLI. Provider-specific defaults are `gpt-5.6-sol` for OpenAI and
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

## Coding-agent evaluations

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

- `dail-tracker` is enabled and excludes private Siting tools.
- `siting` installs both MCP and geospatial extras, filters to Siting tools, and is disabled by
  default so a clean public clone remains lean. Enable it only in a workspace that has the private
  overlay and external layer data.

The existing Claude configuration remains available for operators who use it. Historical Claude
benchmarks, provider-specific analytics, and the retained Claude backend are not migration targets.
