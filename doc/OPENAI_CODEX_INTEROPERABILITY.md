---
tier: REFERENCE
status: LIVE
domain: infra
updated: 2026-08-24
supersedes: []
read_when: auditing, reproducing, or adapting this repository's OpenAI and Codex interoperability for another project
key: REFERENCE|LIVE|infra
---

# OpenAI / Codex interoperability implementation and readiness guide

This is the implementation record and reproducibility checklist for making the two
applications in this repository usable with OpenAI models and Codex, while retaining
the existing Claude path where it remains useful. “Implemented” below means source
review and offline test coverage; it does not substitute for an organisation's account
approval, data-protection review, staging canary, or domain-specific human sign-off.
It is intentionally more detailed than [OPENAI_MODELS.md](OPENAI_MODELS.md): use that
document for day-to-day operation and this one when auditing the change or applying
the pattern to a different project.

## Scope and result

The repository contains two applications with deliberately different model boundaries:

| Application / surface | Result | Model boundary |
| --- | --- | --- |
| Dáil Tracker public application, API, and core | Model-neutral | Its deployed runtime does not require the OpenAI SDK or a model key. |
| Dáil Tracker MCP | Codex-compatible | Codex can use read-only local repository/data tools through project MCP configuration. |
| Siting deterministic engine and HTTP API | Model-neutral | The same engine inputs and layer snapshot produce the same assessment without model availability. |
| Siting natural-language intake and appraisal | OpenAI-compatible, Claude retained | A provider adapter may translate free text to a validated specification or explain a deterministic result. |
| Coding-agent evaluation runners | Codex-compatible, Claude retained | One provider-neutral adapter invokes Codex CLI or the existing Claude Agent SDK. |
| Durable engineering lessons | Codex-compatible, provider-neutral authority | Tracked guidance and scoped lesson cards are canonical; personal agent memories are optional recall only. |

The important architectural decision is that an LLM is an *edge service*, never the
source of Dáil data, planning evidence, legal conclusions, or application state.
The Siting engine still owns calculations, evidence selection, and structured report
content. The model can parse a user's description into an input schema and turn a
deterministic brief into plain language, but it cannot make the deterministic API
non-deterministic or silently change its evidence base.

## What was changed and where

### 1. Siting gained an optional OpenAI Responses API provider

The provider boundary is in
[`planning/product/core/assistant.py`](../planning/product/core/assistant.py).
It now supports `SITING_LLM_PROVIDER=auto|openai|claude`:

- `openai` requires a configured OpenAI client and API key.
- `claude` retains the pre-existing local Claude CLI integration.
- When selecting a provider, `auto` chooses a configured OpenAI client and otherwise
  chooses a locally available Claude CLI. It does **not** retry a failed OpenAI request
  through Claude; cross-provider retry is a separate, explicit product policy.

OpenAI has a deliberate default model (`gpt-5.6-sol`) and optional overrides:

```powershell
$env:OPENAI_API_KEY = "set-this-in-the-user-or-deployment-secret-store"
$env:SITING_LLM_PROVIDER = "openai"
$env:SITING_OPENAI_MODEL = "gpt-5.6-sol"
$env:SITING_OPENAI_REASONING_EFFORT = "medium"
```

`SITING_LLM_MODEL` and `SITING_LLM_REASONING_EFFORT` are provider-neutral aliases.
An explicit Python function or report-CLI argument has higher precedence than the
environment. Valid OpenAI reasoning values are `none`, `low`, `medium`, `high`,
`xhigh`, and `max`.

The adapter uses the Responses API with these safeguards:

- It lazy-loads `openai`, so importing or running the deterministic Siting engine
  never requires the optional SDK.
- It sends `instructions` separately from user/evidence input, uses `store=False`,
  applies an explicit timeout, and can send a SHA-256-derived `safety_identifier`
  rather than an operator's raw identifier.
- The configured timeouts are 120 seconds for description parsing, 240 seconds for
  lay narration, and 1,200 seconds for the longer consultant appraisal. A work project
  should set and test values appropriate to its own service-level objectives.
- Free-text intake and consultant-appraisal structures use strict JSON Schema
  outputs. Object schemas require their declared fields and reject undeclared fields.
- Refusals, incomplete/failed/cancelled calls, empty output, and malformed JSON all
  fail closed as `AssistantError` rather than leaking partially parsed values into the
  engine.
- SDK exception text is not re-emitted because transport errors can echo request
  content. The module does not log prompts.
- The resolved provider, model, reasoning effort, prompt version, and hashed safety
  identifier are part of the runtime cache key. A configuration change therefore cannot
  silently reuse a response created under a different model policy.
- System instructions label the project description and case evidence as untrusted
  data, not instructions. Generated prose remains non-citable; the deterministic
  report remains the evidence artefact.

The supporting integration surfaces were made provider-neutral in
[`planning/product/core/narrative.py`](../planning/product/core/narrative.py),
[`planning/product/ui/data_access/siting_assistant.py`](../planning/product/ui/data_access/siting_assistant.py),
[`planning/product/ui/pages/siting_assistant.py`](../planning/product/ui/pages/siting_assistant.py),
and [`planning/product/tools/siting_report.py`](../planning/product/tools/siting_report.py).
The report tool exposes `--provider`, `--model`, and `--reasoning-effort`, so the
same controlled choice is available outside the UI.

The UI labels the selected backend, shows the parsed assumptions as values a user can
inspect/override before assessment, and can still show the deterministic result when
the language tier is unavailable. Consultant prose is also checked against the
deterministic verdict rules, with one corrective pass if it makes prohibited decision
predictions or graded-odds claims.

The private Siting HTTP API was deliberately **not** changed into a model-backed
endpoint. It remains the deterministic product contract. This avoids a paid,
non-repeatable model call being hidden behind an ordinary assessment request.

### 2. The OpenAI dependency is isolated to the Siting AI extra

[`pyproject.toml`](../pyproject.toml) defines:

```toml
[project.optional-dependencies]
siting-ai = ["openai>=2.53,<3", "tiktoken>=0.12,<1"]
```

The OpenAI client is therefore not a dependency of the public Dáil Tracker runtime.
For a complete private Siting development environment, use:

```powershell
py -3.12 tools/dev_env.py sync siting-ai
```

Keep `OPENAI_API_KEY` in the user's environment or the deployment secret store; do
not put it in `.env` files that might be committed, test fixtures, source code, or
the Codex project configuration.

### 3. Coding-agent evaluations can run through Codex

[`tools/evals/provider_adapter.py`](../tools/evals/provider_adapter.py) is the shared
adapter used by the historical benchmark/smoke runners:

- `sdk_smoke.py`
- `routing_probe.py`
- `package_bench.py`
- `harness_bench.py`
- `build_bench.py`
- `cost_of_change_bench.py`
- `claude_md_prune_bench.py`

Provider selection is controlled without changing the runner call sites:

```powershell
$env:DAIL_EVAL_PROVIDER = "codex"       # codex | claude | auto
$env:DAIL_EVAL_MODEL = "gpt-5.6-sol"
$env:DAIL_EVAL_REASONING_EFFORT = "medium"
$env:DAIL_EVAL_TIMEOUT_SECONDS = "900"
```

`auto` chooses an authenticated `codex` executable on `PATH` first, then the
existing `claude_agent_sdk`. The Codex route runs `codex exec --json` as a shell-free
subprocess. It is workspace-scoped, ephemeral, ignores user behaviour configuration,
uses the requested sandbox, sets approval to `never`, disables web search, passes only
the MCP configuration selected for that benchmark, and reads the prompt from standard
input. Its JSONL events are normalised into the pre-existing benchmark result shape.

This is intentionally a compatibility layer, not a claim that the two agent products
have identical controls. Codex CLI cannot reproduce Claude's generic `allowed_tools`
or `max_turns` limit exactly. The adapter records that limitation and compensates with
the sandbox, wall-clock timeout, disabled web, explicit MCP filtering, and ephemeral
session. Codex reports token usage but not the Claude-style `cost_usd`; comparison code
must tolerate a missing cost field.

No Python OpenAI SDK is needed for the Codex evaluation backend. It uses the operator's
Codex authentication. Running an actual benchmark can use model quota, so it remains an
explicit operator action; the unit tests use faked subprocess/SDK output.

### 4. Codex receives project instructions and narrowly scoped MCP tools

[`AGENTS.md`](../AGENTS.md) is the durable, provider-neutral entry point for project
rules. The repository's [`.codex/config.toml`](../.codex/config.toml) adds these
compatibility settings:

- `project_doc_fallback_filenames = ["CLAUDE.md"]` allows legacy nested Claude
  guidance to act only as a migration fallback. Native `AGENTS.md` takes precedence.
- The public `dail-tracker` MCP server is enabled and its private Siting tools are
  disabled.
- The private `siting` MCP server has the Siting dependencies and an allow-list of its
  own tools, but is disabled by default. A clean public clone will neither install
  private geospatial dependencies nor surface private tools.
- The environment supplied to each MCP server sets UTF-8 and native-library thread
  caps, keeping a normal Codex session from changing the data-service constraints.
- [`mcp_server/resource_policy.py`](../mcp_server/resource_policy.py) treats both
  `codex.exe` and `claude.exe` as agent sessions when applying its memory-pressure
  policy, rather than assuming that only the original agent is active.

Enable the Siting MCP server only from a private configuration/profile that has the
private overlay and external layer data. Do not simply flip its `enabled` setting in a
public clone and assume it is a usable or safe deployment.

### 5. Durable lessons are accessible without tying truth to a model vendor

The durable-knowledge design uses files the project can review and version:

1. Root/nested `AGENTS.md` files hold rules that must apply on every run.
2. [`tools/discoveries.jsonl`](../tools/discoveries.jsonl) holds compact,
   trigger-keyed lessons.
3. Public supporting evidence lives in [`memory/`](../memory/), documented by
   [`memory/README.md`](../memory/README.md).
4. Private Siting decisions live in the private overlay's
   `planning/product/claude/memory/` hierarchy. The `claude` directory name is a
   legacy location, not a requirement to use Claude.

`tools/discoveries.py` can resolve the public and private repository lesson roots and
uses a workstation-local Claude memory directory only as a compatibility fallback.
The fallback is not a source of shared project truth. Its contents may be stale and
are not portable to a clean clone.

For a work-project migration, treat any discovery that only resolves through a
workstation memory fallback as an open migration item. Curate the required detail into
an explicitly public or private repository card after sensitivity/currency review. A
stronger long-term format records both an unambiguous repository-relative `detail_path`
and a `scope` (`public` or `private`) in each discovery record, then verifies those
paths in CI. Do not bulk-copy a personal assistant-memory directory merely to make a
lookup appear to work.

The Codex `UserPromptSubmit` hook calls
[`tools/hooks/discovery_hint.py`](../tools/hooks/discovery_hint.py). It selects at
most two short matching discovery lines and has a 1,200-token additional-context cap;
it does not inject full memory cards or absolute workstation paths. The `Stop` hook
calls [`tools/hooks/closeout_gate.py`](../tools/hooks/closeout_gate.py). After 20
assistant turns, a missing closeout produces one blocking prompt per session; a valid
record uses `promoted`, `already-captured`, or `no-durable-delta` with a meaningful
note. Review and trust those two project hooks once in Codex with `/hooks`.

Codex personal Memories are enabled for recall but automatic generation is disabled:

```toml
[memories]
generate_memories = false
use_memories = true
disable_on_external_context = true
```

This makes the privacy choice explicit. `disable_on_external_context` prevents
automatic extraction after MCP/web/tool-search context, but it does not make every
ordinary local-file or shell interaction safe to retain. Use `/memories` deliberately.
Imported or generated local Codex memory is supplemental personal state, never the
only copy of a repository rule, decision, or verification command.

The public MCP search boundary was tightened in
[`mcp_server/fts_index.py`](../mcp_server/fts_index.py) and
[`mcp_server/server.py`](../mcp_server/server.py):

- `search_project(query, kind="memory")` searches only checked-in public cards under
  `memory/`.
- Workstation-local assistant memory needs the explicit
  `kind="external_memory"` opt-in and returns bounded snippets under a
  `memory://external/` namespace, rather than paths that appear to be repository
  files.
- The Siting Responses API receives no Codex/Claude engineering memory. It receives
  only trusted policy, the current request, deterministic engine output, and any
  explicitly supplied case evidence. A future product-memory feature must have its
  own consent, retrieval, provenance, and retention design.

### 6. Claude remains a supported compatibility path, not the authority

The retained Claude CLI/SDK paths prevent a disruptive migration, and historical
Claude benchmarks remain meaningful. They are not prerequisites for Dáil Tracker,
Siting's deterministic engine, or Codex. Claude-specific lifecycle hooks, transcript
formats, and local memory files were not treated as a universal interchange format;
they are either optional migration inputs or are replaced by provider-neutral,
tracked guidance.

## Reproducibility and audit checklist

Use this table to assess another project. A checked box should mean the cited evidence
exists in that project's repository, not merely that a developer remembers enabling it.

| Area | What a complete setup has | How to check it |
| --- | --- | --- |
| Product boundary | A written statement saying which requests are deterministic and exactly where an LLM is allowed. | Trace the public API and ensure an API request cannot make an implicit model call. |
| OpenAI transport | Optional dependency, lazy client import, explicit provider selection, timeouts, and errors that fail closed. | Temporarily omit the SDK/key and run deterministic tests; the deterministic application must still import and work. |
| Structured task | Strict schema or equivalent validation between the model and business logic. | Unit-test refusal, incomplete response, unknown fields, missing fields, and malformed output. |
| Privacy | No key in source; prompt/data logging is intentional and scrubbed; storage, retention, and end-user identifiers are reviewed. | Search for `OPENAI_API_KEY`, request logging, raw user identifiers, and model-response persistence. |
| Prompt injection | System policy is separate from user/evidence data; untrusted corpus cannot act as instructions. | Add adversarial text to a fixture and prove output stays within the structured/business contract. |
| Model configuration | A documented default model plus env/CLI override and validated reasoning level. | Print effective runtime configuration without printing keys or request content. |
| Agent evaluations | One provider-neutral contract, explicit Codex sandbox/timeout/MCP policy, and offline tests. | Test provider selection, command construction, partial JSONL, timeout, and fallback paths without a model call. |
| Codex instructions | Tracked `AGENTS.md` at root and relevant nested product roots; legacy files are only migration bridges. | Start Codex at each relevant working directory and inspect its discovered project instructions. |
| MCP privacy | Public and private tools are distinct; private server/tool config is disabled or absent by default. | Start a clean public clone and verify that private tools cannot be listed or invoked. |
| Durable knowledge | Repository-reviewed public/private lesson stores, an explicit scope, and a bounded retrieval surface. | Check every discovery record points to an approved in-repo card or is clearly marked as temporary migration state. |
| Personal memories | Disabled by default where privacy requires it, or enabled only with a documented user decision. | Review client memory configuration and prove no build/runtime feature relies on `~/.codex`. |
| Verification | Automated tests cover the safety and provider-boundary behaviour; documentation index/checks are current. | Run the commands in the next section from a clean environment. |

## Work-project security and operational gate

Complete this review before exposing any model-assisted feature outside a developer
machine. It is intentionally separate from “the code imports and the unit tests pass.”

- [ ] The data owner has approved every field that can leave the environment: user text,
  uploaded files, retrieved records, deterministic report content, identifiers, and
  metadata.
- [ ] Prompts are minimised and redact secrets, privileged material, unnecessary
  personal data, and irrelevant internal evidence before transmission.
- [ ] The work product supplies redaction, consent, tenant isolation, and input-size
  controls appropriate to its data classification. This repository's adapter protects
  transport/shape boundaries, but it is not itself a complete redaction or consent
  workflow.
- [ ] `store=False` is configured where this product needs it, **and** the organisation
  has separately reviewed its OpenAI account/data-processing terms, residency,
  retention, logging, incident, and deletion requirements. `store=False` alone is not
  a complete privacy programme.
- [ ] Application logs, tracing, browser telemetry, queues, caches, and error reporting
  do not retain raw prompt/response bodies unless that retention is explicitly approved.
  In this Siting UI, Streamlit caches parsed descriptions and generated prose for up to
  one hour; a work deployment must assess that cache as part of its data-flow review.
- [ ] If `SITING_OPENAI_SAFETY_IDENTIFIER` is used, it is an opaque stable value. The
  adapter SHA-256 hashes it before sending, but a predictable employee/customer number
  may still be guessable; use a server-generated opaque ID or keyed HMAC if policy
  requires stronger pseudonymisation. It is a process environment setting in this
  implementation, not a request-scoped authenticated end-user identity.
- [ ] A model-facing HTTP endpoint, if one is added later, has authentication,
  authorisation, tenant isolation, rate/spend limits, abuse controls, and auditable
  operational metadata. This repository deliberately does not publish such an endpoint.
- [ ] Production selects the intended provider explicitly (for example, `openai`) rather
  than relying on `auto`, unless a cross-provider routing policy has been reviewed and
  tested.
- [ ] Prompt-injection regression tests contain hostile user text and hostile retrieved
  documents. Structured output validates shape, not factual correctness; domain rules
  still validate the result deterministically.
- [ ] A provider outage, quota limit, refusal, timeout, or bad response degrades to
  deterministic functionality or a clear retry state. It must never silently fabricate
  an assessment or decision.
- [ ] Human review remains required for consequential legal, financial, medical, safety,
  eligibility, or similarly high-impact decisions.

## Verification evidence and repeatable commands

The compatibility work has offline coverage for the model boundary, provider adapter,
lesson retrieval, hooks, and scoped MCP indexing. These commands are safe to run
without a paid model call:

```powershell
# Siting OpenAI/Claude boundary, strict output parsing, and narrative integration
.\.venv\Scripts\python.exe -m pytest planning\product\test\test_assistant.py planning\product\test\test_narrative.py -q

# Codex/Claude evaluation adapter and result comparison
.\.venv\Scripts\python.exe -m pytest test\tools\evals\test_provider_adapter.py test\tools\test_bench_compare.py -q

# Durable lessons, compatible hooks, and memory/index privacy boundary
.\.venv\Scripts\python.exe -m pytest test\tools\test_discoveries.py test\tools\test_efficiency_hooks.py test\tools\test_closeout_gate.py test\tools\test_session_closeout.py test\mcp_server\test_fts_index.py test\mcp_server\test_code_index.py -q

# Documentation integrity and normal changed-file-aware verification
.\.venv\Scripts\python.exe tools\build_doc_index.py --check
uv run --locked --group dev --extra pipeline --extra api --extra mcp python tools/dev.py verify
```

Use `planning/product/tools/siting_report.py --help` to confirm that `--provider`,
`--model`, and `--reasoning-effort` are available. Do not treat a successful test run
as proof that a real account is authorised to make calls. A live integration smoke test
requires an authorised key, a deliberately selected low-risk test input, and a clear
decision about whether that request is allowed to leave the environment.

## Clean-work-project setup sequence

For a work project adopting this pattern, do the following in order:

1. Draw the deterministic/model boundary first. Decide which user flows can tolerate
   non-deterministic language assistance and which must remain evidence-backed,
   repeatable, or auditable.
2. Create a small provider interface at that edge. Keep business schemas and
   validation independent of provider SDK types. Make a provider failure visible to
   the caller rather than silently inventing a fallback result.
3. Add the OpenAI dependency as an optional, narrowly scoped extra. Load it only in
   the adapter and store its key in the deployment/user secret mechanism.
4. Use the Responses API with separate trusted instructions, a bounded input, timeout,
   `store=False` when appropriate for the product policy, strict structured output
   where business logic consumes a result, and privacy-safe identifiers where needed.
5. Retain an existing provider only behind the same interface. Do not scatter
   `if provider == ...` branches through UI, business logic, and API code.
6. Write `AGENTS.md` files for shared rules, then add a small tracked lesson store.
   Split public and private knowledge physically and make each retrieval surface search
   only its intended scope. Do not make personal agent memory the canonical knowledge
   base.
7. Add Codex configuration deliberately: explicit MCP tool scope, approval/sandbox
   policy for automated evals, and hooks only after reviewing their payload/privacy
   effects. Use `/hooks` to approve project hooks; use `/memories` only after deciding
   whether a chat may retain information.
8. Add offline tests before running a live model. Test the unhappy paths as carefully
   as the happy structured-output example.
9. Run a separately authorised live smoke test only after the above is complete.
   Record the model, date, input classification, retention choice, expected schema, and
   observed cost/usage without recording sensitive request content.

## Known limitations and hand-off conditions

This repository is OpenAI/Codex-compatible, but a few conditions should be checked
before calling any clone or work-project adaptation “fully set up”:

- A real Siting language request requires both an installed `siting-ai` extra and an
  authorised `OPENAI_API_KEY`; the source tree intentionally does not make a paid
  call during installation or test.
- A Codex evaluation requires an authenticated `codex` executable on `PATH`; it does
  not use the OpenAI Python SDK or read `OPENAI_API_KEY`.
- The private Siting overlay, its external data, and its nested `AGENTS.md` must be
  present and tracked in the private repository. A public checkout should not claim
  to exercise private Siting MCP or private lesson retrieval.
- The locally present `planning/product/AGENTS.md` is excluded from the public worktree;
  ensure it is intentionally tracked in the private overlay before treating it as shared
  guidance for a clean private clone.
- The private overlay's standalone bootstrap story is not established: its OpenAI
  dependency and lock configuration live in the public monorepo. A work project with
  separately cloned private product code must provide an equivalent dependency/lock
  contract rather than assuming the extra exists there.
- Legacy workstation Claude memory is a migration fallback only. Before cloning this
  pattern to work, migrate the small set of needed lessons into explicit public/private
  repository cards, give each an unambiguous repository-relative location, and review
  them for sensitivity and currency. Do not bulk-import a personal memory directory.
- A clean-clone readiness review must confirm that the relevant lesson cards,
  `memory/README.md`, and private `AGENTS.md` are actually versioned in their intended
  repository. An ignored or local-only card is useful personal context, not durable
  team knowledge.
- At this implementation point, the discovery index is still partly a migration bridge:
  32 indexed rows resolve as one public card, three private Siting cards, and 28 legacy
  workstation-only cards. The public resolving card is itself untracked, and not every
  public `memory/` card is tracked. Migrate the needed cards before using this as a
  clean-clone knowledge base.
- Current Codex clients may expose Claude-memory import through desktop Settings or
  `/import`, but that client feature is not a repository dependency. Imported state is
  not synchronization and must not replace the tracked store.
- Codex and Claude have different execution controls. Treat benchmark comparisons as
  comparable outcomes under documented constraints, not proof of identical tool or
  turn-level semantics.
- Offline tests prove request construction and guard behaviour, not that a paid account
  has access to the intended model, correct billing, acceptable latency, or safe output
  quality. The current implementation has no paid live integration test and no full
  prompt-injection red-team suite; run an approved synthetic staging canary and add
  adversarial tests before declaring a production deployment complete.

## Official references

- [OpenAI latest-model guidance](https://developers.openai.com/api/docs/guides/latest-model)
- [OpenAI Structured Outputs guide](https://developers.openai.com/api/docs/guides/structured-outputs)
- [Codex `AGENTS.md` guidance](https://learn.chatgpt.com/docs/agent-configuration/agents-md)
- [Codex Memories guidance](https://learn.chatgpt.com/docs/customization/memories)
- [Codex hooks guidance](https://learn.chatgpt.com/docs/hooks)
- [Claude Code import guidance for Codex](https://learn.chatgpt.com/docs/import)
