---
tier: SPEC
status: DRAFT
domain: infra
updated: 2026-07-16
supersedes: []
read_when: running the agent-efficiency research piece, or revisiting how AI agents are configured to master this project
key: SPEC|DRAFT|infra
---

# Research prompt — enabling AI agents to efficiently & cost-effectively master this project

**Origin:** expands the 2026-07-16 session prompt ("assess this project on how well we are loading context…") and the 2026-07-14 follow-up. Companion to `doc/AI_CONTEXT_OPTIMISATION_PLAN.md` (the internal audit — this piece is the *external* research counterpart).

**How to run:** paste the prompt below into a fresh session (or `/deep-research`) — it is self-contained.

---

## The prompt

Research and produce a cited report on best practices for setting up a large, long-lived software/data project so that AI coding agents can **master it efficiently, behave deterministically, and add value at minimum token cost**. Ground every recommendation in published sources; then map each finding onto our project's current setup and say concretely what we should change, keep, or drop.

### Context: what we already do (assess against best practice, don't re-derive)

We run a "second brain" approach for an Irish parliamentary-data project (Python/Polars ETL → parquet → DuckDB SQL views → Streamlit UI):

1. **Persistent file-based memory** — one fact per markdown file with frontmatter (`type: user|feedback|project|reference`), wiki-style `[[links]]`, and a single `MEMORY.md` index (~5k tokens) loaded every session. ~200 memory files.
2. **CLAUDE.md** (~1.2k tokens) front-loading conventions: never Read data files, query via MCP tools instead, delegate broad search to subagents, scope every grep.
3. **YAML page contracts** (`utility/page_contracts/*.yaml`) — the source of truth for what each UI page may query, render, and export; a "logic firewall" checker enforces that pages contain no business logic.
4. **An MCP server** exposing ~60 small aggregated query tools so agents never load raw parquet.
5. **Doc frontmatter** (tier/status/domain/read_when) being piloted for retrieval routing.
6. **Model routing idea (untested):** use the strongest reasoning model (e.g. Claude Fable/Opus-tier) to plan and iterate on designs, and a cheaper/faster model to execute the mechanical coding — is there published evidence on planner/executor model splits?

### Research questions

**A. Knowledge architecture ("atomic facts" / second brain)**
1. What does published work say about *atomic* knowledge representation for LLM consumption — one-fact-per-file notes, knowledge graphs, triple stores, GraphRAG — versus long prose docs? Where is the crossover point where an index + retrieval beats always-loaded context?
2. "No assertions without facts": what grounding/attribution techniques (citation-forcing, retrieval-augmented verification, claim–evidence linking, NLI-based fact checking) measurably reduce hallucinated assertions in agent outputs? What do Anthropic, OpenAI, and Mistral each recommend?
3. Should memory be *smaller* (tighter index, load-on-demand) or *richer* (more context up front for better answers)? Find empirical results on context-length vs. answer-quality trade-offs ("lost in the middle", context-rot studies, long-context benchmarks) and on when preloading beats retrieval.

**B. Deterministic agent behaviour via contracts**
4. What evidence exists that *machine-readable contracts/schemas* (YAML/JSON specs, tool schemas, structured output constraints, grammar-constrained decoding) make agent behaviour more deterministic and reviewable than natural-language instructions? Cover OpenAI structured outputs / function calling, Anthropic tool-use and skills, Mistral's function-calling guidance, and any academic evaluations.
5. How far should the contract pattern extend beyond UI pages — data contracts for ETL outputs, SQL view contracts, test contracts? What do data-engineering practices (dbt contracts, Great Expectations, OpenAPI-first) suggest about maintenance cost vs. drift prevention?

**C. Token economics & context loading**
6. Best practices from the three vendors for cost control in agentic workloads: prompt caching, context editing/compaction, KV-cache-friendly prompt structure, batch APIs, model cascades/routing (strong-model plans, cheap-model executes). What measured savings do published case studies report?
7. Corpus classification to stop token waste: techniques for labelling/tiering a large heterogeneous text corpus (docs, scraped HTML, OCR text, PDFs) so agents retrieve the right slice — embedding-based routing, metadata frontmatter, doc-tier taxonomies, deny-lists for context-detonating files. What do RAG and agentic-retrieval papers say about classification granularity?
8. When is it cheaper to *summarise once and cache* (e.g. module READMEs, generated codemaps) vs. *retrieve on demand*? Any published numbers on repo-map/codebase-index approaches (Aider repo-map, Sourcegraph/Cody context, GitHub Copilot workspace indexing)?

**D. Agent project-mastery patterns**
9. What do Anthropic (Claude Code best practices, CLAUDE.md guidance, subagents, skills), OpenAI (Codex/agents guidance, AGENTS.md convention, swarm/agents SDK practices), and Mistral (agents API, La Plateforme guidance) each publish about *onboarding an agent to an existing codebase*? Extract the overlapping consensus and the genuine disagreements.
10. Multi-agent orchestration economics: when do planner/executor or reviewer/worker splits pay for themselves vs. burn tokens? Look for real measurements (Anthropic multi-agent research posts, academic multi-agent-system evaluations, practitioner postmortems), including failure modes (runaway loops, redundant re-reading).
11. What guardrail patterns (permission deny-lists, sandboxing, budget caps, canary checks) do practitioners use to prevent single-incident token blowouts? (We had one ~2.7M-token runaway; the audit found guardrails matter more than trimming always-on context.)

### Source requirements

- **Vendor primary sources:** Anthropic docs/engineering blog (Claude Code best practices, context management, prompt caching, multi-agent posts); OpenAI docs/cookbook (agents guide, structured outputs, cost optimisation); Mistral docs/blog (agents, function calling, fine-tuning-vs-prompting guidance).
- **Scholarly:** arXiv/ACL/NeurIPS papers on long-context degradation, RAG vs. long-context, agent memory architectures (e.g. MemGPT/Letta lineage), grounded generation and hallucination reduction, multi-agent cost/benefit evaluations.
- **Practitioner:** engineering blogs and postmortems from teams running coding agents at scale (Aider, Cursor, Sourcegraph, Devin/Cognition, Factory, etc.), plus credible independent writeups. Prefer pieces with measurements over opinion pieces.
- Cite every claim; distinguish vendor marketing from measured results; note publication dates (this field moves fast — weight 2025–2026 sources).

### Deliverable

A report with: (1) an executive summary of the 5–10 highest-leverage changes for *this* project ranked by expected token/quality ROI; (2) per-question findings with citations; (3) a consensus table — where OpenAI, Anthropic, and Mistral guidance agrees and disagrees; (4) a "do not do" list of practices the evidence says are wasted effort (with our two already-falsified hypotheses as priors: splitting MEMORY.md's link-shelves into one-line hooks *adds* tokens; the always-on 6.1k budget is not the problem); (5) concrete next actions mapped to `doc/AI_CONTEXT_OPTIMISATION_PLAN.md` so the two documents stay one plan.
