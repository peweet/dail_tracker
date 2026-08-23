---
tier: REFERENCE
status: LIVE
domain: infra
updated: 2026-08-05
supersedes: []
read_when: deciding whether the current agent harness improves correctness or efficiency
key: REFERENCE|LIVE|infra
---

# Agent harness measurement — 2026-08-05

## Verdict

The public smoke suite does **not** demonstrate an accuracy improvement from enabling the
project harness. ON and OFFCLEAN had identical aggregate correctness: each produced 14 perfect
attempts and one partial attempt across 15 attempts. ON reduced tool calls and tokens, but took
65.3% longer overall because MCP-heavy tasks were substantially slower. Treat the current
harness as an enforcement and efficiency improvement for simple repository tasks, not as proven
product-quality uplift.

## Run contract

- Run ID: `4072b1f9-6d49-42b2-a4e0-aa5e8f1f19b1`
- Source revision: `87b766a99c141a2bdb5341d905d2ec42e2167ca6` (clean)
- Harness SHA-256: `ce3378c2604c2bf3eed9eae45a348461e9333e661bb708e42d0f5f7f8994db86`
- Task-suite SHA-256: `21fbe6977a69dfbfcf151826e326b46e6bf422c4d44cfa3471ea8b9245d9fcc3`
- Provider/model: Codex / `gpt-5.6-sol`, medium reasoning
- Design: five public tasks, three repeats, paired OFFCLEAN and ON arms
- Isolation: cleanroom without Git metadata, evaluator/scorer, or `planning/product`
- Provider errors: 0/30
- Billing: ChatGPT login; provider returned no dollar cost. Token usage consumed shared plan limits.

## Aggregate results

| Measure | OFFCLEAN | ON | ON delta |
|---|---:|---:|---:|
| Mean score | 0.978 | 0.978 | 0.000 |
| Perfect attempts | 14/15 | 14/15 | 0 |
| Total elapsed seconds | 517.477 | 855.356 | +65.3% |
| Tool calls | 60 | 43 | -28.3% |
| MCP calls | 0 | 10 | +10 |
| Raw input tokens | 1,980,172 | 1,536,547 | -22.4% |
| Uncached input tokens | 489,228 | 364,067 | -25.6% |
| Cache-read tokens | 1,490,944 | 1,172,480 | -21.4% |
| Output tokens | 8,825 | 7,020 | -20.5% |
| Reasoning-output tokens | 2,197 | 2,078 | -5.4% |

## Task-level results

| Task | OFFCLEAN score | ON score | OFFCLEAN mean seconds | ON mean seconds | ON latency delta |
|---|---:|---:|---:|---:|---:|
| never-sum | 1.000 | 1.000 | 30.9 | 9.5 | -69.2% |
| code-nav | 1.000 | 1.000 | 27.9 | 16.2 | -41.9% |
| conventions | 1.000 | 1.000 | 24.0 | 22.6 | -5.8% |
| data-shape | 0.889 | 0.889 | 51.2 | 75.4 | +47.2% |
| memory-xbrl | 1.000 | 1.000 | 38.4 | 161.3 | +320.1% |

Both partial `data-shape` attempts reported an award-supplier grain instead of the current
award/lot grain; the miss occurred once in each arm. The ON `memory-xbrl` attempts made nine MCP
calls and ranged from 70.6 to 259.6 seconds. The same task remained correct in both arms, so the
additional latency did not buy measured accuracy on this suite.

## Provider-free measurements

- Reusable prompts: 31; every prompt remained below the 600-word ceiling.
- Twelve revised shared prompts grew from 1,338 to 1,911 words: +573 words (+42.8%).
- Prompt gate: 508.8 ms mean over ten runs.
- Spawn guard: 137.1 ms mean over twenty runs.
- Session context: 873.1 ms mean over ten runs; 709/1,600 context characters used.
- Focused regression suite: 79 passed.
- Project retrieval: Recall@1 60%, Recall@5 90% over ten authored queries.
- Speech and question spot checks: correct member in top five for 3/4 queries in each suite.
- Precedent retrieval timed out at 90 seconds; the full direct suite timed out at five minutes.
- MCP project retrieval timed out at 90 seconds under the low-memory policy.
- Twenty-seven older customization edits remain in the existing unvalidated backlog.

## Decision

Keep the structural gates and role contracts: they are fast, deterministic controls. Do not claim
that the harness improves product correctness based on this run. Before widening use, reduce the
MCP latency on `memory-xbrl`, route dataset-shape questions to authoritative metadata, and run a
larger private holdout containing implementation tasks and critical provenance boundaries.
