# Recall validity before memory expansion

Date: 2026-10-18. Scope: public coding-agent harness discovery CLI and prompt hints.
Trigger: stale memory, corrected discovery, superseded evidence, wrong-scope hint,
learned memory, neural archive compression, L3TC or NNCP.

## Problem and decision

The prior hint selector ranked trigger matches and historical investigation cost,
then suppressed an ID for the rest of a session. It did not consult scope,
supersession or evidence currency. The memory garbage-collection report covers a
personal Claude store and does not enforce the discovery-recall boundary.

A concrete observed conflict was an old supplemental note recommending the
retired `.git-siting` overlay while current root guidance makes `planning/product`
the authoritative nested repository. Personal notes are supplemental; current
user instructions and applicable repository guidance govern the task.

Keep readable cards and the existing keyword/BM25 surfaces. Add deterministic
validity checks to discovery recall. Do not add a graph server, autonomous memory
rewriter, learned reranker or neural codec without evidence of an operational need.

## Implementation plan and acceptance

1. Prove the returned CLI/hook seam with failing regression cases: wrong scope,
   explicit supersession, source changes and corrections within one session.
2. Share scope/validity/source checks in `tools/memory_policy.py`; retain legacy
   rows as labelled unverified leads and replace stale claims with recheck leads.
3. Key session suppression by record and observed evidence state. Preserve the
   existing two-row and 1,000-character context limits.
4. Exercise malformed records, missing evidence, expiry, traversal and conflicting
   duplicate IDs; preserve other eligible findings when one row is invalid.
5. Run focused regressions, repository verification, independent review and
   explicit-path commit closeout. Preserve unrelated dirty files.

Source checks read no more than four text references per candidate, 256 KiB per
reference. SHA-256 detects same-size/same-mtime changes. A matching source snapshot
is a currency observation, not proof that an interpretation is true. Expiry is
explicit, avoiding an invented universal age limit for durable rules.

Private scope requires an explicit CLI option or a hook cwd in the private
checkout. Known Siting/private-path legacy metadata is restricted too. These are
recall filters, not tenant security, authorization, or an OS read boundary.
Unlabelled legacy rows still require curation; this change does not retrospectively
verify them or parse generated personal Codex memory.

## Related primary literature

| Source | Relevant mechanism | Decision here |
| --- | --- | --- |
| [LongMemEval](https://arxiv.org/abs/2410.10813), 2024-10-14 | Evaluates temporal reasoning, knowledge updates and abstention alongside retrieval | Include update/abstention cases; do not report authored regression cases as held-out model quality |
| [LongMemEval-V2](https://arxiv.org/abs/2605.12493), 2026-05-12 | Coding-agent trajectories, file-backed evidence, state and strategy retrieval | Keep small evidence-bearing records; separate current state from history |
| [STALE](https://arxiv.org/abs/2605.06527), 2026-05-07 | Tests stale-memory detection, resistance to obsolete premises and policy adaptation | Test whether outdated claims are actually absent from served context |
| [Zep](https://arxiv.org/abs/2501.13956), 2025-01-20 | Temporal validity, invalidation and episode provenance | Adopt explicit invalidation and references without a graph backend |
| [StateMemBench](https://arxiv.org/abs/2608.19652), 2026-08-20 | Distinguishes retrieving evidence from selecting the current valid state | Check validity before ranking; retain unresolved cases rather than choosing silently |

The original [crosstalk simulation](https://github.com/KeAWang/associative-memory-blog/blob/main/crosstalk.py)
uses independent random vectors and defaults to exact stored-key queries. Its
interference ratio motivates distractor tests but does not estimate card capacity,
paraphrase recall or provenance quality. Its softmax normalizer cancels in that
ratio. It was inspected, not executed.

[MLP Memory v4](https://arxiv.org/abs/2508.01832v4) imitates a kNN next-token
distribution from model hidden states. It needs training/inference integration
beyond this harness. Its latency comparison distinguishes first-token latency from
throughput. HaluEval is detection accuracy; Table 3 and Appendix B/Table 5 swap
the Dialogue/QA values 66.86 and 64.07. Those results are not adoption evidence
until reconciled. Model quality and speed claims were not reproduced.

## Implementations and activity observations

Checked 2026-10-18 against official repositories; dates below identify observed
commits/releases, not a promise of ongoing support or the absence of newer work.
No packages or services were installed.
The independent review could not fetch the linked Mem0 commit; its activity
observation therefore has less corroboration than the other spot-checked sources.

| Implementation | Observed activity | Applicable idea and boundary |
| --- | --- | --- |
| [Graphiti](https://github.com/getzep/graphiti), Apache-2.0 | [v0.30.2](https://github.com/getzep/graphiti/releases/tag/v0.30.2), 2026-09-08 | Temporal invalidation, provenance and grouping; full graph/LLM extraction stack is disproportionate here |
| [Mem0](https://github.com/mem0ai/mem0), Apache-2.0 | [0724291](https://github.com/mem0ai/mem0/commit/0724291), 2026-10-14 | Scoped/filterable retrieval; managed benchmark algorithms and scores must not be attributed to the OSS package |
| [Letta Code](https://github.com/letta-ai/letta-code), Apache-2.0 | [d945263](https://github.com/letta-ai/letta-code/commit/d945263), 2026-09-11 | Versioned file-backed memory; autonomous editing/dreaming is not adopted |
| [Hindsight](https://github.com/vectorize-io/hindsight), Apache-2.0 | [v0.10.2](https://github.com/vectorize-io/hindsight/releases/tag/v0.10.2), 2026-09-29 | Scoped banks and evidence/belief separation; database/model services are not required for this repair |
| [LongMemEval-V2 implementation](https://github.com/xiaowu0162/LongMemEval-V2) | [2cc8c54](https://github.com/xiaowu0162/LongMemEval-V2/commit/2cc8c54), 2026-08-09 | Closest evaluation pattern: file-backed trajectories, manifests and evidence assembly |

Mem0's current managed-platform section describes an ADD-only algorithm and says
its proprietary benchmark optimizations are not available in the OSS SDK. That
statement must not be generalized to OSS update/delete semantics; the versioned
OSS update path was not independently audited in this task.

## User-supplied learned compression implementations

[L3TC](https://github.com/alipay/L3TC-leveraging-rwkv-for-learned-lossless-low-complexity-text-compression)
is the official RWKV-based learned lossless text-compression implementation. Its
inference example uses CUDA, a checkpoint/tokenizer and 2,048-token segments.
Observed [91dedf6](https://github.com/alipay/L3TC-leveraging-rwkv-for-learned-lossless-low-complexity-text-compression/commit/91dedf6)
is dated 2025-04-14; no standard license file was identified. Reported compression
ratios/speeds depend on the paper's corpus and setup and were not reproduced.

[NNCP's project page](https://bellard.org/nncp/) identifies v3.3, dated 2024-06-05,
with C/LibNC source, CPU/GPU support and an MIT license. The supplied
[v2.1 paper](https://bellard.org/nncp/nncp_v2.1.pdf) describes a different, older
Transformer-based implementation. Do not transfer v2 hardware/determinism claims
to v3 without checking the applicable implementation. The project's enwik9
example is an offline workload; it does not establish useful live lookup latency.

Both compress physical bytes, which must be decoded before text retrieval or use
in a model prompt. Neither source establishes a seekable per-card retrieval,
provenance, validity or supersession contract. Compression is not confidentiality.
Keep the live index/cards directly readable.

If archive storage becomes material, first compare standard lossless codecs on
the actual admitted archive. A later neural-codec trial must include exact
round-trip hashes, corpus identity, model/tokenizer/decoder manifests, total
storage overhead, encode/decode time, RAM/VRAM and single-record extraction cost.
No compression implementation or benchmark is claimed here.

## Verification record and remaining experiments

Observed red: six focused CLI/hook/path-boundary nodes failed before production
edits: private and superseded claims were served, source matches were unlabelled,
traversal resolved outside the card root, and corrected/source-changed records
were suppressed by an ID-only marker.

First green: `.venv/Scripts/python.exe -m pytest -q test/tools/test_discoveries.py
test/tools/test_efficiency_hooks.py` passed 32 tests. Additional edge-case cases
first exposed malformed-scope handling and duplicate IDs with different triggers.
The expanded three-file focused suite passed 53 tests before independent review.
Independent review then exposed private detail-root fallback and falsey optional
metadata. New CLI/hook output tests reproduced the private pointer leak, and
parameterized policy cases reproduced acceptance of malformed null/false values.
Detail lookup is now scope-bound, personal fallback is explicit, and malformed
present metadata is excluded. Pre-existing dirty discovery-index rows were kept
untouched; this evidence card and its README link record the repair.
Further review reproduced three namespace-collision failures in private context:
public rows gained private detail pointers and private rows selected public cards.
Both CLI and hook now use a shared row-aware detail resolver. Explicit row scope
anchors provenance; ambiguous unscoped slugs omit the pointer. The final focused
policy/CLI/hook/prompt suite passed 78 tests before the final repository gates.

Final independent review passed. Four isolated actual-hook before/after
demonstrations also passed against the baseline commit: wrong-scope exclusion,
superseded-record exclusion, corrected-revision recall and a source change that
preserved both file size and mtime. The last case produces a recheck lead instead
of the cached claim. The local evidence artifact is
`artifacts/memory-recall-20261018/before-after-demo.json`; its synthetic cases are
authored regression demonstrations, not an external quality benchmark.

The full-profile `verify --no-cache` passed nine of ten checks, including 268 SQL
tests and 14 extractor integration tests. Its final fast lane recorded 4,495
passed, five failed and eleven setup errors. Independent review attributed these
to Windows named-pipe/Git subprocess failures and unrelated stale judicial
parquet fact cards/runtime metadata. All 18 targeted Git/Windows checks then
passed outside the sandbox (`permission-rerun.json` in the same artifact folder).
This rerun does not turn the failed broad gate into a success. Concurrent
unrelated edits also limit the original run to its observed source snapshot.

The canonical full-profile `check` passed all static commands through fixture
divergence. Its final fast suite was interrupted at 12% and has no final result
(`check.log`); it is not a successful broad-check receipt. A later two-node
metadata follow-up also has no completed result. The completed full verification
above remains the evidence for the two unrelated metadata failures. Neither the
judicial data/cards nor the runtime manifest was regenerated by this task.

These are synthetic deterministic regression checks, not held-out memory quality.
Next evaluation: freeze 30-50 external task cases and source snapshots, including
paraphrases, changed evidence, conflicting versions, scope and continuation.
Compare one recall intervention at a time with balanced repeated runs. Measure
task correctness, source coverage, stale-claim use, unnecessary investigation,
uncached tokens, tool rounds and median/tail latency. Keep engineering efficiency
separate from customer-answer correctness and source quality.

NOT RUN: paid model evaluation, learned-codec execution, paper reproduction,
actual live-client hook activation/native compaction, or a private product audit.
MCP FTS memory search and direct/personal-memory reads remain separate surfaces.
