# Council retrieval pilots: freeze evidence and validate cached vectors

Recorded 2026-10-04. Scope: public council-minutes retrieval experiments, separate
from live DuckDB FTS and private planning decisions.

## Confirmed failures and repairs

- Scout display quotes collapsed line breaks. Full body hashes matched, but exact
  substring validation rejected the Limerick target; the same issue affected four
  of eight source quotes. Preserve literal whitespace in judgment evidence.
- Independent review reproduced stale provenance acceptance, rank inflation from
  duplicate hits, and reuse of an altered but same-shape vector cache. The pilot now
  checks the declared selection corpus hash and source metadata, deduplicates ranks,
  and validates checksums, unit norms, package/config identity, and script hashes.
- A core-only worktree `.venv` created by a review command caused basedpyright to
  report 60 missing-import errors while full-profile Python checks used the primary
  development environment. `pyproject.toml` selects `.venv` relative to the checkout.
  Read-only reviews must use the named existing interpreter and avoid `uv` sync.
- The isolated CUDA runtime installed and the pinned BGE files downloaded and hashed.
  GPU allocation nevertheless failed with `cudaErrorIllegalState`, including a
  one-element tensor outside the sandbox. This did not establish BGE quality. CPU
  model loading was also refused by its resource guard when memory was insufficient.
- Model2Vec 0.9.0 defaults to truncating at 512 tokens, despite the pinned Potion
  model's much larger tokenizer metadata limit. Pass `max_length=None`, inspect the
  actual encoder IDs, and validate output norms; model metadata alone is insufficient.
- Sparse worktree setup omitted tracked runtime metadata, producing hundreds of test
  errors. The tracked data was only about 260 MB; restoring it and running local
  tests with Windows temporary-pipe permissions reduced the gate to one unrelated
  procurement link-contract failure. Do not mistake checkout setup for code evidence.
- Windows PowerShell `*>` can report a false shell failure for native stderr even
  with a zero native exit code. The broad check logged every check successful and
  4,328 passing tests; a controlled stderr/exit-zero command reproduced shell exit
  one, while explicit `$LASTEXITCODE` capture proved native zero. Preserve native
  exit codes explicitly in future background gate artifacts.

## Reuse contract

1. Freeze judgments before viewing pilot rankings. Keep corpus identity, exact target
   body hashes, literal quotes, URL, council, meeting date, and extraction status.
2. One known positive per query supports known-target hit counts and reciprocal rank;
   it does not support exhaustive recall or nDCG. Separate natural questions and
   literal controls, retain OCR caveats, and require human judgment review.
3. Compare BM25, dense, and hybrid on exactly the same eligible rows. Preserve the
   live lexical settings; do not tune selection after inspecting retrieval results.
4. Bound Python storage as well as DuckDB memory. Check Windows physical and commit
   memory. Keep dependency setup, public weight download, inference, and verification
   as separately observed jobs. Model recognition and downloaded bytes are not
   evidence of successful inference.
5. Use a pinned model revision and local-only inference. Validate cached vector bytes,
   norms, and the full embedding protocol before reuse. Report exact script hashes
   when the producing checkout is dirty or uncommitted.

Evidence: `tools/evals/council_retrieval_pilot.py`, its focused tests, frozen
`tools/evals/council_retrieval_queries.json.example`, and
`tools/evals/COUNCIL_RETRIEVAL_PILOT.md`. The initial local BM25 artifact evaluated
15,365 passages and 16 seeds: target first 6/16, top five 14/16. These remain partial
agent-authored labels. Model2Vec's CPU challenger found 2/16 targets in the top ten
versus BM25's 14/16, and RRF found 7/16; do not adopt it on these seeds. The measured
dense stage took 27.791654 seconds, including model loading and full-corpus preparation.
BGE remains unmeasured and downstream token savings are not established.
