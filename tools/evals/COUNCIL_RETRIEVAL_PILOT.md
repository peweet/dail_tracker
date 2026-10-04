# Council retrieval pilot

Current decision (2026-10-04): keep DuckDB BM25 for council-minute retrieval. The
tested Model2Vec challenger and its equal-weight RRF hybrid performed worse on the
frozen seed. BGE-M3 remains unmeasured because CUDA allocation failed. No live search
or private planning decision path was changed.

This is an offline, known-item retrieval evaluation. It accepts a source-linked
parquet corpus (the production-shaped columns are `document_id`, `chunk`, `body`,
`source_url`, `council`, `meeting_date`, and `source_status`) or a small JSON/JSONL
fixture. Parquet rows are filtered in DuckDB before materialisation: the source URL
must be HTTP(S) and body text must be nonempty. Duplicate `(document_id, chunk)` rows,
stale body hashes, missing targets, and inexact evidence quotes fail closed before
ranking.

The hard pilot limits are 20,000 eligible passages and 64 MiB of UTF-8 body text;
JSON inputs are limited to 128 MiB. Parquet counts and text bytes are checked before
materialising rows. This command evaluates retrieval candidates; it makes no planning
decision or factual inference from a match.

## Usage

```text
.venv/Scripts/python -m tools.evals.council_retrieval_pilot \
  --corpus path/to/council_minutes.parquet \
  --queries tools/evals/council_retrieval_queries.json.example \
  --output path/to/report.json \
  --backend bm25 --memory-limit 512MB
```

`queries.json` is schema version 1:

```json
{
  "schema_version": 1,
  "judgment_status": "agent_authored_seed_requires_human_review",
  "queries": [{
    "id": "q-1",
    "kind": "known_positive",
    "query": "river flood scheme",
    "document_id": "doc-a",
    "chunk": 0,
    "body_sha256": "<sha256 of the exact UTF-8 body>",
    "evidence_quote": "exact substring from the body"
  }]
}
```

The checked-in seed additionally declares `corpus_sha256_at_selection` and each
target's URL, council, meeting date, and source status. All declared fields must
match the current corpus. Minimal synthetic query files may omit those additional
fields. The `.json.example` suffix keeps this public seed outside the repository's
blanket ignore rule for generated JSON; its contents are JSON.

BM25 uses an in-memory DuckDB connection, one thread, the configured memory limit,
`LOAD fts`, and the live FTS tokenizer setting `ignore='[^a-z0-9]+'` with DuckDB's
porter defaults. It does not install or download the extension. `--backend all` adds
an explicitly pinned `sentence-transformers` dense arm and RRF fusion (`k=60`, top 50
from each arm). Dense runs require `--revision` with exactly 40 hexadecimal characters;
they use `BAAI/bge-m3` by default, `trust_remote_code=False`, normalized vectors, and
the selected CPU or CUDA device. Token lengths are checked before encoding and an
exceeding document or query fails rather than being silently truncated.
The model must already be cached at that revision: model, tokenizer, and configuration
loading use `local_files_only=True`. The ranking command makes no model download.

Reports include exact corpus, query-file, and source Git hashes; separate known-target
hit-at-1/5/10 and reciprocal-rank-at-10 metrics per arm; top ten IDs, scores, and
source metadata without full bodies; timings, package observations, warnings, and
`token_economics: not measured: no downstream LLM`. These are seed judgments requiring
human review and partial labels; the pilot makes no exhaustive-corpus claim.

Embedding cache files are written atomically only when `--cache-dir` is supplied. A
cache is reused only when its full corpus/query identity, ordered text hashes, model
revision, length limit, device, dtype, and backend all match. Existing report paths
are refused rather than overwritten.
The manifest also binds package versions and the pilot script hash; stored vector
checksums and unit norms are checked before reuse. Reports carry the script hash,
worktree dirty state, per-arm timings, and separate natural/control query summaries.

## Isolated CUDA runtime

From the checkout root, PowerShell:

```powershell
uv venv --python 3.12 .cache/council-pilot-runtime
uv pip install --python .cache/council-pilot-runtime/Scripts/python.exe -r tools/evals/requirements-council-pilot.txt --index https://download.pytorch.org/whl/cu128 --index-strategy unsafe-best-match
$env:HF_HUB_DISABLE_XET = '1'
$env:HF_HUB_DISABLE_TELEMETRY = '1'
$env:HF_HUB_DISABLE_IMPLICIT_TOKEN = '1'
.cache/council-pilot-runtime/Scripts/hf.exe download BAAI/bge-m3 1_Pooling/config.json config.json config_sentence_transformers.json modules.json pytorch_model.bin sentence_bert_config.json sentencepiece.bpe.model special_tokens_map.json tokenizer.json tokenizer_config.json --revision 5617a9f61b028005a4858fdac845db406aefb181 --cache-dir .cache/council-model-cache --max-workers 1
$env:HF_HUB_OFFLINE = '1'
.cache/council-pilot-runtime/Scripts/python.exe -m tools.evals.council_retrieval_pilot --corpus data/gold/parquet/council_minutes_corpus.parquet --queries tools/evals/council_retrieval_queries.json.example --output .cache/council-comparison.json --backend all --cache-dir .cache/council-model-cache --revision 5617a9f61b028005a4858fdac845db406aefb181 --max-length 2048 --memory-limit 256MB --min-free-mb 2048
```

The download is a separate operation requesting only public model files. The chosen
ten files total 2,293,315,801 bytes. The pinned runtime was installed locally on
2026-10-04; it does not belong to the normal app/dev/CI environment. Allow additional
disk space for PyTorch and enough free physical **and commit** memory before loading
the model. The CUDA arm uses fp16 and batch size one. CPU mode uses fp32 and needs
more memory and time. Windows checks both memory figures before corpus/model loading;
other platforms rely on the hard corpus bounds and need an operator resource check.

## First baseline, 2026-10-04

The first BM25 run used 15,365 source-linked passages, 16 frozen queries over eight
councils, DuckDB 1.5.5, one thread, and a 256 MiB DuckDB memory limit. Total wall time
was 21.244532 seconds, including loading, indexing, and all queries.

| Seed group | Queries | Target first | Target in top 5 | Target in top 10 |
| --- | ---: | ---: | ---: | ---: |
| Natural questions | 8 | 2 | 7 | 7 |
| Literal controls | 8 | 4 | 7 | 7 |
| All seeds | 16 | 6 | 14 | 14 |

Aggregate reciprocal rank at ten was 0.557292. These are known-target **counts**,
not exhaustive relevance recall or nDCG. The seed was selected before pilot rankings,
contains five text and three OCR target passages, retains substantial lexical overlap,
and requires human review. Other retrieved passages may also be relevant; a target
miss does not establish an answer failure. Source URLs and extraction fidelity have
not been revalidated against the live documents.

Corpus SHA-256: `9e6db6dc3f07a0c092f291374b31b690863fc19f947404ae0e0017434a6976b2`.
Query SHA-256: `5f36fcc8ec147fdcf3a38e21fd63225fb30608a10fb98466e0ae31ad5bd593bd`.
The initial issued report is local at `.cache/council-semantic/bm25.json` in the
primary checkout; later reports also record the exact producing script hash.

Encoder token counts concern the selected encoder's tokenizer. Downstream
LLM tokens, monetary savings, and prompt compression remain unmeasured. Compare the
dense and hybrid arms on the same frozen corpus before deciding whether to adopt them.

## CPU challenger and measured comparison

`--dense-backend model2vec --model minishlab/potion-base-8M --device cpu` selects
the static embedding challenger. Its three required files total 30,920,628 bytes;
the pinned revision is `bf8b056651a2c21b8d2565580b8569da283cab23`. The model is
MIT licensed, English, and produces 256-dimensional vectors.
[Pinned model files](https://huggingface.co/minishlab/potion-base-8M/tree/bf8b056651a2c21b8d2565580b8569da283cab23).

The CPU-only requirements are separated from the CUDA environment. To reproduce
the challenger in PowerShell, with the DuckDB FTS extension already cached:

```powershell
uv venv --python 3.12 .cache/council-cpu-runtime
uv pip install --python .cache/council-cpu-runtime/Scripts/python.exe -r tools/evals/requirements-council-pilot-cpu.txt
$env:HF_HUB_DISABLE_XET = '1'
$env:HF_HUB_DISABLE_TELEMETRY = '1'
$env:HF_HUB_DISABLE_IMPLICIT_TOKEN = '1'
.cache/council-cpu-runtime/Scripts/hf.exe download minishlab/potion-base-8M model.safetensors tokenizer.json config.json --revision bf8b056651a2c21b8d2565580b8569da283cab23 --cache-dir .cache/council-model-cache --max-workers 1
$env:HF_HUB_OFFLINE = '1'
$env:TOKENIZERS_PARALLELISM = 'false'
.cache/council-cpu-runtime/Scripts/python.exe -m tools.evals.council_retrieval_pilot --corpus data/gold/parquet/council_minutes_corpus.parquet --queries tools/evals/council_retrieval_queries.json.example --output .cache/council-cpu-comparison.json --backend all --dense-backend model2vec --model minishlab/potion-base-8M --revision bf8b056651a2c21b8d2565580b8569da283cab23 --device cpu --cache-dir .cache/council-model-cache --max-length 2048 --memory-limit 256MB --min-free-mb 1024
```

Model2Vec 0.9.0 defaults to a 512-token limit. The adapter explicitly passes
`max_length=None`, checks the actual untruncated token IDs against the pilot limit,
and disables multiprocessing. Special tokens are excluded and unknown tokens
removed, matching the library's encoder. The pinned local snapshot must contain
all three files before the local-directory loader is called; inference downloads
nothing. The first measured run used the existing isolated CUDA runtime, which
also contains the same CPU package pins; the dedicated CPU install recipe has
not been independently rebuilt.
[Versioned encoder source](https://github.com/MinishLab/model2vec/blob/v0.9.0/model2vec/model.py).

All arms used the same 15,365 passages and 16 queries, with no selection or tuning
after seeing rankings:

| Arm | Target first | Target in top 5 | Target in top 10 | Reciprocal rank at 10 |
| --- | ---: | ---: | ---: | ---: |
| BM25 | 6/16 | 14/16 | 14/16 | 0.557292 |
| Model2Vec dense | 0/16 | 1/16 | 2/16 | 0.040179 |
| BM25 + Model2Vec RRF | 2/16 | 3/16 | 7/16 | 0.175521 |

For the eight natural questions, BM25 found seven targets in the top ten, dense
found none, and RRF found three. These partial, agent-authored labels require human
review; they support declining this model for this pilot, not a universal ranking
of embedding models. Candidate quality for unrelated relevant passages remains
unjudged.

The uncached run took 107.682323 seconds: BM25 build/query 79.023015 seconds,
dense load/tokenize/encode/score 27.791654 seconds, and RRF 0.002107 seconds.
These include corpus-wide preparation, not steady-state query latency. The earlier
BM25-only run took 21.244532 seconds; timing variation is not an engine-speed claim.
Model2Vec counted 5,862,478 document tokens (maximum 1,397) and 193 query tokens;
these are encoder counts and imply no downstream LLM-token or monetary savings.

Issued artifact: primary checkout `.cache/council-semantic/cpu-comparison.json`.
Producing script SHA-256:
`49d18b9c3c9e495023bc67f1f7c922e4a979eecb2e2f131a747c5c620667e5e9`.
It records the uncommitted script identity separately from Git HEAD. Later typing
checks and defensive assertions do not change these rankings; reproduce to issue a
new artifact.

Final replay: primary checkout `.cache/council-semantic/cpu-comparison-final.json`,
producing script SHA-256
`e21cd028b33f8b183d31353aaf106e706d7b8d9ececf7753bec041d09d2f1c31`.
Its corpus/query hashes and every ranked output match the initial run. Old caches
were rejected and regenerated; independent read-only review confirmed the new report
and cache identities. Total time was 71.213628 seconds, including 49.484766 seconds
for BM25 and 20.779013 seconds for the dense stage; timings remain workload observations.

## How the options fit this project

- **BM25:** current measured choice for finding source-linked council passages.
- **Model2Vec:** inexpensive CPU computation, but rejected for this retrieval seed.
  SIF/Zipf weighting is baked into its token vectors during model construction;
  inference tokenizes text and averages those vectors. It does not reweight words
  before tokenization or shorten a downstream LLM prompt.
  [Model2Vec method](https://minish.ai/packages/model2vec/introduction/).
- **BGE-M3:** retained experimental candidate for contextual, multilingual retrieval.
  Dense, sparse, and multi-vector capabilities exist, but this pilot implements
  only its dense arm and has no successful BGE inference measurement.
  [Model card](https://huggingface.co/BAAI/bge-m3).
- **all-MiniLM-L6-v2:** a concrete match for the billion-scale training claim:
  its card lists 1,170,060,424 training tuples, primarily sentence pairs, rather
  than billions of complete documents. Its default truncation is 256 word pieces;
  many current passages exceed that, so another truncation-free protocol is needed
  before comparison. It was not benchmarked here.
  [Model card](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2).
- **Turbopuffer:** vector/BM25 search infrastructure, a separate choice from the
  encoder. This local comparison does not establish a need to migrate storage.
  [Hybrid search documentation](https://turbopuffer.com/docs/hybrid).

The useful first integration, if a later model earns it, is a candidate-finding
layer for differently worded questions over council minutes, retaining URL/date/OCR
provenance. VS Code repository navigation is a separate internal evaluation. Expand
and human-review the relevance set, including paraphrases and hard negatives, before
changing live retrieval. Measure tokens in the downstream prompt separately.

## Execution and test status

On this workstation, PyTorch 2.11.0+cu128 detected the RTX 3060 Laptop GPU and BGE's
391 weight entries loaded. Inference then failed with `cudaErrorIllegalState` before
any passage was encoded. A one-element CUDA tensor failed with the same error both
inside and outside the sandbox. The BGE dense/hybrid comparison is therefore
uncompleted. A small BGE CPU smoke attempt also stopped at its 3,072 MiB resource
guard when only 1,478 MiB was free. The Model2Vec comparison above completed;
downstream token savings remain unmeasured.

Observable test seam: source-linked local candidate rankings, validated judgments,
and independent per-arm known-target metrics. The initial test failed collection
because the pilot module was absent. Later red checks reproduced altered-cache reuse,
missing progress/provenance/bounds checks, duplicate-rank MRR of 0.333333 instead of
0.5, and a malformed-manifest `TypeError`. The CPU-backend test then failed with
`TypeError: _dense() got an unexpected keyword argument 'dense_backend'`; after
implementation it proves local-only loading and encoding more than 512 tokens
without truncation. After repairs:

```text
.venv/Scripts/python.exe -m pytest -q test/tools/evals/test_council_retrieval_pilot.py
9 passed
```

Tests use tiny synthetic rows, a real DuckDB parquet fixture, and a fake model/tokenizer
at the inference-client seam. The fake proves local-only configuration and refusal of
over-limit inputs; it does not prove model inference quality. Production text is not
copied into unit fixtures. Independent read-only review accepted the repaired contract.
Focused static typing of the pilot and tests also passed with zero errors. The first
full gate passed seven static checks and 4,326 fast tests, with one procurement
authority-link failure outside the pilot. Its isolated link suite later passed
11 tests; the suite-state difference is unresolved. Final verification is recorded
at handoff separately from these focused results.

The final full-profile `tools/dev.py check` log records success for all twelve
pre-test task groups and `4328 passed, 70 skipped, 384 deselected, 1 xfailed` in
382.87 seconds. The earlier authority-link failure did not recur; its suite-state
difference remains unresolved. A preceding final `verify --no-cache` completed
seven static checks but was interrupted before issuing its fast-suite result.

Windows PowerShell's `*>` redirect gave a misleading shell failure when uv wrote
installation progress to stderr. A controlled native command exiting zero reproduced
`NativeExitCode=0` with `PowerShellSuccess=false`. Preserve `$LASTEXITCODE` explicitly
or capture native exit codes with Python subprocess; the original broad gate's native
exit code was not saved separately. Evidence: task checkout `.cache/council-check.log`
and primary checkout `.cache/council-semantic/native-stderr-smoke-captured.log`.
