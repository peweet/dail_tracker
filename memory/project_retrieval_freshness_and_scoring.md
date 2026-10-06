# Project retrieval needs freshness and honest scoring

Trigger: stale search_project metadata, Unicode queries, content_spans, or recall evaluation.

Pilot A on 2026-10-06 found a process-lifetime metadata cache, ASCII-only query
extraction, and an evaluator that searched arbitrary result strings for a gold path
while ignoring the separate content-span surface.

The cache now binds checkout and fact-card source identity, serializes transitions,
and rechecks admitted source-path/stat fingerprints every 120 seconds. Unchanged
fingerprints avoid reparsing; a source switch invalidates immediately. Stat fingerprints
do not detect same-size edits with preserved timestamps. Existing tracked-source policy
and the intentional fact-card input remain unchanged.

Query extraction preserves Unicode word characters without introducing another name
normalizer. Recall uses exact normalized paths, separate metadata/content scores,
valid content spans, disclosed excluded denominators and per-case JSON audit records.
Metadata-first union and either-surface coverage are distinct baselines, not an assertion
that the two server result collections have been fused.

Observed red evidence: accented FTS queries returned no match; a same-size/same-mtime
fact-card source switch returned the previous source's rows; invalid content spans and
content-only union rows earned credit; excluded golds broke denominator accounting;
and an older overlapping rebuild overwrote a newer snapshot. The initial freshness
tests failed on missing cache internals rather than demonstrating the original stale
behavior; subsequent source-switch and concurrency reds demonstrate behavior against
the pre-repair implementation. Exact nodes and commands are in the ignored task logs
under artifacts/pilot-a-retrieval-20261006/.

Green: the focused command `.venv/Scripts/python.exe -m pytest -q
test/mcp_server/test_code_index.py test/mcp_server/test_fts_index.py
test/tools/evals/test_app_retrieval_recall.py` passed 58 tests. Independent review passed.

Lesson: prove cache identity, invalidation, unchanged-input reuse and concurrent
assignment at the returned-result seam. Evaluate both retrieval surfaces without
filename-substring credit or accidental promotion of content rows to metadata hits.
An authored source-recall smoke check does not establish lower model token use,
better answers or useful novel discoveries; those require a held-out task trial.

Verification boundary: local MCP servers stayed disabled. Repository gates exclude
test/mcp_server/test_mcp_server_smoke.py::test_stdio_client_server_roundtrip.
Filtered verification receipts must not be reused as ordinary unfiltered successes.
