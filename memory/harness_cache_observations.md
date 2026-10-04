# Harness cache observations and comparison order

Observed and repaired 2026-10-04.

## Trigger and cause

The harness benchmark ran every repeat in fixed variant order and reported no
order/cache-observation metadata. Its Codex adapter filled absent usage counters
with zero for legacy consumers. That makes an absent cache measurement look like
a cache miss unless the original field presence is preserved.

Codex reports cached input as part of total input. The adapter's normalized
`input_tokens` is fresh input; use `raw_input_tokens` for a Codex cache denominator,
authorized by the native input field's presence. Claude reports fresh input,
cache reads, and cache creation separately. Reads greater than fresh input, or
zero fresh input with positive cache reads, are valid Claude observations.

## Repair and evidence

`tools/evals/harness_bench.py` now counterbalances per-task variant order across
repeats, records the selected scope/order and observed cache fields, and labels
provider cache control as uncontrolled. `EvalResult.usage_reported_fields`
preserves source-field presence without changing legacy normalized numeric keys.
Missing counters/denominators stay nullable in the new observation fields.

The real adapter-to-attempt regression
`test_actual_codex_adapter_usage_reaches_attempt_cache_observation` first failed
for total input 100 and cached input 90, then passed with a 0.9 reuse ratio. The
Claude boundary regression first failed for fresh input 0, reads 90, creation 10,
then passed with the same ratio. The focused benchmark/adapter suite passed
43 tests after the repair; it uses provider fakes and makes no model calls.

A subsequent review found that `int()` could turn malformed native counts into
apparently valid observations. The parser-to-observation regression
`test_actual_codex_adapter_rejects_malformed_usage_before_cache_observation`
failed for fractional, boolean, and negative values before repair. Known Codex
token counters now require nonboolean, nonnegative integers; malformed usage
becomes unknown with a content-free diagnostic while text/tool parsing continues.
The expanded regression passed six cases, including numeric strings, null, and
objects. The focused suite then passed 49 tests with:

```powershell
uv run --locked --group dev --extra pipeline --extra api --extra mcp pytest -q test/tools/evals/test_harness_bench.py test/tools/evals/test_provider_adapter.py
```

Independent review extended this same boundary to the shared observation helper,
missing later completions, and contradictory cache aliases. Nine cases first
failed across `test_cache_observation_rejects_noninteger_native_counts`,
`test_codex_completion_without_usage_does_not_reuse_prior_counts`, and
`test_codex_conflicting_cache_aliases_are_unknown`. Strict integer observations,
per-completion clearing, and conflict rejection made the same focused command
pass 62 tests. Native counter aliases remain present for compatibility; they are
not separate quantities to sum.

A fresh process is not proof of a cold provider cache. Balanced ordering reduces
order confounding but does not establish cache isolation, billing savings, or
general task-quality improvement. Use live smoke results only for their tested
scope. Copilot's local OTel file can repeat usage across parent spans, child spans,
logs, and metrics; do not sum those streams indiscriminately.
