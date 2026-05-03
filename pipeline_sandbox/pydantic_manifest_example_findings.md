# pydantic example — findings

Companion to [pydantic_manifest_example.py](pydantic_manifest_example.py). The script runs four small demos against a real file in the repo (`data/manifests/manifest.json`) so you can decide whether pydantic is worth adopting for non-tabular data — config, page contracts, manifests, probe outputs.

Run it:

```bash
python pipeline_sandbox/pydantic_manifest_example.py
```

## What the script does

| # | Demo | Question it answers |
|---|---|---|
| 1 | Permissive load of the real file | Can a forgiving model parse all 14 historical entries without breaking? |
| 2 | Strict load of the same file | Where does the on-disk shape disagree with what we'd ideally want? |
| 3 | Strict load of a deliberately-bad dict | What does a typo or extra field actually look like as an error? |
| 4 | Round-trip a permissive parse | Does `model_dump` give back what we put in, or does anything silently disappear? |

Two models, both ~10 lines:

- `RunManifestPermissive` — accepts everything historical, all fields `Optional`. What you'd run in production.
- `RunManifestStrict` — same fields, but `extra='forbid'` and `bool` / `timedelta` marked strict. What a test fixture would assert against.

## What the script found on the real manifest

**Demo 1 (permissive):** all 14 entries parse cleanly. 6 finished, 8 aborted before `manifest_finalise` ran. Useful as a sanity check that a single permissive model covers your whole history.

**Demo 2 (strict):** 4 of 14 entries fail. Field-level counts: `time_to_run: 4`, `endpoints_ok: 3`. Both come from [manifest.py:64](../manifest.py#L64):

```python
manifest_list[-1]["time_to_run"]  = str(time_taken)    # timedelta stringified
manifest_list[-1]["endpoints_ok"] = str(endpoints_ok)  # bool stringified
```

Not catastrophic — nothing reads these fields back today — but `"True"` is more fragile than `True`, and `"0:00:02"` can't round-trip into a `timedelta` without parsing. If you ever wire either field into logic, you'd find this out the hard way. Strict pydantic surfaces it now, with the field name.

**Demo 3 (bad fixture):** a typo'd `endpoint_ok` (missing the `s`) and a junk `rows_extracted` field both produce errors that name the offending field directly. This is the main thing pydantic gives you over hand-rolled `if "x" in d` checks: errors point at the field, not at the line where downstream code crashed.

**Demo 4 (round-trip):** no fields lost. The check is there for a specific case: if a model misnames a field (e.g. `endpoint_ok: bool` for an on-disk `endpoints_ok`), permissive parsing will quietly drop the value and `model_dump` will be missing it. Comparing key sets catches this; equality checks don't.

## Honest pros and cons

**Pros**
- Errors name the field, not the call site.
- One model serves both production (permissive) and tests (strict, with `extra='forbid'`).
- Already installed (pydantic 2.12.5) — no new dependency.
- ~30 LOC per modelled boundary.

**Cons**
- Effort: each new boundary needs a model + a test + (sometimes) a producer-side fix to make the on-disk shape match.
- Strictness has to be tuned. Too strict and historical data fails; too loose and the model isn't telling you anything pandera doesn't already.
- Doesn't replace anything. Pandera still does the row-level work; pydantic only helps where data isn't tabular.

**Where it doesn't apply**
Tabular silver/gold frames. Pandera is the right tool there and is faster.

## If this looks useful, the next step is small

1. Move the `RunManifestPermissive` model into `manifest.py` and have `create_run_manifest` / `manifest_finalise` build dicts via `model_dump()` instead of writing literals. Drops the `str(...)` calls at line 64 as a side effect.
2. Add `test/test_manifest_schema.py` with three asserts: permissive parses every existing on-disk entry; a known-good fixture parses strict; a known-bad fixture raises with the expected field name.
3. Decide later whether to repeat the pattern for page contracts and config. The script in this folder is a template for that decision — point it at a different file, change the model, see what it surfaces.

If it doesn't look useful, the cost of not adopting is that future malformed manifests / contracts / configs will surface as `KeyError` or `AttributeError` somewhere downstream rather than as a named-field error at load. Liveable for a project this size.
