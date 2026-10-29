# Native validation at the API fetch boundary

Observed 27 October 2026. Oireachtas GET/POST response pages now use a cached
`jsonschema-rs` 0.51.0 validator before downstream flattening. The five existing
Draft 2020-12 schema assets remain authoritative. Unknown endpoints and hosts skip
validation, as before, and do not compile a native validator.

## Compatibility boundary

The fast path accepts freshly decoded HTTP JSON. It is called immediately after
`Response.json()` in [the HTTP engine](../../services/http_engine.py).
General [schema validation](../../services/schema_validation.py) continues to use
Python `jsonschema`: the native binding accepts tuples as arrays and integral
`Decimal` values as integers, while the previous public helper rejects them.

If native validation rejects a page, the existing Python validator produces the
same `SchemaValidationError`, endpoint, JSON path, error selection and cause.
The schemas are not duplicated. Additional upstream fields remain permitted.
The dependency is pinned in the pipeline extra; it is not a new core app dependency.

The upstream binding documents [compiled validator reuse](https://pypi.org/project/jsonschema-rs/0.51.0/).
This change uses `validator_for(...).is_valid(...)` for the successful fetch path.

## Evidence

A test first failed because a valid HTTP page still entered the Python validator.
After implementation, 158 schema, HTTP and pagination tests passed. Coverage includes
all five endpoint schemas, renamed/missing fields, additive metadata, duplicate-key
decoding, non-finite values, unchanged direct Python-object rules, native-cache reuse,
unknown endpoints and both GET and POST transport seams. Generated counts also compare
50 examples per schema against Python, including large integers and floats. Each of the
five real trimmed endpoint fixtures explicitly proves that valid GET JSON avoids Python
validation; all five POST cases prove the same route. HTTP tests use fake responses.

The local Windows comparison uses authored pages, with three serial batches of ten
warm calls per mode at 20 and 500 rows. It compares the current public Python helper,
a cached `Draft202012Validator`, and the actual native fetch helper. Mode order alternates
between batches. Python 3.12; `jsonschema` 4.26.0 and `jsonschema-rs` 0.51.0.

At 500 rows, median validation time per authored page was:

| Endpoint | Cached Python | Native fetch helper |
| --- | ---: | ---: |
| Members | 8.640 ms | 0.049 ms |
| Legislation | 7.788 ms | 0.040 ms |
| Questions | 6.407 ms | 0.035 ms |
| Votes | 6.426 ms | 0.036 ms |
| Debates | 6.381 ms | 0.036 ms |

These are validation-only diagnostics, not overall extraction speedups. They exclude
network, response parsing, flattening, first compilation, invalid-path timing, native
RSS, Linux and deployed throughput. Existing Python error construction remains on drift.
The local source-bound receipt records schema, implementation, lock and benchmark hashes.

## Checks

```powershell
uv run --locked --group dev --extra pipeline --extra api --extra mcp python -m pytest test/services/test_api_schema_native.py test/services/test_api_schemas.py test/services/test_http_engine.py test/services/test_member_paginated.py test/services/test_services_votes.py -q
uv run --locked --group dev --extra pipeline --extra api --extra mcp python tools/dev.py verify --no-cache
```

The second command selects checks against the entire current public Git diff.
Unrelated concurrent changes can therefore affect its result. Focused test success is
not a full-suite, live-source, pipeline-data or release acceptance claim.
The recorded full-profile run passed repository Ruff lint, then stopped on formatting in
the pre-existing dirty `tools/hooks/dated_doc_hint.py`. Focused dependency declaration,
lockfile consistency, source formatting and type checks passed separately.
Independent read-only review passed, including all-five-endpoint native-success coverage
and the current source/schema receipt hashes. No live or deployment checks were run.
