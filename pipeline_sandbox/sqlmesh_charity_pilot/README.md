# SQLMesh charity pilot

This is an isolated evaluation of SQLMesh over the existing two-view charity
family. It does not register models in the application, alter the production
SQL views, or add SQLMesh to the application's dependencies.

## Scope and acceptance criteria

The pilot duplicates these production contracts:

- `v_charity_financials_by_year`: one row per `(rcn, period_year)`
- `v_charity_sector_totals_by_year`: one row per `period_year`

The trial is successful only if SQLMesh 0.236.1 can:

1. load and lint both DuckDB models;
2. expose the `financials_by_year -> sector_totals_by_year` dependency;
3. run the downstream model's isolated unit test;
4. plan and materialise both models from the real local Parquet file;
5. run blocking grain, null, and minimum-row audits against materialised data;
6. provide enough planning, audit, and lineage value to justify the additional
   project state and duplicated model metadata.

## Reproduce

Run from this directory so DuckDB resolves the deliberately relative Parquet
path in `models/financials_by_year.sql` correctly:

```powershell
$pilotRoot = (Resolve-Path .).Path
New-Item -ItemType Directory -Force .state | Out-Null
$env:SQLMESH_HOME = Join-Path $pilotRoot '.state\sqlmesh_home'
$env:UV_CACHE_DIR = Join-Path $pilotRoot '.cache\uv'
$env:SQLMESH__DISABLE_ANONYMIZED_ANALYTICS = 'true'
$env:PYTHONUTF8 = '1'
uv run --no-project --with sqlmesh==0.236.1 sqlmesh info
uv run --no-project --with sqlmesh==0.236.1 sqlmesh lint
uv run --no-project --with sqlmesh==0.236.1 sqlmesh test
uv run --no-project --with sqlmesh==0.236.1 sqlmesh plan --auto-apply --no-prompts --skip-linter
uv run --no-project --with sqlmesh==0.236.1 sqlmesh audit
uv run --no-project --with sqlmesh==0.236.1 sqlmesh dag .state/lineage.html
```

The DuckDB catalogue, SQLMesh state, caches, logs, and generated DAG stay under
ignored directories in this pilot.

`SQLMESH_HOME` is required in restricted environments because SQLMesh writes a
user identity file even when anonymised analytics is disabled. `PYTHONUTF8`
avoids a Windows console error when the standalone audit command prints its
Unicode pass markers.

## Result (2026-08-04)

Tested with SQLMesh 0.236.1 and DuckDB against the real 10.8 MB annual-reports
Parquet file.

| Check | Result |
| --- | --- |
| Project load / connection | Pass: 2 models loaded; warehouse connected |
| Lineage | Pass, but partial: 2 model nodes and 1 edge; no Parquet source node |
| Unit test | Pass: 1 downstream aggregation test |
| Plan / materialisation | Pass: both full models built in 0.53 s and 0.12 s |
| Blocking audits | Pass: all 6 grain, null, and row-count audits |
| Lint | Fail: `ambiguousorinvalidcolumn` cannot resolve `READ_PARQUET` columns |

The materialised base model exactly matched the production view across 82,748
rows. The sector model had the same 13 years and charity counts. Ten rows were
not bit-for-bit equal because floating-point `SUM` order changed after
materialisation; the largest absolute money delta was 0.00030517578125.

## Recommendation

Do not migrate this family to SQLMesh yet. The plan preview, isolated model
test, and automatic blocking audits work, but the current two-view runtime
registration already expresses this small dependency with much less state and
metadata. For Parquet-backed views, SQLMesh also loses source lineage and the
most valuable column-resolution lint rule unless another staging convention is
introduced. Reconsider only if model scheduling, preview environments, or
cross-family dependency management becomes an observed operational problem.

## Known boundary under evaluation

`READ_PARQUET(...)` is a DuckDB table function, not a SQLMesh model. SQLMesh can
line up and audit the two transformations, but the Parquet file itself is not a
first-class lineage node. Its path is also resolved from the command's working
directory. In SQLMesh 0.236.1, the `ambiguousorinvalidcolumn` linter cannot
resolve the table function's output columns, so the materialisation command
must skip that otherwise useful linter check. A production adoption would
therefore need a shared path macro or a staging/external-model convention, plus
a decision about whether SQLMesh should own materialisation rather than the
application's current runtime views.
