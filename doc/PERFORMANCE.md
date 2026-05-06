# Performance & efficiency plan

End-of-project pass to compress the data tree, speed up Streamlit cold starts, and shorten pipeline runs. The shape of the wins is dictated by what *this* project actually does: produce ~3 GB of bronze/silver/gold artifacts, then bootstrap a Streamlit + DuckDB UI on top.

The goal is not to chase every micro-optimisation. It is to land the handful of changes whose impact has been *measured* on this codebase's actual data.

---

## Measured baseline (as of 2026-05-06)

Run the bench script in `scripts/bench_zstd.py` (Phase 1 ships it) to refresh these numbers. Today:

| Layer | Size on disk | Notable composition |
|---|---|---|
| `data/bronze/` | 1.6 GB | 818 MB iris PDFs (incompressible), ~500 MB JSON (highly compressible), 157 MB lobbying CSVs |
| `data/silver/` | 1.3 GB | 21 MB parquet (90% snappy, 10% zstd), ~1.28 GB CSV mirrors |
| `data/gold/` | 106 MB | 4.3 MB parquet (100% zstd), ~102 MB CSV mirrors |
| **Total** | **~3.0 GB** | |

Compression headroom (measured on representative samples):
- Bronze JSON → zstd: 24×–248× (votes JSON 71 MB → 292 KB)
- Bronze CSV → zstd: 8.1× average
- Silver parquet on snappy → zstd: 2.32× (`questions.parquet` 17 MB → 7.3 MB)
- Silver/gold parquet already on zstd: no further win
- PDFs: ~1× (already deflate-compressed internally)

**Achievable end state:** ~1.1 GB on disk (~62% reduction). Most of that comes from two writer changes plus the API JSON dump path.

---

## Phase 1 — zstd everywhere on writes (biggest single win)

**Target:** silver and gold parquet writes use zstd; silver and gold CSV mirrors written as `.csv.zst`; bronze JSON dumps written as `.json.zst`. Reads stay transparent because Polars / DuckDB / pandas all auto-detect on extension.

### Concrete changes

**`lobby_processing.save_output()`** — already writes both `.csv` and `.parquet`. Switch parquet to zstd; switch CSV to `.csv.zst`:

```python
def save_output(df: pl.DataFrame, filename: str, overwrite: bool = True) -> None:
    LOBBY_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    LOBBY_PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = LOBBY_OUTPUT_DIR / f"{Path(filename).stem}.csv.zst"
    parquet_path = LOBBY_PARQUET_DIR / f"{Path(filename).stem}.parquet"
    if not overwrite and csv_path.exists():
        return
    with open(csv_path, "wb") as f:
        cctx = zstd.ZstdCompressor(level=3)
        with cctx.stream_writer(f) as compressor:
            df.write_csv(compressor)
    df.write_parquet(parquet_path, compression="zstd", compression_level=3)
```

**`lobby_processing.save_gold_outputs()`** — DuckDB writes parquet + CSV from SQL queries. Update both:

```python
result.write_parquet(GOLD_PARQUET_DIR / f"{name}.parquet", compression="zstd")
# CSV: stream-write through zstd
csv_path = GOLD_CSV_DIR / f"{name}.csv.zst"
with open(csv_path, "wb") as f:
    cctx = zstd.ZstdCompressor(level=3)
    with cctx.stream_writer(f) as compressor:
        result.write_csv(compressor)
```

**`oireachtas_api_service.save_results()`** — switch JSON dumps to `.json.zst`. The 200 MB `questions_results.json` becomes ~8 MB. The 71 MB `votes_results.json` becomes ~290 KB.

**`enrich.py` and other silver writers** — same pattern: `compression="zstd"` on every `write_parquet` call. Audit by grepping `write_parquet`.

### Read-side impact

- **DuckDB** reads `.csv.zst` natively via `read_csv_auto`. Re-baseline `save_gold_outputs` to load `.csv.zst` paths if any silver tables are CSV.
- **Polars** reads `.csv.zst` via `pl.read_csv("file.csv.zst")` (auto-detects from extension since 0.20).
- **pandas** reads `.csv.zst` via `pd.read_csv(..., compression="infer")`.
- **No SQL view changes needed** — DuckDB's `read_parquet()` is codec-agnostic.

### Compression level — how to pick

- **Level 3** (Polars/DuckDB default): 75% of max ratio at 5× the encode speed. Right for everyday pipeline runs.
- **Level 19** (max sane): the numbers above. Slow encode (~10× slower), same fast decode. Reserve for archival snapshots or one-off rebuilds.

### Effort

~1 afternoon. Audit all `write_parquet` and `write_csv` call sites; flip the flag; verify a clean pipeline run produces correct outputs. Add `zstandard>=0.22` to `pyproject.toml` dependencies (already a transitive of polars/pyarrow but make it explicit since direct calls now use it).

### Expected savings

Silver + gold CSV mirrors (~1.4 GB → ~175 MB) + silver snappy parquet (~19 MB → ~8 MB) + bronze JSON (~500 MB → ~25 MB) = **~1.7 GB freed**.

---

## Phase 2 — drop redundant CSV mirrors

The silver and gold layers currently write each table twice: once as `.parquet`, once as `.csv`. The CSV exists for human inspection ("`open in Excel`") and ad-hoc export. After Phase 1, the CSV mirror is `.csv.zst` — *not* directly Excel-friendly.

Two cleaner paths:

### 2a. Keep .csv.zst, document how to read

Tell future-you / collaborators: `duckdb -c "COPY (FROM 'file.csv.zst') TO 'file.csv'"` or `zstd -d file.csv.zst`. Saves bytes; preserves the human-inspection idea. Cheapest path; aligns with Phase 1.

### 2b. Drop CSV mirrors entirely

Pipeline writes parquet only. UI export buttons (`utility/ui/export_controls.export_button`) already convert pandas DataFrames to CSV at download time — that path doesn't depend on a `.csv` file existing on disk. The remaining users of disk-CSV are:

- Manual debugging (`grep`, `head`, opening in Excel) — replaceable with a `dt-inspect` CLI helper that takes a parquet path and prints/exports.
- The Cursor/Claude `Read` tool — can already read parquets via tooling.

**Saves another ~175 MB** on top of Phase 1 (the .csv.zst mirrors themselves) and removes a class of "is the CSV in sync with the parquet?" foot-guns. **Tradeoff:** loses convenient `cat`/Excel access.

Recommend 2a first (cheap), 2b only if the disk pressure is real.

---

## Phase 3 — persistent DuckDB database (Streamlit cold-start win)

Right now every Streamlit cold start runs `get_lobbying_conn()` which reads ~20 `sql_views/lobbying_*.sql` files, parses each, executes `CREATE OR REPLACE VIEW`, and only *then* services the first query. The same happens for every other domain (attendance, payments, votes, etc.) on its own cached connection.

Bake the views into a DuckDB file once, ship it with the app:

```python
# scripts/build_duckdb.py — run as a pipeline post-step
import duckdb
from pathlib import Path

con = duckdb.connect("data/dail_tracker.duckdb")
for sql_file in sorted(Path("sql_views").glob("*.sql")):
    con.execute(sql_file.read_text(encoding="utf-8"))
con.close()
```

Then `lobbying_data.get_lobbying_conn()` becomes:

```python
@st.cache_resource
def get_lobbying_conn() -> duckdb.DuckDBPyConnection:
    return duckdb.connect("data/dail_tracker.duckdb", read_only=True)
```

### Wins

- Cold-start cost goes from "parse + execute N CREATE VIEW statements every connection" to "open a file" (~ms).
- Single connection serves *all* domains — no more separate `get_lobbying_conn` / `get_payments_conn` / `get_votes_conn`.
- Views resolve at file-build time, not Streamlit startup → SQL syntax errors are caught in CI (Phase 2a of `CI_CD.md`) rather than crashing the live app.
- DuckDB file is ~tens of MB and ships with deploys naturally.

### Tradeoff

The DuckDB file is now an *artifact* — pipeline runs need to rebuild it. Add a `python scripts/build_duckdb.py` step at the end of `pipeline.py`. Versioning: DuckDB files are tied to a specific DuckDB minor version; pin `duckdb` in `pyproject.toml` so the producer and consumer agree.

### Effort

~1 day including renaming the per-domain `get_*_conn()` functions and updating the imports.

---

## Phase 4 — Streamlit cache & rerender audit

Things to verify on every page:

1. **Every fetch function uses `@st.cache_data(ttl=...)`.** Already true in `utility/data_access/lobbying_data.py`; audit other domains.
2. **Cache keys are correct.** A function with mutable default args or a closure over global state will re-run on every call. Quick grep: any `@st.cache_data` decorator on a function whose arguments don't fully determine the result.
3. **Avoid eager fetching above the fold.** Heavy fetches inside an `st.expander` or below a navigation gate should not run before the user expands/navigates. Use `st.fragment` so a section reruns independently from the rest of the page.
4. **`st.fragment` for independent sections.** A year-pill change on a profile page shouldn't refetch the hero stats. Wrapping a section in `@st.fragment` localises the rerun.
5. **Pagination is real, not cosmetic.** A "show 20 / show all" toggle that fetches all 5,000 rows is doing the slow thing. Push `LIMIT` and `OFFSET` into SQL.

### Effort

~1 day for a sweep across `utility/pages_code/*.py`.

### Wins

Hard to project without measurement. The lobbying page in its current shape reloads the politician index, org index, recent returns, and revolving door summary on *every* rerender. Even with caching, the first paint is bottlenecked on whichever query is slowest.

---

## Phase 5 — pipeline runtime: parallelism + incremental builds

`pipeline.py` runs top-level scripts via `subprocess` (per `project_pipeline_architecture.md`). Most of those scripts are independent — `oireachtas_api_service`, `attendance`, `payments`, `interests`, `lobbying`. They can run in parallel.

### 5a. Parallelise pipeline.py

Use `concurrent.futures.ProcessPoolExecutor` to run the independent scripts concurrently. Wall-clock time drops to roughly the longest single script's runtime instead of the sum.

```python
from concurrent.futures import ProcessPoolExecutor
import subprocess

INDEPENDENT_STAGES = [
    ["python", "oireachtas_api_service.py"],
    ["python", "attendance.py"],
    ["python", "payments.py"],
    ["python", "member_interest.py"],
    ["python", "lobby_processing.py"],
]

with ProcessPoolExecutor(max_workers=4) as pool:
    list(pool.map(subprocess.run, INDEPENDENT_STAGES))
# Then dependent stages
subprocess.run(["python", "enrich.py"])
```

### 5b. Incremental processing

A full pipeline run today reprocesses everything from scratch. Most days, only a sliver has changed (a new attendance PDF, a new lobbying CSV). Skip work whose inputs haven't changed:

- Hash the input files; store the hash next to the output.
- On each run, recompute output only if the input hash differs.
- Easiest place to start: the bronze → silver step in each domain.

### 5c. Cache HTTP API responses during dev

Add `requests-cache` or `hishel` (httpx) so repeated dev iterations of `oireachtas_api_service.py` don't hit the API. Disabled in CI/prod, on by default locally:

```python
if os.getenv("DEV_HTTP_CACHE", "1") == "1":
    requests_cache.install_cache("data/.http_cache", expire_after=86400)
```

### Effort & wins

5a is ~1 hour. 5b is ~1–2 days per domain. 5c is ~30 min. The biggest pipeline-runtime win is 5a; 5b only matters once you start running the pipeline frequently.

---

## Phase 6 — Polars lazy + streaming pass

Current pipeline code is mostly eager (`pl.DataFrame`). Two upgrades, in order of effort:

### 6a. Convert long chains to LazyFrames

Anywhere you see `df.with_columns(...).filter(...).group_by(...).agg(...)` over multiple statements, switch to `df.lazy().with_columns(...).filter(...).group_by(...).agg(...).collect()`. The query optimizer fuses operations, prunes unused columns, and pushes filters down. Common 2–10× speedup with no behavioural change.

### 6b. Streaming for files larger than RAM

For datasets where you can't load everything into RAM (relevant when bronze JSONs in `data/bronze/questions/` get larger), `LazyFrame.collect(streaming=True)` processes the file in chunks. Polars 1.x has streaming for most ops; spot-check by running with `streaming=True` and verifying outputs.

### 6c. Categorical encoding for high-cardinality strings

For columns repeated at scale (`lobbyist_name`, `public_policy_area`, `member_name`), cast to `pl.Categorical` once, then operate on the smaller integer codes. Helps with both memory and group-by speed.

### Effort & wins

~1 day for 6a across the pipeline. 6b/c are spot fixes for specific bottlenecks once measured.

---

## Phase 7 — partition gold parquets by year

For tables with strong temporal access patterns (`current_dail_vote_history`, `payments_fact`, lobbying contact detail), write hive-partitioned parquets:

```
data/gold/parquet/lobbying_contact/year=2024/data.parquet
data/gold/parquet/lobbying_contact/year=2025/data.parquet
```

DuckDB prunes partitions automatically when a query has a `year = ?` predicate. A "show this politician's 2024 returns" query reads one partition file instead of the whole table.

### Tradeoff

More files = more inode pressure on Streamlit Cloud. Only worth it for tables that are >10 MB and have clear temporal queries. Don't partition tables that are small or queried fully.

### Effort

~half a day per table. Defer until a specific page is measurably slow.

---

## Project-specific gotchas worth knowing

- **Polars vs pandas split is real.** Per `project_polars_vs_pandas_split.md`, pipeline = Polars, UI = pandas. Compression refactor goes in pipeline (Polars writers); cache audit goes in UI (pandas + Streamlit).
- **`pipeline_sandbox/` is sandboxed.** Per `project_pipeline_sandbox_rule.md`, performance work touching the canonical pipeline (`pipeline.py`, `enrich.py`, `normalise_join_key.py`) goes in those files directly — but anything experimental (new compression strategies, new lazy chains) belongs in `pipeline_sandbox/` first.
- **Default IO is CSV.** Per `project_pipeline_architecture.md`, the pipeline writes both CSV and Parquet today. Phase 1 keeps that pattern but compresses the CSV; Phase 2 makes the case for dropping the CSV mirror.
- **Streamlit Cloud free tier has memory limits.** Smaller artifacts and faster cold starts (Phase 1 + Phase 3) directly translate to "the app boots before the proxy times out".

---

## Out of scope (explicit non-goals)

- **Replacing parquet with Arrow IPC / Feather.** Faster to read, but loses ecosystem support (DuckDB / S3 / external tools all expect parquet). Not worth the lock-in.
- **Custom binary formats.** The win is measurable but small vs zstd-parquet, and breaks every external integration.
- **Cython / Rust / numba acceleration.** This is a data pipeline, not a compute kernel. The cost is in IO, not Python loops, after Phase 6.
- **GPU.** No.
- **Distributed compute (Dask, Ray).** Pipeline runs on one machine, total dataset is ~3 GB pre-compression, ~1 GB post. A laptop is plenty.

---

## Suggested order of execution

| # | Phase | Effort | Disk saved | Time saved | Impact |
|---|-------|--------|------------|------------|--------|
| 1 | Phase 1 — zstd everywhere | 1 afternoon | ~1.7 GB | small (faster reads) | **High** |
| 2 | Phase 5a — pipeline parallelism | ~1 hour | 0 | minutes per pipeline run | **High** |
| 3 | Phase 3 — persistent DuckDB | ~1 day | ~negligible | ~hundreds of ms cold start | **High** |
| 4 | Phase 4 — Streamlit cache audit | ~1 day | 0 | varies | **Medium** |
| 5 | Phase 6a — Polars lazy pass | ~1 day | 0 | 2–10× on hot pipeline paths | **Medium** |
| 6 | Phase 5c — HTTP cache for dev | 30 min | 0 | minutes per dev iteration | **Medium** |
| 7 | Phase 2a — keep .csv.zst, document | 30 min | already in Phase 1 | 0 | **Low** (hygiene) |
| 8 | Phase 5b — incremental pipeline | days per domain | 0 | minutes per pipeline run | **Medium**, deferred |
| 9 | Phase 7 — partitioned parquets | ~half day per table | 0 | varies | **Low**, on-demand only |
| 10 | Phase 2b — drop CSV mirrors entirely | 1 day | ~175 MB | small | **Low**, opinion-dependent |

The first three items are the bulk of the realistic prize: most of the disk saved, most of the cold-start saved, most of the pipeline runtime saved. Items 4–6 are quality-of-life wins. Items 7–10 are situational.

---

## Open questions / decisions to make

- **Drop the CSV mirrors entirely?** Phase 2b is a stylistic call. If `head`/Excel access is something you actually do, keep them. If not, drop them.
- **Where should the persistent DuckDB file live?** `data/dail_tracker.duckdb` (next to gold) is the obvious place, but it's an artifact, so it could go in `build/` and not be tracked. Decide before Phase 3.
- **What compression level?** Default to 3 in pipeline writers; reserve 19 for an explicit `--archival` flag on the pipeline. Easy to change later.
- **Is Phase 6 worth doing pre-CI/CD?** If pipeline runs stay manual / occasional, no. If you wire up scheduled CI runs (Phase 6 in `CI_CD.md`), pipeline runtime starts to matter and the lazy pass earns its keep.
- **Does the bronze data ever leave your laptop?** If yes (S3, backup), Phase 1 is mandatory — the JSON ratio (24×–248×) translates directly to bandwidth and storage cost. If everything stays local, it's only about disk space.

---

## How to measure wins

For each phase, capture before/after numbers:

```python
# scripts/bench_pipeline.py
import subprocess, time, shutil
def measure():
    sizes = {l: shutil.disk_usage(f"data/{l}").used for l in ["bronze","silver","gold"]}
    t0 = time.time()
    subprocess.run(["python", "pipeline.py"], check=True)
    return {"sizes": sizes, "wall_seconds": time.time() - t0}
```

Numbers go in `doc/PERFORMANCE_LOG.md` after each phase lands. Without measurement these phases are vibes.
