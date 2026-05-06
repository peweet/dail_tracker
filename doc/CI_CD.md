# CI/CD plan for Dáil Tracker

A phased plan for adding continuous integration and (eventually) deployment automation to this repo. The shape is dictated by what *this* project actually is: a Python data pipeline (Polars / DuckDB / pandas) feeding a Streamlit UI, with pipeline outputs persisted as Parquet and surfaced through SQL views.

The goal is not to bolt on every possible check. It is to automate the things that have already broken and the things that are tedious to verify by hand.

---

## Starting point (Phase 0 — what already exists)

- `pyproject.toml` configures ruff and pytest:
  - `ruff` lint rules: `E`, `W`, `F`, `I`, `UP`, `B`, `SIM`; line length 120; target `py311`.
  - `pytest`: `testpaths = ["test"]`, `pythonpath = ["."]`.
  - Dev extras: `pytest`, `pytest-cov`, `ruff` under `[project.optional-dependencies].dev`.
- No `.github/` directory yet.
- No `test/` directory yet — `pytest` will exit code 5 ("no tests collected") until a placeholder lands.
- Repo lives at `github.com/peweet/dail_tracker` (per `pyproject.toml`).
- Streamlit entry: `utility/app.py`. Pipeline entry: `pipeline.py`. Health-check script already exists: `pdf_endpoint_check.py`.

That covers tooling config. Everything below is workflow, automation, and policy on top.

---

## Phase 1 — Basic CI on push (the minimum)

**Goal:** every push and PR runs lint + tests on Ubuntu, Python 3.11. Green check = "doesn't fail to import, doesn't fail ruff, smoke test passes."

**Workflow file:** `.github/workflows/ci.yml`

```yaml
name: CI

on:
  push:
  pull_request:

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip
      - run: pip install ruff
      - run: ruff check .
      - run: ruff format --check .

  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: pip
      - run: pip install -e ".[dev]"
      - run: pytest
```

**Why two jobs and not one?** They run in parallel, you see *which* failed, and the lint job is fast (~5 s installing only ruff) so the feedback loop on style errors is short.

**Tradeoff:** the `test` job installs the full dev extras (`polars`, `pandas`, `numpy`, `PyMuPDF`…). First run is slow. `cache: pip` keyed on `pyproject.toml` makes repeat runs fast.

**Required to land:** a placeholder `test/test_smoke.py` so pytest has at least one test to collect.

---

## Phase 2 — Project-specific smoke tests (catch the real bugs)

The bugs *this codebase* hits are not generic Python bugs. They are: SQL views that fail to bind because a parquet column was renamed, Streamlit pages that crash on import because of a stray symbol, pipeline scripts that produce parquets without an expected column.

A few targeted smoke tests would have caught actual recent issues. Listed roughly in priority order.

### 2a. SQL view bootstrap smoke

The lobbying page rendered "Most-lobbied politicians: Not yet available" because four `sql_views/lobbying_*.sql` files referenced a column (`unique_member_code`) that was missing from `most_lobbied_politicians.parquet`. The DuckDB connection bootstrap exploded silently and every fetch fell through to an empty DataFrame.

A test that does what `utility/data_access/lobbying_data.get_lobbying_conn()` does — open a DuckDB connection and load every `sql_views/lobbying_*.sql` — would have caught the binder error before merge.

```python
# test/test_sql_views.py
from pathlib import Path
import duckdb
import pytest

VIEW_GROUPS = {
    "lobbying":   "sql_views/lobbying_*.sql",
    "attendance": "sql_views/attendance_*.sql",
    "vote":       "sql_views/vote_*.sql",
    "payments":   "sql_views/payments_*.sql",
    "legislation":"sql_views/legislation_*.sql",
}

@pytest.mark.parametrize("name,glob", VIEW_GROUPS.items())
def test_view_group_bootstraps(name, glob):
    conn = duckdb.connect()
    for sql_file in sorted(Path(".").glob(glob)):
        conn.execute(sql_file.read_text(encoding="utf-8"))
```

**Caveat:** the views read from `data/gold/parquet/*.parquet`, which may not be committed. Either (a) commit a tiny synthetic fixture parquet for each referenced source, (b) build the parquets in CI by running a trimmed pipeline, or (c) only run this test when the data dir is present. Cleanest is (a) — a `test/fixtures/` directory of toy parquets that match the column contracts of the real ones. Then the smoke test is also a *contract* test: any time the pipeline output schema drifts, this test fails.

### 2b. Parquet schema contracts

A companion to 2a. For each gold parquet, assert the column set matches what the SQL views expect:

```python
# test/test_parquet_contracts.py
import duckdb, pytest

CONTRACTS = {
    "data/gold/parquet/most_lobbied_politicians.parquet": {
        "full_name", "chamber", "lobby_returns_targeting",
        "distinct_orgs", "total_returns",
    },
    # …
}

@pytest.mark.parametrize("path,expected", CONTRACTS.items())
def test_parquet_columns(path, expected):
    cols = {r[1] for r in duckdb.connect()
            .execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()}
    missing = expected - cols
    assert not missing, f"{path} missing columns: {missing}"
```

This is the cheapest way to make pipeline output a *contract* the views can rely on. Either the pipeline keeps producing the columns, or this test fails and forces the conversation.

### 2c. Streamlit page import smoke

Most Streamlit page bugs are import-time crashes (a missing symbol, a typo in a `from … import`). A test that imports each page module without running Streamlit catches those:

```python
# test/test_page_imports.py
import importlib, pytest
from pathlib import Path

PAGES = [p.stem for p in Path("utility/pages_code").glob("*.py")
         if not p.name.startswith("_")]

@pytest.mark.parametrize("page", PAGES)
def test_page_imports(page):
    importlib.import_module(f"utility.pages_code.{page}")
```

Won't catch runtime/Streamlit-context bugs but catches every import-time regression for free.

### 2d. Lobby/pipeline name-normalisation parity

`normalise_join_key.normalise_df_td_name` is the canonical TD-name normaliser (lowercase + NFD + strip + sort letters). The lobbying SQL views use `LOWER(strip_accents(TRIM()))` — close, but not identical. A small property test that asserts both forms produce the same key for a battery of known names (Micheál Martin, Mary Lou McDonald, etc.) would surface drift.

---

## Phase 3 — Repo hygiene & supply-chain checks

Things that aren't code-quality gates but pay off over time.

### 3a. Pre-commit hooks (mirror CI locally)

A `.pre-commit-config.yaml` with `ruff` and `ruff-format` runs the same checks before commit. Stops you pushing a known-failing change.

```yaml
repos:
  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.11.0
    hooks:
      - id: ruff
        args: [--fix]
      - id: ruff-format
```

Install via `pre-commit install`. The CI run becomes a backstop, not the only line of defence.

### 3b. Dependabot

`.github/dependabot.yml` opens PRs when a dependency releases a new version. For this repo: weekly cadence on `pip` and `github-actions` ecosystems is plenty.

```yaml
version: 2
updates:
  - package-ecosystem: pip
    directory: "/"
    schedule: { interval: weekly }
  - package-ecosystem: github-actions
    directory: "/"
    schedule: { interval: weekly }
```

### 3c. CodeQL (security scanning)

Free for public repos. Add via `Settings → Security → Code scanning → Set up`. Catches a class of real bugs (SQL injection, path traversal) and is essentially free in maintenance terms.

### 3d. pip-audit (known CVEs)

Add to the lint job, or as a third job. Fails CI if any installed dep has a known vulnerability:

```yaml
- run: pip install pip-audit
- run: pip-audit
```

### 3e. Branch protection on `main`

Once CI is green-on-main consistently: `Settings → Branches → main → Require status checks to pass`. Forces every change to go through PR + green CI. Worth doing only after CI is reliable; doing it too early just blocks you from working.

---

## Phase 4 — Scheduled jobs (cron-style)

GitHub Actions can run on a `schedule:` trigger. Useful for things that should happen periodically without a push.

### 4a. PDF endpoint health check

`pdf_endpoint_check.py` already exists. Wrap it in a workflow that runs weekly and opens an issue if any URL 404s. Detects link rot on the Oireachtas side before users hit it.

### 4b. Lobbying freshness check

Per `project_lobbying_automation.md`, lobbying.ie ingestion is manual CSV. A weekly job that hits the DevTools XHR endpoint for the lobbying.ie register and compares the latest published date to the latest `lobbying_period_end_date` in the most recent gold parquet. Open an issue when stale > N days.

### 4c. TODO inventory

Grep for `TODO_PIPELINE_VIEW_REQUIRED` and `TODO_PIPELINE_REQUIRED` markers across the repo, post a weekly summary as a comment on a tracking issue. Keeps deferred pipeline work visible.

### 4d. Pipeline run (the hard one — see Phase 6)

A scheduled run of `pipeline.py` that refreshes the parquet outputs. Discussed separately below because it is genuinely hard.

---

## Phase 5 — CD (deployment)

### 5a. Streamlit Community Cloud

Streamlit Cloud auto-redeploys the app on every push to `main`. No workflow needed. Worth documenting in the repo README so future-you knows the deploy mechanism.

Required: the repo + `requirements.txt` (or a Streamlit-Cloud-compatible `pyproject.toml`), a `streamlit_app.py` entry, and any secrets configured in the Streamlit Cloud UI.

### 5b. Docker image (optional)

If you ever want to host elsewhere (Fly.io, Render, your own VM), publishing to GitHub Container Registry on every tag is straightforward:

```yaml
on:
  push:
    tags: ["v*"]
jobs:
  publish:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - uses: docker/build-push-action@v5
        with:
          push: true
          tags: ghcr.io/peweet/dail_tracker:${{ github.ref_name }}
```

Defer until there's a reason. Streamlit Cloud is free; Docker is for when you need a different host or self-host.

---

## Phase 6 — Data pipeline automation (the ambitious one)

The pipeline produces Parquet files that the app reads. Today those are produced manually on your machine and committed (or ignored). A scheduled job that re-runs `pipeline.py` and refreshes the data is the highest-value automation you could build — and the most fiddly.

**Options:**

1. **Scheduled CI job, commit results back.** Run `python pipeline.py`, push refreshed parquets to `main`. Pros: simple, visible. Cons: bloats git history, parquet files are not great in git, secrets exposed if any source is gated.
2. **Scheduled CI job, attach to a release.** Run pipeline, upload parquets as release assets. The app downloads them at startup. Pros: keeps git clean. Cons: app needs network at start.
3. **Scheduled CI job, push to S3 / R2.** Same as (2) but using object storage. Pros: scalable. Cons: requires cloud account + secrets.
4. **External scheduler (Modal, Prefect, Airflow).** Pros: built for this. Cons: another system to learn and pay for.

**Recommendation when you get here:** start with (1) on a `data` branch (so `main` history stays clean) or (2) once parquets get large.

**Prerequisites before this is worth building:**
- Pipeline must run end-to-end on a fresh machine (no manual intervention, no machine-local config).
- Lobbying CSV ingestion needs the automated path from `project_lobbying_automation.md` to land first.
- Tests for parquet schema contracts (Phase 2b) become *required*, not optional — broken outputs would silently break the deploy.

---

## Project-specific gotchas worth knowing

These would have already caught real bugs:

- **SQL view bootstrap is order-sensitive and unguarded.** `get_lobbying_conn()` loops `sorted(_SQL_VIEWS.glob("lobbying_*.sql"))` and calls `conn.execute()` with no try/except. The first broken view kills the whole connection. Phase 2a smoke catches this in CI.
- **Parquet column drift is silent.** The pipeline produces a parquet, a view reads it. If a column is renamed or removed, the view fails at bind time and the page shows empty data. Phase 2b contract tests catch this at PR time.
- **`pipeline.py` shells out via subprocess.** Per `project_pipeline_architecture.md`, `pipeline.py` runs top-level scripts via `subprocess`. Tests can't mock those calls trivially; integration testing the full pipeline needs CI to actually run it. Until that's set up, focus on importing scripts (catches NameError) and unit testing pure functions inside them.
- **Polars vs pandas split is real.** Per `project_polars_vs_pandas_split.md`, server-side ETL is Polars and UI is pandas. CI tests should mirror this — pipeline tests use Polars, page tests use pandas. Don't introduce one into the other accidentally.
- **`pipeline_sandbox/` is intentionally non-canonical.** Per `project_pipeline_sandbox_rule.md`, code there must not be imported by `pipeline.py`. A static check (grep for `from pipeline_sandbox` in non-sandbox files) is a one-line CI guard.

---

## Out of scope (explicit non-goals)

Things to deliberately *not* build, at least until you actually need them:

- **mypy / pyright type checking.** The codebase isn't fully typed. Adding strict type checks now would be mostly churn. Add when there's a part of the code you want to lock down, not as a global gate.
- **Multi-OS matrix.** Ubuntu is fine. Don't pay the wall-clock cost of also testing macOS/Windows on every push for a Streamlit app that runs on Linux.
- **Multi-Python-version matrix.** `pyproject.toml` claims 3.11–3.13, but in practice you run on one. Pin CI to 3.11; expand only if you start distributing as a library.
- **Coverage thresholds.** Premature when there are no tests. Add `pytest-cov` reporting (no failure threshold) once tests exist; only add a threshold if there's an actual regression coverage drops below 0.
- **Semantic-release / auto-versioning.** Solo project. Tag releases manually when there's something to release.
- **Heavy E2E browser tests of Streamlit.** Possible (Playwright + `streamlit run`) but slow and brittle. Per-page import smoke (Phase 2c) covers 80% of what matters.

---

## Suggested order of execution

| # | Task | Effort | Value |
|---|------|--------|-------|
| 1 | Phase 1 workflow + smoke test | 30 min | High |
| 2 | Phase 2c page-import smoke | 1 h | High |
| 3 | Phase 2a SQL view bootstrap smoke (with synthetic fixtures) | 2–4 h | High |
| 4 | Phase 3a pre-commit | 30 min | Medium |
| 5 | Phase 3b Dependabot | 10 min | Medium |
| 6 | Phase 2b parquet contracts | 1–2 h | Medium |
| 7 | Phase 3c CodeQL | 5 min | Medium (if public) |
| 8 | Phase 4a PDF endpoint check on schedule | 30 min | Medium |
| 9 | Phase 3e branch protection on `main` | 5 min | High once CI is reliable |
| 10 | Phase 4b lobbying freshness check | 2 h | Medium |
| 11 | Phase 6 pipeline automation | days | High but blocked on prerequisites |

The first three items take a single afternoon and cover the failure modes this repo actually hits. Everything below them is incremental.

---

## Open questions / decisions to make

- **Is the repo public or private?** CodeQL and Dependabot are free on public repos and limited on private free tier. The `peweet/dail_tracker` URL suggests public — confirm before relying on those.
- **Do parquet files belong in git?** If yes, Phase 2 fixtures can reuse the real ones. If no, fixtures need to be small synthetic copies.
- **Where does the deployed Streamlit app live today?** Streamlit Community Cloud, a VM, locally only? CD plan branches based on the answer.
- **What's the lobbying data refresh cadence target?** Quarterly ≈ once per pipeline run. Monthly = automated job. Decide before Phase 6.
- **Do you want pre-commit hooks running ruff `--fix` on every commit, or just check?** `--fix` auto-corrects (faster), check-only forces you to think about each warning (slower, more deliberate).
