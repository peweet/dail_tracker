# Logging hardening plan

**Date:** 2026-06-02
**Status:** Plan only — nothing implemented yet. Phases 1 & 2 are ready to build on `main`; Phase 3 is deliberately deferred to go-live.
**Origin:** Logging assessment found two parallel logging systems, a 92 MB unbounded log file, and ~120 MB of hand-redirected stray `.log` files. This doc is the agreed remediation design.
**Related:** ties to `REORG_AUDIT.md` finding **D2** ("collapse the 9 duplicated chain step-runners into one") — the chain-logging fix in Phase 2 is the same code touch, done in one pass. Phase 3 retention belongs with `doc/CI_CD.md` / `doc/CICD_TODO.md` scheduled jobs.

---

## Why this is needed (plain summary)

The project has a genuinely good **per-run logging system**: every time you run the full pipeline (`python pipeline.py`), it creates a timestamped folder `logs/runs/<run_id>/` containing the orchestrator log, one log file per chain, and a `manifest.json` recording what ran, how long it took, exit codes, and the git commit. That part is well designed — keep it.

The problem is that this system is **only used by the full pipeline**. The newer per-domain chain scripts (`attendance_refresh.py`, `iris_refresh.py`, … — 9 of them) do **not** plug into it. When you run one on its own, nothing is saved to a file. So in practice we've been capturing their output by hand with shell redirects (`python attendance_refresh.py > attendance_run.log`), and those hand-made files have piled up at the repo root and in `tmp/`. Separately, an old catch-all log file (`logs/pipeline.log`) has no size limit and grew to 92 MB.

None of these files are committed to git (they're all ignored), so this is a tidiness / "can I find what happened" problem, not a security or data-leak problem. It becomes genuinely important at go-live, when we need to be able to look back at unattended pipeline runs.

---

## The one idea that shapes the whole fix

The chain scripts print their progress with plain `print()` statements (the `──── [1/6] … ────` banners, the "done in 4.2s" lines, the Seanad summaries). Python's logging file system **cannot capture `print()` output** — so simply "turning on logging" in a chain would save an almost-empty file while the useful output still disappears.

The full pipeline already gets around this: instead of relying on logging handlers, it **copies the entire output stream** of each chain into a file as the chain runs (a "tee" — like the Unix `tee` command, output goes to the screen *and* a file at once).

**So the fix for standalone chains is the same trick: copy their whole output stream to a file.** This is simpler than converting hundreds of `print()` calls into logging calls, and it captures everything — both `print()` and any logging — into one file. It also keeps the console output clean (no timestamps bolted onto every banner line).

---

## Phase 1 — Stop the bleeding *(do now; small, low-risk, ~30 min)*

| # | Change | What it does, in plain terms |
|---|---|---|
| 1.1 | **One shared log format** | Today the full pipeline and the chains format log lines slightly differently (different separators and field order). Define the format string in one place (`services/logging_setup.py`) and have everything import it, so all logs look the same and are easy to read/grep. |
| 1.2 | **Put a size cap on the old catch-all file** | `logs/pipeline.log` currently grows forever (that's the 92 MB file). Cap it so it can never exceed ~10 MB; when it fills, it automatically starts a fresh file and keeps only the last 3 (Python's built-in `RotatingFileHandler`). One-line change in `logging_setup.py`. See **Decision A** below for the plain-English choice here. |
| 1.3 | **One-time cleanup of stray logs** | Delete the ~120 MB of accumulated junk: the hand-made logs at the repo root (`attendance_run.log`, `pipeline_run.log`, `dbsect_after_pipeline.log`, `endpoint_check.log`, `streamlit_test.log`, root `pipeline.log`), the stale `services/logs/` folder, the 11 files under `tmp/*.log`, the 92 MB `logs/pipeline.log`, and the half-finished `logs/runs/2026-05-31…` run folder. All are git-ignored, so deleting them has no effect on the repository. |

Phase 1 by itself removes the runaway-file problem and the formatting inconsistency.

---

## Phase 2 — Make the chain scripts save their own logs *(do now; the real fix, ~1–2 hrs)*

This is the part that actually stops new stray logs from ever being created, because there'll no longer be any reason to redirect output by hand.

### 2.1 — Add one shared helper: `chain_logging(chain_name)`

A small context manager in `services/logging_setup.py` that each chain wraps its work in. Its logic:

```python
@contextmanager
def chain_logging(chain_name: str):
    if os.environ.get(ENV_RUN_ID):
        # Case 1: we were started BY the full pipeline (python pipeline.py).
        # The parent is already copying our output to a file — do nothing extra.
        setup_logging()                      # console only
        yield None
        return

    # Case 2: someone ran this chain on its own (python iris_refresh.py).
    # Act as our own mini-pipeline: make a run folder, a manifest, and capture output.
    run_id = make_run_id()
    os.environ[ENV_RUN_ID] = run_id          # so any sub-scripts we launch cooperate too
    setup_logging()                          # console only (the tee below owns the file)
    create_run_manifest(run_id)              # full-parity manifest — agreed decision
    log_path = run_dir(run_id) / "pipeline.log"
    with _tee_stdio(log_path):               # copy ALL output (print + logging) to the file
        try:
            yield run_id
        finally:
            run_finished_at(run_id)          # finalise manifest (status, duration)
```

`_tee_stdio(path)` is a tiny context manager that replaces `sys.stdout`/`sys.stderr` with a writer that forwards everything to both the console and `path` (UTF-8, line-buffered — reusing the same encoding care the pipeline already takes for `→`/`é` on Windows).

**Two subtle but important points (so a future reader doesn't "fix" them):**
- **No duplicated lines.** We deliberately do *not* attach a logging file-handler in the standalone case. The tee owns the file; logging only writes to the console, which the tee then copies. One mechanism writing the file = no double-writing.
- **Sub-scripts are captured for free.** When a standalone chain launches a step (e.g. `attendance_refresh` runs `attendance.py`), that child inherits the already-tee'd output stream, so the child's output lands in the same run-folder file automatically — no extra plumbing per chain.

### 2.2 — Decision (agreed): standalone runs get a full manifest

Standalone chain runs will write a `manifest.json` + a rollup row + update `latest_run_id.txt`, exactly like a full pipeline run. The manifest code (`manifest.py`) already supports this; it just isn't called from the chains today. This gives every run — full or single-chain — the same "what happened" record.

### 2.3 — Apply to all 9 chains

In each `*_refresh.py`, replace the `logging.basicConfig(...)` line with wrapping the body of `main()` in `with chain_logging("<name>"):`.

**Before** (`attendance_refresh.py`):
```python
def main() -> int:
    argparse.ArgumentParser(...).parse_args()
    logging.basicConfig(level=logging.INFO, format="…")   # <-- remove
    started = time.monotonic()
    ...
```
**After:**
```python
def main() -> int:
    argparse.ArgumentParser(...).parse_args()
    with chain_logging("attendance"):
        started = time.monotonic()
        ...
```

Notes per chain:
- `seanad_refresh.py` currently configures **no** logging at all, so its `_log.info(...)` messages are silently dropped today — this wires it up for the first time.
- All 9 chains already import the project root from the shared `paths.py`, so there's no path work to do here.

**Outcome:** `python iris_refresh.py` on its own now produces `logs/runs/<id>/pipeline.log` + a manifest entry, just like the full pipeline. Running under `python pipeline.py` is unchanged (the chain detects it's a child and lets the parent do the capturing). The motivation to ever hand-redirect output disappears, so no new stray logs.

---

## Phase 3 — Go-live grade *(deferred; decide now, switch on at launch)*

Log history matters little during alpha but becomes important once the pipeline runs unattended in production. These are designed now and enabled later:

- **3.1 Retention / auto-cleanup.** A `prune_old_runs(days=…)` function already exists in `services/run_paths.py` but is currently commented out in `pipeline.py`. At go-live, either enable it there or (better) run it from the scheduled CI job described in `doc/CI_CD.md`. Open choice: retention window (suggest 90 days).
- **3.2 Revisit the size cap** from Phase 1.2 once we know real run volumes.
- **3.3 Upload logs as CI artifacts.** The run-folder layout was designed to be zip-friendly for exactly this. On a failed scheduled run, upload `logs/runs/<id>/` so failures can be inspected after the fact. Add a line to `doc/CICD_TODO.md`.
- **3.4 Adjustable detail level + optional machine-readable logs.** A `DAIL_LOG_LEVEL` environment switch, and optionally a JSON log format for ingestion tools. Cheap once the format is centralised in Phase 1.
- **3.5 Failure alerting.** Out of scope here; noted as the seam where this meets the notification work in `test/HANDS_OFF_TEST_PLAN.md`.

---

## Decision A — the old catch-all file (plain English)

There's an old, shared log file at `logs/pipeline.log`. A handful of smaller scripts (`iris_si_bill_enrichment.py`, `si_entity_enrichment.py`, `services/dbsect_harvest.py`, `services/oireachtas_api_main.py`) write to it when run on their own. Two ways to handle it:

- **Option A (recommended, Phase 1.2): keep it, but cap its size.** Tell it "never grow past ~10 MB; when full, start fresh and keep the last 3." Tiny change, nothing else moves. This is what the plan assumes.
- **Option B: get rid of it entirely.** Make even these small scripts create their own timestamped run folder, like everything else. Tidier and fully consistent, but it changes how a shared logging function behaves and touches those 4 scripts, so slightly more work and slightly more risk.

**Recommendation: Option A now** (it solves the only real problem — runaway size — with one line), and consider Option B later if we want everything to be perfectly uniform. *This is the only open question in the plan; everything else is agreed.*

---

## Risks & how we'll check it works

- **Accidental double-capture.** A chain run *under* the full pipeline must not also create its own second run folder. The `ENV_RUN_ID` check prevents this; we'll confirm `python pipeline.py --select attendance` produces exactly one run folder.
- **Windows character encoding.** The tee must force UTF-8 (as the pipeline already does) so arrows/accents survive the default Windows console encoding.
- **No duplicated handlers.** `setup_logging` already refuses to add handlers twice; verify chains don't import a step module that configures logging before `chain_logging` runs.
- **Acceptance checks:**
  1. `python attendance_refresh.py` → exactly one `logs/runs/<id>/pipeline.log` containing the banners, plus a manifest row.
  2. `python pipeline.py --select iris` → one run folder, no nested folder; iris output still captured as before.
  3. Run a standalone script (e.g. `si_entity_enrichment.py`) repeatedly → `logs/pipeline.log` stays under the cap instead of growing without limit.

---

## Effort & order

1. **Phase 1** (~30 min): shared format, size cap, one-time cleanup.
2. **Phase 2** (~1–2 hrs): the `chain_logging` helper + wiring all 9 chains. This is the payoff — standalone runs become self-documenting and stray logs stop appearing.
3. **Phase 3** (at go-live): retention, CI artifact upload, detail-level switch, alerting.

Phases 1–2 are low-risk, land directly on `main`, and are not blocked by the larger `src/` reorg. When the reorg happens, the single `chain_logging` helper is the natural home for the consolidated chain-runner (REORG_AUDIT D2).
