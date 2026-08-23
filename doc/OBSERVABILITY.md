# Observability — logging, pipeline tracing, source traffic light

**Status:** shipped 2026-07-20. Stdlib only (plus `orjson`, already a dep). No new packages.

One idea runs through all of it: **structured JSON on stdout, correlated by an id, emitted
only when something is worth recording.** That is what survives the move to cloud, where the
filesystem-shaped mental model (open `logs/runs/<run_id>/`, read the step files) stops working
because there is no durable filesystem — only a stream a collector ingests.

---

## The one primitive

`services/logging_cloud.py` is the whole engine. Three public seams:

| Seam | What it does |
|---|---|
| `configure_logging()` | Installs JSON-to-stdout logging (unguarded `dictConfig` — the uvicorn-safe path) |
| `bind_context(run_id=…, step=…)` | Stamps process-wide fields onto every line — the cross-process correlation key |
| `log_event(name, **fields)` | Emits one structured line `{event: name, …}` — the tracing verb |

`cloud_mode()` (env `DAIL_LOG_CLOUD=1` or `LOG_FORMAT=json`) gates all of it. Unset on a laptop,
so local runs keep rotated text files under `logs/` exactly as before.

---

## Three consumers, one shape

### 1. API request logging

`api/main.py` — a middleware binds a request id (reusing an inbound `X-Request-ID`), logs one
line per request with method/path/status/`duration_ms`, and surfaces unhandled exceptions and
`SourceUnavailable` outages. Slow requests (>1 s) log at WARNING.

### 2. Pipeline tracing

`pipeline.py::_run_chain` emits one `pipeline_step` event per chain — step, ordinal, status,
`exit_code`, `duration_ms` — carrying the run's `run_id`. Cloud-only: a local run already has
the per-step files under `logs/runs/<run_id>/steps/`, so tracing there would be the "useless
data" the trace exists to avoid. Example line:

    {"event":"pipeline_step","step":"source_health","status":"ok","duration_ms":812.4,
     "run_id":"2026-07-20T22-00-00Z-trace01", …}

On cloud, `event:pipeline_step` filtered by `run_id` is the whole run as a trace — the
streamable form of what `manifest.json` records to disk.

### 3. Source traffic light — `tools/migration/check_source_cadence.py`

`build_source_health.py` writes a **snapshot** (ok/warning/failed/skipped, recomputed and
overwritten each run). A snapshot cannot tell a blip from an outage — one failed check looks
the same whether the source hiccuped or was taken down a fortnight ago. The cadence checker
adds the missing dimension, **time**, on top of the cadence axis it already carried.

> **History (2026-07-21):** this started as a separate `source_health_ledger.py`, which
> duplicated the cadence checker's health classification. The two were folded into ONE tool —
> `tools/migration/check_source_cadence.py` — per the "unify the traffic light" decision. It
> lives in the migration toolkit because its cadence ledger (`source_cadence.csv`) is still
> being curated; it returns to operational `tools/` and CI once curation is done.

Two axes, one status per source (worst first):

| Status | Axis | Meaning |
|---|---|---|
| `TAKEN_DOWN` | health | health failing continuously past 14 days — presumed gone |
| `BROKEN` | health | health check failed (fresh or recent) — a real problem now |
| `OVERDUE` | cadence | newest pull older than cadence × grace |
| `DUE` | cadence | a known release window passed; newest pull older than one cadence |
| `REVIEW` | cadence | cadence not yet human-curated (`curated!=yes`) |
| `UNKNOWN` | cadence | no freshness signal (health skipped / never run against bronze) |
| `OK` / `STATIC` | cadence | within cadence / one-off historical file |

The health axis needs **no** curation — a `BROKEN`/`TAKEN_DOWN` source is flagged even while
its cadence row is still `REVIEW`. Telling `BROKEN` from `TAKEN_DOWN` needs memory, so a tiny
state file (`data/_meta/source_cadence_state.json`) records when each source first went bad;
duration is measured off wall-clock, so irregular check cadence doesn't distort it.

    python tools/migration/check_source_cadence.py --due-only   # only the problems
    python tools/migration/check_source_cadence.py --strict     # exit 1 on OVERDUE/BROKEN/TAKEN_DOWN

**Two dependencies to make each axis meaningful:** the health axis needs a snapshot with real
signal — a run with `DAIL_CHECK_LINKS=1` (offline, online sources are `skipped`). The cadence
axis needs `source_cadence.csv` rows curated (`curated=yes`); until then they read `REVIEW`.

---

## Cloud-agnostic: what reads this on each host

The contract is the lowest common denominator — **JSON on stdout, one object per line** — so
every target consumes it natively, no per-vendor code:

| Host | Collection | Set |
|---|---|---|
| Hetzner / any VM | Promtail/Vector → Loki; Grafana queries `run_id`/`event` | `LOG_FORMAT=json` + Promtail |
| AWS | ECS stdout → CloudWatch; Logs Insights filters fields | `LOG_FORMAT=json` in task def |
| GCP | Cloud Run stdout → Cloud Logging `jsonPayload.*` | `LOG_FORMAT=json` |
| Local | rotated files under `logs/` | leave env unset |

**Not overwhelmed with detail** — the levers: `LOG_LEVEL` per process; `_NOISY_LIBRARIES`
(httpx etc.) pinned to WARNING; request/run grouping; and events emitted on change, not on
tick. A dashboard reads two log signals — `event:request` (API) and `event:pipeline_step`
(runs) — each carrying `run_id`/`request_id`; source health is a periodic canary
(`check_source_cadence --strict`), not a per-tick log stream.

## Deliberately not built

- **Alerting sink** (Sentry / PagerDuty) — needs an account decision; the ERROR events are the
  hook when one exists.
- **Severity field** for GCP/CloudWatch native error highlighting — one formatter line, worth
  adding once a host is chosen.
- **Scheduled `--check-links` run** — the thing that makes the health axis fully live.
- **`source_cadence.csv` curation** — 165 rows are `curated=no`, so the cadence axis reads
  `REVIEW`. Curating them (operator knowledge of release calendars) activates DUE/OVERDUE.
- **App-surface for the traffic light** — a Streamlit/React panel over the cadence rollup; the
  data is ready, the UI is not.

## Files

| File | Role |
|---|---|
| `services/logging_cloud.py` | JSON logging, context binding, redaction, `log_event` |
| `services/logging_setup.py` | ETL logging; delegates to cloud mode when opted in |
| `api/main.py` | request-id middleware + request/error events |
| `tools/migration/check_source_cadence.py` | the source traffic light (cadence + health-duration) |
| `pipeline.py` | `pipeline_step` trace + `source_ledger` chain |
| `tools/source_health_ledger.py` | the traffic light with memory |
| `tools/build_source_health.py` | the snapshot it reads (pre-existing) |
| `test/services/test_logging_cloud.py`, `test_logging_setup_cloud_delegation.py`, `test/tools/test_source_health_ledger.py` | tests |
