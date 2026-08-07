# Framework decoupling plan — safeguarding the app against any one framework

**Status:** active decoupling. Migration is not planned; framework-neutral safeguards are being shipped incrementally.
**Scope:** reduce dependency on Streamlit specifically, and on any single framework
generally. Siting is **out of scope** here (beta; its API shape is a separate design
question — a POST compute endpoint, not a GET read).

## Current implementation status (2026-08-07)

Shipped without changing the Streamlit product:

- migration ratchets are wired into CI and the focused verification lane;
- every retrieval module uses the framework-neutral cache adapter;
- quarantine rendering is separated from quarantine-ledger access;
- the order-dependent shared stylesheet is a reusable static CSS asset;
- a machine-readable frontend acceptance contract freezes routes, parameters, classes, and CSS hashes;
- cloud logging and request correlation are implemented.

Open work remains page/domain shaped: frontend-ready API read models, pure URL-state
normalisation for complex pages, and reconciliation of the legacy Streamlit-specific
page-contract pack.

The goal is not "move to React." The goal is that **no framework choice is load-bearing**,
so the app survives Streamlit becoming a bad bet — whether that's a breaking release, a
licence change, a scaling wall, or a product decision.

---

## 1. Baseline measured state (not estimated)

The figures below are the pre-decoupling baseline produced by
`python tools/migration/scan_framework_coupling.py` over **1,031 Python modules**. Regenerate
them when a fresh whole-tree survey is needed; the tool is a survey, not a ratchet. The
current data-access boundary is enforced separately by
`test/utility/test_data_access_framework_boundary.py`.

### 1.1 Streamlit reachability by layer

| Layer | Modules | LOC | Direct `import streamlit` | Transitive | Streamlit-free |
|---|---:|---:|---:|---:|---:|
| `dail_tracker_core/` | 57 | 12,444 | 0 | 0 | **57** |
| `api/` | 27 | 1,963 | 0 | 0 | **27** |
| `services/` | 23 | 4,222 | 0 | 0 | **23** |
| `extractors/` + `iris/` | 115 | 41,792 | 0 | 0 | **115** |
| `tools/` | 39 | 7,445 | 0 | 0 | **39** |
| `mcp_server/` | 6 | 3,100 | 0 | 0 | **6** |
| `utility/data_access/` | 28 | 3,348 | 27 | 0 | 1 |
| `utility/ui/` | 15 | 4,648 | 10 | 0 | 5 |
| `utility/pages_code/` | 29 | 29,395 | 28 | 0 | 1 |
| `utility/` (app shell) | 5 | 7,173 | 2 | 0 | 3 |

**The finding that matters: Streamlit does not leak.** Zero modules in core, api or
services reach `streamlit` even transitively. The ETL (41,792 LOC) and the tooling are
likewise clean. Coupling is confined to `utility/`, and it is confined *by construction* —
`test/dail_tracker_core/test_core_no_streamlit_imports.py` and
`tools/check_streamlit_logic_firewall.py` are what keep it that way. **Those two guards are
the single most valuable safeguard this repo has. Keep them green.**

### 1.2 The real Streamlit API surface

61 distinct `st.*` symbols across the whole app. Grouped by what a migration must do:

| Kind | Call sites | Migration cost |
|---|---:|---|
| markup (`st.html` 550, `st.caption` 268, `st.markdown` 83, …) | **933** | Portable — the HTML already exists as strings |
| state (`session_state` 283, `query_params` 128, `rerun` 71, `Page` 30) | **519** | Rebuild — routing/state contract |
| cache (`cache_data` 379, `cache_resource` 19) | **398** | Evaporates — see §2.1 |
| widget (`segmented_control` 42, `selectbox` 37, `pills` 24, `text_input` 23, `button` 17, …) | **174** | Rebuild — genuine framework coupling |
| layout (`columns` 48, `expander` 36, …) | **101** | Portable — becomes CSS grid/flex |
| data_render (`bar_chart` 14, `altair_chart` 10, `dataframe` 8, …) | **38** | Replace with a chart/table library |

**The whole app contains 174 widget call sites.** That is the true size of the framework
lock-in — not the 36,500 LOC of the presentation layer. `st.html` alone (550 sites) is
more than triple every widget combined: this is an HTML generator wearing a Streamlit hat.

---

## 2. Decoupling opportunities, ranked by cost

### 2.1 Implemented — isolate caching behind a framework-neutral adapter

**The baseline showed that 27 of 28 `utility/data_access/` modules used Streamlit only
for caching** (398 `@st.cache_data` / `@st.cache_resource` decorators, zero widgets, zero
state). That finding was verified by AST symbol classification, not by reading.

Heaviest: `procurement_data.py` (80 decorators), `lobbying_data.py` (38), `legislation_data.py` (19),
`judiciary_data.py` (17), `constituency_data.py` / `corporate_data.py` / `housing_data.py` /
`local_government_data.py` (16 each).

**Implemented:** `utility/data_access/_cache.py` exports `cache_data` / `cache_resource`,
delegating to Streamlit when it is installed and falling back to `functools.lru_cache`
otherwise. Retrieval modules import only that adapter; a regression test rejects direct
Streamlit imports anywhere else in `utility/data_access/`.

### 2.2 Implemented — extract the stylesheet

The design system is plain CSS using the project's own class names. It now lives in
`utility/static/dailtracker.css`; `utility/shared_css.py` is a small Streamlit injection
adapter.

The extraction preserved rule order and is guarded by stylesheet-loading and class-contract
tests. Eight additional page/component-local CSS emitters are inventoried and hashed in
`utility/static/frontend_contract.json`; they remain candidates for later consolidation
([corporate.py:118](../utility/pages_code/corporate.py#L118) is 727 lines, plus
`judiciary.py:176`, `statutory_instruments.py:66`, `public_appointments.py:72`).

**Constraint:** the CSS is order-dependent and its families are fragmented (`.dt-*` spans
lines 671–5753). Ship it as **one** stylesheet preserving order. Do not split by prefix
during a migration — that is a second change riding a big one.

### 2.3 Moderate — close the API parity gap

`python tools/migration/check_api_parity.py` reports **161 of 381 core query functions (42.3%)
reachable from the API or MCP**. The remaining 220 are Streamlit-only: retrievable from a
page, invisible to any other client.

Not all 220 need endpoints — many back filter dropdowns (`distinct_years`,
`distinct_members`, `availability`). But the concentration is informative:

| Module | Unexposed / total |
|---|---|
| `lobbying` | 30 / 37 |
| `member_overview` | 20 / 39 |
| `procurement/signals` | 19 / 20 |
| `procurement/payments` | 17 / 18 |
| `procurement/ted` | 9 / 11 |
| `housing`, `judiciary` | 9 / 15, 9 / 16 |

`procurement/signals` and `procurement/payments` are near-totally API-absent, and those are
the commercial surface.

### 2.4 The real work — 174 widget sites and 519 state sites

Concentrated in `procurement.py` (23 widgets, 4,614 LOC), `member_overview.py` (17),
`ui/components.py` (13), `public_appointments.py` (11), `statutory_instruments.py` (10).
This is the irreducible rebuild. Everything above should happen first, because each item
shrinks this one.

### 2.5 The markup contract — freeze the debt, don't pay it down

AST census of what actually feeds `st.html` / `st.markdown` / `st.write` / `st.caption`
across `pages_code/` + `ui/` — **845 call sites**:

| Argument shape | Sites | Share | Meaning |
|---|---:|---:|---|
| f-string (inline template) | 393 | 46.5% | anonymous component |
| constant (static markup) | 266 | 31.5% | anonymous component |
| call (helper fn) | 95 | 11.2% | **named component** — but 58 are `.join()` |
| concatenation | 49 | 5.8% | anonymous component |
| variable (built above) | 37 | 4.4% | near-component |

**78% of markup is written inline at the call site.** Only ~37 of 845 sites (4.4%) come
from a genuinely named markup helper. So the presentation layer is portable in *content* —
it is HTML carrying our own class names, which is why §2.2 works — but it is **not organised
as a component contract**. A migration would have to rediscover 700-odd anonymous
components by reading them.

The counterweight: **111 functions across 27 files already RETURN markup** (`_html_table`,
`_lane_header`, `_render_si_card`, `entity_cta_html`, `clickable_card_link` …). Those are
components in everything but location — private to their page, unnamed as a contract, but
structurally exactly right.

**The move is not to refactor 700 call sites.** It is to stop the number growing, so
migration cost is capped while feature work continues:

    python tools/migration/scan_framework_coupling.py --check-markup

Baselined at **708 inline sites across 34 files** (`tools/baselines/markup_inline_baseline.json`).
A file that grows its inline count fails the check; the fix is to route new markup through a
function that returns the HTML string. That function is the component. Over time the ratchet
converts the presentation layer into a named component library **as a side effect of normal
feature work**, without a refactor project.

This is the single most effective thing available for making a migration painless, because
it is the only one that improves monotonically without dedicated effort.

---

## 3. Drift — the standardized approach

Drift is the failure mode that kills migrations: features land Streamlit-first, every other
surface lags, and the gap is invisible until someone tries to build on it. The answer is
**three ratchets that make drift fail a build instead of accumulating silently**, following
the existing `tools/check_conventions.py` baseline convention.

| Ratchet | Command | Guards against |
|---|---|---|
| **Core purity** (exists) | `pytest test/dail_tracker_core/test_core_no_streamlit_imports.py` | Streamlit leaking into the portable core |
| **Logic firewall** (exists) | `python tools/check_streamlit_logic_firewall.py` | Business logic leaking into pages |
| **API parity** (new) | `python tools/migration/check_api_parity.py` | Features landing Streamlit-only |
| **URL contract** (new) | `python tools/migration/extract_url_contract.py --check` | Deep links changing silently |
| **Markup contract** (new) | `python tools/migration/scan_framework_coupling.py --check-markup` | Anonymous-component debt growing |
| **Class contract** (new) | `python tools/migration/extract_class_contract.py --check` | Styling vocabulary drifting from the CSS |

**Baseline rule, inherited from `check_conventions.py`: never add to a baseline. Only
remove from it.** `tools/baselines/api_parity_baseline.txt` currently holds 220 entries —
today's debt, frozen. A new core query function with no API consumer fails the check.

**Wire all four into the fast test subset**, so drift costs a red build on the day it
happens rather than a discovery six months later.

---

## 3b. Preserving the styling — the class contract

Generated: [CLASS_CONTRACT.md](CLASS_CONTRACT.md) — produced by
`tools/migration/extract_class_contract.py`.

**Styling survives a framework change if, and only if, the new components emit the same
class names against the same stylesheet.** That makes the class vocabulary a contract of
exactly the same kind as the URL parameters — load-bearing, external-facing in effect, and
unwritten until now.

| Measure | Count |
|---|---:|
| Class names emitted by the UI | 993 |
| Class selectors defined in CSS | 1,326 |
| **The contract** (emitted AND styled) | **977** |
| Dead CSS candidates (styled, never emitted) | 321 |
| Unstyled (emitted, never styled) | 16 |

**977 class names is the whole styling contract.** Reproduce those and the design is
identical — not approximated, identical, because it is the same CSS file doing the work.
This is what makes "replicate the Streamlit view" a mechanical goal rather than a
pixel-matching exercise.

Two by-products that pay off **today**, independent of any migration:

- **16 unstyled classes = live defects.** [committees.py:643-754](../utility/pages_code/committees.py#L643)
  emits `comm-office-card`, `comm-member-card`, `comm-member-card-header/title/meta/dates`,
  `comm-member-list`, `comm-office-list`, `comm-chair-pill`, `comm-status` — eleven classes
  with no CSS rule anywhere in the repo. Those committee cards render unstyled right now.
  Confirmed by grep: the strings appear only in `committees.py` and in the baseline.
- **321 dead-CSS candidates**, roughly a quarter of the stylesheet. Each is payload a
  migration would otherwise carry forward for nothing.

**Caveat on the 321: it is an upper bound, not a confirmed count.** The extractor matches
`class="..."` attributes in string constants and f-strings. Markup assembled by
concatenation (`'<div class="' + cls + '">'`) is not detected, so some "dead" selectors are
reachable in ways the AST cannot see. Verify a selector is genuinely unused before deleting
it. The 16 unstyled, by contrast, are sound in the safe direction — a class with no rule is
a class with no rule.

---

## 4. URL contract — why it is worth freezing

Generated: [URL_CONTRACT.md](URL_CONTRACT.md) — **30 routes, 56 query parameters**, produced
by `tools/migration/extract_url_contract.py`.

Everything else in the UI is yours to change. The URL is not. It is the only part of the
presentation layer with **external referents**:

1. **Links already exist in the wild.** Bookmarks, links shared with journalists, links in
   emails and articles. Changing `?member=` breaks them retroactively, and you cannot tell
   who you broke or apologise to them.
2. **It is the SEO surface.** If public dossier pages get indexed, the URL *is* the search
   result. Search engines treat a changed URL as a new page with zero history.
3. **It is the migration acceptance test.** "Did the React router preserve behaviour?" is
   otherwise a matter of opinion. With this table it is a checklist: 30 routes, 56
   parameters, pass or fail. That converts the scariest, least-verifiable part of a
   migration into something a test can assert.
4. **It is the only UI contract that survives a rewrite.** CSS gets reimplemented, widgets
   get replaced, `session_state` is thrown away. These 56 strings must appear, unchanged,
   in whatever comes next. Writing them down now — **while they still match what ships** —
   costs nothing. Reconstructing them after a rewrite means reading 29 page modules and
   hoping.
5. **It surfaces coupling you would otherwise miss.** The generated table flags **shared**
   parameters — `member` is read by 5 modules, `paid_publisher` / `paid_supplier` by 4 each.
   Those are cross-page contracts nobody wrote down. Renaming one is a five-file change, and
   the table is what tells you that before you start.

**Rule going forward:** parameters may be added freely. An existing one may only be removed
behind a redirect that preserves the old link.

---

## 5. CORS and auth — readiness (design only, not built)

Neither is needed while Streamlit serves the app from one origin with no accounts. Both are
hard blockers the moment a separate frontend or an external consumer appears. Recording the
design now means it is a day's work later, not a design argument.

### 5.1 CORS

A browser frontend on a different origin cannot call `api/` at all today — there is no
CORS middleware. The change is small and belongs in `api/main.py`:

- `CORSMiddleware` with `allow_origins` read from an env var (never `*` once auth exists).
- `allow_credentials=False` while auth is header-based — this is what keeps CSRF out of scope.
- Restrict `allow_methods` to the verbs actually served (`GET`, plus `POST` if siting lands).

**Precondition:** the API is currently open by design. Adding CORS to an open read-only API
is safe. Adding it *after* auth exists requires the credentials decision above to be made
deliberately.

### 5.2 Auth

The prior decision (recorded in the API layer plan) is **open, unauthenticated, no metering**
— matching every official Irish source, and avoiding the unfunded-metering trap. Nothing
here overturns that. What follows is the shape *if* a paid tier is ever built, per the
already-decided "split free/paid by delivery mode, not content" model.

- **API keys, not OAuth.** Consumers are scripts and servers, not end users logging in.
  A hashed key table plus an `Authorization: Bearer` header covers it. OAuth buys nothing
  without user accounts, and user accounts are not on the roadmap.
- **Metering at the edge, not in the app.** Rate limits belong in the reverse proxy
  (nginx/Cloudflare), so the API stays stateless and no request path grows a database write.
- **Journalist/NGO exemption from day one**, per the existing decision — universal among
  comparable projects (OpenSanctions, ProPublica, mySociety).
- **Bulk stays free.** `api/routers/exports.py` already implements default-deny allow-listed
  exports with the privacy filter baked into the materialised file. That model does not change.

### 5.3 The precondition that outranks both

**Caveats must reach the API envelope before any external consumer exists.**
`dail_tracker_core/caveats.py` holds the text and `serialize.envelope` accepts a `caveat`
kwarg, but most endpoints do not populate it. An external client currently receives figures
stripped of the qualifiers that make them true.

Given that provenance is the owner's domain and unqualified figures in published copy is
this project's named costliest failure mode, **this ranks above CORS and above auth**. An
open API serving uncaveated numbers is a worse outcome than no API.

---

## 6. Logging and observability — cloud readiness

Assessed against the code, not the convention doc. The verdict is lopsided: **the ETL is
well instrumented; the serving layer is dark.** That is exactly backwards for cloud, where
the pipeline runs somewhere you control and the server is the thing you cannot attach a
debugger to.

### 6.1 What is genuinely good

`services/logging_setup.py` is a sound design and should not be rewritten:

- One rule, enforced — every log file under `logs/`; `logs/runs/<run_id>/` per pipeline run.
- `RotatingFileHandler` capped 5 MB × 3 ([logging_setup.py:38](../services/logging_setup.py#L38)),
  which is what killed the old unbounded 89 MB log.
- Orchestrator-aware: skips the file handler when `DAIL_PIPELINE_RUN_ID` is set
  ([logging_setup.py:105](../services/logging_setup.py#L105)), so one helper is safe for
  scripts that are both standalone and pipeline steps.
- No `logging.basicConfig` in entrypoints. **132 modules use `getLogger`** — adoption is real.

### 6.2 The gap: the serving layer has no logging

- **`api/` has 5 logging references across 2 files** (`routers/ministerial.py`,
  `routers/catalog.py`). No logging configuration in `api/main.py`, no request logging, no
  exception logging, no correlation IDs. A 500 in production would leave nothing but
  uvicorn's default access line.
- **`utility/` has one** (`ui/components.py`). The Streamlit app is effectively unlogged.

So today, when something breaks for a *user*, there is no record. When something breaks in
the *pipeline*, there is an excellent one.

### 6.3 What cloud specifically breaks

1. **File-based logging is the wrong default in a container.** Files under `logs/` vanish on
   restart and consume the container disk meanwhile. `setup_logging` does add a
   `StreamHandler` ([logging_setup.py:54](../services/logging_setup.py#L54)) so stdout works —
   the fix is to make the file handler opt-in via env, not to rebuild anything.
2. **Plain-text format is not queryable.** `_FORMAT` is
   `"%(asctime)s - %(name)s - %(levelname)s - %(message)s"`
   ([logging_setup.py:40](../services/logging_setup.py#L40)). Log aggregators filter and
   alert on JSON fields. Add a JSON formatter selected by env, keeping the human format for
   local runs.
3. **The `if root_logger.handlers: return` guard is a hazard under a server.**
   ([logging_setup.py:46](../services/logging_setup.py#L46) and
   [:94](../services/logging_setup.py#L94)). Uvicorn configures root logging at startup, so
   a later `setup_logging()` call silently no-ops and the app's logging config never applies.
   Fine for scripts, wrong for the API — the API needs its own explicit `dictConfig` at
   lifespan rather than reusing this helper.
4. **No request correlation.** Nothing ties log lines to one request. A middleware assigning
   a request ID and binding it to a `contextvar` is the standard fix and is small.
5. **Level is hardcoded INFO.** Should read `LOG_LEVEL` from env.
6. **Nothing routes ERROR anywhere.** Cloud needs errors to reach a person — Sentry, or at
   minimum an aggregator alert rule.
7. **No secret redaction.** Currently harmless because there are no runtime secrets. The
   moment an LLM API key or R2 token exists, a logged exception can leak it. A redaction
   filter should land **with** the first secret, not after.

### 6.4 Recommended shape

Keep `services/logging_setup.py` for ETL unchanged. Add a sibling for serving:

- `services/logging_cloud.py` — explicit `dictConfig`, JSON formatter when `LOG_FORMAT=json`,
  stdout only, level from `LOG_LEVEL`, redaction filter, called from the FastAPI lifespan
  (never guarded by a handler check).
- Request-ID middleware in `api/main.py`, bound to a `contextvar` and emitted on every line.
- Log the things that would otherwise be invisible: unhandled exceptions, slow queries
  (the siting engine's measured 577 ms p50 makes this concrete), and cache misses on the
  hot DuckDB path.

**Priority:** below the caveat work (§5.3), above CORS. It is the difference between
diagnosing a production problem in minutes and guessing.

### 6.5 SHIPPED (2026-07-20)

`services/logging_cloud.py` + wiring in `api/main.py`, stdlib only, no new dependencies.
`services/logging_setup.py` untouched — ETL logging is unchanged.

| Piece | Where |
|---|---|
| `dictConfig`, unguarded (the uvicorn no-op fix) | `configure_logging()` |
| JSON formatter, `extra=` fields become top-level keys | `JsonFormatter` |
| Request id via `ContextVar`, `X-Request-ID` in and out | `RequestIdFilter` + middleware in `api/main.py` |
| Credential redaction | `RedactionFilter` / `redact()` |
| Noisy third-party libraries pinned to WARNING | `_NOISY_LIBRARIES` |
| Startup/shutdown, unhandled exceptions, slow requests (>1s → WARNING) | `api/main.py` |

Env: `LOG_LEVEL`, `LOG_FORMAT=json|text`, `LOG_SERVICE`.

**Verified by driving real requests** through `TestClient`, not by tests alone: request ids
generate and propagate, a caller-supplied `X-Request-ID` is reused across the hop, 404s and
data routes log correctly. Two findings the instrumentation produced immediately:

1. **`api_conn()` startup costs ~1,621 ms** — previously unmeasured. That is the cold-start
   figure any container or serverless plan has to budget for, and it is the strongest
   argument in §5 of the cloud discussion for a warm process over a request-scoped one.
2. **`httpx` logs one INFO line per outbound request** — hence `_NOISY_LIBRARIES`.

Tests: `test/services/test_logging_cloud.py`, 32 cases covering JSON shape, redaction
(including a pin that **sha256 digests must NOT be redacted** — `tools/data_manifest.py`
logs them legitimately), request-id lifecycle, env resolution, and the uvicorn
already-has-handlers hazard.

**Not done, deliberately:** no Sentry/alerting sink (needs an account decision), no
Streamlit-side logging (the app is the next surface, but pages are the layer most likely to
be replaced), and no log shipping config (belongs with the host choice).

### 6.6 Cross-process correlation — the ETL, SHIPPED (2026-07-20)

The harder half of the problem: on one machine you follow discrete ETL steps by reading
files under `logs/runs/<run_id>/steps/`. The filesystem *is* the correlation. On cloud that
collapses to one stdout stream, and `run_id` lived only in the directory name
([run_paths.py:61](../services/run_paths.py#L61)) — not in the log record. So the grouping
key was lost the moment logs became a stream.

Fixed by promoting `run_id` from a path to a **field**, without touching the 132 modules
that call `getLogger`:

- `logging_cloud.bind_context(**fields)` — process-wide static fields injected onto every
  record by `StaticContextFilter`. The cross-process correlation primitive.
- `logging_setup.setup_logging` / `setup_standalone_logging` now **delegate** to
  `configure_logging` when `cloud_mode()` is true, binding `run_id` (from the arg or the
  inherited `DAIL_PIPELINE_RUN_ID`) and `step` (the script name). The 132 callers log to
  root exactly as before; only the root configuration changed.

**Opt-in, so the laptop is unaffected.** `cloud_mode()` is true when `DAIL_LOG_CLOUD` is
truthy or `LOG_FORMAT=json`. Default local runs keep rotated text files under `logs/`.

Verified by driving a real ETL entrypoint with the flags set:

    {"ts":"…","level":"INFO","logger":"extract","service":"lobbying_refresh",
     "message":"fetched 412 returns","run_id":"2026-07-20T21-00-00Z-smoke001",
     "step":"lobbying_refresh"}

Every discrete process in a pipeline run now emits lines carrying the same `run_id`. That is
the whole answer to "simple to follow on one machine, hard on cloud": the collector groups
by `run_id`, and one run's steps are one filter away again.

### 6.7 Cloud-agnostic — what reads this, on each host

The contract is deliberately the lowest common denominator: **structured JSON on stdout, one
object per line, nothing else.** Every target consumes that natively, no per-vendor code:

| Host | How it collects | What to set |
|---|---|---|
| **Hetzner / any VM** | Promtail or Vector tails the container/systemd stdout → Loki; Grafana queries by `run_id`/`request_id` | `LOG_FORMAT=json`; run Promtail |
| **AWS** | ECS/Fargate stdout → CloudWatch Logs; JSON auto-parsed into fields; Logs Insights filters `run_id` | `LOG_FORMAT=json` in task def |
| **GCP** | Cloud Run/GKE stdout → Cloud Logging; JSON auto-mapped to `jsonPayload.*`; filter `jsonPayload.run_id` | `LOG_FORMAT=json`; nothing else |
| **Local dev** | reads `logs/` files as today | leave env unset |

One caveat to make it fully idiomatic on GCP/CloudWatch: those platforms key severity off a
specific field (`severity` on GCP). The `level` field is present and filterable now; a
one-line formatter tweak to also emit `severity` would light up their native error
highlighting. Deferred until a host is actually chosen — it is a five-minute change and
premature before then.

**Answering "overwhelmed with detail":** three levers are in place. Levels (`LOG_LEVEL` per
process), third-party suppression (`_NOISY_LIBRARIES` — `httpx` alone was a line per request),
and the request/run grouping so volume is navigable rather than flat. What is *not* yet built
is sampling of high-frequency success lines — unnecessary at current traffic, worth revisiting
if request volume makes the access log itself expensive.

## 7. Delivery order

1. **Done:** wire the five ratchets into the focused verification lane (§3).
2. **Done:** isolate data-access caching behind `utility/data_access/_cache.py` (§2.1).
3. **Done:** extract the shared stylesheet without reordering it (§2.2).
4. **Done:** add `services/logging_cloud.py` and request-ID middleware (§6.4).
5. **Next:** populate caveats through `serialize.envelope` (§5.3).
6. **Deferred:** add CORS middleware only when a second frontend is real (§5.1).
7. **Next:** close API parity where it is product-relevant: `procurement/signals`,
   `procurement/payments` (§2.3).

The completed steps are worth having **whether or not a migration ever happens**. They reduce
single-framework dependency, and every one of them makes the current Streamlit app more
testable, more debuggable, or more portable — today. That is the test for whether decoupling
work is honest: if it only pays off in a hypothetical future, it is speculative; if it pays
off today, it is insurance.

---

## Tooling shipped with this plan

| Tool | Purpose |
|---|---|
| `tools/migration/scan_framework_coupling.py` | AST import graph + `st.*` surface census |
| `tools/migration/extract_url_contract.py` | Generates + drift-checks `doc/URL_CONTRACT.md` |
| `tools/migration/check_api_parity.py` | Ratchet: core query fns unreachable from the API |
