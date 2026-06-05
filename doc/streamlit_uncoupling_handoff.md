# Context Handoff — "make Streamlit a thin presentation layer" (dail_tracker)

> Hand this whole file to the parallel Claude process doing larger-scale prep.
> It is self-contained: that process needs nothing else from the originating
> conversation. A single-file PILOT experiment has been executed to validate the
> approach (§3); the mandate (§6) is to use its evidence to refine the plan at scale.

You are refining a migration plan at scale. Stay in PLANNING mode unless told
otherwise — but the pilot proves the pattern is real, not theoretical.

## 1. The goal

Make Streamlit a THIN presentation layer over a Streamlit-free core, so a future
React frontend or FastAPI API can consume the same logic with minimal pain
("inversion of control"). Origin doc: `doc/fastapi_query_core_uncoupling_plan.md`
(reviewed — instinct sound, but it conflates concerns; see below).

The hefty/risky rewrite people fear is the REACT rewrite. The core-extraction
work is what makes that future cheap — and the extraction itself is mostly
RELOCATION of code that already exists, under a UI whose behavior never changes.

## 2. Settled architecture (don't re-litigate; refine the edges)

**A. Dependency rule:** arrows point DOWN only. Nothing below imports anything above.

```
data/gold + sql_views/   (exists; the "firewall" — joins/GROUP BY/value
                          semantics live in SQL views, AST-enforced in CI)
       ▲
dail_tracker_core/        (Streamlit-FREE: db, queries, results, registry,
                          identity, tokens)
       ▲
adapters: utility/ (Streamlit, thin) | api/ (FastAPI, LATER) | React (LATER)
```

**B. Three independent threads — keep them separate:**
1. **CORE SEAM** — the Streamlit-free `dail_tracker_core`. Highest priority.
2. **SEMANTICS REGISTRY** — single source of truth per dataset.
3. **FASTAPI EXPOSITION** — its own discussion; FastAPI CONSUMES (1)+(2), does not
   define them. Do NOT bundle FastAPI into the core work.

**C. Two product tracks (different tools):**
- Track A = raw gold/silver dataset ACCESS → thin generic registry-driven
  endpoint, documented as a DATA DICTIONARY (or Datasette). Swagger adds little.
- Track B = curated INTELLIGENCE/dossiers → FastAPI + Pydantic + Swagger pays.
- FastAPI's real home is Track B.

**D. HARD CONSTRAINT: stay DuckDB-native.** Business RULES already live in
`sql_views/*.sql`; anything serving data must run DuckDB to inherit that firewall
for free. (This down-weights SQLite-core Datasette.)

**E. Semantics / caveats — two distinct kinds, extracted differently:**
- Caveat COPY (human strings, e.g. "co-occurrence ≠ causation"): centralize, but
  BIND TO THE DATASET (registry), not as loose global constants — the failure
  mode is "dev forgets to attach it." Registry holds the CLAIM; each interface
  owns RENDERING.
- Enforced RULES (`value_safe_to_sum`, PII suppression, ceiling≠spend): NOT
  strings — leave them in views/core functions. Don't fork them into constants.
- Caveats are NOT one string per dataset — they're typed FAMILIES: causation /
  coverage / provisional / method-confidence / legal-state / privacy. A dataset
  carries a COMBINATION.
- **TWO-EXTRACTION model:** (a) dataset-invariant caveats → registry metadata;
  (b) row-conditional caveats driven by a column value (SI `legal_state` → "not
  checked", `value_kind` → "ceiling not spend") → belong with the COLUMN
  value-domain, NOT page strings. The doc misses (b), which is the more dangerous
  half ("in force" vs "not checked").

**F. Presentation-layer contracts that would block a React swap** (audit each as
CONTRACT = move below UI, vs RENDERING = UI may rebuild):
1. **Routing / ADDRESSABLE STATE** — 283 uses of `session_state`/`query_params`
   across 12 pages, with an improvised per-page URL vocab (`?id=` here, `?fund=`
   there, `?clear=`, `?year=`). Standardize ONE param vocabulary; addressable
   state in the URL, ephemeral state in session. Ideally URL params map 1:1 to API
   endpoints. Most valuable thing to formalize; the doc omits it.
2. **IDENTITY RESOLUTION** — `utility/data_access/identity_resolver.py` (name →
   canonical `unique_member_code`) is domain logic in the UI, self-admitted as a
   band-aid for views not emitting the code. Fix = views emit the code; resolver
   disappears.
3. **FORMAT vs DERIVE line** — heuristic: if two frontends could reasonably
   DISAGREE on it, it's FORMAT (UI keeps it: pluralisation, date/number format,
   "—" placeholders). If they'd be WRONG to disagree, it's DERIVE (move to core:
   ranking, in-progress-year fallback, surname disambig, concentration ratios). Do
   NOT over-extract pure formatting.
4. **STATE CLASSIFICATION** — the QueryResult states (no-data vs source-unavailable
   vs not-checked vs manual-review) are decided in core; only the rendering
   differs per interface.
5. **SEMANTIC TOKENS** — party→colour, status→colour/label, confidence→badge are a
   shared CONTRACT (extract the mappings as data); the hex/font is throwaway UI.
6. **CHART/EXPORT DATA PREP + PAGINATION/top-N caps** → contract (move down); chart
   RENDERING is throwaway UI.

**G. The AST logic firewall** (`tools/check_streamlit_logic_firewall.py`, CI-gated)
currently enforces "no raw parquet in pages" — NOT "no business derivation in
pages." Plan to EXTEND its spirit to police logic-location, same machinery.

**H. Migration = strangler-fig**, behavior-preserving, page-by-page, green-baseline
each step (the project already runs this exact playbook for an unrelated package
reorg: `shared/`, `charity/`, `wikidata/` slices). Every step independently
shippable + reversible. Core sits UNDER running pages; output stays byte-identical
because the SQL is identical. ~70–80% of the work is `git mv` + strip an
`@st.cache` decorator into a thin wrapper. NEW code (registry, QueryResult) is
small + additive.

## 3. The pilot experiment (just executed — your evidence)

**File chosen: attendance.** Smallest self-contained page with a genuine,
fairness-critical BUSINESS RULE trapped in rendering — not just a caveat string or
a dedup.

**What was extracted:** the "highest/lowest attendance split + ministers excluded
from the LOWEST list" logic from
`utility/pages_code/attendance.py::_render_good_bad`. Ministers are kept in
highest but removed from lowest, because the source TAA PDFs don't record
ministerial attendance — showing them in a "lowest" shame list is misleading and
unfair.

**Created (purely additive, then 1 page wired):**
- `dail_tracker_core/__init__.py`
- `dail_tracker_core/attendance.py` → `split_attendance_hall(df, *, hall_size)`
  returns `AttendanceHall(highest, lowest)`; + `is_minister_mask(series)`
- `test/test_core_attendance_hall.py` → 18 tests, ALL PASS
- `utility/pages_code/attendance.py` → 12-line derivation replaced by a 2-line call

**Results / wins proven:**
- 18/18 pass incl. a PARITY test replicating the exact old inline expression and
  asserting byte-for-byte equality (`assert_frame_equal`) → behavior preserved.
- Surfaced a latent bug: the rule used
  `is_minister.astype(str).str.lower() != "true"`; if the view ever emits
  `is_minister` as `0/1`, ministers silently reappear in the shame list. Now
  pinned by `test_numeric_is_minister_is_not_excluded` (fails loud).
- Page compiles; import resolves in Streamlit (root on `sys.path`, proven by
  `from config import` working); logic firewall clean (29 files, 0 violations).
- A previously-untestable rule is now 18 deterministic tests in <1s.
- Bar was "clear win, not rewrite-for-its-sake" — met.

## 4. Per-case diagnosis (extraction-win ratings — extend/correct at scale)

| Case | Diagnosis | Win |
|---|---|---|
| `procurement_data.py` | ✅ EXEMPLAR (thin; semantics in views) — pattern to copy | — |
| `_sql_registry.py` | ✅ already Streamlit-free → moves to `core/db` verbatim | mechanical |
| `attendance.py` | 🟡 PILOT DONE — minister rule extracted+tested | done |
| `member_overview.py` | 🔴 own DuckDB conn + ~28 inline SELECTs (biggest leak, high risk, 2477 lines — NOT a first pilot) | high/risky |
| `corporate.py` | 🔴 page-level groupby/agg (`_dominant_ftype`, fund pivot) w/ display_only marker | high/complex |
| `payments.py` / `_original` | 🟠 duplicate `_CAVEAT`/`_QUARANTINE_NOTE` (registry track) | dedup |
| `identity_resolver.py` | 🟠 domain logic in UI; little PURE logic to test (I/O) | architectural |
| `statutory_instruments.py` | 🟡 inline value-domain map (`__unchecked__`→"Not checked") = column semantics in page (extraction (b)) | medium |
| `votes` / `public_appointments` | 🟡 heavy `query_params` routing-state sprawl | routing track |

## 5. Constraints / guardrails

- Solo maintainer. Ships ONE lean Streamlit app on Streamlit Cloud (uv.lock, core
  deps only; entrypoint `utility/app.py`, Python 3.12). A separate API service =
  real ops cost. Don't front-load auth/billing.
- Hard editorial rule: NO inference / NO causal language in public outputs;
  provenance + source links required on public-facing enriched rows.
- Behavior-preserving + green-baseline (currently ~573–578 passed / 0 / ~80
  skipped). Every slice must keep it green.
- A SEPARATE, concurrent "reorg" effort is moving pipeline files into top-level
  packages (`shared/`, `charity/`, `wikidata/`, `reference/`, `pdf_infra/`). Don't
  collide: that's pipeline-internal; THIS is `utility/` + `dail_tracker_core/`.
  This effort touches the pipeline ZERO, so it has smaller blast radius vs the
  active ETL frontier.
- `dail_tracker_core` is the eventual package name; today it's a top-level dir
  (resolves via root-on-path, same as `config.py` / `services/`).

## 6. Your mandate

Using the pilot as the proven template, refine the plan to scale:

a. **Generalize the extraction pattern:** the core package structure
   (`db`/`queries`/`results`/`registry`/`identity`/`tokens`), and how thin
   `st.cache` wrappers preserve TTL/cache semantics (a known sharp edge — caching a
   QueryResult that wraps a DataFrame can trip Streamlit hashing).
b. **Define the dataset REGISTRY schema** (view, exposed cols, PII-suppressed cols,
   caveat families, column value-domain maps) and show it's the SAME object that
   drives Track-A access, PII gating, caveats, and freshness (it can extend the
   existing `source_registry.generated.json`).
c. **Sequence the migration as behavior-preserving PRs ordered by EXTRACTION DEBT /
   leak size** (not commercial value): procurement (reference) → thin pages →
   member_overview + corporate (surgical) → additive registry/identity/routing.
d. **Specify the firewall-rule EXTENSION** (no contract logic in pages).
e. **Keep FastAPI/Track-B explicitly demoted** to "enabled later, not required."

Produce a refined, sequenced plan. Flag anything in the pilot you'd do differently.

## 7. Artifacts to inspect

- `doc/fastapi_query_core_uncoupling_plan.md` — the original plan under review
- `dail_tracker_core/attendance.py`, `test/test_core_attendance_hall.py` — the pilot
- `utility/pages_code/attendance.py` — the wired thin page (`_render_good_bad`)
- `utility/data_access/procurement_data.py` — the exemplar thin pattern
- `utility/data_access/_sql_registry.py` — the already-Streamlit-free seam
- `tools/check_streamlit_logic_firewall.py` — the AST firewall to extend
