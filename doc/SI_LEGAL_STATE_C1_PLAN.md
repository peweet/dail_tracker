# C1 — SI legal-state from the eISB Legislation Directory: full build plan

> **Status (2026-06-03):** sandbox extractor BUILT + VALIDATED across all years.
> Not yet wired into pipeline/app. This is now a **shipping** task, not a
> feasibility one. Read the "Current state + kickoff" section at the very bottom
> first if you're resuming this in a dedicated window. This plan turns the SI page
> from a discovery index into a **statutory-chronology tool** by adding a sourced,
> confidence-scored legal-state to every SI.

## CURRENT STATE + KICKOFF (read first if resuming)

**What is already built and validated (sandbox, read-only, nothing wired in):**
- `pipeline_sandbox/si_legislation_directory_extract.py` — full multi-year crawler.
  Crawls eISB Directory 2016–2026 (≈154 range pages), caches HTML to
  `data/bronze/eisb_directory/`, derives legal-state, writes parquet + coverage JSON,
  and self-tests join coverage. Re-runs are instant (cache hits).
- `pipeline_sandbox/probe_si_legal_state_c1.py` — the original single-year probe.
- **Output:** `data/sandbox/parquet/si_current_state.parquet` (7,355 rows) +
  `data/_meta/si_current_state_coverage.json`.
- **Columns produced:** `si_id, si_year, si_number, directory_title, current_state,
  affecting_sis (list), affecting_si_urls (list), this_si_eli_url, how_affected_raw,
  state_source, state_source_url, directory_updated_to, confidence`.
- **Validated:** 99.5% gold join on `(si_year, si_number)` (5,896/5,924), consistent
  99–100% per year (2026 lower, recent SIs); **22.6% of gold SIs carry a non-made
  state**; state dist = 5,421 in_force / 1,335 revoked / 161 amended / 10
  partially_revoked / 428 other. All 3 confirm-links per SI resolve HTTP 200
  (this SI's ELI text, the directory row, and the affecting/revoking SI's ELI text).

**What's LEFT to ship (the PR sequence in §10 below):**
1. Promote the parquet to gold (`data/gold/parquet/si_current_state.parquet`) — either
   have the extractor write there, or LEFT-join it inside `si_entity_enrichment.py`.
2. `sql_views/legislation_si_current_state.sql` (`v_si_current_state`) + LEFT-JOIN it
   into `sql_views/legislation_si_index.sql` so `v_statutory_instruments` gains the
   state columns (page stays `SELECT *`, display-only).
3. Contract test in `test/test_sql_views.py` (enum check, revoked⇒source_url, coverage
   gate ≥95%) + a CI fixture parquet (mind the `*.parquet` gitignore negation rule).
4. UI: legal-status chip on the SI detail panel with the confirm link + caveat
   (`utility/pages_code/statutory_instruments.py` ~L718–901), then index-card pills +
   "Legal status" facet + KPI.

**Known refinement before gold:** whole- vs provision-level revocation is heuristic
(the `partially_revoked` bucket under-fires — only 10). Tighten the provision-marker
lookbehind in `derive_state()` against fixtures (§4/§7) before promoting.

**Kickoff prompt (paste into a fresh window):**
> Ship C1 — SI legal-state from the eISB Legislation Directory. Read
> `doc/SI_LEGAL_STATE_C1_PLAN.md` first (the "Current state + kickoff" section).
> The sandbox extractor `pipeline_sandbox/si_legislation_directory_extract.py` is
> built and validated (99.5% gold coverage, parquet at
> `data/sandbox/parquet/si_current_state.parquet`). Move it toward shipping per §10:
> first tighten the whole-vs-partial revocation rules in `derive_state()` against
> fixtures, then promote the parquet to gold, add `v_si_current_state` +
> LEFT-JOIN into `v_statutory_instruments`, add the contract test, then the
> detail-panel legal-status chip with confirm link + the "verify before legal
> reliance" caveat. Sandbox/logic-firewall rules apply; the join stays in SQL, the
> page stays display-only. No-inference rule: never positive-assert "in force";
> null state = "not checked".

---

> Feasibility is proven — `probe_si_legal_state_c1.py` parsed 2018 (14 pages → 662
> SIs) and joined back to gold at **99.5%** on `(si_year, si_number)`.

## 0. What we are (and are not) claiming

- **In scope (sourced from eISB):** whether an SI has been **amended / revoked /
  partially-revoked / not affected**, by which SI, and on which provision.
- **Out of scope / kept `unknown`:** commencement status (the Directory does not
  track it; we only have `si_signed_date` = made date + the unparsed
  `si_effective_date_text`), and editorial states like `spent`/`obsolete` (never
  inferred). The plan's enum is trimmed to what the source supports.
- **Hard rule:** never assert an SI is "in force" as a positive legal claim. We
  only ever surface the *negative* state (amended/revoked) that eISB explicitly
  records, plus a link to verify. Everything carries the caveat
  *"Discovery/indexing only — verify the official eISB entry before legal reliance."*

## 1. Source

eISB **Legislation Directory** chronological tables (Office of the Attorney
General — reusable):
- Year index: `https://www.irishstatutebook.ie/isbc/si{YYYY}.html`
- Range pages: `…/isbc/si{YYYY}_1-50.html`, `_51-100.html`, … (one table per page)
- Columns: **SI Year/Number · Title · How Affected · Affecting Provision**
- "How Affected" examples (verbatim from probe):
  - `Not affected` → in force as made
  - `Revoked` (bare) → whole instrument revoked
  - `Reg. 2(3) revoked S.I. No. 591 of 2023 , reg. 2(2)` → provision-level
  - `Reg. 2(1) amended S.I. No. 626 of 2025 , reg. 4(a)` → provision-level amend
- Page header carries "Updated to <date>" — capture it as the source freshness.

## 2. Data model — `v_si_current_state` (proposed)

New gold parquet `data/gold/parquet/si_current_state.parquet`, one row per SI.

| column | source | notes |
|---|---|---|
| `si_id` | derived `{year}-{number:03d}` | matches gold SI id |
| `si_year`, `si_number` | directory | join key |
| `current_state` | derived from How Affected | enum below |
| `made_date` | gold `si_signed_date` | already have; not from directory |
| `commenced_date` | `null` for now | Directory has none → `unknown` |
| `revoked_by` | list[str] of `S.I. No. N of YYYY` | whole-instrument revokers |
| `amended_by` | list[str] | affecting SIs (amend) |
| `partially_revoked_by` | list[str] | provision-level revokers |
| `affected_provisions` | raw list | e.g. `Reg. 2(3)` |
| `how_affected_raw` | directory cell verbatim | audit/provenance |
| `state_source` | `"eISB Legislation Directory"` | constant |
| `state_source_url` | range-page URL | clickable |
| `directory_updated_to` | page "Updated to" date | freshness |
| `checked_at` | crawl timestamp (passed in) | not `Date.now()` in-script |
| `confidence` | derived | see §4 |

**`current_state` enum (trimmed to what's derivable):**
`in_force_as_made` · `amended` · `partially_revoked` · `amended_and_partially_revoked`
· `revoked` · `other_affected` · `unknown`.

## 3. Extractor — `pipeline_sandbox/si_legislation_directory_extract.py`

Promote the probe to a real (sandbox) extractor:
1. For each year in range (start with gold's min..max SI year), GET the year index,
   extract range-page links, GET each (force `encoding="utf-8"`; server mislabels).
2. Polite crawl: `time.sleep(~0.4)`, retries, **cache** range pages to
   `data/bronze/eisb_directory/si{YYYY}_{lo}-{hi}.html` and skip refetch unless the
   index "Updated to" date changed (cheap freshness gate).
3. Parse each table → rows; derive `current_state` (§4); collect affecting-SI lists.
4. Write `si_current_state.parquet` with **`compression="zstd", compression_level=3,
   statistics=True`** (parquet-write convention).
5. Emit `data/_meta/si_current_state_coverage.json`: rows, state distribution,
   join-coverage vs gold, directory_updated_to, n_pages — the honest A5-style gate.

## 4. The hard part — whole vs provision-level, and confidence

Parsing rules (a "How Affected" cell can hold several sub-entries):
- Cell empty / `Not affected` → `in_force_as_made`, confidence 0.95.
- A `revoked` token **not** preceded by a provision marker (`Reg.|Art.|Sch.|Para.|S.`)
  and not scoped to a sub-provision → **whole** `revoked`, confidence 0.9.
- `revoked` scoped to a provision → `partially_revoked`.
- `amended` present, no whole-revoke → `amended`.
- both partial-revoke and amend → `amended_and_partially_revoked`.
- anything else non-empty → `other_affected` (low confidence, show raw text).
- Multi-row cells: take the **most severe** state (revoked > partially_revoked >
  amended > in_force) for the headline `current_state`, but keep every sub-entry in
  `how_affected_raw` and the affecting-SI lists.

**Known refinement from the probe:** the `partially_revoked` bucket under-fired —
tighten the provision-marker lookbehind and test against fixtures (§7). Until that
is solid, prefer to label uncertain revokes `partially_revoked` (less severe) rather
than overclaim a whole `revoked`.

## 5. SQL view + how it reaches the page (logic-firewall-clean)

Two gold parquets, joined **in SQL** (never in the page):

- New view `sql_views/legislation_si_current_state.sql` → `v_si_current_state`
  (reads `si_current_state.parquet`). Filename prefixed `legislation_` so
  `legislation_data.py`'s `get_legislation_conn()` glob registers it.
- Extend `sql_views/legislation_si_index.sql` so `v_statutory_instruments`
  **LEFT JOINs** `v_si_current_state` on `si_id`, exposing
  `current_state, revoked_by, amended_by, state_source_url, directory_updated_to,
  confidence` as new columns. LEFT (not inner) — absence ≠ in force; a missing row
  → `current_state = NULL` rendered as "status not checked".
- `legislation_data.py::fetch_si_entity_index()` is unchanged (`SELECT *`); it just
  gets the new columns. Page stays display-only.

Alternative (if we don't want to touch the existing view): do the LEFT JOIN inside
`si_entity_enrichment.py` so the columns land in `statutory_instruments.parquet`
directly. Either keeps the join out of the page.

## 6. App integration — `utility/pages_code/statutory_instruments.py`

Concrete placements (current structure):
1. **SI detail panel** (~L718–901): a **legal-status block** directly under the
   title, beside the existing eISB/Iris provenance. A coloured chip
   (`In force as made` grey/green · `Amended` amber · `Revoked` red · `Status not
   checked` neutral) + plain-English line:
   *"Revoked by S.I. No. 591 of 2023 (per the eISB Legislation Directory, updated to
   29 May 2026)."* with a link to `state_source_url`. Render the
   *"verify before legal reliance"* caveat whenever `current_state != in_force_as_made`.
   Reuse the clickable-link pattern already used for `eisb_url`.
2. **SI index cards** (~L689–713): a small status pill on each card so the list is
   scannable (In force / Amended / Revoked / —).
3. **KPI strip** (~L291–340): add a metric, e.g. *"X% still in force as made"* or a
   revoked count, computed display-only over the frame.
4. **Facet tabs** (~L515–644): add a **"Legal status"** facet alongside
   Year/Department/Operation/EU so users can filter to e.g. revoked SIs.
5. **Legal pack export (plan item C5):** include `current_state`, `revoked_by`,
   `state_source_url`, `directory_updated_to`, and the caveat string in the export.
6. **No-inference rule:** present only the eISB-sourced fact + link; never derive
   "spent/obsolete". A null state shows as "not checked", not "in force".

## 7. Tests

- **Contract** (`test/test_sql_views.py`, mirroring the existing SI tests ~L362–391):
  `v_si_current_state` executes; required columns present; `current_state` ∈ enum;
  every `revoked`/`partially_revoked` row has a non-null `state_source_url`; no row
  has `commenced_date` set (until that axis is built).
- **View join:** `v_statutory_instruments` still executes and now carries
  `current_state`; LEFT-join introduces no row duplication (count unchanged).
- **Extractor unit:** small fixture HTML with one `Not affected`, one bare `Revoked`,
  one `Reg. X revoked …`, one `… amended …` → assert derived states + affecting lists.
- **Coverage gate:** join coverage vs gold ≥ 95% (probe measured 99.5%).
- **CI fixture parquet** for `si_current_state` (per the test-fixtures architecture;
  remember the `*.parquet` gitignore negation rule).

## 8. Pipeline wiring & freshness

- Add `si_legislation_directory_extract.py` to the **iris chain** (or its own
  `legislation` step), running **before** `si_entity_enrichment.py` if we join in
  enrichment, or independently if we join in SQL.
- Freshness: re-crawl on iris refresh; the year-index "Updated to" date is the cheap
  change signal; persist `directory_updated_to` + `checked_at` so the UI can show
  "status as of <date>".

## 9. Risks & mitigations

| Risk | Mitigation |
|---|---|
| Overclaiming "revoked" when partial | severity rules + fixtures; default to less-severe on uncertainty; show raw `how_affected` |
| Implying "in force" | never positive-assert; null = "not checked"; caveat on non-made states |
| SI id mismatch (zero-pad) | join on `(si_year, si_number)` ints, not string id |
| Bilingual duplicate links / layout drift | parse by table structure, assert table shape; coverage gate catches breakage |
| eISB politeness / blocking | cache + sleep + UA; only re-crawl changed years |

## 10. Phasing (small PRs)

1. **PR1** — extractor + `si_current_state.parquet` + coverage JSON (sandbox only;
   nothing user-facing). Land the parsing rules + fixtures here.
2. **PR2** — `v_si_current_state` view + LEFT-join into `v_statutory_instruments` +
   contract tests.
3. **PR3** — detail-panel legal-status block + caveat (smallest user-facing change).
4. **PR4** — index-card pills + "Legal status" facet + KPI metric.
5. **PR5** — legal-pack export fields (ties into C5).

**Safest first PR:** PR1 (no UI claims). **Highest-leverage user-facing PR:** PR3.
