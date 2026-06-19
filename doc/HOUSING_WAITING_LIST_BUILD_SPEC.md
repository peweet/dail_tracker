# Build spec — "Who's on the waiting list" + national Housing screen

Status: **DETAILED PLAN** (not built). Depth companion to
`doc/HOUSING_WAITING_LIST_FEATURE_PLAN.md` (read that for the why / scope decisions).
Decisions locked by the user: view-first → national Housing page; league table at
BOTH county + LA grain, default county; per-capita via CSO PEA08; citizenship
included with sensitivity handling; phase-2 CSO surfacing kept as a separate plan.

This spec is the data-anchored implementation design: exact view shapes, exact
sources/keys verified against gold, page structure, components, sequencing, tests,
and risks.

---

## 1. Data layer — two registered views (all logic in SQL, UI stays thin)

Per the project firewall, every aggregation (unpivot, %, rollup, per-capita, median)
lives in pipeline-owned registered views; the page only renders. Both views are
gold-sourced, registered in dependency order, swallow_errors-safe.

### 1a. `v_ssha_waiting_list_composition` — the breakdown bars
One tidy row per (grain, area, year, dimension, category). Powers the national
screen, the county view, and the constituency expander from ONE source.

Columns:
| col | type | notes |
|---|---|---|
| grain | text | 'national' \| 'county' \| 'la' |
| area | text | 'Ireland' for national; county (27) or LA (31) name otherwise |
| year | int | 2024, 2025 |
| dimension | text | 'time_on_list' \| 'tenure' \| 'employment' \| 'household' \| 'citizenship' |
| category | text | human label (e.g. 'More than 7 years', 'Private rented') |
| ord | int | display order within a dimension (time buckets MUST stay <6mo→7yr+; others = NULL → UI orders by count desc) |
| count | bigint | households |
| pct | double | count / area-dimension-year total, rounded 1dp |

Construction (per source table, then UNION ALL):
1. **UNPIVOT** each SSHA wide table on its category columns `EXCLUDE (la, year, total)`
   → `(la, year, category, count)`. (DuckDB `UNPIVOT ... INTO NAME category VALUE count`.)
2. Tag `dimension` per source (A1.8→time_on_list, A1.7→tenure, A1.2→employment,
   A1.4→household, A1.9→citizenship). Attach `ord` for time_on_list via a CASE on the
   column slug (8 buckets); NULL elsewhere. Map slugs → display labels in the view
   (one CASE per dimension — keeps labels out of the UI).
3. Build the three grains:
   - **la** (31): rows as-is.
   - **county** (27): join the explicit LA→county rollup map (§1c), GROUP BY
     county, dimension, category, year → SUM(count).
   - **national**: GROUP BY dimension, category, year over all LAs → SUM, area='Ireland'.
4. `pct` = 100 * count / SUM(count) OVER (PARTITION BY grain, area, year, dimension).

Result size ≈ (31+27+1) areas × ~30 categories × 2 years ≈ ~3.5k rows. Trivial.

### 1b. `v_ssha_waiting_list_totals` — the county/LA league table
One row per (grain, area, year) headline, with per-capita. Drives the sortable table.

| col | notes |
|---|---|
| grain, area, year | as above (county + la + national) |
| waiting_total | from A1.8 total (== all SSHA tables) |
| waiting_yoy_pct | (2025-2024)/2024 |
| over_7yr_pct | more_than_7_years / total |
| over_4yr_pct | (4-5 + 5-7 + 7+) / total |
| population | CSO PEA08, persons (see §1c) — county/national only; NULL for LA |
| waiters_per_1000 | waiting_total / (population/1000) — county/national only |

### 1c. Two explicit maps (verified against gold — NO string-strip)
- **SSHA LA (31) → county (27)**: most are 1:1 (strip ' County'); the merges are
  Dublin City + Fingal + South Dublin + Dun Laoghaire-Rathdown → **Dublin**;
  Cork City + Cork County → **Cork**; Galway City + Galway County → **Galway**.
  Limerick/Waterford/Tipperary already single. → 27 counties.
- **county (27) → PEA08 County**: PEA08 uses `'Co. Carlow'…'Co. Dublin'` plus city
  rows? VERIFIED: PEA08 `County` has 27 distinct values in `Co. X` form, Year→2025,
  `UNIT='Thousand'`. Population for a county = filter `Sex='Both sexes'`,
  **SUM over the 19 Age Group rows** (no all-ages total row exists), latest year,
  `VALUE` ×1000. Map 'Dublin'→'Co. Dublin' etc. (PEA08 already county-grain, so it
  aligns with our 27-county rollup — this is WHY per-capita is county-level.)

### 1d. Test — extend the existing tripwire
Add to `test_constituency_housing_enrichment_views_build` (or a sibling): both new
views build; composition has all 5 dimensions and 3 grains; national time_on_list
pct sums to ~100; `waiters_per_1000` non-null for all 27 counties (per-capita join
has zero misses); citizenship categories == {Irish, EEA, Non-EEA, UK}.

---

## 2. Page layer — new national `Housing` page

New module `utility/pages_code/housing.py` → `housing_page()`. Register in
`utility/app.py` `st.navigation` as a **top-level group "Housing"** (peer of "Your
Area" / "What They Own" — it's a flagship national dataset), `url_path="housing"`,
icon `:material/home:`. (Open decision: top-level vs nest under "The Money"/"Your
Area" — recommend top-level.)

Section structure (cards on primary; `st.dataframe` only for the secondary table):

1. **Hero** — `evidence_heading("Who's on the social housing list")` + a stat strip:
   total waiting (61,719), YoY, and the **lead civic number "18.9% have waited 7+
   years"**. Source caption: Housing Agency SSHA 2025.
2. **How long people wait** — the time_on_list distribution as ONE horizontal
   stacked stripe (8 ordered buckets, long-tail visually weighted), legend below.
   Uses a new generic `proportion_stripe_html` (see §3).
3. **Who is waiting** — a 2-col grid of dimension cards: **Tenure**, **Employment**,
   **Household**, **Citizenship** — each a labelled proportion stripe (top categories)
   + a one-line factual caption. Citizenship card carries the §Citizenship caption
   from the feature plan (denominator stated, neutral, no derived ratios).
4. **By county** — a sortable secondary `st.dataframe`: county · waiting · per 1,000 ·
   % 7yr+ · YoY. Default sort by waiting desc; per-1,000 is the honest comparator.
   A toggle/segmented control switches the table (and §2-3 bars) between **county**
   and **LA** grain. Selecting a county sets `?county=` (spa_links soft-nav) and
   re-filters the §2 and §3 bars to that area with an "Ireland ▸ {county}" breadcrumb.

National ↔ county is the only interaction; everything else is read-only display.

## 3. Components
- Generalize the existing stacked stripe: add `proportion_stripe_html(segments:
  list[tuple[label, value, colour]], *, show_legend, unit='')` to
  `utility/ui/components.py`, reusing the `.cmt-stripe*` CSS (or new `.dt-stripe*` in
  shared_css.py with a **sequential ramp** for ordered dims and a categorical palette
  for nominal dims). `party_stripe_html` stays; this is its non-party sibling.
- Colour: time_on_list = sequential single-hue ramp (short=light → 7yr+=dark, so the
  long-wait tail reads as "heavy"). tenure/employment/household/citizenship =
  neutral categorical palette (NOT party colours, NOT red/green good-bad).

## 4. Constituency reuse (phase 3)
In `_render_housing`, add a `st.expander("Who's waiting here")` that queries
`v_ssha_waiting_list_composition` filtered to `grain='la'` + the serving council(s),
rendering the same stripes. One new data-access fn; no new view. Keeps the headline
cards uncluttered (expander, collapsed by default).

## 5. Build sequence
1. **Views + maps + test** (`v_ssha_waiting_list_composition`, `_totals`, the two
   maps; register after `constituency_la_crosswalk`/standalone; extend tripwire).
   Vet in isolation — confirm national pct sums ~100, per-capita 27/27, county
   rollup totals == sum of member LAs. **Checkpoint before any UI.**
2. **`proportion_stripe_html`** component + CSS.
3. **National Housing page** — hero, time bar, dimension cards, county table, toggle.
4. **Constituency expander** (reuse).
5. (separate plan) **Phase-2 CSO supply** on the same page: completions trend,
   F2023B weekly rent by county, vacancy, price.

## 6. Firewall / privacy / honesty
- All aggregation in SQL views (unpivot, pct, rollup, per-capita, medians). UI renders
  only. Matches the pipeline-owned-view rule + the data_view/contract conventions.
- **Citizenship**: aggregate-only (already), neutral source-labels, denominator
  stated, inside the breakdown (not hero), no derived "% non-Irish" prose
  ([[feedback_no_inference_in_app]], [[feedback_personal_insolvency_privacy]]).
- **Grain honesty**: county/LA labelled explicitly; per-capita only where a real
  population denominator exists (county/national), never faked at LA grain.
- **Two-year only** (2024/2025) — SSHA 2025 report scope; label the period, don't
  imply a longer trend.

## 7. Risks & mitigations
| Risk | Mitigation |
|---|---|
| PEA08 has no all-ages total row | SUM the 19 age groups for Sex='Both sexes' (verified shape) |
| LA→county merge wrong (Dublin/Cork/Galway) | explicit map + test: county total == Σ member-LA totals |
| UNPIVOT label drift if a column is renamed upstream | dimension/label CASEs are explicit; tripwire asserts category sets |
| citizenship mis-framing | §6 rules; keep out of hero; factual caption reviewed |
| page crowding (national screen tries to do everything) | phase 1 = need only; supply/perf are later phases |
| stripe a11y (colour-only meaning) | legend with labels+counts always on; title attrs per segment |

## 8. Open decisions (small)
1. **Nav placement** of the Housing page: top-level (rec) vs nested.
2. **Default league-table sort**: absolute waiting (rec) vs per-1,000.
3. Whether the constituency expander ships in this batch or a follow-up.

## Cross-references
- Overview: `doc/HOUSING_WAITING_LIST_FEATURE_PLAN.md`
- Shipped views/extractors/tripwire: see overview doc.
- Nav pattern: `utility/app.py` (st.navigation groups, st.Page url_path).
- Stripe to generalize: `utility/ui/components.py::party_stripe_html` (~L1031) + `.cmt-stripe*` CSS.
- Per-capita source: `data/gold/parquet/cso_pea08.parquet` (County 27, Thousand, →2025).
