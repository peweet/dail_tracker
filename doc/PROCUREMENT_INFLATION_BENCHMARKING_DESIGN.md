# Inflation-Adjusted Procurement Benchmarking — Design

**Status:** FEATURE COMPLETE end-to-end (2026-07-07), behind `DAIL_EXPERIMENTAL`. Data → 4-index
registry → real-terms views (awards/CPV+sector/payments) → firewall-clean data_access wrappers →
gated Streamlit UI (CPV benchmark CPI + construction tender-price band; public-spend real trend) →
gated API endpoints (`/v1/procurement/inflation/{indices,cpv,spend-trend}`, hidden + 404 when the
flag is off). All layers tested (131 pass) + Playwright-verified. Only graduation (un-gating +
gold-commit) and optional MCP-tool fields remain — both user decisions. Detail below.

**Status (history):** P0–P1 VIEWS GRADUATED (2026-06-28). The five views below are now in
`sql_views/procurement/` (`procurement_aa_cpi_deflator`, `procurement_awards_real`,
`procurement_cpv_summary_real`, `procurement_cpv_buyer_real`, `procurement_bid_signal_real`),
with 3 contract tests in `test/sql_views/test_sql_views.py` (parity SQL==`Deflator.inflate`
exact on real gold; invariants; band reconciliation) + a `cso_cpi_deflator` test fixture.
They are ADDITIVE and inert (nothing queries them yet) — **consumption must be gated behind
`DAIL_EXPERIMENTAL`**, like `v_procurement_bid_signal`.

**MULTI-INDEX + METHODOLOGY FIX SHIPPED (2026-06-29).** CPI is a household basket — the wrong
deflator for public money / construction (§17). Added, via `extractors/cso_pxstat_extract.py`:
raw `cso_na007`/`cso_na008`/`cso_wpm39`; derived deflators `cso_govt_consumption_deflator`
(NA007/NA008 GFCE current÷constant — the agency-standard public-spend deflator, base 2024),
`cso_construction_materials_deflator` (WPM39, 2021+), `scsi_tpi_deflator` (SCSI tender prices,
1998–2025). `services/deflator.py` now has an **index registry** (`INDEX_REGISTRY` /
`Deflator.load_index(code)` / `list_indices()`; CPI default, unknown→KeyError). The awards view
gains a **sector lens**: construction CPVs (45*/71*) carry `value_eur_real_sector` +
`deflator_index_sector` using the SCSI TPI (the right "cost to procure" index — ~+23% vs CPI for
2018), everything else CPI. New view `procurement_ab_scsi_tpi_deflator` (`v_scsi_tpi_deflator`).
Tests: registry + index-divergence in `test_deflator_function.py`; sector parity in
`test_sql_views.py`; SCSI-TPI fixture added.

**PAYMENTS / SPEND-TOTALS WIRED TO THE GOVERNMENT-CONSUMPTION DEFLATOR (2026-06-29).** New views:
`procurement_ac_govt_consumption_deflator` (`v_govt_consumption_deflator`),
`procurement_payments_real` (per-line `amount_eur_real` + `real_caveat`, built on
`v_procurement_payments` so it inherits privacy/extraction filters), and
`procurement_payments_real_by_year` (annual nominal-vs-real public spend, grouped by
`year × realisation_tier × vat_status` so SPENT/COMMITTED and VAT bases are NEVER summed). Real
spend e.g. 2018 SPENT incl-VAT €911.9m → €1,008.7m (2024 prices). Tests: gov-deflator view (CI
fixture) + payments parity/invariants/tier-grain (integration-only, like charity-overlap; +
gov-consumption fixture). PARITY-CONTRACT FIX: all derived builders now round the index column
THEN take `deflator_to_base` as the ratio of rounded values (was: rounded index but unrounded
ratio → SQL/Python diverged ~1e-6 on the high-precision gov series).

**DATA-ACCESS WRAPPER SHIPPED (2026-06-29).** The firewall read layer for the feature:
`dail_tracker_core/queries/procurement.py` gains retrieval-only `cpv_summary_real()` (nominal +
real CPI band, now carrying `cpv_description`) and `payments_real_by_year()` (gov-consumption,
tier-whitelisted); `utility/data_access/procurement_data.py` gains cached pass-throughs
`fetch_cpv_summary_real_result` / `fetch_payments_real_by_year_result` / `fetch_inflation_indices`
(the registry picker, via `services.deflator.list_indices`). NO computation in either layer — all
deflation stays in the views + `services/deflator.py`. Tests in `test_core_procurement_queries.py`
(columns, tier whitelist, sample-reconciliation, unavailable-on-missing-view); core stays
Streamlit-free.

**UI STEP 1 SHIPPED (gated, 2026-07-07).** The CPV benchmark ("wins" → By category,
`_render_cpv`) gains an EXPERIMENTAL real-terms lens: a default-OFF `st.toggle` (only when
`DAIL_EXPERIMENTAL=1` AND all-time) that adds "in 2025 prices €X–€Y (median €Z)" beside the
nominal typical-award band on each card, plus a shared `_render_real_terms_rail()` — a caveat
banner + an `st.popover` "How is this adjusted?" (index/source/method/caveat straight from
`fetch_inflation_indices` → `services.deflator.list_indices`) + the "⚗ Experimental · local only"
marker. Page computes nothing (firewall + logic-firewall checker green); it looks the real band up
by CPV from `fetch_cpv_summary_real_result`. Playwright-verified on a fresh server (7/7 checks:
default-OFF, band appears on toggle, rail + popover render, no traceback; construction €495k→€566k
matches the data).

**UI STEP 2 SHIPPED (gated, 2026-07-07).** The "paid" section gains a real-terms trend view. New
view `v_procurement_payments_real_trend` (year × tier indicative-floor rollup of the vat-separated
grain, with `real_uplift_pct` computed over ADJUSTABLE rows in SQL — the "+X%" metric stays below
the firewall); core `payments_real_trend()` + `fetch_payments_real_trend_result`. `_render_payments`
gets an "In real terms ⚗" view option → `_render_payments_real_trend()`: the shared rail (gov-
consumption index), a narrative, and a **single-series per-year uplift bar** (2012 +18% → 2024 0%),
plus an explicit "2025+ not yet adjustable — National Accounts pending" note (the deflator ends
2024). **Impact measured first** (per user): the per-year lens de-biases 2012–18 by +10–18%, but the
aggregate is only +4% (recency-skewed) and 27% of SPENT € can't be adjusted — so it's framed as a
per-year trend, NOT a "real total". Chose the uplift chart over nominal-vs-real absolute bars because
those stacked into a false sum and hid the (old-year) story. Firewall + logic-firewall + 127 tests
green; Playwright-verified (8/8 + chart paints 14 bars). **GOTCHA logged:** an `st.bar_chart` y-field
name with spaces/"%" silently renders an EMPTY chart — use a plain column name + `y_label` for the
axis title (the working awards-by-year pattern). **UI STEP 3 + API SHIPPED (gated, 2026-07-07).** Step 3: `v_procurement_cpv_summary_real` gains a
sector-aware band (aggregates `value_eur_real_sector` + `deflator_index_sector`), core
`cpv_summary_real` carries it, and `_render_cpv` shows construction (45*/71*) cards "in 2025 tender
prices" (SCSI TPI) and others "in 2025 prices" (CPI), with a caption naming the split. HONEST NOTE:
the construction *median* barely moves vs CPI (€565,549 → €565,695) — construction awards skew
recent, so this is a correctness/labelling win, not a magnitude one. API: three gated endpoints in
`api/routers/procurement.py` — `/procurement/inflation/{indices,cpv,spend-trend}` — with
`include_in_schema=_EXPERIMENTAL` + a 404 gate, so they're invisible and unreachable on the deployed
API (flag unset) and return data locally. Caveat string on every envelope. Tests:
`test_api_inflation_gating.py` (404 + hidden-from-schema when off) + core/sql column tests; 131 pass.
STILL PENDING (both user decisions): graduation (un-gate + commit the new gold parquets) and,
optionally, MCP-tool real-terms fields.
**Scope:** integrate the existing CPI deflator into tender/bid intelligence so award values
can be shown in original € and today's €, and so benchmark bands are computed on
inflation-adjusted values — without ever implying *adjusted award value = current cost =
recommended bid price*.

Cross-refs: `services/deflator.py`, `dail_tracker_core/qs_valuation.py`,
`sql_views/procurement/procurement_cpv_summary.sql`, `…/procurement_bid_signal.sql`,
`doc/PROCUREMENT_MASTER.md`, `doc/PROCUREMENT_INTELLIGENCE_ROADMAP.md`.
Memory: `project_cso_esri_deflator_scoping_2026_06_21`, `project_bid_signal_experimental_2026_06_21`,
`project_procurement_phase_taxonomy`, `feedback_no_inference_in_app`.

---

## 0. TL;DR

* The deflation engine **already exists and is tested** — `services/deflator.py` (`Deflator`),
  backed by gold `data/gold/parquet/cso_cpi_deflator.parquet` (CSO **CPA07 CPI**, chain-linked,
  base 2025). It is **not** Central Bank of Ireland — the brief's "CBI function" is the CSO CPI
  deflator. ~22 unit tests pin its behaviour.
* **The integration into procurement views was never done.** No SQL view references the deflator
  (grep `deflator|value_eur_real` over `sql_views/**` = 0 hits). That is the actual gap.
* A **second index already exists** in code: the **SCSI Tender Price Index** (TPI),
  `data/_meta/scsi_tender_price_index.csv`, used by `qs_valuation.py`. A **third is scoped but
  unbuilt**: CSO **WPM39** construction-materials WPI. So "multiple indices" is partly built —
  the design just needs to make index choice explicit and pluggable.
* Design principle (non-negotiable, ties to `feedback_no_inference_in_app`): nominal
  `value_eur`/`amount_eur` stay canonical and byte-identical; real-terms is a **derived,
  default-OFF** lens; a missing year yields **NULL, never a silent ×1.0**; ceilings and
  cross-tier sums are never deflated into a benchmark.

---

## 1. The existing inflation-adjustment function

`services/deflator.py`:

| Member | Signature | Returns |
|---|---|---|
| `Deflator.load(path=…)` | classmethod | a `Deflator` from the gold parquet |
| `.factor(year, to=None)` | year→year factor | `index[to]/index[year]`; **None** if either year missing |
| `.inflate(value, year, to=None)` | adjust one value | `value*factor`; **None** if value or year unusable |
| `.deflate_series(df, value_col, year_col, out_col, to=None)` | Polars vectorised | df with `out_col`; NULL where year missing |
| `value_plausible_expr` / `implausible_mask` | fact-layer DQ guard | flag €<100 / €>5e8 parse artefacts |

Backing table `cso_cpi_deflator.parquet` columns: `year, cpi_pct_change, cpi_index_chained,
deflator_to_base, base_year`. `deflator_to_base = index[base] / index[year]`, so
`value_eur * deflator_to_base` = value in base-year (2025) €. Base year factor = 1.0.

**Contracts already enforced by tests** (`test/contracts/test_deflator_function.py`,
`test_cpi_deflator_contract.py`): base-year identity, round-trip lossless (err 3.7e-9),
order-preserving, missing-year→None (never 1.0), function == table all years,
**SQL deflation == Python function** (parity).

### Second/third indices already present or scoped
* **SCSI TPI** (tender-price inflation) — `qs_valuation.py::tpi_for_year/_tpi_for_period`,
  half-yearly 1998H1–2025H1, base 1998H1=100. This is the QS "bring costs to tender date"
  index — **construction tender prices, not consumer prices**.
* **CSO WPM39** (building & construction *materials* WPI, 2021+; legacy WPM28 2015+) — scoped in
  `project_cso_esri_deflator_scoping_2026_06_21`, **not yet built** into a deflator table.

---

## 2. Inputs the function requires (and the gaps for multi-index)

| Brief input | In `Deflator` today | Notes / gap |
|---|---|---|
| amount | `value` (float\|None) | ✓ |
| from date/year | `year` (int) | **YEAR granularity only.** Payments could deflate by quarter via `period`; awards by `award_year`. |
| to date/year | `to` (int, default `base_year`=2025) | ✓ |
| index type | **implicit** (loaded table is CPI) | **GAP** — to support multi-index, carry `index_code` and load per-index instances (see §9). |
| source | **implicit** in gold provenance (CSO CPA07) | Surface it as an output column + caveat so each adjusted € is auditable. |

---

## 3. Procurement fields with dates + values suitable for adjustment

From `v_procurement_awards` (`procurement_awards.parquet`):

| Field | Date anchor | Adjust? |
|---|---|---|
| `value_eur` (`value_kind=contract_award_value`, `value_safe_to_sum=true`) | `award_date` → `award_year` | **YES** — the core case |
| `estimated_value_eur` (pre-award notice estimate, ~27% filled) | notice year (= `award_year` proxy) | YES, but keep separate (it's an estimate, not a disclosed award) |
| `value_eur` where `is_framework_or_dps` / `framework_or_dps_ceiling` | spans many years | **NO** (see §5) |

From `v_procurement_payments` (`procurement_payments_fact.parquet`):

| Field | Date anchor | Adjust? |
|---|---|---|
| `amount_eur` (`value_kind=payment_actual`, `realisation_tier=SPENT`) | `year` (refine to `period`/quarter later) | **YES** |
| `amount_eur` (`po_committed`, `COMMITTED`) | `year` | YES (order year) |

AFS budget tier (`procurement_afs_*`): by-division, annual, accrual aggregate — adjustable by
year but **lower priority** and ideally a government-consumption deflator, not CPI (academic
note, §8).

TED awards (`procurement_ted_*`): separate grain, has year — adjustable, same rules as eTenders.

---

## 4 & 5. What to adjust, what not to

| Value type | Adjust? | Rule |
|---|---|---|
| **Award value** (contract_award_value) | ✅ | deflate from `award_year`; this is a disclosed point-in-time figure |
| **Safe awarded value** (value_safe_to_sum subset) | ✅ | same field; benchmark bands run over *exactly this set* |
| **Estimated value** (estimated_value_eur) | ⚠️ adjust but isolate | it's a buyer's pre-award estimate; never blend into the award band |
| **Payment / PO value** (amount_eur) | ✅ | deflate by year; **never union with awards** (different grain) |
| **Audited expenditure** (AFS) | ⚠️ optional | annual deflation OK; prefer govt-consumption deflator; never union |
| **Framework / DPS ceiling** (framework_or_dps_ceiling) | ❌ | multi-year legal headroom; single-year deflation meaningless; never in a benchmark band |
| **value_shared_across_suppliers** rows | ❌ | one ceiling repeated across supplier rows; would double-count |
| **Multi-year contracts** (`contract_duration_months` spans years; ~41% of awards) | ⚠️ flag | single award-year deflation is an APPROXIMATION → emit `MULTI_YEAR_APPROX` caveat |
| **Missing/null year** (awards 310, payments ~13k) | ❌ leave nominal | real = **NULL**, never silent ×1.0 |
| **Fails `value_plausible`** (€<100 / €>5e8) | ❌ | deflation only scales a parse artefact → exclude from real bands |
| **Any cross-tier sum** | ❌ | the 3-money-grain never-union rule applies to real terms too |

### The one caveat that governs the whole feature (Goal 3)
**Adjusted award value ≠ current cost ≠ recommended bid price.** CPI re-expresses the
*purchasing power* of a *past disclosed figure* in today's money. It does **not** tell you what
the same work costs to procure today (that is construction inflation, materials inflation,
labour rates, scope and market conditions) and it is **not** a bid recommendation. This must
appear in the doc, the API field descriptions, and the UI — see §8.

---

## 6. Output columns (per-row contract / additive)

Nominal columns are **unchanged and canonical**. Real-terms columns are **added**:

| Column | Meaning | Example |
|---|---|---|
| `value_eur` | original amount (nominal, canonical) | `250000` |
| `value_year` (`award_year`) | original year | `2013` |
| `value_eur_real` | adjusted amount | `310096` |
| `real_base_year` | adjusted-to year | `2025` |
| `deflator_index` | index used | `CSO_CPA07_CPI` |
| `deflator_factor` | adjustment factor (`index[to]/index[from]`) | `1.2404` |
| `value_kind` | (already exists) drives whether real is shown/summed | `contract_award_value` |
| `real_caveat` | machine-readable enum | `OK` \| `MULTI_YEAR_APPROX` \| `CEILING_NOT_ADJUSTED` \| `YEAR_MISSING` \| `IMPLAUSIBLE` \| `NO_VALUE` |

`real_caveat` lets every consumer (UI, API, export) render the right warning without re-deriving
the rule. `value_eur_real` is NULL whenever `real_caveat ∈ {YEAR_MISSING, IMPLAUSIBLE}` and is
suppressed-from-bands whenever `∈ {CEILING_NOT_ADJUSTED}`.

---

## 7. Benchmark summary (aggregate columns)

Extend `v_procurement_cpv_summary` and `v_procurement_bid_signal` with **real-terms** bands,
computed by deflating **per row first, then aggregating** (this is the point: it stops mixing
award-years from baking ~24% pure inflation into the "price signal" — see memory). Over
`value_safe_to_sum AND value_eur>0 AND deflator available` only:

| Summary | Column |
|---|---|
| median adjusted value | `median_award_real_eur` |
| interquartile range | `p25_award_real_eur`, `p75_award_real_eur` |
| min / max | `min_award_real_eur`, `max_award_real_eur` |
| outliers | `n_outliers_real` (count beyond p75 + 1.5×IQR on the *adjusted* distribution) |
| buyer-specific range | per `contracting_authority × cpv` (sibling view `v_procurement_cpv_buyer_summary_real`) |
| category-wide range | per `cpv` (the columns above) |
| sample size | `n_awards_valued_real` (rows that had a usable year) + `n_year_missing` (honest gap) |

Each real column sits **beside** its nominal twin so the page can flip lens without losing the
disclosed figure.

---

## 8. Caveat wording (use verbatim)

**Headline (every real-terms surface):**
> Shown in **2025 prices** (adjusted for general consumer-price inflation, CSO CPI). This
> re-expresses a *past disclosed contract value* in today's money. **It is not** what the same
> work would cost to buy today, and **it is not** a recommended bid price.

**The index caveat (Goal: general ≠ construction inflation):**
> General consumer-price inflation (CPI) is **not** the same as construction inflation,
> building-materials inflation, electrical-materials inflation, labour-rate inflation, or
> tender-price inflation. These move at very different rates — e.g. construction materials rose
> ~17% in 2021→22 while CPI rose far less. For a construction or trades benchmark, prefer the
> SCSI Tender Price Index lens where shown; CPI is a general-purchasing-power lens only.

**Multi-year contract caveat (`MULTI_YEAR_APPROX`):**
> This contract runs over multiple years; we adjust from its award year only, so the real-terms
> figure is an approximation (the spend was actually spread across years).

**QS / commercial copy (for the bid-intelligence cards):**
> Use the real-terms band to compare contracts awarded in different years on a like-for-like
> purchasing-power basis. It removes *general* inflation between award years; it does **not**
> remove differences in project size, scope, specification, location, materials/labour mix, or
> market conditions — and a CPV is a category, not a unit of work. Treat it as a sizing
> sanity-check, never as a quote or a tender estimate.

---

## 9. Multi-index support (Goal 4)

Generalise from "the CPI deflator" to a small **index registry** so the same code path serves
CPI now and TPI/WPM39 later. The output `deflator_index` column already makes the choice
auditable; the engine change is additive.

```
INDEX REGISTRY  (index_code → spec)
  CSO_CPA07_CPI : gold cso_cpi_deflator.parquet, annual, base 2025,
                  applies_to = ALL value_kinds, label "Consumer prices (CSO CPI)",
                  caveat "general purchasing power, not construction cost"        [BUILT]
  SCSI_TPI      : data/_meta/scsi_tender_price_index.csv, half-yearly, base 1998H1,
                  applies_to = construction CPV (45*, 71*), label "Tender prices (SCSI)"  [DATA EXISTS in qs_valuation.py]
  CSO_WPM39     : (to build) construction-materials WPI, monthly, 2021+,
                  applies_to = works/materials, label "Construction materials (CSO WPI)"  [SCOPED, not built]
```

`Deflator.load(index_code="CSO_CPA07_CPI")` chooses the table; the registry records granularity,
base, applicable `value_kind`/CPV scope, label, source URL and per-index caveat. Views pick the
default index per surface (CPI everywhere; offer TPI on construction bid-signal cards). **The
index is never silently mixed** — a benchmark band is computed under exactly one index and labels
which one.

---

## 10. SQL / query changes

### 10a. Register the deflator as a view (new)
`sql_views/_shared/cpi_deflator.sql` (or `procurement/`):
```sql
CREATE OR REPLACE VIEW v_cpi_deflator AS
SELECT year, cpi_index_chained, deflator_to_base, base_year,
       'CSO_CPA07_CPI' AS index_code
FROM read_parquet('data/gold/parquet/cso_cpi_deflator.parquet');
```

### 10b. Per-row real columns on the awards view (additive)
```sql
-- in v_procurement_awards, after value_eur / value_kind …
a.value_eur                                   AS value_eur,           -- nominal, canonical
TRY_CAST(substr("Notice Published Date/Contract Created Date", 7, 4) AS INT) AS award_year,
CASE WHEN d.deflator_to_base IS NOT NULL
          AND a.is_framework_or_dps IS NOT TRUE
          AND a.value_plausible IS NOT FALSE
     THEN a.value_eur * d.deflator_to_base END AS value_eur_real,     -- NULL ⇒ leave nominal
d.base_year                                    AS real_base_year,
'CSO_CPA07_CPI'                                AS deflator_index,
d.deflator_to_base                             AS deflator_factor,
CASE
  WHEN a.value_eur IS NULL            THEN 'NO_VALUE'          -- probe found this is ~41% of award rows
  WHEN a.is_framework_or_dps          THEN 'CEILING_NOT_ADJUSTED'
  WHEN a.value_plausible IS FALSE     THEN 'IMPLAUSIBLE'
  WHEN d.deflator_to_base IS NULL     THEN 'YEAR_MISSING'
  WHEN a.contract_duration_months > 12 THEN 'MULTI_YEAR_APPROX'
  ELSE 'OK'
END                                            AS real_caveat
-- FROM read_parquet(... awards ...) a
-- LEFT JOIN v_cpi_deflator d ON award_year = d.year   (LEFT JOIN ⇒ missing year = NULL real)
```
`LEFT JOIN` is load-bearing: a missing award year produces a NULL `value_eur_real`, matching the
function's None contract (never ×1.0). Per `feedback_sql_view_dependency_order`, either read the
deflator parquet inline in each benchmark view or register `v_cpi_deflator` *before* the
procurement views in the connection's view list.

### 10c. Real-terms benchmark band (extend `procurement_cpv_summary.sql`)
```sql
WITH a AS (
  SELECT "Main Cpv Code" AS cpv_code, value_eur, value_safe_to_sum,
         TRY_CAST(substr("Notice Published Date/Contract Created Date",7,4) AS INT) AS award_year
  FROM read_parquet('data/gold/parquet/procurement_awards.parquet')
  WHERE "Main Cpv Code" NOT IN ('', 'NULL') AND "Main Cpv Code" IS NOT NULL
),
r AS (
  SELECT a.*, a.value_eur * d.deflator_to_base AS value_eur_real, d.base_year
  FROM a LEFT JOIN v_cpi_deflator d ON a.award_year = d.year
)
SELECT
  cpv_code,
  -- nominal (unchanged) …
  median(value_eur)        FILTER (WHERE value_safe_to_sum AND value_eur>0)        AS median_award_eur,
  -- real-terms band (only rows with a usable year) …
  median(value_eur_real)   FILTER (WHERE value_safe_to_sum AND value_eur_real>0)   AS median_award_real_eur,
  quantile_cont(value_eur_real,0.25) FILTER (WHERE value_safe_to_sum AND value_eur_real>0) AS p25_award_real_eur,
  quantile_cont(value_eur_real,0.75) FILTER (WHERE value_safe_to_sum AND value_eur_real>0) AS p75_award_real_eur,
  min(value_eur_real)      FILTER (WHERE value_safe_to_sum AND value_eur_real>0)   AS min_award_real_eur,
  max(value_eur_real)      FILTER (WHERE value_safe_to_sum AND value_eur_real>0)   AS max_award_real_eur,
  count(*)                 FILTER (WHERE value_safe_to_sum AND value_eur_real>0)   AS n_awards_valued_real,
  count(*)                 FILTER (WHERE value_safe_to_sum AND value_eur>0 AND award_year IS NULL) AS n_year_missing,
  any_value(base_year)     AS real_base_year,
  'CSO_CPA07_CPI'          AS deflator_index
FROM r GROUP BY cpv_code;
```
Outliers (`n_outliers_real`): a second CTE computing the per-CPV p25/p75 then counting rows
beyond `p75 + 1.5×(p75−p25)` on the **adjusted** distribution. Buyer-specific range = the same
aggregation grouped by `contracting_authority, cpv_code` in a sibling view.

### 10d. Bid-signal real band (`procurement_bid_signal.sql`)
Add `award_*_real_eur` (p25/median/p75) alongside the existing nominal band, deflated per-row
before `quantile_cont`. Ceilings stay excluded (they already are). This is the band where
deflation matters most (mixed award-years per CPV trade).

---

## 11. API field changes

* **MCP `procurement_by_cpv`, `procurement_notice`, `get_supplier`, `search_suppliers`** — add
  `value_eur_real`, `real_base_year`, `deflator_index`, `deflator_factor`, `real_caveat` to the
  row payload; add `median_award_real_eur` + IQR to the CPV summary payload. Each tool's
  description string states the §8 headline caveat. **Do not** route through
  `project_value_estimate` (flagged unreliable in memory).
* **FastAPI `/v1/data`** (`api/main.py`) — the new columns flow through automatically once the
  views expose them; document them in the OpenAPI field descriptions with the caveat.
* All API responses carry the index + factor + caveat so a downstream consumer can never present
  an adjusted figure without its provenance.

---

## 12. Report / UI changes (procurement page)

* **Default-OFF toggle** "Show in 2025 prices" (per the CPI plan: no cited figure changes by
  default). When ON, money figures render real with a persistent **index badge** (`CSO CPI ·
  2025 prices`).
* **Per-figure `st.popover`** "How is this adjusted?" — shows from-year, factor, index, source,
  and the §8 caveat. (`st.popover` is unused elsewhere — genuinely new, per roadmap memory.)
* **Caveat banner** on any real-terms section carrying the general-≠-construction-inflation copy.
* **QS bid-signal cards**: where the CPV is construction (45*/71*), offer the SCSI TPI lens as an
  alternative index toggle, labelled "Tender prices (SCSI)", reusing `qs_valuation.py`.
* Per the logic firewall, all of this renders from view columns / `data_access` wrappers — the
  page computes nothing.

---

## 13. Test cases

Extend `test/contracts/test_deflator_function.py` + `test/sql_views/test_sql_views.py`:

1. **Base-year identity** — `factor(2025)==1.0`; `value_eur_real == value_eur` for 2025 awards.
2. **Round-trip** — `inflate(inflate(x,2013),2025,to=2013) ≈ x`.
3. **Missing year ⇒ NULL** — award with null/unparseable year ⇒ `value_eur_real IS NULL`,
   `real_caveat='YEAR_MISSING'`; **never** equals nominal.
4. **SQL == function parity** — `v_procurement_awards.value_eur_real` equals
   `Deflator.load().inflate(value_eur, award_year)` row-for-row (extends existing parity test).
5. **Ceiling excluded** — `is_framework_or_dps` rows have `value_eur_real IS NULL` and
   `real_caveat='CEILING_NOT_ADJUSTED'`; ceilings never enter any `*_real` band.
6. **Multi-year flag** — `contract_duration_months>12` ⇒ `real_caveat='MULTI_YEAR_APPROX'`.
7. **Plausibility gate** — €0.99 / €2.5bn award ⇒ `real_caveat='IMPLAUSIBLE'`, excluded from bands.
8. **Nominal unchanged** — `value_eur` / `amount_eur` byte-identical with feature on or off.
9. **Order preserved** — for same-year rows, real ranking == nominal ranking (monotone).
10. **Band integrity** — `median_award_real_eur` lies within `[p25_award_real_eur,
    p75_award_real_eur]`; `n_awards_valued_real + n_year_missing == n_awards_valued`.
11. **Buyer vs category** — both grouped views populate and the buyer band ⊆ category min/max.
12. **Index labelling** — every real column row carries a non-null `deflator_index`; no band mixes
    indices.

---

## 14. Example output table

**Per-award (real-terms lens on):**

| tender_id | cpv | award_year | value_eur (nominal) | value_eur_real | index | factor | real_caveat |
|---|---|---|---:|---:|---|---:|---|
| T-1001 | 45310000 (electrical) | 2013 | 250,000 | 310,096 | CSO_CPA07_CPI | 1.2404 | OK |
| T-1002 | 45310000 | 2019 | 280,000 | 308,400 | CSO_CPA07_CPI | 1.1014 | OK |
| T-1003 | 45310000 | 2025 | 300,000 | 300,000 | CSO_CPA07_CPI | 1.0000 | OK |
| T-1004 | 45000000 (works framework) | 2018 | 40,000,000 | *(null)* | CSO_CPA07_CPI | — | CEILING_NOT_ADJUSTED |
| T-1005 | 45310000 | *(missing)* | 195,000 | *(null)* | CSO_CPA07_CPI | — | YEAR_MISSING |

**Category benchmark (CPV 45310000 — electrical, value_safe_to_sum awards):**

| metric | nominal € | 2025 € (real) |
|---|---:|---:|
| median | 252,000 | 271,500 |
| p25 / p75 (IQR) | 110,000 / 540,000 | 124,000 / 560,000 |
| min / max | 9,500 | 1,180,000 | 11,800 | 1,180,000 |
| n valued (n year-missing) | 412 (n=18 nominal-only) | 394 |
| outliers (beyond p75+1.5·IQR, real) | — | 22 |

> *2025 prices, CSO CPI. Re-expresses past award values in today's purchasing power — not the
> current cost of the work and not a bid price. General CPI ≠ construction/materials/labour/
> tender-price inflation.*

---

## 15. Open decisions (user's domain — do not decide autonomously)

Per `feedback_provenance_is_users_domain` / `feedback_pipeline_changes_data_anchored_promotion`:

1. **Base-year label** — "2025 prices" vs a user-selectable target year.
2. **Default index per surface** — CPI everywhere, or TPI as default on construction bid cards?
3. **Ship WPM39 now** (materials index) or defer to phase 2?
4. **Gold promotion** — keep real columns as *derived view* columns (recommended, additive,
   nothing in gold changes) vs materialising into the fact parquet.
5. **Outlier fence** — 1.5×IQR vs a domain-specific threshold.
6. **AFS / audited-expenditure** — adjust with CPI now, or wait for a govt-consumption deflator?

---

## 16. Sandbox probe results (validated 2026-06-28)

Prototype built and run in `c:/tmp/inflation_proto/` (`proto_views.sql` + `run_proto.py`) —
reads gold READ-ONLY, writes nothing to the repo. All views + the 5 checks pass.

**Checks (all PASS):**
* **SQL == `services.deflator.Deflator.inflate` parity** — 15,997 rows, max relative error
  **0.0** (exact; both use the same precomputed `deflator_to_base`).
* base-year (2025) identity (real == nominal); ceilings never deflated (0 leaked);
  caveat enum consistent with `value_eur_real` null-ness (0 mismatches); YEAR_MISSING ⇒ NULL real.

**`real_caveat` distribution over all 62,763 award rows — the real coverage picture:**

| caveat | rows | % | meaning |
|---|---:|---:|---|
| `NO_VALUE` | 26,003 | 41.4% | award row carries no € at all |
| `CEILING_NOT_ADJUSTED` | 15,971 | 25.4% | framework/DPS ceiling (correctly excluded) |
| `OK` | 11,590 | 18.5% | clean single-year deflatable award |
| `MULTI_YEAR_APPROX` | 7,446 | 11.9% | deflated, flagged (duration > 12 mo) |
| `IMPLAUSIBLE` | 1,417 | 2.3% | parse artefact, excluded |
| `YEAR_MISSING` | 336 | 0.5% | year outside the index (mostly 2026) |

→ Only **~30% of award rows (OK + MULTI_YEAR_APPROX) are actually deflated**; the band set
(sum-safe, value>0, adjustable) is **~16k**. The OSS-confirmed engine is sound; the binding
constraint on the feature is **award-level coverage** (41% no value, ~58% no CPV), *not* the
deflation. Lead the feature on totals/trends + buyer-vs-category, and surface the coverage
denominators honestly (`n_real_excluded` / `NO_VALUE`).

**Honest read on buyer-vs-category:** the largest divergences the probe surfaced (e.g. HSE IT
services real median €3.8m vs category €366k) are driven by **contract size**, not inflation
timing — a CPV is a category, not a unit of work. The view is structurally correct, but the
inflation contribution is the smaller year-mix component; do not present the divergence as an
inflation effect.

---

## 17. Methodology validation — is CPI the right index? (checked against agency practice)

**The arithmetic is textbook-correct.** `real = nominal × index[base]/index[year]`, chain-linking
across rebased CPA07 segments via the 12-month %-change, latest-year base ("2025 prices"),
missing-year→NULL — these are exactly how statistical agencies rebase/splice and how palewire
`cpi` / priceR `adjust_for_inflation` work. No flaw in *how* we affect the figure.

**The CHOICE of CPI is a defensible proxy, NOT the agency standard for public money.** What
others actually do:

| Who | Deflating public spend/procurement | Deflating construction |
|---|---|---|
| **HM Treasury (UK)** | **GDP deflator**, not CPI — "reflects prices of all domestically produced goods and services *including Government services*… better suited to public expenditure flows than CPI/RPI" (CPI is a *household* basket and includes imports) | — |
| **US BEA / BLS / FRED** | **GDP (implicit) price deflator** — covers consumers, business, government, foreigners; "policymakers use the GDP deflator to deflate macro aggregates such as tax revenue or public expenditure" | — |
| **BCIS / RICS (UK)** | — | **Tender Price Index** (cost to the client of *procuring* the asset, incl. margins) / Output Price Index; CPI/RPI "do not reflect construction-specific cost movements" (CPI 3.3% vs construction 3.8% = £50k under-estimate on £10m) |
| **palewire `cpi` / priceR** | CPI (general-purpose journalism/data tools) | CPI |

So three *different questions* need three *different* indices, and a single CPI lens silently
conflates them:
1. **"Express a past disclosed award in today's general purchasing power"** → CPI. ← what we do.
   Valid for *comparing magnitudes across years*; this is the honest, narrow claim.
2. **"What were public-spend totals in real terms?"** → **GDP / government-consumption-expenditure
   deflator** (the HM-Treasury / BEA standard). CPI is *second-best* here.
3. **"What would this work cost to procure today?"** → **tender/output price index** (BCIS in the
   UK; the **SCSI Tender Price Index we already have** in `qs_valuation.py` is the Irish analog).
   CPI is *wrong* here — this is the construction-inflation caveat, now agency-backed.

**Ireland-specific nuance (important):** the UK answer ("just use the GDP deflator") does **not**
transplant cleanly — **Irish GDP is distorted by multinational/IP activity**, so a raw Irish GDP
deflator is itself contaminated. The clean Irish equivalent of HM Treasury's GDP deflator is the
**government final consumption expenditure deflator** (or modified-domestic-demand deflator) from
CSO National Accounts — NOT the GNI* implicit deflator (already rejected as a volatile trap, see
the deflator-scoping memory). This contamination is precisely why **CPI is a more defensible
interim proxy in Ireland than it would be in the UK** — but it still carries the household-basket
mismatch and slight Laspeyres over-statement (CPI overstates vs a chained GDP deflator).

**Verdict:** the implementation is valid and honestly caveated *for question 1*. To be
methodologically aligned with how public money is actually deflated, the project should:
* **Re-label, never imply CPI is "the" deflator** — call it "general consumer-price terms (CSO
  CPI)"; the views/caveats already say "not construction/materials/labour/tender-price inflation",
  add "and not the GDP/government-consumption deflator used for public-spend real-terms".
* **Add a CSO government-consumption-expenditure deflator** as the *preferred* index for
  procurement/spend **totals** (the HM-Treasury-equivalent; National-Accounts source — the
  Phase-2 item already scoped). Keep CPI as the transparent fallback.
* **Route construction CPVs (45*/71*) through the SCSI TPI** (already built) — the BCIS-equivalent.

This makes the **multi-index registry (§9) not a nice-to-have but a methodological requirement**:
each value-type/question must declare which index it used (`deflator_index` already does this).

*Sources:* HM Treasury GDP-deflator guidance & PESA; UK Commons Library "Public spending: a brief
introduction"; BLS "Comparing the CPI with the GDP price index"; BEA GDP price deflator; BCIS
"difference between Tender Price and Output Price Indices" / "are you using the right index".
