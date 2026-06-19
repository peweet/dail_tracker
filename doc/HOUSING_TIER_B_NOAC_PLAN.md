# Housing Tier B — NOAC Council Performance (planning)

Status: **SHIPPED 2026-06-19** (first pass). 6 indicators promoted to gold
(noac_h{1,2,3,4,6,7}_*_wide via `--gold`); view
`v_constituency_council_housing_performance` (per-LA value + national-median
benchmark) wired into a "Council housing performance" section on the Constituency
page. H5 (PRS inspections — polarity flag) and the H1 national output series remain
in `_noac_eval/` sandbox, NOT promoted. Retrofit surfaced as % of stock (normalised
by H1 stock), not raw count. See doc/HOUSING_NOAC_CONSTITUENCY_WIRING_PLAN.md for
the wiring detail. Original plan below.

Companion to Tier A (SSHA waiting-list, shipped to
sandbox as `pipeline_sandbox/housing/ssha_appendix_wide_extract_experimental.py`).
Source: `doc/source_pdfs/NOAC_LA_PerfInd_2024.pdf` (Local Authority Performance
Indicator Report 2024, NOAC — 11th edition).

## Goal
Per-local-authority **housing performance** indicators H1–H7, as labelled wide
tables, to power a "How is my council performing on housing?" view (and feed the
existing Constituency page housing section, which today shows only CSO supply
metrics: vacancy, price, completions).

## What is already proven
- `noac_housing_extract_experimental.py` runs green via `fitz` (PyMuPDF) — no
  camelot. 30–31 LAs per indicator; H6 only ~5 LAs report (known NOAC limit).
- BUT it emits **anonymous positional** `col_idx`/`value` long rows (same defect
  Tier A had). Column meaning is lost.

## Why Tier B is harder than Tier A (SSHA)
1. **No `Total` column** to validate against. SSHA's sum(categories)==Total gave a
   free 64/64 correctness gate; NOAC has no equivalent invariant. Need other
   checks (range sanity, cross-year deltas, cross-source reconciliation vs CSO).
2. **Truncated / coded headers.** find_tables returns headers like
   `A. Number of dwellings…`, `B. Number of local aut…` — clipped, and several
   columns share the same prefix. Must map letters → full definitions from the
   report's indicator-key text (lines ~579–634, e.g. `H5A: Total registered
   tenancies`, `H5C: % inspected dwellings not compliant`, `H5D: dwellings
   deemed compliant`, `H5E: number of inspections`).
3. **Multiple sub-tables per indicator.** Not one clean per-LA matrix each. e.g.
   H1 has `Table H1A` (national output 2018–2024), `Table H1B` (build by LA+AHB),
   `Table H1C` (acquisition by LA+AHB) — a mix of national year-series and per-LA
   matrices. Each needs its own handling; the simple "one wide table per
   indicator" shape from SSHA does not transfer directly.
4. **Mixed units in one indicator.** e.g. H3 = re-letting *time* (weeks, col A) +
   *cost* (€, col B); H5 mixes counts, %, and compliance flags. Can't blindly
   coerce all cells to one dtype.

## Per-indicator scoping (from the report's PI list, line ~641)
| Ind | Title | Per-LA columns (provisional) | Unit | Complexity |
|-----|-------|------------------------------|------|------------|
| H1 | Social Housing Stock | A–F: owned / leased / AHB / etc. dwelling + LA counts; plus H1A–H1C output sub-tables | counts | High (multi-subtable) |
| H2 | Housing Vacancies | vacancy rate | % | Low (single value/LA) |
| H3 | Avg Re-letting Time & Direct Cost | A: time, B: cost | weeks, € | Low |
| H4 | Housing Maintenance Direct Cost | cost per dwelling | € | Low |
| H5 | Private Rented Sector Inspections | A: registered tenancies, B: inspections, C: % non-compliant, D: compliant, E: inspections incl re-insp | counts, % | Medium |
| H6 | Long-term Homeless Adults | rate | % / count | Low (only ~5 LAs report) |
| H7 | Social Housing Retrofit | units retrofitted, (cost) | counts, € | Low–Medium |

The "Low" indicators (H2, H3, H4, H6, H7) are the fast wins — single clean per-LA
matrix, just need the column legend. H1 and H5 are the heavy ones.

## Proposed approach (reuse Tier A machinery)
1. **Build a NOAC column-legend dict** (`H1A..H7x → label, unit`) transcribed from
   the report key text. This is the NOAC analogue of the SSHA A1.4/A1.5 legends.
2. **Caption/anchor detection** like Tier A: locate each indicator's per-LA matrix
   by its `Table Hx` caption + the 31-LA row signature, rather than hardcoded
   pages (the current page indices in `noac_housing_extract_experimental.py` are
   already suspect — e.g. h1 `pages:[34,41]`).
3. **Emit one wide parquet per (indicator, sub-table)**: `la, <labelled cols…>,
   year` where applicable. Keep units explicit in column names (`_weeks`, `_eur`,
   `_pct`).
4. **Validation gates** (replacing SSHA's sum==Total):
   - LA coverage ≥ 30 (≥4 for H6).
   - Range sanity per unit (% in 0–100; weeks/€ non-negative; flag extreme YoY).
   - Cross-source: reconcile H1 stock totals and H2 vacancy against CSO
     (`cso_hsa07`, `cso_vac14`) already in gold — a real external check SSHA lacked.
5. **Start with the 5 Low-complexity indicators** (H2/H3/H4/H6/H7) to ship a first
   council-performance card, defer H1/H5 multi-subtable work to a second pass.

## Effort estimate
- Legend dict + caption refactor + H2/H3/H4/H6/H7 wide extractor: ~1 day.
- H1 (multi-subtable) + H5 (mixed units): ~1 day.
- Cross-source validation vs CSO + QC doc: ~0.5 day.
- View + UI wiring (separate, after data vetted): see Tier A wiring; reuse the
  LA→constituency crosswalk that Tier A also needs.

## Open decisions for the user
1. **Scope of first ship** — all 7 indicators, or the 5 Low-complexity ones first?
2. **Grain** — pure LA-level (matches NOAC), or also roll up to constituency via
   the crosswalk (same dependency as Tier A / `constituency_housing_context`)?
3. **Year depth** — 2024 report only, or backfill prior NOAC editions (2018–2023
   appear inside some H1 sub-tables; older editions would need their own PDFs).
4. **Source freshness** — NOAC is annual PDF; add a `freshness.json` entry +
   note that re-extraction is manual on each new edition.

## Cross-references
- Tier A extractor: `pipeline_sandbox/housing/ssha_appendix_wide_extract_experimental.py`
- Existing positional NOAC: `pipeline_sandbox/housing/noac_housing_extract_experimental.py`
- Housing feature overview + reality-check: `doc/SSHA_social_housing_summary.md`
- Live housing UI today: `utility/pages_code/constituency.py` (housing section),
  `sql_views/constituency/constituency_housing_context.sql`
