# Per-LA Annual Financial Statements — feasibility census + build plan

> **UPDATE 2026-06-03 — PHASE 0 SHIPPED.** `pipeline_sandbox/la_afs_extract.py` →
> `data/silver/parquet/la_afs_divisions.parquet`: **72 rows, 9 councils, 9/9 reconcile
> EXACTLY to the printed total, 0 accounting-identity violations.** The predicted parser-debt
> items #1 (generalise the finder) and #2 (file-selector) were resolved by **`best_ie_page`**
> — scan every page, keep the one whose `parse_ie` yields the most divisions AND reconciles
> (rejecting narrative pages and the Note-16 sub-table). This fixed Galway County (was 1/8
> €2.3m → p14 €181.4m gross), Westmeath, and Donegal in one change. Remaining work below is
> Phase 1+ (more councils, seed-URL fixes, Playwright tail). State: [[project_la_afs_fact]].

> **Status:** SCOPING ONLY (2026-06-03). No extractor code written. This is the
> reachability + format census that answers "does a per-council AFS cleanly exist for
> each of the 31 LAs?" and sizes the build. Companion to the national-amalgamated layer
> already shipped (`pipeline_sandbox/afs_amalgamated_extract.py` →
> `data/silver/parquet/afs_amalgamated_divisions.parquet`, 64 rows, **all-31-summed,
> zero per-council rows**). The per-LA layer is item #2 in the procurement ingestion
> backlog (`PROCUREMENT_BUILD_PLAN.md` §8c) — the per-constituency prize.
>
> **Why this doc is separate:** `PROCUREMENT_BUILD_PLAN.md` is being edited by another
> context concurrently; this is filed separately to avoid an edit collision.

## The question it settles

A national figure exists (the amalgamated AFS); **no clean per-council source does.** The
gov.ie collection and the [PSB datacatalogue dataset](https://datacatalogue.gov.ie/dataset/local-authority-annual-financial-statements)
are explicitly *"the amalgamated AFS for all 31 local authorities"* — **no per-LA
breakdown, `Open Data: No`.** So per-LA = **31 bespoke website harvests** of each
council's own audited AFS PDF. The census below confirms those PDFs exist and are
overwhelmingly digital.

## Census method

Throwaway probe (`c:/tmp/afs_census/probe.py`, not repo code) reusing the proven
`procurement_la_seed` fetch (requests + curl fallback for WAF/TLS) and the amalgamated
extractor's `DIVISIONS` keyword set. Per council: fetch a candidate AFS landing →
one-hop crawl finance/statement nav links → harvest AFS-titled PDFs → download the latest
→ fitz digital/scanned test (on a mid-page, so image cover pages don't false-flag) →
run the national I&E-by-division page finder (`≥6 division keywords + "gross expenditure" +
"income"`). Raw results: `c:/tmp/afs_census/afs_census.csv`.

## Census results (2026-06-03)

| Bucket | n | Councils |
|---|---|---|
| **DIGITAL + I&E page auto-found** | **9** | South Dublin, Cork City, Cork County, Westmeath, Galway City, Galway County, Meath, Donegal, Tipperary |
| **DIGITAL, finder missed the page** | **9** | Wicklow, Monaghan, Kilkenny\*, Kildare, Sligo, Clare, Fingal, Louth\*, Dún Laoghaire-Rathdown |
| **SCANNED (OCR risk)** | **2** | Laois, Roscommon |
| **No AFS link found (discovery gap)** | **11** | Wexford, Waterford, Limerick, Offaly, Longford, Kerry, Leitrim, Mayo, Carlow, Cavan, Dublin City |

\* Kilkenny picked a 3-page summary and Louth a 2027 development plan — **wrong-file
artifacts**, not the AFS. File selection must filter to a real AFS document (title +
page-count), not just "newest .pdf with a year".

### How to read this

- **18 digital / 2 scanned among the 20 reached** → the "digital everywhere, ~10% OCR"
  pattern from the PO corpus **holds for AFS too.** OCR is a narrow tail (Laois,
  Roscommon — and even those may be a scanned cover over a digital body; recheck).
- **The 9 "finder missed the page" are real AFS** — their PDFs are 42–61 pages (exactly
  AFS-sized) and digital. The national finder is simply too strict: individual AFS title
  the statement *"Income & Expenditure Account Statement by Division"* / *"Statement of
  Comprehensive Income"* and don't always put the literal words "gross expenditure" and
  "income" on one page. **This is finder-generalisation debt, not a missing source.**
- **The 11 "no-link" are discovery gaps, not absences.** Every LA must publish an AFS by
  statute (prepare by end-March, publish audited by end-June, audited by the Local
  Government Audit Service). The misses are my wrong seed URL (I pointed Limerick/Dublin
  City at a budgets page), a JS-rendered file list (Carlow/Cavan/Mayo — the same councils
  that were JS-rendered for POs), or an AFS nav path the one-hop crawl didn't reach.

## Feasibility verdict: **GO — bounded, additive, ~10% OCR tail**

Real, materially harder than the amalgamated layer (1 PDF/year) but tractable:

- **Reachability:** ~20 confirmed now; ~27–29 obtainable after fixing seed URLs + the 3–4
  JS councils via the project's Playwright (same set as the PO build). 2 genuine OCR.
- **Extraction:** reuse `afs_amalgamated_extract.py` wholesale — `to_num` (incl. €m/M-suffix
  + parens), `DIVISIONS` keyword taxonomy, `parse_ie`, per-year reconciliation, the golden
  fixtures pattern. The per-LA delta is **(a)** a more permissive `find_ie_page`, **(b)** an
  AFS-document file-selector, **(c)** a `council` + `entity` column on every row.
- **Grain:** accrual net-expenditure by service division per council per year — **a
  different grain from the cash-PO layer; never reconcile the two** (carries the existing
  `realisation_tier=SPENT` / `value_kind=net_expenditure_actual` tags, scope = single LA).

## Known parser-debt (enumerated, all bounded)

1. **Generalise `find_ie_page`** — accept "income & expenditure … by division/service
   division" titles without requiring "gross expenditure" + "income" co-located; keep the
   ≥6-division-keyword anchor. (Fixes the 9 digital misses.)
2. **AFS file-selector** — filter harvested PDFs to actual AFS (title regex + ≥30 pages),
   prefer **audited over unaudited**, pick latest year among those. (Fixes Kilkenny/Louth
   wrong-file, and the Galway-County `ie=4` low-division partial.)
3. **Per-council reconciliation gate** — like the amalgamated build, Σ division gross must
   equal the council's printed I&E total before a council's rows are trusted. This is the
   per-council DQ unit.
4. **Seed-URL fixes** for the 11 no-link councils (mostly 1 corrected landing URL each;
   Mayo/Carlow/Cavan need Playwright enumeration — defer with the PO Playwright batch).
5. **OCR tail** — Laois, Roscommon: recheck whether the body is digital; if truly scanned,
   defer (do **not** stand up PaddleOCR for 2 councils — reserved for SIPO).
6. **Note 16 budget-vs-actual** stacked sub-table — the same targeted sub-table parser the
   amalgamated work flagged; **out of scope for v1** (I&E-by-division statement only).

## Sequencing (cheapest value first)

1. **Phase 0 — the 9 clean councils.** Reuse the amalgamated extractor + a `council`
   column; reconcile each. Proves the per-LA fact end-to-end fast.
2. **Phase 1 — generalise the finder** → folds in the 9 digital misses (→ ~18 councils).
3. **Phase 2 — seed-URL fixes** for the reachable no-link councils (Wexford/Waterford/
   Limerick/Offaly/Longford/Kerry/Leitrim/Dublin City) → ~26.
4. **Phase 3 — Playwright tail** (Carlow/Cavan/Mayo/Roscommon enumeration) — **batch with
   the PO Playwright work**, don't duplicate the harness.
5. **Defer** — the 2 OCR councils, Note 16, pre-2016 wording, capital/balance-sheet
   statements.

## Output contract (when built)

- **Fact:** `data/silver/parquet/la_afs_divisions.parquet` (zstd/3/stats + gitignore
  negation) — grain `(council, year, division)`; columns mirror
  `afs_amalgamated_divisions` + `council`, `entity` (county/city/merged), `source_file_url`,
  `printed_total_eur`, `reconciled` (bool). One row per division per council per year.
- **Bronze:** `data/bronze/pdfs/la_afs/{council_slug}/{year}.pdf`.
- **Tests:** mirror `test/test_afs_amalgamated.py` — `to_num`, a golden per-council I&E
  page parse, accounting identity (net = gross − income) on a committed golden parquet,
  per-council reconciliation, taxonomy tags.
- **Distinct from** `afs_amalgamated_divisions` (national) and `la_payments_fact`
  (cash-PO/payment grain) — a third, separate, non-unioned fact (accrual budget grain).

## Reuse, do NOT rebuild
`afs_amalgamated_extract.py` (extractor skeleton, `to_num`, `DIVISIONS`, `parse_ie`,
reconciliation, golden-fixture pattern); `procurement_la_seed.py` (fetch + curl + crawl);
`procurement_la_registry.py` (council → domain/region/entity); the PO build's Playwright
harness (when it lands) for the 4 JS councils. Census artifact:
`c:/tmp/afs_census/afs_census.csv`.
