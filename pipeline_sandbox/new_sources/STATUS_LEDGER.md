# New-source ingestion — sandbox run ledger

Run date: 2026-06-28. Driven by `doc/NEW_SOURCE_INGESTION_PLAN.md`.

> **Isolation contract.** Everything here is **sandbox only**. No `data/gold`
> writes, no `pipeline.py` edits, no API exposure, no promotion. Extractor code
> lives in `pipeline_sandbox/new_sources/`; all data lands under
> `c:/tmp/dail_new_sources/` (outside the repo data tree). Promotion to gold
> requires your review — provenance is your domain, and the graduation gates
> (licence confirmation → page contract → sandbox → fixture test → manifest →
> regression test) are non-negotiable.

## ✅ Ingested (real data pulled, lawful, open licence)

| Source | Plan ID | Script | Output (`c:/tmp/dail_new_sources/silver/`) | Rows | Coverage | Method |
|---|---|---|---|---|---|---|
| data.gov.ie CKAN metadata monitor | P0-6 | `datagov_monitor.py` | `datagov_catalogue.parquet` (+ `datagov_catalogue_summary.json`) | **22,338** | all datasets; metadata only | CKAN API |
| OIC / FOI decisions | P0-5 | `oic_foi_decisions.py` | `oic_foi_decisions.parquet` | **3,407** | full archive 2013→2026-06 | HTML scrape (344 pages) |
| C&AG audit reports index | P0-1 | `cag_reports.py` | `cag_reports.parquet` | **267** | 135 special reports + 103 appropriation accounts + 29 reports-on-accounts | HTML scrape |
| DPC decisions | P1 | `dpc_decisions.py` | `dpc_decisions.parquet` | **61** | full corpus 2019-08→2025-12 | HTML scrape |

**Total: ~26,073 rows.** Every row carries the provenance fields from the plan
(`source_url`, `fetched_at`, `extraction_method`, `confidence`, `privacy_tier`;
money rows flagged `value_safe_to_sum=False`).

### Data-quality flags observed (preserved, NOT silently fixed)
- **OIC:** one decision mis-dated `3017-03-30` (upstream typo for 2017) — kept as-is. `Health Service Executive` vs `The Health Service Executive` need name-normalisation before any join (use the project NFKD normaliser).
- **C&AG:** `source_published_date` is best-effort (first date on the detail page) — some are a sidebar/related-report date, not the publication date (e.g. SR119 shows 2017). Treat as low-confidence until the detail parse is hardened. PDF link captured for the first 200 reports only (`MAX_DETAIL`).
- **DPC:** the listing's `?page=` param is a no-op (returns the full list each request); `unique(source_url)` collapses it to the true 61. Loop should switch to a stop-on-no-new-rows guard before any scheduled use.
- **All four:** raw HTML/JSON was fetched but **not yet persisted to bronze with a SHA-256** — add `cache_raw()` + `source_document_hash` before promotion (the silent-reissue defence the plan §4 mandates).

## 🟡 Scaffolded (code stub + schema; blocked on an input)

| Source | Plan ID | Script | Blocker | Unblock by |
|---|---|---|---|---|
| OGP central arrangements / frameworks | P0-2 | `ogp_frameworks.py` | gov.ie **WAF 403** after a few requests **+ JS-rendered** listing (no static links/JSON) | gov.ie content API endpoint (devtools) or headless browser. **Public catalogue only — never scrape Buyer-Zone supplier lists.** |
| Election results | P0-4 | `election_results.py` | official EC structured data thin; primary source + licence is a human decision | choose electoralcommission.ie vs electionsireland.org/RTÉ + document licence/continuity; target GE2024+GE2020 |
| Companies House UK | P1 | `companies_house_uk.py` | needs a free **OGL API key** (can't provision unsupervised) | register key, set `COMPANIES_HOUSE_UK_KEY`; client is ready |

## ⛔ Deliberately NOT attempted (hard rules — would be unsafe unsupervised)

| Source | Reason |
|---|---|
| **RBO beneficial ownership** | Legally access-restricted (post-2022 CJEU). Ingesting without a lawful access route would be unlawful. |
| **Land Registry / Tailte ownership, CRO bulk** | Paid / licensed bulk access. |
| **Buyer-Zone supplier member-lists** | Gated, gov.ie-domain-only credentials. |
| **Bulk news / social media** | Copyright/ToS exposure (project `not-recommended` rule). |
| **Council minutes promotion, HSE §38/39, department grant registers** | Heavy PDF/OCR + per-source schema sprawl; stay sandbox until quality gates pass. Grant registers also hit the gov.ie WAF. |
| **Property Price Register** | PII-adjacent address data; needs privacy review before any pull. |

## Next steps to graduate any of the ✅ four
1. Re-run with `cache_raw()` persisting bronze + `source_document_hash`.
2. Add a fixture-based regression test per source.
3. Harden the flagged DQ items (C&AG dates, name normalisation, DPC paging guard).
4. Write a page/API contract if it gets a surface; confirm licence in `doc/source_licensing.md`.
5. Only then add a chain to `pipeline.py` and a `v_*` view. **Not before your review.**
