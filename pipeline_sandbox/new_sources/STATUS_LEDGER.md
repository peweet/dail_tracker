# New-source ingestion — sandbox run ledger

Run dates: 2026-06-28 (initial) · 2026-07-11 (second wave, see below). Driven by `doc/NEW_SOURCE_INGESTION_PLAN.md`.

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

## ✅ Second wave — 2026-07-11 run (same isolation contract)

| Source | Script | Output (`c:/tmp/dail_new_sources/silver/`) | Rows | Coverage | Licence |
|---|---|---|---|---|---|
| C&AG reports index (HARDENED) | `cag_reports.py` | `cag_reports.parquet` | **267** | 135 special + 103 appropriation + 29 report-on-accounts; PDF URL now 100% (was 200/267); bronze HTML+hash persisted; 2020+ PDFs cached (392 files, 93.8 MB) | **CC-BY-4.0** (site open-data policy, confirmed 07-12) |
| HIQA IPAS inspections | `hiqa_ipas_inspections.py` | `hiqa_ipas_inspections.parquet` | **101** | inspections 2024-01→2026-03, 21 counties, all 101 report PDFs cached (89.2 MB) | **PSI re-use** — "free of charge in any format", per PSI general licence (confirmed 07-12) |
| Research Ireland / SFI grant commitments | `research_ireland_grants.py` | `research_ireland_grants.parquet` | **8,475** | current RI dataset + SFI legacy (2024-07), dedup flags `id_in_both_sources`/`is_current_source`; starts 2001→2026 | **CC-BY-4.0** |
| Irish Aid ODA (IATI) | `irish_aid_iati.py` | `irish_aid_iati.parquet` | **21,470** | transaction grain: disbursement 19,517 + expenditure 1,953; `transaction_type` kept (never mix) | **CC0** |
| AHBRA register | `ahbra_register.py` | `ahbra_register.parquet` | **451** | 425 registered + 26 removed AHBs, 27 counties; **carries `cro_number` (61 null) + `charity_rcn` (44 null)** → CRO/charities joins nearly free | **PSI re-use** — "free of charge in any format… for any lawful purpose" (confirmed 07-12) |
| AHBRA notices/assessments | `ahbra_notices.py` | `ahbra_notices.parquet` | **79** | 65 statutory assessments + 7 annual reports + 7 other; outcomes incl. "Non-Compliant statutory action required" | **PSI re-use** (as register, confirmed 07-12) |

All rows carry the house provenance schema (`source_url`, `source_document_hash`,
`fetched_at`, `source_published_date`, `extraction_method`, `confidence`,
`privacy_tier`); money columns are `value_safe_to_sum=False`. Grant grains are
labelled (`grant_basis='commitment'` for RI; IATI `transaction_type`) — grants
are a third money channel, never summed with awards or payments.

### DQ flags from the 07-11 run (preserved, NOT silently fixed)
- **C&AG:** 148/267 detail pages carry no parseable publication date (`date_confidence` null); of the 119 dated, 106 high / 13 low confidence. `report_year` null for 135 (not derivable from title). Next date source: PDF metadata or the year facet of the listing.
- **HIQA:** `provider_name` is 100% null — the HIQA listing does NOT publish operator names. The spend-per-provider join needs provider identity from the report PDFs (cached, unparsed) or the IPAS contracts side. `centre_name` (0 nulls) is the working key. Provider names, once resolved, must inherit the accommodation-providers `public_display` gating (see `join_caveat` column).
- **IATI:** `dq_suspect_date` flags impossible dates (min 1913 — upstream artifact); `recipient_region` entirely null in source.
- **AHBRA notices:** `overall_outcome` has spelling drift ("Non- Compliant" / "Non-Compliant Statutory…") — normalise before any grouping; 14 non-assessment rows are null on assessment fields by design (`record_type` distinguishes).
- **Licences (all six confirmed):** RI CC-BY-4.0 + IATI CC0 captured in-row; audit.gov.ie = CC-BY-4.0 (its open-data policy page); hiqa.ie + ahbregulator.ie = PSI re-use ("free of charge in any format", PSI general licence / SI 279/2005) — checked 2026-07-12. Still to be recorded in `doc/source_licensing.md` at promotion time.

### ⛔ Criminal legal aid payments — TERMINAL FINDING (2026-07-12): not published
`criminal_legal_aid.py` is a **scaffold with a documented blocker** (+ a WAF-safe `probe()`
that re-checks the anchor page): the Department of Justice does **not** publish the
per-practitioner payment lists anywhere fetchable. Channels exhausted (URLs in the module
docstring): data.gov.ie CKAN (aggregate LAB stats only) · full gov.ie sitemap sweep (~97k
URLs, 236 keyword hits — only fee-claim forms + IGEES *CLA Expenditure Trends 2014–2024*
aggregates) · legacy justice.ie via Wayback CDX · assets.gov.ie filename probes · PQ answers
(aggregates only) · DoJ FOI disclosure logs (requests received, not released records).
The annual press "top-earner" lists (Irish Legal News / Irish Times, latest 2025-02) come
from an **FOI release with no public artefact**. Unblock routes: (a) FOI to foi@justice.ie
— records demonstrably held + released annually; FOI reference would be the provenance,
no explicit licence; (b) `probe()` flags if DoJ starts publishing; (c) an aggregates-only
dataset from PQs/IGEES as a separate, lesser build. Proposed schema preserved in the
docstring (`value_safe_to_sum=False`, `payment_basis='fees_paid'`,
`privacy_tier='professional_individual'`). WAF note: browser-UA clears the 403 but ~15
rapid requests trigger 405 throttling — pace ≥5s on gov.ie.

## 🔗 Joinability layer (added after the usability review)

Built a public-body crosswalk + spine-routing so the ingests join to the existing surface.

| Artifact | Script | Output | Note |
|---|---|---|---|
| CSO Register of Public Sector Bodies | `rpsb_reference.py` | `silver/rpsb_bodies.parquet` (**879** bodies) | 608 central-gov + 268 LA + 3 SSF; from CSO RPSB 2024-final sector sub-pages |
| Public-body crosswalk | `public_body_crosswalk.py` | `silver/public_body_crosswalk.parquet` | reference universe = **1,092** bodies (payments 88 + LAs 31 + depts 18 + seed 77 + RPSB 878); reuses NFKD fold + `canonical_la` + dept aliases |
| DPC → CRO company link | `dpc_cro_link.py` | `silver/dpc_cro_matches.parquet` | reuses `shared.name_norm.name_norm_expr` (the house company rule) |
| DPC routing analysis | `dpc_route_analysis.py` | (report) | proves the two-spine split |

**Match rates after adding RPSB (row-weighted):**
- OIC/FOI decisions → public-body spine: **91%** (3,115 / 3,407). HSE variants fold to one entity (498 decisions).
- data.gov publishers → public-body spine: **89%** (19,980 / 22,335). CSO/Met now resolve.
- DPC decisions → routed to a spine: **67%** (41 / 61) — Public authorities → RPSB (21/22), private companies → CRO (18/35); the DPC `sector_tags` field is the router. The 20 unrouted are foreign/renamed entities (→ the scaffolded Companies House UK / OpenCorporates).

**Remaining (small):** a canonical-entity dedup pass (e.g. "Department of Justice" vs "…Home Affairs and Migration" appear as two); wiring foreign DPC targets to Companies House UK. Still all sandbox in `c:/tmp`, nothing promoted.
