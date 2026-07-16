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
| C&AG chapters/Votes (DERIVED 07-13) | `cag_chapters.py` | `cag_chapters.parquet` | **1,427** | pure local transform of `cag_reports.pdf_urls` slugs — **640 RoAPS chapters** (~1996–2024, all with year) + **674 per-Vote appropriation-account PDFs** (2007–2024, vote_number+dept from filename) + front-matter/other; only 3 slugs unclassified (combined volumes, correctly dropped) | CC-BY-4.0 (as parent index) |
| C&AG IPAS chapter figures (DEEP-DIVE 07-13) | `cag_ipas_figures.py` (+ `fetch_ipas_chapters.py`) | `cag_ipas_chapter_figures.parquet` | **196** | RoAPS 2024 ch.10 read IN FULL (28pp, born-digital, fitz text — NO OCR needed); every figure hand-curated with page+para ref, 15 categories, **19 explicit unknown rows** (chart-only supplier €s, county map, Annex tick-glyphs, + gaps the C&AG itself flags: 101 centres' contract status unrecorded, cost/night uncomputable from IPAS records). 2015 direct-provision chapter text also cached, not yet extracted | CC-BY-4.0 |
| C&AG IPAS provider candidates — option (b) (07-14) | `cag_ipas_provider_candidates.py` | `cag_ipas_provider_candidates.parquet` | **1,826** | attempt to NAME the anonymised A–G. FINDING: **A–G are UN-nameable for 2024** — they are Vote 40 (Dept of Children) 2024 payments and **Dept of Children is NOT one of our 86 publishers**. BUT IPAS moved to **Dept of Justice** on 1 May 2025, which we DO hold → **165 named accommodation providers 2025–26** (Cape Wrath Hotel €36.5m, Mosney €24.1m, Bridgestock €19.4m, Trailhead €18.8m, Fazyard=Citywest, Tifco…). This is a DIFFERENT Vote+year → CORROBORATION, explicitly NOT an A–G decode. po_committed/payment_actual kept separate | payments-fact derived |
| C&AG IPAS chart/glyph RECOVERY (07-13) | `cag_ipas_chart_recovery.py` | `cag_ipas_chart_recovery.parquet` | **193** | recovered 12 of the 19 unknowns WITHOUT OCR: **Fig 10.4 supplier bars** via raster measurement calibrated on vector axis labels (A €45.5m, B €38.7m, C €35.0m, D €34.0m, E €32.7m, F €22.9m, G €20.9m; sum 229.7 vs stated ~230 ✓) · **Fig 10.3 series** (2019 €127m, 2020 €180m, 2021 €189m, 2022 €365m, 2023 €654m; 2024 measures 1,065 vs known 1,066 = 0.1% error ✓) · **Annex 10A 20×9 grid** via Webdings 0xf06e colour decode (green/amber/red = complete/partial/not); column mapping LOCKED by asserts against Fig 10.6 aggregates (proposal 7✓ CRO 19✓ ownership 1✓ planning 4✓ insurance 8✓; contract 9 vs 10 = 1-off caveat; site-visit column not glyph-encoded → not_decoded). STILL unrecoverable: Fig 10.2 county map (raster choropleth, bands only — IPAS weekly stats are the proper county source) | CC-BY-4.0 |

All rows carry the house provenance schema (`source_url`, `source_document_hash`,
`fetched_at`, `source_published_date`, `extraction_method`, `confidence`,
`privacy_tier`); money columns are `value_safe_to_sum=False`. Grant grains are
labelled (`grant_basis='commitment'` for RI; IATI `transaction_type`) — grants
are a third money channel, never summed with awards or payments.

### 🔗 IPAS enrichment threads — 2026-07-14 (all sandbox)

| Output | Script | Rows | What it gives us |
|---|---|---|---|
| `ipas_by_local_authority.parquet` (+ CSV) | `ipas_by_local_authority.py` (+ `fetch_ipas_weekly_stats.py`) | **31 LAs** | **THE COUNCIL-MAP DATA.** IP applicants by local authority, snapshot 2024-12-29, parsed from the **IPAS weekly stats PDF** (`assets.gov.ie/static/documents/29122024-ipas-stats-weekly-report.pdf` — the same source C&AG Fig 10.2's per-capita choropleth is built from). **Validated: sum = 32,702 = the PDF's own Grand Total, exact.** South Dublin 3,979 · Dublin City 3,403 · Fingal 2,437 · Donegal 1,979 · Galway City 1,742 … Longford 142. Per-capita (the C&AG's actual map metric) needs an LA-population join; weekly cadence ⇒ a LIVE refreshable map, not a snapshot. |
| `cag_ipas_links.parquet` (+ CSV) | `cag_ipas_links_and_law.py` | **15** | Every hyperlink embedded in the two IPAS chapters, HTTP-checked — **all 15 alive**. Includes the IPAS weekly stats, the IGEES analytical paper (source of €92 vs €34/night), the Comprehensive Accommodation Strategy, the National Standards, **the HIQA 2024 monitoring report** (ties to our `hiqa_ipas_inspections`), **an eTenders notice** (`irl.eu-supply.com/…/205491` — ties to procurement), the **Revenue VAT guidance on emergency accommodation**, and the EMN reception-facilities study. |
| `cag_ipas_legal_citations.parquet` (+ CSV) | `cag_ipas_links_and_law.py` | 4 SI + 1 EU reg + Acts | **3 of 4 cited SIs matched to our gold `statutory_instruments`** (with `si_id` + `eisb_url`): **SI 230/2018** European Communities (Reception Conditions) Regs — the binding law · **SI 605/2022** + **SI 376/2023** Planning & Development (Exempted Development) — the change-of-use exemption chain. SI 165/2000 not in our SI data (flagged). Acts: International Protection Act 2015, Planning & Development Act 2000, Housing Act 1966. EU: Reg 2022/2560 (foreign subsidies). |

**`ipas_legal_obligations.parquet`** (`ipas_legal_obligations.py`, 16 rows, + CSV) — **the obligation
chain, read from primary sources.** THE instrument laying out Ireland's duties is **SI 230/2018**
(European Communities (Reception Conditions) Regs 2018, in force 30 Jun 2018), transposing
**Directive 2013/33/EU**. What makes it so hard, each row anchored to a named source:
1. **Reg 4 (material reception conditions) — BREACH ADMITTED BY THE STATE ITSELF.** The Government's
   own Comprehensive Accommodation Strategy: *"There is a legal obligation on the state … to provide
   accommodation to all who request it. **For a second time this year the state is unable to fulfil
   these obligations.**"* (C&AG: 3,285 single males unaccommodated at end-2024.) Reg 6 lets the State
   withdraw/reduce conditions for a recipient's breach — but there is **no capacity defence**: the duty
   binds absolutely while supply does not.
2. **Art.28 of the Directive (monitoring) was transposed ~5.5 YEARS LATE.** SI 230/2018 contains **no**
   inspection regime at all (0 occurrences of "inspect"). The monitoring system was inserted only by
   **SI 649/2023**, in operation **9 Jan 2024** (Reg 27A: "The Authority shall, for the purposes of
   Article 28 of the Directive … monitor compliance") — which is exactly why HIQA's first inspections
   are 2024.
3. **The regulator can't see most of the estate.** HIQA's power bites on *designated* centres (SI 230/2018
   Reg 7); per C&AG fn5 those are "State-owned premises and those competitively procured". HIQA inspected
   centres housing **6,544** residents of **32,702** ⇒ **~20% coverage**, while ~75% of residents sit in
   emergency commercial accommodation. **The worst-regulated part of the estate is also the most
   expensive (€92/night vs €34) and the most contractually incomplete (50% of sampled properties had no
   signed contract).**
4. **Reg 8 (vulnerability assessment) is MANDATORY — "the Minister SHALL within 30 working days" — and
   is NOT BEING DONE.** HIQA §7.7: *"vulnerability assessments are not being carried out at national level."*
5. **The planning exemption switched off the local check.** SI 605/2022 → SI 376/2023 exempt change-of-use
   to Dec 2028 (Strategy asks for 2030). The exemption's one safeguard — notifying the LA — is not
   operated: IPAS "does not keep records" of notifications; evidence existed for 4 of 20 properties.

**Comprehensive Accommodation Strategy** (cached `bronze/ipas_context/`): 2028 target **35,000 beds** =
State-owned 13,000 RIC/AC + 1,000 in-community vulnerable (**=14,000, matches C&AG**) + commercial
11,000 contingency + 10,000 emergency (**=21,000, matches C&AG's "up to a further 21,000"**) — an exact
cross-validation of the chapter. System was designed for 3,000–4,000 arrivals/yr; 100,000 people
accommodated in 24 months (2% of the population). DQ: the Strategy **miscites SI 376/2023 as "SI 376 of 2022"**.

**VAT insight (user-spotted, confirmed by the embedded Revenue guidance):** IPAS emergency
accommodation is **VAT-EXEMPT** (catering is separately liable at 13.5%) — unlike an ordinary
hotel stay. IPAS *cannot reclaim* VAT and its pre-payment checklist did **not** check the VAT
rate → one provider group overcharged **€7.4m** across 3 VAT registrations (Mar 2022–Dec 2023);
only €1.5m refunded; recovery of the balance "remains under review". Authoritative source:
`revenue.ie/…/services-emergency-accommodation-and-ancillary-services.pdf` (embedded in the chapter).

**BedSpace** (the bed-management system, live Feb 2025): no public product/technical page exists —
it is an internal DoJ system. Only C&AG + press coverage (RTÉ/Irish Post, Sept 2025 overpayments
story) and the C&AG's own note that further controls complete in 2026. Treat as a named control,
not an ingestible source.

### 📦 `ipas_facts.parquet` — ONE CANONICAL TABLE (`ipas_facts_consolidate.py`)

**4,762 facts** unioned from 6 per-document extractors into the single canonical schema, with a
**QUALITY GUARD** that fails loudly rather than shipping a degraded corpus (in the spirit of
`tools/check_extraction_quality.py`: row counts hide silent field degradation, so check the fields).

| doc | facts | unknown |
|---|---|---|
| HIQA 101 inspection reports | 3,275 | 482 |
| HIQA overview 2024 | 691 | 9 |
| C&AG RoAPS 2024 Ch.10 (+ chart recovery) | 389 | 19 |
| C&AG 2015 Ch.6 | 229 | 6 |
| IGEES paper | 178 | 13 |

**GUARD: PASS** — 0 rows missing `source_url` · 0 category drift · 0 unclassified · 0
`value_safe_to_sum=True` (never-sum invariant holds) · 0 silent gaps · 0 duplicate `fact_id` ·
**533 explicit unknowns (11.2%)** — never guessed. 22 of 26 canonical categories in use.

**The guard EARNED ITS KEEP on the first run:** the per-document agents had DRIFTED from the
canonical vocabulary — inventing 15 of their own category labels (`compliance_standard`,
`metrics_quality_safety`, `resident_experience_adults`…) plus 193 uncategorised rows and 4 rows
holding no value while not flagged unknown. All crosswalked (`CATEGORY_MAP` — a rename, never a
reclassification) or made explicitly unknown. The guard now FAILS on any future drift, so a later
loop cannot quietly rot the vocabulary.

Still to fold in on the next run (agent in flight): Accommodation Strategy · **National Standards
lookup (gives every one of the 2,668 HIQA judgments a human-readable meaning)** · PID · the
remaining 9 pages of the IPAS weekly stats.

### 🧭 IPAS corpus: RESUMABLE EXTRACTION ARCHITECTURE (2026-07-14)

The asylum corpus is now organised so a FUTURE SESSION CAN RESUME IT, and so anything
missed this pass can be caught by a later loop without re-deriving context.

**`ipas_doc_registry.py` → `ipas_doc_registry.parquet` (16 documents).** Every source doc
carries a `status` (EXTRACTED / PARTIAL / PENDING_FULL_EXTRACTION / PENDING /
SALIENT_EXTRACTED) and a `priority`. **The registry's docstring holds the CANONICAL FACT
SCHEMA** — every per-document extractor emits that same shape so all facts union into one
table. 26 canonical `CATEGORIES`. Priority rule (user's, 07-14): **direct PDF reports get
FULL extraction; references/SIs are CONTEXT — salient points only, unless a point
materially improves the data.**

| status | docs |
|---|---|
| EXTRACTED | C&AG RoAPS 2024 Ch.10 (389 rows) · HIQA overview 2024 (691) · Integration Fund 2022 (65) |
| PARTIAL | IPAS weekly stats (county table only — other 9 pages unread) · Accommodation Strategy (read, not yet in canonical schema) |
| PENDING_FULL | **HIQA's 101 individual centre inspection PDFs** (cached 89 MB, metadata-only so far — the biggest untapped asset: per-centre × per-standard judgments AND the only place PROVIDERS ARE NAMED) · IGEES analytical paper (the true source of €92 vs €34/night) · C&AG 2015 Ch.6 (the 10-years-earlier baseline) |
| PENDING | National Standards (gives every judgment a human-readable meaning) · Project Initiation Doc · eTenders notice 205491 (links corpus → procurement chain) |
| SALIENT_EXTRACTED (context, by design) | SI 230/2018 · SI 649/2023 · SI 605/2022+376/2023 · Revenue VAT guidance · EMN study |

### 🔓 THE JOIN CLOSED (07-14): named operator → centres → compliance → money

**`hiqa_centre_reports.py`** parsed all **101 individual HIQA inspection PDFs** (77 full / 24
partial / **0 failed**) → `hiqa_centre_compliance.parquet` (**2,668** centre × standard judgments)
+ `hiqa_centre_facts.parquet` (**3,275** facts, 482 explicit unknowns). Every judgment extracted by
**two independent paths** (Appendix-1 table + inline narrative) which agree on **99.5%**.

**THE BREAKTHROUGH: providers are named on 101 of 101 reports.** The C&AG anonymised them (A–G);
HIQA's overview never names them; the individual reports always do. 50 spellings → ~32 operators.

**`ipas_operator_money_compliance.py`** joins that to `procurement_payments_fact` (Dept of Justice,
IPAS's home since 1 May 2025): **18 of 33 operator groups matched to public money.**

| Operator | Centres | % NOT compliant | Paid (DoJ) |
|---|---|---|---|
| **Flodale Ltd** | 1 | **38.2%** | €3.49m |
| **Vesta Hotels** | 1 | **33.3%** | €3.23m |
| **Aramark** | 3 | **32.8%** (232 judgments) | **no payment match** ⚠️ |
| **Tattonward** | 1 | **20.6%** | **€13.32m** |
| Birch Rentals | 1 | 20.8% | €6.75m |
| Atlantic Blue | 1 | 17.3% | €4.95m |
| Maplestar | 1 | 14.0% | €14.66m |
| Onsite Facilities Mgmt | 4 | 10.0% | €16.11m |
| **Bridgestock Care** | 4 | 4.6% | **€31.07m** (largest) |

- **Aramark's Knockalisheen (Clare) alone carries 62 RED-rated standards = 25% of every red rating
  in the entire corpus.** Clare's not-compliant rate is 46.1% vs a 9.4% national average.
- ⚠️ **Aramark has a compliance record but NO matching DoJ payment** — likely paid via Vote 40
  (Dept of Children, which we do NOT hold) or under another entity. A real gap, flagged.
- ⚠️ Name variants remain (`Onsite Facilities Limited` / `Onsite Facilities Management Ltd.` /
  `On-Site Facilities Management Ltd`; `Coziq`×2; `Dídean Dóchas`/`Dídean Dachas` [typo]) — the
  house `name_norm` + a confidence gate is required before publication. `match_confidence` carried.
- **NEVER CAUSAL:** compliance window 2024-01→2026-03; payments are Dept of Justice 2025+. Different
  windows. The money is NOT "the price of that compliance record".

**🚩 HIQA'S OVERVIEW REPORT UNDERSTATES NON-COMPLIANCE SEVERITY.** The individual reports and the
overview reconcile on compliant-vs-not (median 5.0pp), but the SEVERITY split diverges
systematically: Std 3.1 overview says 12% not compliant, the underlying reports say **26%**;
Std 1.4: 8% → **19%**; Std 10.3: 29% → **39%**. Verified by a THIRD independent regex straight off
the raw PDF text (30 Partially / 15 Not Compliant of 58 — all three paths agree exactly). **Not
adjusted** — reported as a source-level finding. Also **13 standards in 12 reports where HIQA
CONTRADICTS ITSELF** (Appendix 1 vs its own narrative) — both kept verbatim, `judgment_conflict=True`.

Privacy verified: 0 resident quotes, 0 medical/nationality references, 0 identifiable residents.

### ⏳ TEN YEARS, THE SAME FIRMS, THE SAME DEFECTS (07-14)

`cag_2015_facts.py` → `cag_2015_direct_provision_facts.parquet` (**229** rows) ·
`igees_ipas_facts.py` → `igees_ipas_facts.parquet` (**178** rows). Both born-digital, no OCR; the
gov.ie WAF cleared first try with the browser-UA + Referer pattern.

**2015 baseline:** €57m spend · 35 centres · 4,696 residents · rates **€20.70–€35.50/night** (vs €92
today) · **€251m paid to commercial providers over 5 years with NO RFT EVER ISSUED.**

**🔁 THE SAME COMPANIES.** Figure 6.7 (chart-recovered — the values are printed nowhere) names the
nine firms paid >€10m in 2015: **Bridgestock €40m · East Coast Catering €35m · Mosney €33m · Aramark
€24m* · Millstreet €24m · Fazyard €23m · Onsite FM €13m* · Maplestar €12m · Tattonward €10m**
(*State-owned — the only two; they sum to €37m vs the stated €36m, validating the recovery).
**Bridgestock, Mosney, Aramark, Fazyard, Onsite FM, Maplestar and Tattonward ALL still appear in our
2025 payments and/or the HIQA compliance data.**

**8 defects RECUR · 3 WORSE · 1 resolved:**
- Capacity-based payment regardless of occupancy — **identical contract term** (now quantified: 368
  of 1,636 "vacant" beds were unavailable *and paid for*)
- No competitive process — an RFT finally ran in 2022 (25 contracts); **15 of 20 sampled 2024
  properties were still direct awards**
- **The only penalty (€50/bed/day) was NEVER APPLIED ONCE in 5 years** — and in 2024, "no financial
  penalties applied"
- **Recommendation 6.3 (collate inspections/complaints to judge suppliers) ACCEPTED IN 2016 → the
  compliance tracker appeared April 2025, NINE YEARS LATER.** Before that, breaches were not logged.
- Status-holders stuck: **667 = 16% (2016) → 5,292 = 16% (2024)** — same share, **7.9× the people**
- **WORSE — due diligence:** 2015 common-director links were found by the C&AG, not the Dept; 2024
  ownership evidence on **1 of 20** properties, and IPAS still doesn't check CRO numbers
- **WORSE — occupancy:** 86% → **78.5%**, while payment stayed capacity-based
- **Resolved:** independent complaints appeal (Ombudsman) — 38 complaints in 4 yrs → **581 in 2024**

**IGEES — the costed levers the C&AG never quoted:** cost/night **€50 (2019) → €91 (May 2024)** · of
the **€929m** rise 2018–24, **demand = 76% / unit cost = 24%** · **a median stay costs €43,237**
(17 months) vs €23,604 (11 months) — **cutting processing to 9 months saves ~€18,000 per stay (−42%)**
· emergency accommodation **3.5% → 75%** of residents · **people with status cost €200m in 2024**
(5,500 people, ~20% of the bill) on a cohort with **no legal entitlement**, while **>3,000 entitled
applicants were unaccommodated — the High Court found the State in breach (Aug 2024)**.
⚠️ Frame that last one as the State paying twice for its own delay — a housing-supply failure, NOT
an accusation against any individual.

**Validation:** all four C&AG-cited IGEES figures MATCH their origin (€92, €34, 17 months, 5,292).
Two discrepancies **flagged not adjusted**: IGEES 2024 = €1,005m vs C&AG €1,066m (a *definitional*
€61m gap reconciling to the C&AG's €59.5m grant line — different grains, never union); and
"almost 45,000" applications vs IGEES's own labels summing to 45,497 (just *over*).
**Source defect preserved:** the IGEES prose says "Spain and Germany … 69% and 58%"; its own Figure
3.5 plots the reverse. Recorded as a flag row, not silently fixed.

### 👤 `ipas_entitlements.parquet` (11 rows) — ENTITLEMENT vs REALITY

The human end of the €1.07bn: what an applicant is **owed in law**, beside what the auditor
and inspector **found**. Data layer for a planned "the person" tab on the accommodation-spend
page. Every entitlement quoted from SI 230/2018; every reality-check quoted from a NAMED
source; where performance is unpublished → `reality_status='NOT_PUBLISHED'` (never inferred).

- Accommodation/food/necessities (Reg 4) → **NOT_DELIVERED_TO_ALL** (State admits breach; 3,285 unaccommodated)
- Vulnerability assessment within **30 working days** (Reg 8) → **NOT_PERFORMED** (not done at national level)
- **Right to work (Reg 11): APPLY AFTER 8 MONTHS**, grantable at 9 if no first-instance decision; 6-month renewable permission → **GATED_BY_DELAY** (median processing ~17 months). ⚠️ Period shortened by later amendments — verify current figure before publishing.
- Education (Reg 17) → **GAP_FOUND** (15% of school-age children not attending)
- Health care (Reg 18) → **DEPENDS_ON_UNPERFORMED_ASSESSMENT** (vulnerable mental-health entitlement keys off the Reg 8 assessment that isn't happening)
- Standards-compliant accommodation → **UNENFORCEABLE_AND_MOSTLY_UNMONITORED** (HIQA has no sanction power; 86% of settings outside remit)
- Privacy/dignity → **WORST_PERFORMING_STANDARD** (Std 4.3: 38% not compliant)
- Safeguarding/vetting → **SERIOUS_GAPS** (35% of staff not Garda-vetted)
- Information in your language (Reg 3) → **NOT_PUBLISHED** (no measure exists)
- Complaints/Ombudsman → **IN_USE** (581 complaints in 2024; 21 to the Ombudsman)

**TONE RULE carried in the data (`tone_rule` column):** state the law and the audited
finding; never editorialise about migration; never name, age, locate or quote an individual
resident. UI direction: a muted outline/silhouette figure — humanises without othering; no
stock photography of identifiable people.

### DQ flags from the 07-11 run (preserved, NOT silently fixed)
- **C&AG:** 148/267 detail pages carry no parseable publication date (`date_confidence` null); of the 119 dated, 106 high / 13 low confidence. `report_year` null for 135 (not derivable from title). Next date source: PDF metadata or the year facet of the listing.
- **C&AG chapters:** titles are DE-SLUGGED from filenames (apostrophes/case lossy, `extraction_method='filename_slug'`, confidence medium) — harden from PDF tables of contents before promotion. Slug conventions drift by era (5 patterns handled, profiled in the script docstring). Volume PDFs occasionally carry WRONG-year filenames upstream (e.g. 2022 volume file named "…-2024.pdf") — year must come from the parent index row, never the filename. Key content finds: **RoAPS 2024 ch.10 "Management of international protection accommodation contracts"** + 2015 "Procurement and management of contracts for direct provision" (the IPAS/asylum corroboration layer); "Central government funding of local authorities" is a RECURRING annual chapter (2020–2024) — partial cover for the LGAS-remit gap; 37 health + 15 procurement/contract + 9 housing chapters.
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

## ✅ Third wave — 2026-07-14 run (ABP inspector-report REASONING layer)

Driven by memory `project_planning_feasibility_product_learnings_2026_07_14`. **Check-existing-first paid off:** the case-level appeal OUTCOMES are already ingested + promoted (`extractors/planning_appeal_outcomes.py`, ACP Cases_2016_Onwards ArcGIS, PC02, CC-BY) — decision/authority/category/location/dates. The GAP was the WHY. This ingest adds the inspector-report **text + reasoning structure** on top, driven off the same ACP case list.

| Source | Script | Output (`c:/tmp/dail_new_sources/silver/`) | Rows | Coverage | Method |
|---|---|---|---|---|---|
| ABP/ACP inspector reports (reasoning layer) | `abp_inspector_reports.py` | `abp_inspector_reports.parquet` (+ `abp_report_misses.txt`) | **13,720** ✅ **2020+ BACKFILL COMPLETE (queue = 0)** | **nationwide, all 32 planning authorities, 2020–2026.** 16,432 decided cases attempted → 13,720 reports + 2,712 confirmed no-report = **84% hit rate** (exactly as projected). By year: 2020:1,828 · 2021:1,849 · 2022:1,253 · 2023:2,475 · 2024:2,632 · 2025:2,302 · 2026:969 | deterministic PDF URL `pleanala.ie/.../reports/<grp>/r<cid>.pdf` (grp=first-3 of 6-digit core) → **fitz text, NO OCR** |

Born-digital, rich: mean **31 pages / 58k chars** per report. Section-presence flags (appropriate_assessment, flood, traffic/road-safety, EIA, reasons_for_refusal, grounds_of_appeal, conditions, reasons_and_considerations) + `inspector`, `recommendation_verdict`, `recommendation_snippet`, `is_scanned`/`needs_ocr`. Full house provenance per row (`source_document_hash`, `fetched_at`, `extraction_method`, `confidence`, `privacy_tier=public`). Extracted `.txt` cached to bronze with SHA-256.

**Final labels (the ML value — see memory `project_abp_inspector_corpus_ml_asset`):** **9,188 GRANT/REFUSE verdicts** parsed (75.4% of 12,186 Appeals — GRANT 5,360 / REFUSE 3,828) after the 07-16 parser hardening; inspector named on 10,408 (76%); **218 rows flagged `needs_ocr`** (1.6% scanned — the bounded OCR queue, matching the projected rate). Disk **2.27 GB** (13,720 `.txt` + 218 `needs_ocr` `.pdf`; 405 legacy PDFs pruned 07-16, 146 MB reclaimed — every text row is re-fetchable from a deterministic URL + SHA-256).

**Parser hardened + fixture test added 07-16** (`test_abp_inspector_reports.py`, 15 cases, portable — real wordings embedded, no c:/tmp dep). The fixture caught two real bugs: the recommendation-heading regex (a) missed a heading at start-of-text and (b) didn't handle multi-part section numbers ("9.0 Recommendation"), so the heading-window path was quietly under-firing corpus-wide. Fix (`[\d.]*`) recovered +112 verdicts on a free `--reparse`. The test also pins the "null-is-correct" cases (condition-only appeals, s.9(5) confirmations) so a future change that forces a verdict is caught.

**Steady state:** re-run the same command to top up — it pulls only newly-decided cases (~60/week), and recently-decided 404s are retried as ACP publishes them (see the publication-lag trap below).

**RESUMABLE by design.** `--max-fetch N` caps new network fetches per run; already-parsed cases and known-404s (`abp_report_misses.txt`) are skipped, so re-running just continues the queue. `--reparse` re-derives all parsed fields from the cached bronze `.txt` with **zero network** — parser improvements are re-applied to the whole corpus for free (used twice below).

### Parser hardening (data-driven, via `--reparse`)
Verdict wording varies far more than first assumed ("should be granted", "be granted for the following reasons", "the Board grant planning permission", bare "Grant permission subject to conditions", "should be refused" with no "recommend" at all). Rewrote to find the recommendation WINDOW then the operative decision VERB (earliest verb wins, so "granted notwithstanding the reasons for refusal" reads GRANT correctly).
- Appeals verdict coverage: **32% → 82%** (on the 160-row corpus), holding at **76%** on 412 rows.
- Inspector name: **56% → 71%** (signature block under the "improper or inappropriate way" boilerplate).

### 🔍 OCR / image audit (12,062 pages of the 412 cached PDFs classified)
**Verdict: the corpus is 98.3% born-digital — do NOT mass-OCR it.** Page classes: text 7,972 · text+image 3,872 · **image_only 177** · blank 41.
- **OCR IS needed for a tiny, bounded set: 7/412 reports (1.7%)** — 6 are **100% image-only with ZERO extractable characters** (314820, 319761, 318344, 319055, 314163, and 309087 at **90 pages**), plus 316217 (an effectively **empty/broken PDF**: 18 blank pages). ≈177 pages total. **These were silently landing as empty-text rows** — now caught by new `is_scanned` / `needs_ocr` / `image_only_pages` columns, with `extraction_method='ocr_required'` and `confidence='none'`.
- **OCR is NOT worth it for the figures on born-digital pages.** 4,049 pages carry a raster, but only 176 pages are >50% image-covered — and those are essentially all the scanned ones. Extraction check: **the largest raster on a text page is the An Bord Pleanála letterhead logo**; the rest are small illustrative site maps/photos. The substantive content is in the text, which extracts cleanly. Unlike the C&AG chapters (charts held €m values worth chart-recovery), inspector-report figures are **not data-bearing** — OCR'ing them would burn GPU for nothing.
- **All 6 scans are legacy 3xxxxx cases; zero of the recent 500xxx are scanned** → scanning is an OLDER-report phenomenon, so a 2020+ cutoff further reduces the OCR queue. Measure the true rate as the backfill runs rather than projecting.
- **Storage synergy:** go text-only for the 98.3%, and **keep the PDF only for `needs_ocr` rows** — they're the only ones needing re-processing.
- OCR route when run: per project rules — **GPU, Tesseract path, bounded batches** (PaddleOCR crashes the local box; RAM-exhaustion risk on big batches). 309087 alone is 90 pages — chunk it.

### ⚠️ Two traps in the ACP feed (cost a 93%-404 batch to find — read before reusing this source)
1. **`DECISION` is NOT null for undecided cases — it holds a STATUS STRING.** A live case carries
   `DECISION = "Case due to be decided by 21-07-2026"` with `DECIDED_ON = None`. So a
   `DECISION IS NOT NULL` filter does **nothing** and happily queues thousands of live cases that
   cannot possibly have a report. **The only reliable "is it decided" signal is a non-null `DECIDED_ON`.**
   (1,474 undecided cases were being queued; all guaranteed 404s.)
2. **polars sorts NULLS FIRST on a descending sort, and `map_elements` returns NULL for NULL input.**
   The `_publishable` flag came back `None` (not `False`) for every undecided case, so they floated to
   the **TOP** of the queue — the exact inverse of the intent. Result: a batch of 350 that was 325×404.
   Fix: `.fill_null(False)` + `nulls_last=True`.
- **Consequence — a 404 is NOT always permanent.** The misses cache was blacklisting ~287 recent cases
  whose reports simply weren't published yet (publication lag: ACP uploads the report *after* the
  decision; the newest `501xxx` prefix 404s at ~98%). A permanent blacklist would have lost exactly the
  most valuable recent precedents. Now: misses are skipped only if the case was decided > `MISS_RETRY_DAYS`
  (180d) ago; recently-decided misses are **retried**, and bogus undecided-case misses are **purged** on
  each run. Hit rate after the fixes: **79%** (was 7%).

### DQ flags (preserved, NOT silently fixed)
- **A null verdict is often CORRECT, not a miss** — only `category='Appeals'` carries grant/refuse. Condition-only appeals ("I recommend that Condition No. 2 be revised"), s.9(5) URHA vacant-site confirmations, s.5 Referrals, AA-screening and LAP/SID cases have no grant/refuse by nature. Do not "fix" these to a verdict.
- **Residual gaps:** ~24% of Appeals still unparsed for verdict, 29% of reports have no inspector name — older 3xxxxx layouts and split/multi-part recommendations. Needs a fixture test before promotion.
- **URL pattern verified** for BOTH numberings (post-rebrand 500xxx *and* legacy 3xxxxx); self-test on r315397 (the CSSI case, 484 KB) passes each run.
- **Licence to confirm** before promotion: ACP case register is CC-BY (as the promoted outcomes chain uses); the report PDFs are official public docs but re-use terms must be recorded in `doc/source_licensing.md`.
- **Scale — no silent cap.** 25,679 cases enumerated; ~84% have a report → **~21,500 reports available, 412 pulled (≈2%)**. A full pull is roughly **7+ hours and ~15 GB** at the 0.4 s polite delay. That is a resource/politeness decision for the user, NOT taken unilaterally. Continue with: `python -m pipeline_sandbox.new_sources.abp_inspector_reports --max-fetch N` (repeat; it resumes).

**Next to graduate:** fixture test (315397 = GRANT, a known REFUSE, a condition-only null); decide structured target (section-scoped text spans vs flags-only); confirm licence. **NO JOINS YET** — per user, the `abp_case` → `planning_appeal_outcomes` / `planning_applications_silver` precedent surface is deliberately deferred until the corpus is complete and reviewed.

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
