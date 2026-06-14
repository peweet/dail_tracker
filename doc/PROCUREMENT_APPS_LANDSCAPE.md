# Procurement Apps & Data Sources — Landscape (to check)

**Created:** 2026-06-13. A reference list of the existing procurement-data products, aggregators, official
sources and civic peers, gathered while benchmarking Dáil Tracker's money-flow work. Use it to check what's
out there. Tagged by what each is and how it relates to us.

> Frame: most of the **commercial** ones are *sales-intelligence* (help suppliers WIN business, paid). We are
> *civic transparency* (help citizens/journalists SEE money, free). The overlap is the spend/award data; our
> edge is cross-register linkage (lobbying/political-finance/charity) + provenance, which none of the
> commercial tools do. See `doc/MONEY_FLOW_DATA_AUDIT.md` for the full comparison.

---

## 1. Commercial sales-intelligence (the paid "competitors" we benchmarked)

| Product | URL | What it is | Notes / relevance |
|---|---|---|---|
| **Tussell** | https://www.tussell.com/ | UK market-intelligence: spend + contracts + frameworks, **supplier groups** (entity resolution), pipeline from pre-tender signals, 80k decision-maker contacts | The gold standard for *spend analytics*. Strengths we lack: entity resolution, pipeline, contacts. Sample buyer profile: tussell.com/insights (e.g. DWP, HMRC, Home Office). |
| **Stotles** | https://www.stotles.com/ | UK **& Ireland** pre-tender pipeline / buying-intent signals, expiring contracts 12–24mo out | **Aggregates eTenders Ireland** — direct competitor on Irish data. Has an [eTenders-Ireland notices source page](https://www.stotles.com/explore/notices/sources/etenders-ireland). |
| **Spend Network** | https://spendnetwork.com/ | Global tender + spend data, OCDS, contracts | Data-supply / OCDS angle; powers some OpenOpps. |
| **GovSpend / SmartProcure** | https://govspend.com/ · https://smartprocure.us/ | US: ~1bn purchase-order records, ~30k agencies, **line-item pricing** ("what competitors charged") | The PO/pricing-intelligence model; closest to our payment fact but with unit pricing we don't have. |
| **GovWin IQ (Deltek)** | https://iq.govwin.com/ | US federal/SLED **pipeline & forecasting** | The pure pipeline-forecasting play. |

## 2. Tender aggregators / alert services (supplier-facing, lighter)

| Product | URL | What it is |
|---|---|---|
| **Open Opportunities (OpenOpps)** | https://openopps.com/sources/etenders-ireland/ | Monitors eTenders Ireland **daily** + global; alerts. OCDS-based. |
| **Tendersinfo** | https://www.tendersinfo.com/global-ireland-tenders.php | Global tender alerts incl. Ireland; daily notices + awards. |
| **TendersOnTime** | https://www.tendersontime.com/ireland-tenders/ | "Biggest tender aggregator"; multi-source incl. Irish gov sites. |
| **GlobalTenders** | https://www.globaltenders.com/ireland-tenders | Global tender listings, Ireland section. |
| **BidDetail** | https://www.biddetail.com/ireland-tenders | Ireland tenders/eprocurement listings. |
| **IrelandTenders.com** | https://www.irelandtenders.com/ | Ireland-focused portal + RSS feed. |
| **publicprocurement.ie** | https://publicprocurement.ie/etenders-feed/ | eTenders feed / bid-support. |

## 3. Official sources & open data (where the data actually comes from)

| Source | URL | What it is | We use it? |
|---|---|---|---|
| **eTenders (NEW, live)** | https://www.etenders.gov.ie/ | Official Irish e-procurement portal (Eurodyn EPPS). **Current CfTs:** /epps/prepareCurrentOpportunities.do?currentType=cft | ⚠️ live tenders **NOT** ingested (Playwright-only; sandbox probe done) |
| **eTenders (LEGACY)** | https://irl.eu-supply.com/ | OLD eu-supply platform — **decommissioned**, access ceased 21-May-2025, archive-only. Its "2030+ tenders" are long-window **DPS/Qualification Systems**, not live tenders. | no (dead) |
| **OGP Open Data CSV** | https://data.gov.ie/dataset/contract-notices-published-on-etenders | "Contract Notices Published on eTenders" — **quarterly**, 2013→ (sub-threshold + OJEU). | ✅ awards extracted (⚠️ our pinned URL is a quarter stale — covers to 2025-12-30 vs current 2026-03-31) |
| **TED (EU Official Journal)** | https://ted.europa.eu/ | EU-threshold tenders + awards (above-threshold only). | ✅ awards + live tenders ingested (454 open) |
| **Open Contracting (OCDS) — Ireland** | https://data.open-contracting.org/en/publication/58 | OCDS-formatted Irish procurement (JSON/Excel/CSV). | not yet (richer alternative to the CSV) |
| **OGP open-data collection** | https://www.gov.ie/en/office-of-government-procurement/collections/opendata/ | The OGP's open-data hub. | — |

## 4. Civic / transparency peers (our actual peer group + models)

| Product | URL | What it is | Why it matters to us |
|---|---|---|---|
| **USAspending.gov** | https://www.usaspending.gov/ | US federal spend transparency — awards vs **outlays**, recipient profiles | The model: separates awarded from paid (our tiers), free, civic. |
| **ProZorro / BI-Prozorro** | https://bi.prozorro.org/ | Ukraine public-procurement transparency + analytics | Great *because* its data is centralized/complete — which ours is NOT (don't copy its completeness-implying viz). |
| **DOZORRO** | https://dozorro.org/ | TI-Ukraine civic monitoring + AI red-flagging | Civic-monitoring model. |
| **TheyWorkForYou** | https://www.theyworkforyou.com/ | UK parliamentary transparency | Our spiritual reference (PRODUCT.md). |
| **UK Contracts Finder / Find a Tender** | https://www.gov.uk/contracts-finder · https://www.find-tender.service.gov.uk/ | Official UK tender + award notices | The UK official-portal equivalent. |
| **OpenCorporates** | https://opencorporates.com/ | Global company register / entity resolution | Relevant to our `dim_supplier` / CRO-matching gap. |
| **360Giving** | https://www.threesixtygiving.org/ | UK grants transparency standard | Model for the grants channel (charity funding we can't see today). |

## 5. Quick orientation for "check them"

- **To see what *we'd* compete with on Irish data:** Stotles (eTenders-Ireland), OpenOpps.
- **To see the spend-analytics bar:** Tussell buyer profiles (tussell.com/insights).
- **To see the pricing-intelligence model:** GovSpend.
- **To see the civic model done well:** USAspending.gov, ProZorro.
- **For our missing pieces:** OpenCorporates (entity resolution), 360Giving (grants), OCDS-Ireland (richer structured data).

> Caveat on the commercial ones: most are **paywalled** (demo/trial only). The aggregators (OpenOpps, Tendersinfo)
> show free listings. Terms-of-use for scraping the official portals are unconfirmed — the *data* is public
> procurement record, and these aggregators scrape it, but mind request rate + ToU if we ingest.
