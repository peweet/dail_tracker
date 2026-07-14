# ROADMAP_SOURCES.md — candidate ingestion sources, monetization, and product direction

Forward-looking, honest, and explicitly **not** a claim of implementation. Every
source here is `candidate` / `not-found` / `partial` unless a chain and output
are cited. The implemented surface is in [`SOURCES.md`](SOURCES.md); the full
card-level scoping notes are in [`ENRICHMENTS.md`](ENRICHMENTS.md).

> **Do not claim these are built.** In particular: C&AG reports, PAC report
> ingestion, election results, OGP central frameworks, a general data.gov.ie
> crawler, FOI/OIC decisions, regulatory-enforcement feeds, RBO, and Companies
> House UK are **not implemented** at the time of writing. Where a related
> component *is* built (e.g. SIPO finance, CBI authorisation registers), it is
> flagged so the boundary is unambiguous.

---

## Candidate new ingestion sources

Columns: **Source · Status · Active retrieval link · Records to ingest · Joins to · Value · Difficulty · Caveats.**

### Highest priority

| Source | Status | Active retrieval link | Records to ingest | Joins to | Value | Difficulty | Caveats |
|---|---|---|---|---|---|---|---|
| **C&AG audit reports** | `candidate` (not found as ingested dataset) | `audit.gov.ie` | Title, pub date, report type, sector, topic, public body, dept, findings, recommendations, cost figures, procurement/grant/governance/project tags, source PDF | public bodies, procurement, grants, accommodation spend, health, housing, infrastructure, local government | **High** — independent audit layer that corroborates spending concerns | Medium–High (dense PDFs; topic extraction) | Large dense PDFs; NLP or manual topic extraction |
| **Public Accounts Committee reports + transcripts** | `candidate` | `oireachtas.ie/en/committees/` (partly via API) | Meeting, public body, witnesses, report discussed, recommendations, transcript links, govt response, follow-up status | members, debates, existing committees view, C&AG | High — political response layer to audit findings | Low–Medium | Partly in Oireachtas API already — dedupe by debate ID |
| **OGP central arrangements / frameworks catalogue** | `candidate` (distinct from implemented eTenders/OGP **awards**) | `ogp.gov.ie` / eTenders frameworks | Arrangement name, CPB, category, framework/DPS/panel type, lots, supplier list (if public), start/end/expiry, buyer eligibility, route-to-market guidance | procurement awards, suppliers, buyers | High — opportunity intelligence; complements award history | Medium | **Award history is built; the framework *catalogue* is not.** |
| **Department grant registers** | `candidate` | per-dept `gov.ie` pages | Dept, scheme, recipient, amount, location, purpose, year, source URL | charities, companies, constituencies, lobbying, announcements | High — captures public money **outside** procurement | High (format varies wildly per dept) | Some "grants" are statutory entitlements, not discretionary |
| **Capital projects / investment tracker** | `candidate` | `gov.ie` Investment Projects & Programmes Tracker | Project, sponsoring dept/body, sector, location, cost estimate/band, status, timeline, contractor (if avail), update history | procurement, constituencies, public bodies | High — infrastructure / public-investment intelligence | High | Cost bands not always project-level |
| **Election results / Electoral Commission** | `candidate` (**not** SIPO finance, which is built) | `electoralcommission.ie`, `electionsireland.org` | Election event, candidate, party, constituency, count-by-count, first prefs, elected/eliminated, turnout, quota, margin, boundary context | members, constituency, party | High — seat safety, spend-per-vote, campaign-finance context | Low–Medium | Boundaries change between elections; third-party source = continuity risk |
| **FOI / OIC decisions** | `candidate` | `oic.ie` | Public body, decision date, FOI Act sections, topic, outcome, commercial-sensitivity/public-interest tags, decision URL | public bodies, topics | High — transparency-dispute surface | Medium | — |
| **RBO beneficial ownership** | `candidate` / access-constrained | `rbo.gov.ie` | Ultimate beneficial owners (≥25%) — **only where lawful** | CRO companies, suppliers, lobbying entities | High — human ownership behind entities | High | Restricted post-2022 CJEU ruling; legitimate-interest access; comply with law/terms |
| **Companies House UK** | `candidate` | `developer.company-information.service.gov.uk` | UK company / PSC / officer data | suppliers, lobbyists with UK parents, RoMI directorships | Medium–High — cross-border resolution | Low (free API) | Anglo-Irish name overlap needs careful matching |

### Regulatory enforcement (high-signal accountability layer)

| Source | Status | Active retrieval link | Value | Caveats |
|---|---|---|---|---|
| Central Bank — **authorisation registers** | `partial` (**sandbox, ingested**) | `registers.centralbank.ie/downloadspage.aspx` | Substrate for Corporate Notices CBI panel (13.8k firms) | Already shipped as sandbox; name+ref only |
| Central Bank — **enforcement / prohibition notices** | `candidate` | `centralbank.ie/.../enforcement-actions` | Pre-distress accountability (fines, sanctions) | JS-rendered hub; some name individuals (privacy); ~120–180 releases since 2010 |
| Data Protection Commission decisions | `candidate` | `dataprotection.ie/en/dpc-guidance/decisions` | GDPR enforcement signal | Dominated by big-tech decisions |
| Corporate Enforcement Authority (ex-ODCE) | `candidate` | `cea.gov.ie/en-ie/` | Company-law prosecutions vs directors | Small caseload |
| WRC / Labour Court decisions | `candidate` | `workplacerelations.ie/en/cases/`, Labour Court search | Repeat-respondent pattern signal | High volume; individual respondents (privacy) |
| Coimisiún na Meán | `candidate` | `cnam.ie` | Emerging media/platform regulation | New body; small back-catalogue |
| Revenue tax-defaulters list | `candidate` | `revenue.ie/.../press-office/` | Strong corroborating signal | High legal/reputational sensitivity; treat as evidence pointer, never auto-link |

### Medium priority

| Source | Status | Value | Caveats |
|---|---|---|---|
| HSE Section 38/39 funding | `candidate` | Funded-then-lobbying patterns | Naming varies; §38 vs §39 matters |
| HEA funding / statistics | `candidate` | Constituency-level HE funding | Per-scheme schemas differ |
| Sport Ireland funding allocations | `candidate` | Constituency funding distribution | Multi-year double-count risk |
| Arts Council funding decisions | `candidate` | Constituency funding distribution | Multi-year double-count risk |
| Tailte / GeoHive / property / valuation | `candidate` | Ground-truth on declared property | Paid bulk access; address resolution |
| EU Cohesion / CAP / EU funding | `candidate` | Major agri/regional spend share | CAP beneficiary privacy framework |
| EU Transparency Register (lobbying) | `candidate` | Domestic↔EU lobbying context | Voluntary scope; different categories |
| Systematic data.gov.ie metadata discovery | `candidate` | Source discovery layer | **No general crawler exists today** |
| Council minutes / agendas (sandbox → gold) | `sandbox` | Local decision-making | In `council_minutes/`, WIP, not promoted |
| Council named votes / material-contravention votes | `candidate` / `sandbox` | Local accountability | Availability patchy per council |
| Planning docs / development contributions | `candidate` | Development-finance context | Address/parcel resolution is a project on its own |
| Public consultation submissions | `candidate` | Issue-level engagement | Only where public + safe to process |
| Oireachtas publications index | `candidate` (operational) | New-asset discovery for PDFs | 10,000-result cap rules out full backfill |
| Tribunals / Commissions of Investigation | `candidate` | Historical corruption signal | Research-grade topic extraction |
| HowTheyVote.eu (Irish MEPs) | `candidate` | MEP coverage gap | Maintainer-dependency if used as primary |

---

## Deprecated / broken (re-verify before use)

| Item | Issue |
|---|---|
| HSE payments source URL | Source **deleted** in HSE's 2026 rebuild; never archived. Our 16,972 rows (2021–25) are the **only surviving public copy**; dead URL repointed to landing page. |
| `sipo.ie/funding-of-political-parties/`, `/election-expenses/`, `/referendums/` | 500/404 — use the parent collection `sipo.ie/en/collection/9f7db-publications`. |
| `gov.ie/.../9c6d5-capital-tracker/` | 404 — use the Investment Projects & Programmes Tracker URL. |
| `pleanala.ie` body name | Renamed to **An Coimisiún Pleanála** (Planning and Development Act 2024). |
| README links to `doc/COMPETITIVE_LANDSCAPE.md` and `doc/archive/API_LAYER_PLAN.md` | **Both files do not exist** — dead links (removed in this doc update). |
| Several `ENRICHMENTS.md` URLs | Flagged "unverified" in that doc's URL audit — re-check before pasting anywhere user-facing. |

---

## Monetization / commercial usefulness

> The strongest monetization route is **not** a general civic-dashboard
> subscription. It is **Irish public-sector and procurement intelligence.**

### Potential products

1. Procurement intelligence SaaS.
2. Paid supplier / buyer / category dossiers.
3. Tender and renewal alerts.
4. Competitor tracking.
5. Public-body payment intelligence.
6. API / data feed for consultants, analysts, journalists, NGOs, bid teams.
7. Bespoke market reports.

### Likely paying users

SMEs selling to government · bid writers · public-sector sales teams · tender
consultants · procurement analysts · journalists · NGOs · compliance /
due-diligence teams · public-affairs firms.

### Commercial strengths

- The **joins**: procurement + payments + CRO + lobbying + charity + corporate-notice in one place.
- Supplier and buyer profiles; CPV/category market maps.
- Source provenance and money-grain safety baked in.
- Irish-specific public-sector focus (no incumbent does this combination).

### Commercial blockers

- AGPL code licence (commercial licence exists as the route around it).
- Source-data licensing varies (Iris Oifigiúil is Crown copyright — facts only).
- Refresh reliability (full automated cloud refresh still pending).
- UI/product polish; lack of saved searches, alerts, API keys, billing.
- Trust/liability around money claims; OCR/PDF caveats; entity-resolution confidence.

### Verdict

> Monetization is viable, especially around procurement / public-sector
> intelligence. Current commercial potential is around **7/10**. With reliable
> refresh, clearer source coverage, polished supplier/buyer workflows, alerts,
> exports, API keys, and a licence review, it could become an **8–8.5/10** niche
> product.

(Detailed plan: [`COMMERCIALISATION_PLAN.md`](COMMERCIALISATION_PLAN.md);
procurement product direction: [`PROCUREMENT_INTELLIGENCE_ROADMAP.md`](PROCUREMENT_INTELLIGENCE_ROADMAP.md).)

---

## UI / product clarity

The app is broad and **dataset-led**. It should become **workflow-led**.

### Target workflows

1. Search anything.
2. Research a politician.
3. Research a supplier.
4. Research a buyer / public body.
5. Analyse a CPV / category market.
6. Verify a money claim.
7. Check a company / charity.
8. Explore lobbying / access.
9. Explore local-authority accountability.
10. Inspect a law / SI / bill.

### Procurement UI improvements

Full supplier dossier · buyer dossier · CPV market map · live tender/opportunity
alerts · framework intelligence · award-vs-payment side-by-side · single-bid /
competition explanation · incumbent/renewal intelligence · source freshness &
confidence badges · explain-this-number panels · exports and saved searches.

### Trust UI improvements

Label each figure as **official fact · extracted fact · derived match · signal ·
caveat · sandbox · experimental.** Show: last refreshed · source links ·
coverage · whether a figure is **safe to sum** · entity-match confidence.
