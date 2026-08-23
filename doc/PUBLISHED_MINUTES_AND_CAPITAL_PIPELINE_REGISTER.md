# Published Minutes & Capital Pipeline — Source Register

**Date:** 2026-08-01 · **Status:** RESEARCH — no extraction built, nothing promoted.

Catalogues which Irish bodies publish board minutes and capital-project pipelines, and what
each is worth as a pre-tender signal for procurement intelligence. Supersedes the failed
`pipeline_sandbox/semistate_minutes/` probe.

## Provenance convention

`Verified` means a live page was fetched during this session and the documents were seen; the
URL is given so the claim is re-checkable. `Reported` means a search result or secondary
source only, not fetched. `Indicative` means inferred or blocked. Rows marked † were fetched
by me directly rather than by a research agent.

An HTTP 403 is recorded as **blocked**, never as "does not publish" — several bodies sit
behind a WAF that refuses automated fetches. Blocked rows are open questions.

## 1. Why the previous probe returned zero

`pipeline_sandbox/semistate_minutes/semistate_probe.py` reported 0 documents across 28 bodies.
That was a method failure, not source scarcity. Three defects:

1. **Discovery by URL guessing.** `GOV_PATHS` (semistate_probe.py:75-77) hardcodes eight paths
   and follows one hop. Real minutes listings sit at paths that list cannot reach — HSE serves
   its minutes from a query-string filtered listing, Tailte Éireann from a deep governance
   path.
2. **A substring dedup bug.** `slug(body) in slug(c["body"])` (semistate_probe.py:163) treats
   `slug("RTÉ")` as `"rt"`, a substring of `shannon_foynes_port_company`. RTÉ was dropped from
   the candidate list before probing began. RTÉ publishes 31 minutes PDFs.
3. **A candidate universe drawn from the lobbying register**, which structurally excluded the
   largest publishers: HSE, TII, NTA, the ETBs and the universities.

Four bodies that were **in** the probed list do publish and were missed anyway: EPA, NTMA,
Land Development Agency and Uisce Éireann.

## 2. Bodies that publish board minutes

Ranked by harvest value. All static HTML with direct PDF links unless noted.

| Body | Docs | Range | Format | Notes | Band |
|---|---|---|---|---|---|
| NTA | 139 | 2009–2026 | PDF text-layer | Longest run found | Verified |
| DCU Governing Authority | ~115 | 2004–2025 | PDF, full ~14pp | No redaction in sample; richest clean source | Verified |
| HSE | — | to Apr 2026 | PDF | Query-string listing, not a static path † | Verified |
| LDA | 77 | Jan 2019–Nov 2025 | PDF | Redacted (commercial/GDPR) | Verified |
| NTMA | ~60 | Jan 2015–May 2025 | PDF | | Verified |
| TII | 57 | Jan 2020–Jan 2026 | PDF text-layer | Accordion on reports page | Verified |
| RTÉ | 31 | Feb 2022–Apr 2026 | PDF | Recent files marked `_Redacted` | Verified |
| UL | 23 | Dec 2023–Nov 2025 | PDF, full ~13pp | Redaction confirmed in text | Verified |
| EPA | 19 | 2019–2026 | PDF | **Advisory Committee, not the Board** | Verified |
| Tailte Éireann | 14 | Jul 2023–Jun 2026 | PDF | † | Verified |
| TCD Board | archive to 2002-03 | | PDF | Agendas **and** minutes, open access † | Verified |
| Uisce Éireann | per-meeting | 2017–2026 | **Inline HTML** | Decision summaries, no PDF, no OCR needed | Verified |
| Univ. of Galway | 4 | Oct 2025–Mar 2026 | PDF, 1pp | Abridged summaries by design | Verified |
| DDLETB / KWETB / LCETB | dozens each | 2013–2026 | PDF | ETBs publish as a class | Verified |
| HEA | — | to Jan 2026 | — | Open board minutes | Verified |
| TU Dublin / MTU | — | to Oct 2025 / 2026 | PDF | Both open access | Verified |
| Central Bank Commission | ≥9 | Mar 2022–Apr 2025 | PDF | Listing is JS-rendered — needs a headless browser | Reported |

**Do not publish** (checked, page fetched): ESB, Bord na Móna, Coillte, An Post, daa, CIÉ,
Iarnród Éireann, Bus Éireann, Dublin Port, Port of Cork, Shannon Foynes, Shannon Group,
Enterprise Ireland, Teagasc, VHI, Bord Bia, Sport Ireland, An Coimisiún Pleanála, UCC,
Respond, and the Section 38 hospitals checked (St James's, Beaumont, Children's Health
Ireland).

**FOI-gated, not published:** EirGrid, Fáilte Ireland.

**Blocked by WAF — open, not absent:** SEAI, IDA Ireland, Gas Networks Ireland, Horse Racing
Ireland, BIM, Dublin Bus, UCD, Maynooth, Mater, Clúid, Tuath, Cork ETB.

## 3. The capital pipeline — the higher-value find

### MyProjectIreland is a queryable Feature Service, not a map †

```text
https://services1.arcgis.com/eNO7HHeQ3rUcBllm/arcgis/rest/services/myProjectIreland_All_Projects/FeatureServer/0
```

`{"count":1936}` projects from `where=1=1&returnCountOnly=true`. `maxRecordCount` is 2000 and
pagination is supported, so one request retrieves the national capital pipeline. Fields:
`Name`, `Body`, `Description`, `Fund`, `Location`, `Region`, `Eircode`, `Link`, `Investment`,
`Completion`, `Status`, `Cities`, `Year`.

`Status` is a five-stage lifecycle whose first two stages are pre-tender by name:

1. Strategic Assessment and Preliminary Business Case
2. Pre-Tender — Project Design, Planning and Procurement Strategy
3. Post-Tender — Final Business Case
4. Implementation
5. Post Completion Review and Benefits Realisation

`{"count":680}` projects sit at stage 1 or 2 — named schemes, with a responsible body and an
expected completion quarter, not yet at market. `Completion` is clean quarters (`"Q1 2027"`);
`Body` is the buyer (`"Kildare County Council"`); `Eircode` gives a location join.

**Two corrections.** There is **no euro field**. `Investment` is a sector category — real
values include `"Enterprise, Skills and Innovation Capacity"` and `"Rural Development"` — and
`Fund` is free text. `doc/archive/new_public_money_legal_sources_claude_backlog.md:100` claims
a cost range; that is wrong and should be fixed.

Second, it is an **annual snapshot and currently stale**: data is end-2024, gathered Q1 2025,
published May 2025, all geohive items last modified 29 January 2025, no newer service exists.
This is a yearly strategic refresh, not a live trigger.

**Licence gate.** The item's licence asserts Government copyright over the Tailte basemap and
warns further data may need permission before reuse. Given the OPW flood-extent precedent
where CC-BY-NC-ND blocked paid siting use, this must be resolved before any commercial use.
Free civic use is the easier argument.

**Discrepancy on record.** A research agent working the same question concluded the tracker is
"a map viewer with no discoverable export or feature-service endpoint" and "currently
unusable". That is not a contradiction of the finding above so much as evidence for a separate
point: the endpoint is **not reachable from the gov.ie policy page or the WebAppViewer**. It is
found only through the ArcGIS Online item metadata
(`arcgis.com/sharing/rest/content/items/749b87eef77445fd886b387cd66b2db0?f=json`). The queries
above returned live JSON, so the service exists; it is simply undocumented on the publisher's
own pages. Anyone re-checking this should start from the item ID, not from gov.ie.

### Current project-level sources that outrank the tracker

The tracker is stale. These are current, and two are structured.

| Source | Grain | Currency | Band |
|---|---|---|---|
| Social Housing Construction Status Report | **XLSX, project-level** with delivery stage per scheme; no euro column | Quarterly, Q2-2025 | Verified |
| HSE Capital Plan 2026 | Project-level with euro values, 82pp text-layer PDF — *"Provide €67.37m towards… New National Maternity Hospital"*; *"Tender Phase 2 Paediatric Department Cork University Hospital"* | Annual, Feb 2026 | Verified |
| DCC Capital Programme 2026-2028 | Project-level, year-by-year — *"Bluebell Phase 1 \| €7.39m/€14.78m/€14.78m \| Total €36.96m"* | 2026–2028 | Verified |
| LDA projects page | Named sites with delivery status, no euro | Rolling | Verified |
| UCC five-year capital plan | Named, priced — Tyndall expansion "over a €130 million investment" | Approved Mar 2026 | Verified |
| Dept of Education school building tracker | Reportedly names ~1,400 schools by stage | Rolling | Reported — gov.ie 403 |

The HSE plan is notable for naming **tender stage explicitly** in project lines, which is the
signal the product wants stated rather than inferred.

The LA AFS capital appendix is confirmed to itemise **by service division only**, never by
scheme — consistent with `memory/reference_la_afs_capital_appendix.md`. It is a retrospective
grain and not a pipeline source.

### Published capital plans are mostly totals-only

Across utilities, energy, water and transport, most capital plans give category totals rather
than named priced projects, which limits their pre-tender value.

| Source | Granularity | Band |
|---|---|---|
| Dublin Port Masterplan 2040 | Named SID projects (Alexandra Basin, MP2, 3FM) | Verified doc; €-values Reported |
| daa Capital Investment Programme | Named sub-programmes (Apron Rehabilitation, T1 Façade) | Verified |
| CRU Price Review 6 | 29 priority transmission projects, ~€2,012.5m TAO allowance; named list in an unfetched paper | Reported |
| CIÉ / Iarnród Éireann AR | Named priced projects (€1.5m hydrogen locomotive retrofit) | Verified |
| ESB Networks PR6 | Category counts only ("27 new DSO substations", "90km of 400kV cable") | Verified |
| Uisce Éireann CIP | Totals only | Verified |
| NTA CIP | Totals by mode | Verified page; PDF interior Indicative |
| Rosslare Europort | Totals only | Verified |
| Coillte | No capital plan — forest plans are silvicultural | Verified |

**Corrections to repo state:** Ervia was dissolved into Gas Networks Ireland on 1 June 2024
but still appears in `pipeline_sandbox/semistate_minutes/candidates.csv:15`. The IAA no longer
runs air traffic control — that is AirNav Ireland — though it still regulates daa capex.

## 4. Private sector: no harvestable decision records

Under the Companies Act 2014 board minutes are inspectable by directors only and general
meeting minutes by members only. Neither is public. No co-operative examined — Kerry Co-op,
Dairygold, Tirlán, Lakeland, Aurivo, Arrabawn, Carbery — publishes AGM minutes or a poll-style
record; only financial statements. That check is **incomplete**: Centenary, Ornua and the
credit union league were not reached, and rfs.gov.ie blocked fetching, so the Registry of
Friendly Societies access route and fee remain unknown.

AGM **poll results** of listed companies are published — each resolution with votes for and
against, percentages, withheld votes and total cast — but there is no bulk route. Both the
Euronext OAM and the FCA National Storage Mechanism are JavaScript applications with no public
query API; NSM offers a per-search CSV export. Harvesting is per-company. Structure
consistency across issuers rests on a single examined example and is unverified.

The IFA annual report carries a **National Council attendance grid** (pp.84-85 of the 2025
report): named members by position and county against twelve meetings, keyed blank/`A`/`P`/`V`
for present, apology, proxy and vacant †. Its footnotes create a denominator trap — *"Potato
Chair eligible to attend two out of three meetings"* and a member who stepped down in
September — so a naive attendance rate would penalise people who were never eligible. This is
the same defect class as `memory/project_attendance_denominator_fix.md`. It is also named
private individuals, which places it outside the paid product under the spinout ethics
firewall.

## 5. Recommendation

Take capital pipelines before minutes. Pipelines are structured or semi-structured and state
the project directly; minutes are prose requiring extraction to reach the same fact. Sequence:

1. **Social Housing Construction Status Report first.** It is the only fully structured,
   machine-readable, project-named source found, it carries a delivery stage per scheme, and
   it refreshes quarterly. Lowest build cost and highest currency of anything here.
2. **HSE Capital Plan second** — project-level euro values and explicit tender-stage language,
   refreshed annually, one text-layer PDF.
3. **Then the Feature Service**, as the breadth layer: 1,936 projects across all sectors in one
   request. Backtest it before claiming anything — join the 680 pre-tender projects to existing
   awards and eTenders data to measure how long stage 2 takes to reach market, and which never
   did. That runs offline against data already held.
4. **Resolve the licence** before any of this touches a paid surface.
5. **Then harvest minutes**, narrowest-first: TII and NTA for transport, LDA for housing. These
   are static HTML with text-layer PDFs, so the existing council-minutes machinery applies with
   a new seed list rather than new extraction code.
6. Leave the WAF-blocked bodies as open questions until checked with a headless browser.

**Trade-off:** the Feature Service is annual and currently stale, so it cannot support a
real-time alert and must not be sold as one. It tells you a project exists; the live eTenders
feed still tells you it went to market. The two compose — neither replaces the other. The
quarterly housing report is the only source here fresh enough to carry an alerting claim, and
it covers one sector.

**Non-goals:** no private-sector or co-operative minutes harvester (the source class does not
exist); no AGM poll-result ingestion (per-company scraping, no procurement relevance).

## 6. Open

- CRU2025197 / CRU202588 name the 29 priority transmission projects; both resisted text
  extraction. Worth one manual look — it is ~€2bn of grid work.
- TII publishes no fetchable capital plan; its minutes are the substitute.
- Whether the Feature Service is refreshed annually or was abandoned after the May 2025
  publication. This is the single question that decides whether it is a durable source.
- The twelve WAF-blocked bodies in §2, plus the gov.ie pages that 403'd throughout (OPW, Dept
  of Education school tracker, IDA, Maynooth, UCD campus development). gov.ie blocks automated
  fetches; `pipeline_sandbox/council_minutes/pw_fetch.py` is the existing tool for that.
- Whether the Social Housing Construction Status Report carries a euro column in any quarter —
  the Q2-2025 file examined does not.

Any extraction built from this needs `doc/EXTRACTION_QUALITY_CHECKLIST.md` with `## Completeness`
and `## Recall`, or `tools/check_conventions.py` R12 fails it. Re-run `tools/build_doc_index.py`
after this file lands.
