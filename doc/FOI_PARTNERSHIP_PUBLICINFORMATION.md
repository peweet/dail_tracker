# Partnership assessment — publicinformation.ie (gingertechie)

Status: **ASSESSMENT** (2026-06-22). Evaluates the two Codeberg repos as a partnership/integration
target and identifies concrete FOI request candidates derived from *our* corpus. Companion to
`doc/FOI_PARTNERSHIP_PUBLICINFORMATION.md` (this is the real, specific version of that generic plan — the
partner is NOT running Alaveteli; they've built their own stack).

Repos (cloned to `c:/tmp`, AGPL-3.0):
- `publicinformation-data` — Python FOI/CSO extraction pipeline (972 tracked files)
- `publicinformation-web` — Astro static site + Bunny edge-functions (113 tracked files)

## File-read coverage (validation)

- **Web repo: 113 / 113 files read in full** (all `.astro`, `.ts`, edge-functions incl. the whole
  `foi-outreach/` module + tests, scripts, config, docs).
- **Data repo: all code + docs + schemas read in full** — every `.py` (pipeline orchestration,
  all `steps/*/process.py`, `src/lib/*`, both pipelines' tests), every `.md`, `pipeline.json`,
  `output_schema.json`, `baseline.json`, `eval/*`, `public/schema.sql`, and the consolidated
  `public/*.json` outputs.
- **NOT opened individually:** the 480 per-body raw `*.csv` and 177 raw scraped `*.html` artifacts
  under `pipelines/foi_pipeline/data/...`. These are *inputs/intermediates*, not code; their merged
  product is `public/foi-disclosures.json` (40 MB, 64k records), which I inspected directly. Reading
  each raw CSV adds nothing the consolidated output and the canonicalization code don't already show.
  (Several have Windows path-length issues anyway; `core.longpaths` was enabled to fetch them.)

## What the partner has actually built

**A complete, independent FOI infrastructure** — not Alaveteli, a bespoke stack:

| Layer | What it is |
|---|---|
| **CSO pipeline** | Ingests the CSO Public Register of Public Bodies → **883+ bodies** with stable `public_body_id` (≥1000, id-map persisted), `parent_id`, `government_department_id`, `sector` (ESA/NACE), `nace_code`, `legal_status`, **`cro`**, `data_vintage`. Fuzzy-matches names → gov.ie URLs. |
| **FOI pipeline** | 21 steps: scrape foi.gov.ie → find FOI pages/emails → find disclosure-log files → parse PDF/XLSX (pdfplumber + camelot fallback) → detect/repair headers → canonicalize columns (175+ synonyms incl. Irish bilingual) → normalize dates → canonicalize `decision_status` (9 values) → dedup → **topic-tag** → libSQL upload. |
| **Web** | Astro site: body directory + tree, per-body FOI listings, topic browse, search, CSV/JSON downloads. **Bluesky/ATProto OAuth + a "karma" reward system** for crowd corrections. |
| **Outreach engine** | `foi-outreach.ts` + module: create→send→await→classify FOI emails. Per-request `correlation_token` reply address, Scaleway TEM outbound, EmailConnect inbound webhook (HMAC), bounce/auto-reply detection, **Mistral LLM reply-classification** (`provides_url`/`provides_files`/`no_log_exists`/`refusal`/…). |

**Headline asset for us:** `public/foi-disclosures.json` — **64,014 extracted FOI request records**
across ~130 bodies with disclosure logs, each: `public_body_id`, body name, `file_url`,
`foi_reference_id`, `date_received`, `decision_date`, `requester_type` (Journalist/Business/…),
`decision_status`, `request_description`. Plus `topics.json` (keyword topic groupings; Housing alone
= 3,088 matches) and a live **search edge-function** (`search.publicinformation.ie/search?q=`).

## Data quality — the creator is right, and here's the quantified picture

From their own `baseline.json` / `eval/*` (they measure this honestly — a good sign):

| Metric | Value | Implication |
|---|---|---|
| PDF extraction success | 94.9% | files parse… |
| **Clean extraction (no structural issues)** | **55.2%** (771/1397) | …but ~45% have artifacts |
| Null-first-row (missing headers) | 30.5% | header detection is the main failure |
| Newline-split-row artifacts | 29.6% | descriptions split across PDF cells |
| Column-mapping accuracy | 98.7% | mapping itself is strong |
| `decision_status` canonicalized | 73.8% | 26% have null/garbage status |
| **"Valid" records (real date AND non-empty summary)** | **29,678 of ~64k (~46%)** | <half are analysis-grade |
| Bodies with ≥1 valid record | **41 of ~130** | long tail of bodies = near-empty |
| FOI email extraction accuracy | **26.7%** | the outreach engine's input list is thin |
| Disclosure-page discovery F1 | 0.667 | misses/false-positives on log pages |

**Verdict:** quality is *mixed, not unusable.* The ~30k valid records (well-dated, real
descriptions) are genuinely good — the Bord Bia/Housing samples read cleanly. The other half is
header-detection and PDF-artifact noise, plus a long tail of bodies with almost nothing. **Treat it
as a lead-generation / corroboration source, not a clean fact table.** Their `missing_columns` flag
and `errors.json` give us an honesty signal to filter on (mirrors our own grain-caveat discipline).

## Join potential with our corpus — feasible, name-bound

Their `public_bodies` register is the bridge. Match surfaces:

| Their key | Our key | Quality |
|---|---|---|
| `public_bodies.name` | `payments_fact.publisher_name` / procurement `authority` | **Primary join** — fuzzy/normalized. Our `name_norm` + NFKD fold tooling is built for exactly this. |
| `public_bodies.cro` | `cro_company_num` (payments/procurement CRO match) | **Clean join** where the body is incorporated (minority, but high-value). |
| `government_department` / `_id`, `parent_id` | our `sector` / publisher hierarchy | Department-family rollups. |
| `sector` / `nace_code` | our `spend_category` / CPV | Thematic, coarse. |

Scale check (from `data_coverage`): we carry **69 payment publishers**, **1,889 procurement
authorities**, **20,833 suppliers**; they carry **883 bodies**. The overlap is the ~130 bodies that
publish disclosure logs — all of which are in our payments/procurement universe. **A one-time
`public_body_id ↔ publisher_id` crosswalk** (built once with our name-norm tooling, hand-checked for
the top ~100 by spend) makes everything else automatic. This is Phase 0, exactly as the generic plan
predicted.

## Integration architecture (concrete, given their stack)

```
[their libSQL]  ──search.publicinformation.ie/search?q= (live JSON)──┐
[their public/*.json on Codeberg] ──foi-disclosures.json (batch)─────┤
                                                                     ▼
                                            [our extractor: foi_disclosures source]
                                                     │ join via public_body_id↔publisher_id crosswalk
                                                     ▼  silver → gold → views → Streamlit
                                  "FOI disclosures" element on body / Follow-the-Money pages
                                                     │
              ┌──────────────────────────────────────┘
              ▼  (outbound funnel)
"Ask this body via FOI" button on a gap → their outreach engine (correlation-token email)
              → Mistral-classified reply → resolved_url flows back into the pipeline
```

- **Inbound (read):** poll their search API for live queries; ingest `foi-disclosures.json` in batch
  for the corpus. No new platform to run.
- **Outbound (funnel):** their outreach engine is the "ask" mechanism we speculated about — already
  built, including LLM classification of replies. We supply the **targeting intelligence** (where the
  gap is); they supply the **send/track/classify** plumbing. Clean division, no codebase merge.
- **Crowd/identity:** their ATProto "karma" model is interesting but orthogonal; don't adopt it now.

**Boundaries:** AGPL-3.0 — fine for consuming their published data; if we *embed* their code we
inherit AGPL (we won't — we integrate at the data layer over HTTP/JSON). Legal/moderation liability
for sent FOI requests and published responses stays on *their* side of the line. Re-poll, don't cache
responses as final (their data is re-extracted and corrected).

## How likely is OUR data to launch FOI? Very — it's a gap-targeting engine

Our corpus's distinctive asset is that **it already knows where the published record ends** (coverage
bounds, `realisation_tier`/`amount_semantics` grain flags, broken-source canaries, single-bid
signals). That is precisely FOI request *targeting*. The loop:

> **our gap → check their disclosure log (was this already asked?) → if granted, fetch the released
> doc; if refused/never-asked, file via their outreach engine → response closes the gap.**

The "check their log first" step (their `search.ts`) is what makes this efficient — you don't
re-file what a journalist already got released.

## Specific FOI candidates (issue · body · what to expose · exact ask)

Derived from live MCP queries against our corpus + documented gaps. Each is a *prompt to look*, not
an allegation — single-bid/overlap signals are factual, never verdicts.

### 1. Children's Hospital — what the €130.7m settlements actually covered  ★ flagship
- **Body / department:** National Paediatric Hospital Development Board (NPHDB).
- **Expose:** Our payments ledger shows NPHDB → BAM Building = €130.7m, of which **€126.7m is two
  conciliator settlements** (Recommendation No. 25 = €107.6m in 2024; No. 29 = €19.1m in 2025, each
  tagged "Notice of Dissatisfaction issued"). The *heads of claim* — what these settlements paid for,
  and the sub-contractor tier beneath BAM — are published nowhere (`po_committed` grain only).
- **Exact ask:** *"Please provide, for the period 1 January 2024 to date: (a) Conciliator's
  Recommendations No. 25 and No. 29 relating to the main construction contract with BAM Building Ltd;
  (b) the Notices of Dissatisfaction referenced against each; (c) a schedule of amounts paid under
  each recommendation broken down by head of claim; and (d) any records identifying sub-contractor
  payments funded by these settlements."*
- **First check their log:** search `conciliator` / `BAM` / `children's hospital` in their data.

### 2. DCEDIY asylum/Ukraine accommodation spend — per provider, per location  ★ highest-value gap
- **Body / department:** Department of Children, Disability and Equality (DCEDIY).
- **Expose:** This is the single biggest hole in our money corpus (memory:
  `project_dept_children_asylum_spend_gap`) — and our **source canary shows DCEDIY's own PO PDFs are
  failing ingestion right now** (`not_expected_filetype`, circuit-breaker tripped, **9 files
  skipped**). The IPAS/BOTP accommodation spend (Mosney, Cape Wrath, IGO etc.) is €m-scale and
  invisible at provider/location grain.
- **Exact ask:** *"A schedule of all payments of €20,000 or more to accommodation providers under the
  International Protection Accommodation Service (IPAS) and the Beneficiaries of Temporary Protection
  (Ukraine) programme for calendar years 2023, 2024 and 2025, broken down by provider name, centre
  location, total paid, contract value, and contract start/end date."*

### 3. Cancelled / non-commenced accommodation contracts — break-fee payouts
- **Body / department:** DCEDIY (companion to #2; memory `project_ipas_planning_si_transparency`).
- **Expose:** Tóibín PQs reference cancelled-contract payouts and centres operating without planning
  permission; the payout amounts aren't in any structured feed.
- **Exact ask:** *"Details of all cancellation, break-fee or termination payments made to
  accommodation contractors whose contracts were cancelled or never commenced between 2022 and 2025,
  listing provider, location, reason, and amount paid."*

### 4. University of Galway — single-tender justifications  (factual signal, not accusation)
- **Body / department:** University of Galway (Dept of Further & Higher Education sector).
- **Expose:** From `procurement_competition` (TED 2024+): **73.9% single-bid lot rate — the highest
  of any Irish buyer with ≥40 lots** (68 of 92 lots; 39 uncompetitive notices). Research universities
  legitimately single-source, so the *written justification* is the thing to see.
- **Exact ask:** *"For all contracts awarded in 2024 and 2025 where only one tender was received: the
  procurement procedure used, the estimated and awarded value, and the documented justification for
  each negotiated-procedure-without-prior-publication or single-tender award."*
- **Caveat to publish with it:** single-bid is an EU integrity *indicator*, a prompt to look — not
  proof of wrongdoing.

### 5. Dublin City Council — contracts awarded without competition
- **Body / department:** Dublin City Council.
- **Expose:** DCC has the **largest single-bid volume** (65 single-bid lots; **51 uncompetitive
  notices**) of any authority in our TED set.
- **Exact ask:** *"A register of all contracts awarded in 2024–2025 without a competitive tender
  process, including the legal basis/derogation relied upon, supplier, value, and duration."*

### 6. Capita / Bord Gáis Energy / Fexco — outsourced-service contract terms
- **Body / department:** the contracting authorities (e.g. Dept of Social Protection for Capita
  customer-services).
- **Expose:** `procurement_lobbying_overlap` flags firms on *both* the award and lobbying registers
  with large values (Capita €78.2m / 13 lobby returns; Fexco €43.5m / 7). Co-occurrence only — **no
  causal link** — but the underlying contract scope/terms are FOI-able where not on eTenders.
- **Exact ask (Capita example):** *"The contract(s), statement(s) of work and total amounts paid to
  Capita Customer Solutions Ltd for customer-service/administration functions 2022–2025, and the
  procurement procedure under which each was awarded."*
- **Caveat:** present as transparency on outsourced public services, never as implied influence.

## Recommendation

Strong, complementary partner — they own the FOI *plumbing* (extraction + outreach + identity); we
own the *spend/accountability corpus* and the *gap-targeting intelligence*. Sequence:
1. **Phase 0 — build the `public_body_id ↔ publisher_id` crosswalk** (our name-norm tooling; hand-check top ~100 by spend). Everything depends on it.
2. **Phase 1 — inbound:** ingest `foi-disclosures.json` + wire their search API; show an "FOI disclosures" element on body / Follow-the-Money pages (filter to their `valid`-grade records).
3. **Phase 2 — funnel:** "Ask this body via FOI" buttons on quantified gaps (candidates above) → their outreach engine; classified replies flow back.
4. **Phase 3 — close the loop:** released figures feed the relevant facts with FOI-sourced provenance + grain caveat.

Open questions for the partner: (a) is the libSQL search API rate-limited / OK for our build-time
polling? (b) will they expose the outreach `/create` endpoint to us, or do we deep-link users to
their UI? (c) can they add `public_body_id` stability guarantees we can pin a crosswalk to?
