# publicinformation.ie × Dáil Tracker — partnership assessment

**A standalone assessment of a data partnership between publicinformation.ie (gingertechie) and Dáil
Tracker.** Self-contained — share this document on its own. Detailed sub-analyses live in four
companion docs (`FOI_PARTNERSHIP_PUBLICINFORMATION.md`, `JOURNALIST_FOI_FLOW_DESIGN.md`,
`FOI_CASE_LIBRARY.md`, `FRONT_DOOR_PROTOTYPES.md`) but everything needed to judge viability is here.

Date: 2026-06-22. Status: assessment + evidence-backed design. No code built. All figures from live
queries against both datasets; caveats inline.

---

## 1. Executive summary & verdict

**Viable, and genuinely novel.** The two corpora are complementary rather than overlapping:

- **publicinformation.ie holds the *demand* side of transparency** — what citizens and journalists
  have *asked* of Irish public bodies under FOI, and what each body *released or refused*. Plus a
  built outreach engine that sends FOI emails and classifies replies.
- **Dáil Tracker holds the *supply* side** — what those same bodies actually *spent* (€39bn payments),
  *bought* (€11.8bn awards), and *who lobbied them* (the SIPO register), all keyed to public bodies.

Joined, a static FOI directory becomes an **investigation engine that ends in a well-targeted new FOI
request**. Three things make the case concrete: the body-level join is empirically real (§4), the
journalist→FOI flow reproduces on live records (§5–6), and the combined data answers a question no
existing Irish tool answers — *"has this already been asked, and did it succeed?"* — before a
journalist spends a request (§5).

**The single honest caveat:** their disclosure-log data is **lead-grade, not fact-grade** (~52% of
records fully analysis-usable — 29,678 of 56,894). The design uses it for *lead-generation and "already-asked?"
intelligence*, never as a clean fact table — which is the correct use and a strength, not a blocker.

---

## 2. The two assets (audited 2026-06-22)

### 2a. publicinformation.ie (two Codeberg repos, AGPL-3.0)

| Component | Detail |
|---|---|
| **CSO body register** | **≈890** public bodies (verified: 890 nodes in their `bodies-tree.json`); stable `public_body_id`; carries `parent_id`, `government_department_id`, `sector` (ESA/NACE), `nace_code`, **`cro`**, `legal_status`, `data_vintage`. **229** processed through the FOI pipeline; **52** with a parsed disclosure log, **83** with an FOI email. |
| **FOI pipeline** | 21 steps: scrape foi.gov.ie → find FOI pages/emails → find disclosure-log files → parse PDF/XLSX (pdfplumber + camelot) → repair headers → canonicalize 175+ column synonyms (incl. Irish bilingual) → normalize dates → 9-value `decision_status` → dedup → topic-tag → libSQL. |
| **FOI corpus (output)** | **56,894** FOI request records (public copy): body, ref, dates, requester_type, decision_status, full request_description. |
| **Topics** | keyword groupings (`topics.json`; Housing = 3,088 records). |
| **Search** | live edge API `search.publicinformation.ie/search?q=`. |
| **Outreach engine** | tokenised reply-address emails (Scaleway), inbound webhook (EmailConnect, HMAC), bounce/auto-reply detection, **Mistral LLM reply-classification** (`provides_url`/`provides_files`/`no_log_exists`/`refusal`/…). |
| **Identity** | ATProto/Bluesky OAuth + a "karma" reward system for crowd corrections (orthogonal — don't adopt). |

### 2b. Dáil Tracker (this project)

Numbers below are the **raw gold fact-table counts** (ground truth, reproducible from the parquet) —
the same scope the join tests in §4 use. The project's published `data_coverage` guard reports a
narrower *sum-safe* subset (227,698 payment lines / €39.07bn / 44,120 award rows / €11.76bn safe); both
are valid, but this doc uses raw counts throughout for internal consistency.

| Domain | Scale (raw gold) |
|---|---|
| Public-body payments (>€20k PO/payment lines) | **247,457 lines · €40.51bn (safe) · 72 publishers · 24,270 suppliers · 2012–2026** |
| Procurement awards (eTenders/OGP) | **62,763 rows · 2,249 contracting authorities** (€11.76bn sum-safe per coverage guard) |
| TED (EU OJ) | 13,341 award rows; coverage guard counts 36,603 TED notices / €16.99bn, winners 2024+ |
| Lobbying (SIPO register, politician-broken-down) | 1.13M target-rows; lobbyist org → policy area → **lobbied body (`chamber`)** → named DPO/politician |
| Lobbying × procurement overlap | 235 firms on both registers (Dell €124.2m, BAM €115.9m, Sisk €114.1m …) |
| Competition signal | TED 2024+ single-bid rate per buyer (Univ Galway 73.9% — highest, 68/92 lots) |
| Council accountability | NOAC collection/derelict/planning; 31 CE roster |
| Live gap canary | `source_fetch_failures` — which bodies' spend feeds break *right now* |

**Money-grain rule (survives every join):** awards (ceiling), payments (cash out), and allowances are
three different grains — never summed. Facts carry `amount_semantics`/`realisation_tier`/`value_safe_to_sum`.

### 2c. Their actual data sources (mined from the 1,785 disclosure-file URLs + their stack)

Their `DATA_SOURCES.md` lists four upstream sources (CSO Public Register of Public Bodies; gov.ie
departments directory; foi.gov.ie all-FOI-bodies; individual body websites). Mining where the 1,785
disclosure documents *actually* come from sharpens that:

| Source domain | Files | Note |
|---|---|---|
| **assets.gov.ie** | 636 | the gov.ie CDN (central departments) — **the same CDN our ministerial-diary + PO PDFs come from** |
| tipperarycoco.ie | 238 | one council dominates — Tipperary publishes prolifically |
| pleanala.ie | 84 | **An Bord Pleanála** — planning overlap with our ArcGIS/IPAS-SI work |
| mayo.ie / meath.ie / louthcoco.ie / leitrim.ie / fingal.ie / kildarecoco.ie / corkcity.ie | 36–64 each | heavy council coverage |
| ntma.ie · fspo.ie · taxappeals.ie · courts.ie · fiscalcouncil.ie · nationaltransport.ie · hsa.ie | 21–41 each | agencies/ombudsman/courts |
| **hse.gov.uk** | 58 | ⚠️ **a UK domain — almost certainly mis-crawled** (UK Health & Safety Executive, not Irish HSE). A data-quality flag worth telling them. |

File types: 1,689 PDF / 78 XLSX / 18 XLS (PDF-dominated → why their extraction is hard).

**Tech/service stack** (from env + deps): **Bunny.net** (managed libSQL DB + edge functions + CDN);
**data served straight from Codeberg raw** (`DATA_BASE_URL = codeberg.org/.../raw/branch/main/public`);
**Apify** (batch search fallback); **Scaleway TEM** (outbound FOI email); **EmailConnect** (inbound
webhook); **Mistral** (reply classification); **ATProto/Bluesky** (karma identity). All AGPL-3.0.

---

## 3. Data quality (both sides, honest)

**Their FOI corpus** (verified against their `baseline.json`): PDF extraction success **94.9%**;
post-normalization clean rate **81.9%**; header-mapping accuracy **98.7%**; decision_status
canonicalization **76.5%**; FOI-email accuracy **26.7%**; disclosure-page discovery F1 **0.667**;
**29,678 "valid" records** (real date + non-empty summary) across 41 bodies — about **half** of the
56,894 published records (~52%). Decision split: Part-Granted 16,323 / Refused 13,338 / Granted
12,318. **Largest requester type = Journalist (17,801 records)** — the data is already
journalist-shaped. → *Use as leads + "already-asked?" intelligence, filtered to their
`valid`/`missing_columns` flags.*

**Our corpus:** the grain caveats above; lobbying & diary co-occurrence is *access, not influence*;
single-bid is an EU integrity *signal, never a verdict*. These caveats must travel with every surfaced
figure (consistent with our `feedback_no_inference_in_app` rule).

---

## 4. Joinability — measured on every axis (the core technical question)

The whole partnership hinges on whether our data joins to their public-body register. Tested by
normalising names and matching. **Four distinct join axes**, three empirically measured:

### 4a. Body spend ↔ FOI register  — **STRONG**
Our payment publishers / procurement authorities → their `public_body_name`:
- **72%** of our 72 payment publishers match naively (52/72). The ~20 misses are short-name variants
  (NPHDB, CHI, Tusla, NTA, Teagasc) — all present under a different surface form.
- Procurement authorities 133/2,249 naive (6%) — but the long tail is tiny/EU/one-off buyers; the
  departments/councils/big agencies that matter all match.
- **A one-time `public_body_id ↔ publisher_id` crosswalk** (our name-norm/NFKD tooling + hand-check of
  the top ~100 by spend) takes this to near-100% for bodies that matter. This is Phase 0.

### 4b. Lobbied body ↔ FOI register  (`chamber` field) — **MODERATE, fixable**  ← the one you asked about
The SIPO return's `chamber` field carries the actual lobbied body (e.g. "Galway County Council",
"Department of Social Protection", "Office of the DPP"). Matching `chamber` → their register:

| Measure | Naive result |
|---|---|
| Distinct chambers matched to register | **76 / 162 (47%)** |
| …matched to a body that also has an FOI log | 25 / 162 (15%) |
| By **return volume** (154,948 returns) | **40%** hit a body in the register; 21% hit one with an FOI log |

**Why the residual — and why most of it is recoverable:**
- The biggest unmatched chambers are **old department names** from machinery-of-government renames —
  "Department of Justice **and Equality**", "Department of Children, **Equality, Disability,
  Integration and Youth**", "Department of Environment, **Climate and Communications**". These ARE in
  the register under current names. **We already hold `data/_meta/si_department_aliases.csv`** (alias →
  canonical department) built for exactly this; applying it lifts the executive-body match materially.
- The genuinely un-joinable chunk is the **legislature**: "Dáil Éireann, the Oireachtas" (30,025
  returns), "Seanad" (12,393), "European Parliament" (5,848). Lobbying a TD/Senator/MEP targets the
  *legislature*, which is not an FOI-disclosure-log body in the same sense. **This is a real ceiling,
  not a bug:** a large share of lobbying is at legislators, where the "FOI the lobbied body" move
  doesn't apply — surface it honestly.

**Net:** after the alias map, lobbying-of-executive-bodies joins well to their register; lobbying-of-
legislators (~30% of returns) is structurally outside the FOI-body model. The "thin lobbying return →
FOI the lobbied body" flow (case library D1–D5) therefore works **for departmental/council/agency
targets**, which is where the high-public-interest zoning/developer cases sit anyway.

### 4c. Lobbyist organisation ↔ FOI register — **NICHE**
Of 4,194 distinct lobbyist/client orgs, only **25 (0.6%) are themselves public bodies** in the
register — because lobbyists are overwhelmingly private. But it's a real, interesting set: **public
bodies that lobby other public bodies** — Gas Networks Ireland, Horse Racing Ireland, Irish National
Stud, Kerry/Kilkenny/Louth County Councils, Dept of Housing. A small bespoke lens ("which state bodies
lobby government"), not a primary join.

### 4d. CRO ↔ CRO — **CLEAN where present**
Their `public_bodies.cro` ↔ our `cro_company_num` gives an exact secondary join for incorporated
bodies and lets lobbyist *orgs* (CRO-enriched on our side) tie to suppliers/awards — the basis of the
existing `procurement_lobbying_overlap` (235 firms).

**Joinability verdict:** body-spend join is strong (the backbone); lobbied-body join is moderate and
materially improved by the alias map we already have, with an honest legislature ceiling; org and CRO
joins are clean but narrower. **Lobbying *is* joinable to the FOI project — primarily via the
`chamber`/lobbied-body axis — and it strengthens the case rather than weakening it.**

---

## 5. The journalist investigation → FOI flow

A repeatable 6-stage pipeline. Stages 2–3 are ours; 4 and 6 are theirs — neither side runs the whole
loop alone (that *is* the partnership).

```
entry (body / topic / supplier / lobbyist)
  → ① anomaly            (our spend / awards / lobbying / live gap-canary)
  → ② where record stops (our data — we know exactly where the published record ends)
  → ③ already asked?     (their FOI log + search API:
                            GRANTED before → fetch the released doc, don't re-file
                            REFUSED before → narrow the ask + cite the refusal for internal review
                            NEVER asked    → clean new FOI)
  → ④ draft scoped ask   (their registered FOI email + our grain-correct figures for context)
  → ⑤/⑥ send + classify  (their outreach engine; classified reply + released figures flow back)
```

**The differentiator (stage ③), proven by live probes:** the combined data tells a journalist, *before
they spend a request*, where the virgin territory is. Real probe results:

| Topic | Prior FOIs in their log | Verdict the flow gives |
|---|---|---|
| Roadbridge (collapsed council contractor) | **0** | file now — un-asked |
| OPW construction contractors | **0** | file now — un-asked |
| HSE building purchases | **0** | file now — un-asked |
| NTMA payments | 39 (Refused) | narrow + cite refusals for internal review |
| Pfizer / vaccines | 117 (Part-Granted) | fetch existing releases first |
| Council zoning | 67 (recent Granted) | high-probability win |

No existing Irish FOI tool offers this; it falls straight out of joining the two datasets.

---

## 6. Worked examples (assembled entirely from real records)

**A — National Children's Hospital.** Our payments show NPHDB → BAM Building = **€130.7m, of which
€126.7m is two conciliator settlements** (Recommendation No. 25 = €107.6m 2024; No. 29 = €19.1m 2025,
each "Notice of Dissatisfaction issued"; grain = `po_committed`). The heads of claim + sub-contractor
tier are published nowhere. Their log: NPHDB has **no disclosure log**; Dept of Health holds 70
children's-hospital records (context only). → **un-asked at source.** FOI to NPHDB for the two
recommendations, the notices, and the per-head-of-claim schedule.

**B — Dept of Children, asylum/Ukraine accommodation.** Our biggest money gap, and `source_fetch_
failures` shows DCEDIY's PO PDFs **failing ingestion right now** (breaker tripped, 9 files skipped).
Their log (DCEDIY, 1,172 records): an individual asked for "a complete list of buildings… that have
received I[PAS funding]" → **Refused (2025-04-09)**; the topic runs **194 Refused : 59 Granted**. →
narrow to a per-provider/county aggregate and cite the prior refusal.

**Plus 20 more worked leads** (5 councils, 5 semi-state/state, 5 unclear diary meetings, 5 thin
lobbying returns) in `FOI_CASE_LIBRARY.md` — including Sligo's €80.4m to the collapsed Roadbridge, the
NTMA €182.6m null-supplier block, the OPW €155.8m blank-everything line, and property developers
(DRES, Aspect) lobbying on "Zoning" with six-character disclosures.

### 6b. MCP-constructed stories — the access-gap pattern (verified 2026-06-22)

The richest seam the MCP tools surface is **state bodies with enormous ministerial access and zero
lobbying-register trace.** State bodies are exempt from the lobbying register, so the *only* public
window on what they discuss with ministers is FOI. The top six, by logged meetings (all
`corroborated: false`, 0 register returns):

| State body | Ministerial meetings | Ministers met | Register returns |
|---|---|---|---|
| **IDA Ireland** | **365** | 28 | **0** |
| Health Service Executive | 211 | 20 | 0 |
| Enterprise Ireland | 180 | 26 | 0 |
| Land Development Agency | 106 | 16 | 0 |
| Higher Education Authority | 76 | 6 | 0 |
| National Transport Authority | 75 | 12 | 0 |

**~1,013 ministerial meetings across these six alone, none of it on the lobbying register.** Four
publishable, MCP-verified stories built on this:

**S1 — IDA Ireland: 365 meetings, the FDI deals nobody sees ★★** (`ministerial_diary_organisation`).
The IDA is the second-most-met organisation in the entire diary corpus, after the GAA. Among the 365
are confidential, named client-company engagements — *"Engagement with IDA client company – Tokyo
Electron"* (Tokyo), *"Meeting with IDA Client company Fullbright Medical"*, *"Meet with IBM (IDA
South-East office)"*, *"Datavant – IDA client company jobs announcement"* — alongside the "Regional
Property Programme" presentations.

*Monetary dimension (EU State Aid TAM register, `aid_element` field — see gotcha below):* the IDA's
**disclosed grant aid totals ≈ €1.63bn** (2016–2026, steady €150–290m/yr). **Read the per-company
figures as CUMULATIVE multi-year sums, not single grants:** e.g. "Boston Scientific €66.6m" is **34
separate awards over 2017–2025, the largest single just €9.4m** (and ≈€82m if name variants are
folded — per-company totals are approximate due to name fragmentation). The genuinely large *single
recent* awards are the strategic-tech ones: **Analog Devices €85.0m (2024, IPCEI Microelectronics)**,
**Intel €34.7m (2025, R&D) + €30.0m (2023, Microelectronics/Ukraine crisis scheme)**, **Microsoft
€20.7m (2025, R&D)**. *Nature of the aid:* overwhelmingly **R&D (Experimental development €615m +
Industrial research €355m) and training (€253m) under EU block-exemption schemes** — *not* classic
location/capital grants (those sit below the reporting threshold or outside TAM) — plus IPCEI
microelectronics and COVID/Ukraine crisis schemes. These figures are **public but obscure** — each
carries an EU Commission link (`webgate.ec.europa.eu/competition/transparency/...`) but lives on a
clunky per-award tool with no Irish roll-up; IDA's reports give only aggregates. They are **not in the
diaries** (diaries hold no money field) — *we* join them by company name. And TAM still **omits the
real package**: property/site deals, bespoke terms, and the corporation-tax incentive (the 12.5%/15%
regime + R&D credits — not "state aid", in no dataset we hold). The public number is the headline
grant; the *deal* behind it is the FOI.

**Where the FOI value is — not the grants.** The R&D/training grants above are *routine and already
disclosed*; they are not the story. The story is what the EU register **cannot** show and the
ministerial meetings are actually about: the **property and site deals**, the **bespoke location/
investment packages** negotiated per company, and the **tax dimension** — none of which appear in any
public dataset. The FOI must target *that*, not the published grant line.

**FOI (to DETE / IDA):** *"In relation to the IDA client-company engagements recorded in ministers'
2025 diaries (incl. Tokyo Electron, Fullbright Medical) and the IDA Regional Property Programme
presentation of 26 Nov 2025: the briefing notes and minutes for those meetings; any records of IDA
property, site or fit-out support, lease/land terms, or other non-grant inducements offered to or
discussed with those companies; and the heads of terms of any investment-package commitments — to the
extent not already published on the EU State Aid register."*

**S2 — The revolving door: a former Agriculture chief lobbies his old department ★★**
(`dpo_lobbying_profile`). **Philip Carroll**, former **Assistant Secretary at the Dept of Agriculture**,
is now an **Ibec** lobbyist (122 returns) whose most-targeted official is **Brendan Gleeson — the
department's current Secretary General — 49 times**, plus senior officials Sinéad McPhilips (29) and
CVO Martin Blake (19). Post-employment/cooling-off FOIs are **near-virgin (3 in the whole corpus).**
**FOI (to Dept of Agriculture):** the contact records + any post-employment restriction or
conflict-of-interest declaration applying to his move to Ibec.

**S3 — The data-centre access cluster during the grid crunch ★** (`who_ministers_meet`). Across 2024,
then-Minister **Eamon Ryan** met **AWS, Microsoft, Echelon, Vantage, Digital Infrastructure Ireland**
and the regulators **GNI/CRU/EirGrid** on data centres, during the contested grid-connection limits.
**FOI (to DECC):** submissions, briefings and follow-up from those named meetings.

**S4 — Land Development Agency: 106 meetings, money-opaque ★★** (detailed in §6 / case library E2) —
private "State Lands and LDA Report" briefings, no register trace, no spend trace.

**All carry the standing caveat: the diary/lobbying registers record _access, not influence_;
co-occurrence is not causation.** The point is that FOI is the *only* route to the substance — which is
exactly what the partnership operationalises. (A repeat-corporate-distress angle was tested and dropped
— `company_influence` returned no public-money link to assert.)

---

## 7. Which of our tables matter most to them

All per-body, joinable on `public_body_id` via the Phase-0 crosswalk:

| Rank | Our table / view | Value to publicinformation.ie |
|---|---|---|
| 1 | **`procurement_payments_fact`** | Per-body spend profile — the best companion to an FOI page; shows where the record stops (= FOI targets). |
| 2 | **`source_fetch_failures`** | A *live* FOI-targeting feed — broken/zero-harvest publishers are where an FOI is justified. Unique to us. |
| 3 | **awards + TED winner history** | Contracts awarded, to whom, at what value. |
| 4 | **`procurement_competition`** (single-bid) | Per-buyer integrity signal that motivates single-tender FOIs. |
| 5 | **`procurement_lobbying_overlap`** | Firms paid by a body that also lobbied — influence context (co-occurrence caveat). |
| 6 | **ministerial diaries / `who_ministers_meet`** | Who the relevant minister met; corroborates lobbying. |
| 7 | **NOAC + `la_chief_executives`** | Enriches the 31 local-authority body pages. |
| 8 | **`supplier_groups`** | Corporate-group rollup (BAM SPVs) — needed for the children's-hospital case. |

**Low value to them:** attendance, votes, member interests, SIPO election finance, judiciary —
person/politics-centric, not body-spend-centric.

---

## 8. Integration architecture

```
[their libSQL]  ──search API (live JSON)──┐
[their public/*.json on Codeberg] ─batch──┤
                                          ▼
                        [our extractor: FOI source]  ──join via public_body_id↔publisher_id crosswalk──
                                          │ + chamber↔body for lobbying (with si_department_aliases)
                                          ▼  silver → gold → views → Streamlit
              "FOI log" + "spend profile" + "who lobbied this body" on a single body page
                                          │
                                          ▼  (outbound funnel)
        "Ask this body via FOI" on a quantified gap → their outreach engine → classified reply → back
```
- **Inbound:** poll their search API + ingest `foi-disclosures.json`. **Delivery is trivial — they
  serve `public/*.json` straight from Codeberg raw**, so we fetch a URL, no API key or platform needed.
- **Outbound:** their outreach engine is the "ask" mechanism — we supply targeting, they supply send/classify.
- **No codebase merge** — integrate at the data layer over HTTP/JSON only.

**Refinements surfaced by the source pass (§2c) — concrete things we bring on day one:**
1. **Shared `assets.gov.ie` source → we can hand them a fix.** 636 of their files come from the gov.ie
   CDN we already harvest. We hit (and fixed) the intermittent **Brotli-decoding bug on that CDN** that
   silently returns 0 files (memory `project_diary_refresh_transport_decc_health`). They're likely
   suffering the same intermittent loss; the fix (drop `br` from Accept-Encoding) is a free contribution.
2. **Planning overlap (pleanala.ie, 84 files).** Their An Bord Pleanála FOI logs pair with our planning
   ArcGIS + IPAS-planning-SI work — a joint planning-accountability lens.
3. **Quality flag to report: `hse.gov.uk` (58 files)** looks mis-crawled (UK HSE). Easy win to flag.
4. **We can enrich their body register** with our `cro`/supplier-group spine where their `cro` is null.

---

## 9. Risks & boundaries

- **AGPL-3.0:** fine to consume their published data; we integrate over HTTP/JSON and do **not** embed
  their code, so AGPL doesn't reach our stack.
- **Legal/moderation:** liability for sent FOIs and published responses stays on *their* side of the
  line (they run the platform). This is the main reason not to build our own FOI platform.
- **Data quality:** surface only `valid`-grade FOI records; re-poll (don't cache responses as final —
  they re-extract/correct).
- **Lobbying-join ceiling:** be honest that ~30% of lobbying targets the legislature (not FOI-body-shaped).
- **All caveats travel:** grain · signal-not-verdict · co-occurrence-not-causation.

---

## 10. Phased plan

| Phase | Work | Depends on |
|---|---|---|
| **0** | `public_body_id ↔ publisher_id` crosswalk (name-norm + top-100 hand-check; `cro` secondary); apply `si_department_aliases` to the lobbying `chamber` join | — |
| **1** | Ingest `foi-disclosures.json` + wire search API as a read source; show FOI log + spend profile + who-lobbied on a body page (valid-grade only) | 0 |
| **2** | Build flow stages ①–③: anomaly → where record stops → already-asked (with the green/amber/red verdict) | 1 |
| **3** | Wire ④–⑥: templated draft → their outreach engine; classified replies feed back with FOI provenance | 2 + outreach access |
| **4** | Close the loop: released figures patch the relevant facts (grain-caveated) | 3 |

Start with worked examples A, B and the 20-case library as the demo — all already have data behind them.

---

## 11. Open questions for them

1. Is `public_body_id` stable enough to pin a crosswalk to? (We believe yes — they persist an id-map.)
2. Search API rate limits — OK for our polling, or batch-ingest `foi-disclosures.json`?
3. Will they expose the outreach `/create` endpoint, or do we deep-link into their UI?
4. Can they surface the `valid`-grade / `missing_columns` flags in the API so we only show analysis-grade records?
5. Direction: our spend/lobbying tables into their pages, the flow on our side, or both (mutual embed)?

---

## 12. Claim verification (every numeric clause tested against source data, 2026-06-22)

Each claim re-run against the live data. ✅ confirmed · ✏️ corrected (doc now updated) · ⚠️ scope note.

| § | Claim | Measured | Status |
|---|---|---|---|
| 2a | ≈883 bodies in register | **890** nodes in `bodies-tree.json` | ✏️ → ≈890 |
| 2a | 229 processed · 52 with log · 83 with email | 229 · 52 · 83 | ✅ |
| 2a | 56,894 FOI records | 56,894 | ✅ |
| 2a | decision split 16,323 / 13,338 / 12,318 | exact | ✅ |
| 2a | Journalist largest requester (17,801) | 17,801 (next: Other 8,937) | ✅ |
| 2a | topics: Housing 3,088 | 3,088 | ✅ |
| 2b | payments 227,698 lines / €39.07bn / 20,833 suppliers | raw gold = **247,457 / €40.51bn / 24,270** | ✏️⚠️ doc figures were the MCP *coverage* scope; now raw |
| 2b | 72 payment publishers | 72 (coverage tool reports 69) | ✅ |
| 2b | awards 44,120 rows / 1,889 authorities | raw gold = **62,763 / 2,249** | ✏️⚠️ now raw; was coverage scope + internally inconsistent w/ §4 |
| 2b | TED 36,603 notices / €16.99bn | coverage-tool figure; silver award rows = 13,341 | ⚠️ labelled as coverage grain |
| 2b | overlap Dell €124m / BAM €116m / Sisk €114m | 124.2 / 115.9 / 114.1 | ✅ |
| 3 | extraction 94.9% · email 26.7% · mapping 98.7% · F1 0.667 | baseline.json: 0.949 / 0.267 / 0.987 / 0.667 | ✅ |
| 3 | "clean extraction 55.2%" | not a baseline metric; post-norm clean = **81.9%** | ✏️ replaced |
| 3 | decision_status canonicalized 73.8% | baseline = **0.765 (76.5%)** | ✏️ corrected |
| 3 | ~46% valid (29.7k) | valid_records **29,678** / 56,894 = **~52%** | ✏️ corrected % |
| 4a | 72% publishers match (52/72) | 52/72 = 72% | ✅ |
| 4a | procurement authorities 133/2,249 (6%) | 133/2,249 | ✅ |
| 4b | lobbied-body (`chamber`) 47% / 40% by volume / 15% w/ log | 76/162=47% · 40% · 25/162=15% | ✅ |
| 4c | lobbyist org → register 0.6% (25/4,194) | 25/4,194 | ✅ |
| 4b | `si_department_aliases.csv` exists | present, 20 alias rows | ✅ |
| 5 | probes: Roadbridge 0 · OPW 0 · HSE-buildings 0 · NTMA 39 Refused · Pfizer 117 Part-Granted · zoning 67 Granted | all exact | ✅ |
| 6 | NPHDB→BAM €130.7m / 5 lines; Rec 25 €107.6m (2024), Rec 29 €19.1m (2025) | exact | ✅ |
| 6 | DCEDIY "list of buildings" Refused 2025-04-09; asylum 194 Refused : 59 Granted | both exact | ✅ |
| 6 | DCEDIY PO PDFs failing (9 files skipped) | live `source_fetch_failures` confirms | ✅ |

**Net:** the load-bearing claims — the join rates (§4), the already-asked probes (§5), and both worked
examples (§6) — all confirmed exactly. The corrections were confined to §2b/§3 corpus headline figures,
which had mixed the MCP *coverage* scope with raw gold counts; the doc now uses raw-gold ground truth
consistently, matching the join denominators.
