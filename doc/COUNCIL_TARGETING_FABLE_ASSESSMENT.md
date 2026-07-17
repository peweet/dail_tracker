---
tier: RECORD
status: LIVE
domain: local-gov
updated: 2026-07-09
supersedes: []
read_when: before scoping new county-council data work (AFS, minutes/agendas, named votes) or deciding what council data source to pursue next
key: RECORD|LIVE|local-gov
---

# County Council Data — Targeting Assessment (Fable model)

**Date:** 2026-07-09 · **Assessor:** Fable 5 (4 independent lenses + adversarial synthesis), commissioned as: *"target county council — particularly the budgets i.e. AFS, the scanned agendas/minutes of meetings, and anything else relevant."*
**Lenses:** (A) AFS & budgets · (B) minutes & agendas · (C) wider un-ingested source sweep · (D) adversarial skeptic (opportunity cost + BI-spinout fit).
**Nature:** data-targeting assessment. Every load-bearing claim was verified this session against live council/government sites, the bronze cache, the silver/gold parquet, or extractor source code — not against memory. Assessment only: nothing pulled to gold, no code changed, no OCR run. Reopening decisions flagged in §8 belong to the owner.

---

## 1. Bottom line

**The county-council target is materially richer and cheaper than the project's recorded state says it is — because the recorded state is stale in three load-bearing places.** The AFS "dead-end" (2026-06-19) was priced against a blocker taxonomy that no longer holds: the single biggest unlock is not scraping, OCR, or Playwright but a per-council-instead-of-per-year defect in the extractor's camelot fallback that strands ~25–28 **already-downloaded, born-digital** council-years; all four "Playwright-only" councils now expose plain PDF links; Kerry's "un-locatable" AFS sits on an open docstore. **31/31 councils with at least one recent AFS year is realistic, mostly without OCR.** On minutes, the coverage tiering is wrong in both directions: Cork City — tiered "no named votes" — has ~149 named division blocks (including budget-amendment votes) sitting in already-extracted text, while two flags (South Dublin, Fingal) were derived from the wrong documents; the honest named-vote ceiling is ~6 councils, never 31, and verifiably never Dublin City. On "anything else": the best new families are **central**, not 31-bespoke — DHLGH's consolidated adopted-budgets publication (all 31 councils, by division, 2019–2026, one parser), LGAS statutory audit reports (central since 2012), Derelict Sites annual returns, and Revenue's LPT adjustment-factor table. The scanned-document problem the tasking led with turns out to be the *smallest* part of the answer: excluding the Louth minute-books (an HTR problem, drop), the entire scanned backlog is roughly 1,700–1,800 pages ≈ 3 GPU-hours, and it should run *last*, after the free unlocks. Commercially, none of this belongs in the paid product — councillor-keyed data is firewalled a fortiori by the diary precedent, and councils-as-customers (the one live commercial angle) needs zero new council data.

## 2. The five findings that reframe the target

1. **The AFS tail is mostly a code defect, not a data problem.** `extractors/la_afs_extract.py` marks a council `ok` if *any* year lands (line ~724) and builds the camelot-fallback set per council, not per (council, year) (line ~741); `merge_camelot` maps all camelot rows to a single year (line ~767). Result: Clare landed 2019 via fitz, so its five failing *digital* years never reach camelot; `la_afs_camelot_rows.json` holds only 8 DLR rows ever. ~25–28 cached born-digital council-years (Clare, DLR, Fingal, Monaghan, Kildare, Sligo-revenue, Wicklow, Meath-2020) convert with **zero scraping and zero OCR**.
2. **The "Playwright/not-located" blockers are stale.** Carlow, Cavan, Mayo, Roscommon have redesigned sites with plain-anchor AFS PDFs (Mayo: 13 AFS 2013–2025; Cavan/Roscommon verified `.pdf` hrefs; the recorded blocker URLs now 404). Kerry has born-digital AFS 2022–2024 on `docstore.kerrycoco.ie`. Discovery fixes are S-effort each; no browser automation needed.
3. **Cork City named votes were missed.** 26 of 73 already-extracted Cork City minutes files contain named prose divisions (`FOR: Comhairleoirí … (21) / AGAINST: … (9)`), ~149 blocks Jan 2024–Apr 2026, including **named budget-amendment votes** and 54 AGM election votes. A ~1-day prose parser roughly doubles-to-triples the national named-vote record from data already on disk. Laois is verified parseable with the existing Carlow ✓-table parser (hours); Kilkenny is a prose variant (hours); Fingal publishes born-digital ModernGov minutes with named roll-calls (verified on a real Part 8 vote: 18 named FOR / 19 named AGAINST).
4. **DHLGH already consolidates every council's adopted budget.** One publication per year (2019–2026, born-digital, verified: 2026 edition = 77pp with per-LA expenditure *and* income by service division for all 31 councils, plus LPT/rates/ARV tables). One parser over 8 national PDFs yields a `(council, year, division)` **BUDGETED** fact for all 31 LAs — including every AFS-missing council — joinable against `la_afs_divisions` for budget-vs-actual. This kills the "31 budget-book scrapers" cost objection outright (and several books, e.g. Kerry's, are scanned anyway).
5. **The best "anything else" families are central.** LGAS statutory audit reports (opinion + findings + CE responses, per council per year since 2012, templated ~4–8pp born-digital PDFs on assets.gov.ie); Derelict Sites Act annual returns (per-LA levies imposed/collected/outstanding — ~€20.45m uncollected nationally end-2023, 10 councils never levied); Revenue's LPT local-adjustment-factor table (31 rows/yr, official HTML — each row a whole-council money vote). Plus two **zero-ingestion** wins: development-contribution tables minable from the in-house PQ corpus, and s.183 land-disposal notices already sitting in the ingested agenda/minutes corpus (44 files / 164 mentions).

## 3. Where the lenses disagreed, and the ruling

- **Skeptic vs AFS lens on the dead-end.** The skeptic ruled "not reopened — GPU OCR touches ~1 of 9 councils" from the *recorded* miss taxonomy; the AFS lens re-derived the taxonomy from live sites, the bronze cache, and the extractor source, and found the record stale. **The AFS lens wins on facts** — but the skeptic's underlying warning ("dead-end re-litigation via the OCR pretext") is *validated*, not refuted: the reopening case is a code fix plus stale discovery blockers; OCR remains a minor, final-step cost. Both lenses independently agree OCR is not the story.
- **Adopted budgets: DROP (skeptic) vs PURSUE (AFS lens).** The skeptic's two objections were cost (31 scrapers) and fourth-grain contamination. Finding #4 removes the cost objection; the grain objection **stands and is adopted as a hard condition**: the budget fact ships only with its own `value_kind`/never-union caveat (BUDGETED vs SPENT/COMMITTED/accounts), separate chart colour, and copy that never equates adopted budget to outturn. The AFS parser's deliberate auto-rejection of Note-16 budget pages stays untouched.
- **Minutes ambition.** The sweep and minutes lenses converge with the skeptic: named votes are structurally sparse (roll-calls only on request; voice vote is the default). The honest map is **Carlow (live) + Cork City + Laois + Kilkenny + Fingal (+ Waterford AGM roll-calls); South Dublin unknown until its portal is harvested; Dublin City and Galway City verifiably have no named record.** The page must never present the voting record as 31-council. Within that ceiling, the observed roll-calls are uniformly high-salience reserved functions (budget amendments, Part 8s, development-plan submissions, no-confidence motions) — sparse but exactly the votes worth having.
- **Commercial scope.** Unanimous: every councillor-keyed family (votes, s.142 payments, ethics declarations) is **free-civic only, permanently** — the ministerial-diary precedent applies a fortiori (elected-member personal data sold into a bid/lobbying context performs the inference the caveats disclaim). AFS/budget data has no ethics bar but also no paying buyer; it stays civic. The one live commercial angle — councils as *customers* for competition/single-bid benchmarking (BI assessment P2 #14) — is a **sales task on procurement data already in gold** (31 LA publishers, ~€19bn — the gold fact is more complete than memory recorded), requiring zero new council data.

## 4. The target map

### 4a. AFS — tail re-priced (detail: Appendix A matrix)

| Slice | Councils / years | Blocker today | Unlock | Effort |
|---|---|---|---|---|
| Cached digital, camelot-starved | Clare 20–24, DLR 21–25, Fingal 16–24, Monaghan 23–24, Kildare 21/23–25, Sligo-rev 22/24/25, Wicklow 24, Meath 20 (~25–28 council-yrs) | extractor logic (per-council fallback) | per-(council,year) camelot fix + re-run | **S** |
| Discovery-stale | Kerry, Mayo, Carlow, Cavan, Roscommon | stale landings / extensionless hrefs | registry updates + href-regex fix | **S each** |
| Scanned residue | Louth 20–24 (5 files), Wexford 17–22 (check 23/24 first — may be born-digital, per the Waterford/Laois precedent), Kildare ×3 + Sligo ×2 backfill, Leitrim ×3 | scanned | GPU OCR ~800–900pp, reconcile-gated | **M, last** |
| Freshness riders | amalgamated AFS 2024 edition now published; 6 stale covered councils largely fixed by rows above | — | re-run | **S** |

### 4b. Named votes — honest ceiling

| Council | Evidence | Effort | Yield |
|---|---|---|---|
| Cork City | ~149 named prose divisions on disk, incl. budget amendments | ~1 day | ~40–60 divisions/yr × 31 members; backfill to 2014 possible (born-digital) |
| Laois | Carlow-style ✓ tables, fitz-verified | hours | sparse, high-salience |
| Kilkenny | named prose votes verified | hours | sparse |
| Fingal | named roll-calls verified on ModernGov | 1–2 days (scraper shared w/ Dublin City) | Part 8s etc., born-digital |
| Waterford | AGM roll-calls (mayor/SPC elections) | hours | annual alignment signal |
| South Dublin | unknown — prior flag was a false positive (sampled the LG Act 2001 PDF) | 1-day portal scrape to find out | unknown |
| Dublin City, Galway City | **verified no named votes** ("put and carried" / aggregate tallies) | — | none; never promise |

### 4c. New families (sweep verdicts)

| Family | Source shape | Verdict |
|---|---|---|
| LGAS statutory audit reports | central, since 2012, templated born-digital | **PURSUE** (top of new-source list) |
| DHLGH adopted budgets | central, 8 PDFs, all 31 councils | **PURSUE** (fourth-grain guard mandatory) |
| s.142 councillor payments | SDCC + Dublin City = CC-BY CSVs on data.gov.ie (verified endpoints); ~6–10 more as HTML/PDF | **PURSUE phased** — structured publishers first, hard stop before 31-bespoke |
| Derelict Sites annual returns | central DHLGH return | **PURSUE** |
| LPT local adjustment factor | Revenue HTML, 31 rows/yr | **PURSUE** (trivial) |
| s.183 land disposals | already in ingested corpus | **PURSUE as in-house mining pass** (privacy pass on individual counterparties) |
| Development contributions | minable from in-house PQ corpus | **PURSUE as in-house mining pass** |
| Part 15 ethics registers | scanned handwritten forms; only some councils publish | **GATED PILOT only** (Dublin City + Galway County; off-box/GPU OCR; redact home addresses per SIPO precedent) |
| CE s.136 monthly reports; council annual reports; local-election donations; CPOs | narrative / patchy / PII-care | **PARK** |
| FOI logs; vacant-sites levy (superseded by RZLT); RPS; rates-standalone; per-council housing scrapes; audit-committee reports | weak signal or better central source | **DROP** |

## 5. Prioritised plan

**Tier 1 — free unlocks, no OCR, no new infrastructure (order of days, S each):**
1. Camelot fallback fix (per-(council,year) fail-set; `merge_camelot` year mapping) + re-run → ~25–28 council-years from cache; likely adds Kildare, Sligo (revenue), Wicklow as new councils and un-stales Clare/DLR/Fingal/Monaghan.
2. Discovery fixes: Kerry direct URLs; Mayo extensionless hrefs; Carlow/Cavan/Roscommon new landings → up to 5 more councils; 31/31 in reach.
3. Cork City named-vote parser + Laois (Carlow parser) + Kilkenny variant; fix the coverage-CSV tiers and the page's "currently Carlow" copy; fix the 8 malformed `meeting_date` rows and garbled Louth agenda rows in gold.
4. LPT LAF extractor; amalgamated-AFS 2024 refresh.

**Tier 2 — central corpora (M each):**
5. DHLGH adopted-budgets parser → `la_budget_divisions` (all 31 × 2019–2026 × division), fourth-grain caveat mandatory.
6. LGAS audit-report ingestion (opinion + findings per council-year; enumerate via per-year collection pages — the index page renders client-side; gov.ie fetch is local-box-only per the WAF memory).
7. ModernGov scraper (meetings.fingal.ie + councilmeetings.dublincity.ie, shared code) → Fingal named votes + Dublin City's 13-year agenda/minutes/s.183-item corpus.
8. s.142 phase 1 (SDCC + Dublin City CSVs) into the existing Pay card, with an honest "N of 31" coverage line.
9. Derelict Sites annual returns.

**Tier 3 — the bounded OCR batch (only after Tiers 1–2; single-process GPU, off-venv at c:/tmp, server detector, reconcile gates):**
10. Scanned AFS residue (~800–900pp; check Wexford 2023/24 born-digital first).
11. Non-book scanned minutes (957pp ≈ 1.6 GPU-hrs: Galway ×2, Leitrim, Westmeath) — yields aggregate tallies, s.183 notices, resolutions; **not** named votes.
12. Louth current signed minutes as a small rolling job (~120pp/yr).

**Tier 4 — gated/parked:** Part 15 two-council pilot; s.183 extractor privacy pass; development-contribution PQ mining; South Dublin portal probe; Cork City vote backfill to 2014.

## 6. Explicitly do NOT

- OCR the Louth minute-books (986pp, 150MB files, handwriting-era = HTR, near-zero current-accountability value).
- Build Playwright infrastructure for the "viewer" councils (blocker no longer exists) or 31 budget-book scrapers (superseded by the DHLGH publication).
- Promise or imply 31-council named-vote coverage; never present Dublin City as having named votes.
- Rebuild NOAC (standing rule), scrape localauthorityfinances.com (cross-validation reference only), or OCR pre-2016 archives.
- Sum/union across the four money grains (payments/PO, AFS revenue, AFS capital, adopted budget) — the new BUDGETED grain needs its own guard before it ships.
- Trust `minutes_sources.csv` roll-call flags or the current coverage JSON reasons — both contain verified errors; re-derive from documents.
- Put any councillor-keyed data (votes, s.142, ethics) anywhere near the paid product.

## 7. Commercial verdict (BI spinout)

County-council *data* targeting contributes nothing the paid product needs: councils-as-buyers is already complete in gold (31 LA publishers, ~€19bn; the paid supplier-dossier sample contains zero council references and is blocked on nothing here). Councils-as-*customers* — the assessment's P2 #14 "possibly highest-WTP segment" — is live but is a **sales motion**: fold one council/OGP/LGMA contact into the P0 pre-sell conversations, armed with a one-off hand-built competition/single-bid benchmark from existing views. Ingest nothing new for it. The BI pre-sell clock outranks all civic council work; Tier 1 above fits the skeptic's ~3-focused-days envelope whenever that capacity exists.

## 8. Owner decisions required before execution

1. **Reverse the AFS dead-end flag?** The 2026-06-19 call was sound on the evidence it had; the evidence has changed (§2.1–2.2). Tier-1 items 1–2 are the test: if the camelot fix + discovery fixes deliver as priced, the flag was stale; if not, re-park permanently.
2. **Admit a fourth money grain?** `la_budget_divisions` ships only with the never-union guard (§3, §6).
3. **Privacy passes:** s.183 individual counterparties; Part 15 home-address redaction; Kerry's own "edited for Data Protection" note confirms councils treat minutes as PII-bearing.
4. **Coverage-record hygiene:** `la_afs_coverage.json` self-contradicts and `UNAVAILABLE_REASON` hardcodes solved blockers — fix alongside Tier 1 so the UI honesty notes stay true.

## 9. The one move

If only one thing is done: **the camelot per-(council,year) fix + re-run** — one small refactor converts ~25–28 already-cached, born-digital council-years, likely takes AFS from 22 toward 27+ councils, and costs no scraping, no OCR, no new infrastructure. If a second: the **Cork City named-vote parser** — the largest single expansion of the councillor voting record in the project, from text already on disk.

---

# Appendix — the four lens reports (verbatim)

## Appendix A — AFS / County-Council Budgets lens

### (i) Definitive coverage matrix

Ground truth = parquet queries (2026-07-09) on `data/silver/parquet/la_afs_divisions.parquet` (776 rows, 22 councils, 97 council-years) and `la_afs_capital_divisions.parquet` (782 rows, 22 councils). The two 22-council sets differ: **Sligo is in capital only (2021–2025); Clare is in revenue only (2019)**.

| Council | Revenue years | Capital years | Latest rev. | Recorded blocker (coverage JSON) | Blocker TODAY (evidence) |
|---|---|---|---|---|---|
| South Dublin | 2016–2025 (10) | 2016–2025 | 2025 | — | none |
| Westmeath | 2017–2025 (8) | 2017–2025 | 2025 | — | none |
| Dublin City | 2017–2025 (6) | 2016–2025 | 2025 | — | none |
| Galway City | 2021–2025 (5) | 2021–2025 | 2025 | — | none |
| Laois | 2025 (1) | 2025 | 2025 | (was "scanned") | solved — 2025 born-digital; backfill = OCR |
| Waterford | 2025 (1) | 2025 | 2025 | (was "scanned") | solved for 2025; 2024 not even in bronze |
| Offaly | 2022–2025 (4) | 2022–2025 | 2025 | — | none |
| Limerick | 2017–2024 (8) | 2017–2024 | 2024 | — | none |
| Tipperary | 2016–2024 (8) | 2016–2024 | 2024 | — | none |
| Meath | 2016–2024 (7, gap 2020) | 2016–2024 | 2024 | — | 2020 cached, digital, fitz-fails |
| Galway County | 2017–2024 (7) | 2017–2024 | 2024 | — | none |
| Cork City | 2016–2024 (6) | 2016–2024 | 2024 | — | none |
| Donegal | 2019–2024 (6) | 2019–2024 | 2024 | — | none |
| Cork County | 2021–2024 (4) | 2020–2024 | 2024 | — | none |
| Leitrim | 2023–2024 (2) | 2023–2024 | 2024 | — | 3 bronze files misnamed 2026/27/28, SCANNED |
| Longford | 2023–2024 (2) | 2023–2024 | 2024 | — | none material |
| Kilkenny | 2023 (1) | 2023 | 2023 | — | bronze `2024.pdf` is a 3-page stub; real 2024 AFS needs discovery |
| Monaghan | 2016–2022 (6) | 2016–2022 | 2022 | — | 2023+2024 cached, DIGITAL, fitz-fail |
| Fingal | 2022 (1) | 2022 | 2022 | — | 2016–2024 cached, DIGITAL, fitz-fail |
| DLR | 2020 (1, camelot) | 2020 | 2020 | — | 2021–2025 cached, DIGITAL, fitz-fail |
| Clare | 2019 (1); **no capital** | — | 2019 | — | 2020–2024 cached, DIGITAL, fitz-fail |
| Louth | 2016–2017 (2) | 2016–2017 | 2017 | "unusual_layout" | 2020–2024 cached, **SCANNED**; 2025 file = 17pp digital summary |
| Sligo | **none** | 2021–2025 | — | "unavailable" | bronze 2016–2025 cached; 2022/24/25 DIGITAL (capital parses, revenue I&E fitz-fails); 2018/2020 scanned |
| Kildare | **none** | — | — | "unavailable" | bronze 2018–2025 cached; 2021/23/24/25 DIGITAL fitz-fail; 2018/20/22 scanned |
| Wicklow | **none** | — | — | "unusual_layout" | 2024 cached, DIGITAL 48pp, fitz-fails → camelot candidate |
| Wexford | **none** | — | — | "scanned_image" | 2017–2022 cached, all SCANNED; post-2022 publication unverified |
| Kerry | **none** | — | — | "not_located" | **SOLVED**: direct PDFs at docstore.kerrycoco.ie (AFS 2022/2023/2024; 2024 verified born-digital, 51pp, standard Table A) |
| Mayo | **none** | — | — | "interactive_viewer" | **FALSE**: 13 AFS 2013–2025 as plain HTML `getattachment/GUID/attachment.aspx` anchors (2025 verified born-digital, 43pp, Table A). Harvester requires `.pdf`-style hrefs |
| Carlow | **none** | — | — | "interactive_viewer" | **STALE**: carlow.ie now has an AFS publication page + `carlow.ie/media/N/download` links (file content not downloaded/verified) |
| Cavan | **none** | — | — | "interactive_viewer" | **STALE**: redesigned site has plain `.pdf` AFS links (`cavancoco.ie/file-library/.../annual-financial-statement-2021.pdf`); old `financial-statements.htm?StructureID_str=` URL now 404s |
| Roscommon | **none** | — | — | "interactive_viewer" | **STALE**: plain `.pdf` links (`roscommoncoco.ie/en/download-it/finance-publications/annual_financial_statement/...2023.pdf`) |

Freshness: 7 councils at 2025, 9 at 2024, 6 stale (Monaghan/Fingal 2022, Kilkenny 2023, DLR 2020, Clare 2019, Louth 2017). Confirms the "only ~7 reached 2025" memory.

**Coverage-record hygiene problems** (the UI reads this JSON): `data/_meta/la_afs_coverage.json` contradicts itself — top-level `coverage_by_council` labels Kildare/Sligo "unavailable"/"not yet available" while its own `by_council` refresh section shows their PDFs were found and failed *parsing* (`no-IE-page`, `too-short(15pp)`, `no-reconcile`); `UNAVAILABLE_REASON` in `extractors/la_afs_extract.py` (line ~349) still hardcodes Waterford and Laois as "scanned" though both now ship 2025 data. All four "interactive_viewer" reasons are outdated. Leitrim's registry harvests files named `...2028.pdf` (site misnames files; those three are scanned docs of unknown true year).

### (ii) The tail, re-priced

**The central discovery of this audit: the bottleneck is not scraping, OCR, or Playwright — it is one line of extractor logic.** `extractors/la_afs_extract.py:724`: `stat["status"] = "ok" if all_rows else last_status` (→ 'ok' if ANY year landed → no camelot retry), and line 741 builds the camelot fail-set per *council*, not per *year*; `merge_camelot` (line ~767) additionally maps all camelot rows to a single year derived from `files[0]`. Consequence: Clare landed 2019 via fitz → status "ok" → its five failing *digital* years never reach camelot. `data/_meta/la_afs_camelot_rows.json` holds only 8 DLR rows — camelot has only ever run for zero-year councils.

Per-council re-pricing (bronze census + fitz text-layer probe, all verified this session):

| Council | Blocker class today | Unlock | Effort | Worth it? |
|---|---|---|---|---|
| Clare 2020–24 | (d) layout, all DIGITAL, cached | per-year camelot | S | YES — 5 yrs, restores capital too |
| DLR 2021–25 | (d) layout, DIGITAL, cached | per-year camelot (camelot already proven on DLR) | S | YES — 5 yrs incl. 2025 |
| Fingal 2016–24 | (d) layout, DIGITAL, cached | per-year camelot | S | YES — up to 7 yrs |
| Monaghan 2023–24 | (d) layout, DIGITAL, cached | per-year camelot | S | YES |
| Kildare 2021/23/24/25 | (d) layout, DIGITAL, cached | per-year camelot | S | YES — new council |
| Sligo 2022/24/25 (+2016) | (d) revenue-layout, DIGITAL (capital parses!) | per-year camelot | S | YES — closes rev/capital asymmetry |
| Wicklow 2024 | (d) layout, DIGITAL, cached | per-year camelot | S | YES — new council |
| Meath 2020 | (d) layout, DIGITAL | per-year camelot | S | marginal (gap-fill) |
| Kerry 2022–24 | (c) discovery — SOLVED | add `direct` docstore URLs | S | YES — new council, born-digital |
| Mayo 2013–25 | (c) discovery — extensionless hrefs | accept `getattachment` links (or `direct` list) | S | YES — new council, ~10 yrs |
| Carlow | (c) discovery — `media/N/download` hrefs | same regex fix + verify content | S–M | YES (content unverified) |
| Cavan | (c) discovery — stale census URL | update landing to redesigned site | S–M | YES |
| Roscommon | (c) discovery — stale census URL | update landing | S–M | YES |
| Kilkenny 2024 | (c) discovery — stub PDF cached | find real 2024 AFS | S | marginal |
| Louth 2020–24 | (a) SCANNED (5 files, 38–72pp) | GPU OCR | M | YES — 7-yr freshness jump |
| Wexford 2017–22 | (a) SCANNED (all cached) | first check if 2023/24 AFS is born-digital; else GPU OCR | M | YES — last fully-dark county |
| Kildare 2018/20/22 | (a) SCANNED | GPU OCR | M | backfill only |
| Sligo 2018/2020 | (a) SCANNED | GPU OCR | M | backfill only |
| Leitrim "2026–28" | (a) SCANNED, misnamed | GPU OCR + year from content | M | low priority |

Scanned queue total: ~17–19 documents ≈ 800–900 pages — well within the proven on-box GPU envelope (PP-OCRv5/RTX 3060, single-process, outside project venv, per the 2026-06-26 proof; use the server detector — the mobile detector garbles €-tables per memory).

### (iii) Verdict on the dead-end call

**Reopen — and the strongest reason is not GPU OCR.** The 2026-06-19 dead-end call implicitly priced the tail as "9 bespoke scrapes + OCR + Playwright". Evidence now shows:
1. ~25–28 council-years of *already-downloaded, born-digital* PDFs fail only because the camelot fallback is per-council/single-year. A small refactor (fail-set keyed on (council, year); camelot script already accepts slugs; per-year row mapping) converts them with zero scraping and zero OCR.
2. The Playwright-blocked class (Carlow/Cavan/Mayo/Roscommon) is empty — all four now expose plain anchors; two verified born-digital.
3. Kerry, "not located", has direct PDFs sitting on an open docstore.
4. GPU OCR then covers a modest, bounded scanned residue (mainly Louth + Wexford + backfills), no longer the dominant cost.

Post-fix ceiling: 31/31 councils with ≥1 recent year is realistic; the genuinely hard residue is historic backfill of scanned years, which can stay dead. Sequencing matters: run the camelot fix and discovery fixes *first*; re-check Wexford's 2023/24 publication before OCR-ing its archive (the Waterford/Laois precedent shows scanned councils go born-digital on new publications).

### (iv) Adopted-budgets corpus

**Do not build 31 council-book scrapers — DHLGH already consolidates the adopted budgets.** Verified this session: gov.ie collection "local-authority-budgets" carries one publication per year, 2019–2026 (`assets.gov.ie/static/documents/82d163b3/FINAL_Local_Authority_Budget_Publication_2026.docx.pdf` and siblings; 2024/2025/2019 HEAD-checked at 1.9–2.8 MB). The 2026 edition (downloaded, inspected with fitz): 77pp, born-digital, containing (a) per-LA revenue expenditure/income/LPT/commercial-rates/ARV table for all 31 councils (pp 17–18), and (b) **per-LA expenditure AND income by service division at sub-service granularity** for all 31 councils (pp 21–73, Divisions A–H). One department-produced layout per year → **one parser over 8 national PDFs** yields a `(council, year, division)` adopted-budget fact for all 31 LAs — including the 8 AFS-missing councils — that joins directly against `la_afs_divisions` for budget-vs-actual. Risks: layout drift in pre-2022 editions (unverified); columnar text flow needs positional (x-coord) extraction like the existing geom parser; it is a **fourth money grain** (BUDGETED) — needs its own never-union caveat vs the three existing grains.
Council budget books remain the fallback and are *not* uniformly born-digital (Kerry's Budget 2026 book: 34pp, 0 text chars — scanned; Mayo publishes Adopted Budgets 2019–2026 as plain attachments). data.gov.ie has per-table budget CSVs for a few councils only (Fingal Tables B/D/E; smartdublin) — validation aids, not a corpus.

### (v) LGAS + central sources

- **LGAS statutory audit reports — accountability gold, centrally hosted.** Per-council per-year "Statutory Audit Report to the Members of X" PDFs on assets.gov.ie, organised in per-year gov.ie collections ("Audit Reports 2023", "audit-reports-since-2012"), e.g. Carlow 2023 at `assets.gov.ie/246980/a70f34e1-93cc-4d46-8ab9-b0fe8c4082fe.pdf`; 2024 reports already appearing (Cavan 2024 publication page). ~31/yr since 2012 ≈ ~370 short born-digital docs: audit opinion + findings + CE responses. Also a yearly "Overview of the Work of the LGAS" synthesis. Caveat: the collection *listing* page renders its document list client-side (saved HTML contains no per-report links) — enumeration needs the per-year collection pages or the gov.ie search endpoint; direct asset fetches work.
- **gov.ie fetchability from this box:** WebFetch (datacenter) 403s; **browser-UA curl from this residential box returns 200** on all gov.ie pages and assets tried (consistent with `GOVIE_HEADERS` memory). Cloud refresh of these sources would fail; local refresh is fine.
- **Amalgamated AFS:** datacatalogue.gov.ie confirms Open Data: No / no downloads (matches extractor docstring), but the gov.ie AFS collection now lists `FINAL_Amalgamated_AFS_2024_web.pdf` — a freshness update available for the existing `afs_amalgamated_divisions` fact.
- **localauthorityfinances.com** (not .ie): Dr Gerard Turley & Stephen McNena, University of Galway; per-council HTML pages built on the 2026 Budget data for all 31 LAs; TLS cert currently expired; no bulk download found. Treat as an independent *cross-validation* reference for the budget fact (their figures derive from the same DHLGH data), not as a source.

### (vi) Ranked recommendations

1. **Per-year camelot fallback fix** in `extractors/la_afs_extract.py` (fail-set per (council, year); fix `merge_camelot`'s single-year mapping) + re-run. Effort S. Value: ~25–28 council-years from cached digital PDFs; likely adds Kildare, Sligo (revenue), Wicklow as new councils and un-stales Clare/DLR/Fingal/Monaghan. Highest value-per-effort in this entire lens.
2. **Discovery fixes: Kerry (direct docstore URLs), Mayo (+Carlow/Cavan/Roscommon) — accept extensionless CMS hrefs / update stale landings.** Effort S each. Value: up to 5 new councils, mostly born-digital.
3. **DHLGH Local Authority Budgets publication parser** → new `la_budget_divisions` fact, all-31 × 2019–2026 × division, enabling budget-vs-actual per council per division. Effort M. Must carry a fourth-grain never-union caveat.
4. **LGAS statutory audit report ingestion** (opinion + findings per council-year, since 2012). Effort M. Pairs with NOAC scorecards already live on the local_government page.
5. **Refresh amalgamated AFS with the 2024 edition.** Effort S.
6. **GPU-OCR scanned batch** (Louth 2020–24, Wexford, Kildare 3, Sligo 2, Leitrim 3 ≈ 800–900pp) — only after 1–2, and after checking whether Wexford 2023/24 is born-digital. Owner rails apply: single-process, off-venv at c:/tmp, server detector, reconcile gate on OCR output. Effort M.

**Explicitly NOT do:** 31 bespoke council budget-book scrapers (superseded by DHLGH pub; some books are scanned); Playwright infrastructure for the "viewer" councils (blocker no longer exists); rebuilding NOAC (already promoted); scraping localauthorityfinances.com; OCR-ing deep historic archives (pre-2016) — diminishing returns stands *there*; any reconciliation across the AFS revenue / AFS capital / payments / budget grains.

Key evidence files: `data/_meta/la_afs_coverage.json`, `data/_meta/la_afs_capital_coverage.json`, `data/_meta/la_afs_camelot_rows.json`, `extractors/la_afs_extract.py` (lines 724, 741, 767), bronze cache `data/bronze/pdfs/la_afs/<slug>/`, scratchpad samples (`kerry_afs2024.pdf`, `kerry_budget2026.pdf`, `mayo_afs2025.pdf`, `la_budget_pub_2026.pdf`, `govie_*.html`).

## Appendix B — Minutes & Agendas lens

### (i) Corpus state today (verified)

Gold CSVs (`data/_meta/`), row-counted via pandas:

| File | Rows | Verified detail |
|---|---|---|
| `la_councillors.csv` | 916 / 31 councils | cols: local_authority, lea, name, party, status, source |
| `la_council_meeting_coverage.csv` | 31 | tier + has_votes flags — **materially wrong, see (ii)** |
| `la_councillor_votes.csv` | 185 / Carlow only | member, meeting_date, motion, vote |
| `la_meeting_agendas.csv` | 221 / 31 councils | Wicklow 25, Galway Co 23, Louth 7, Galway City 3, rest 6 each |
| `la_standing_orders.csv` | 8 councils | matches page claim "8 of 31" |

Page (`utility/pages_code/your_councillors.py`) claims match gold: votes "currently Carlow", SO ~8/31. **But the page now undersells reality** — see (ii).

**Gold DQ warts found:** 8 rows in `la_meeting_agendas.csv` have filename fragments as `meeting_date` (all 7 Louth rows = `signed-minutes-council-m…`; 1 Waterford = `1_draft_plenary_minute`). Louth agenda text is also fragmentary. Small fix, should be done at next touch.

Sandbox (`pipeline_sandbox/council_minutes/`): 191 corpus .txt across 19 council dirs (Cork City largest at 73); `quarantine/quarantine.jsonl` 157 rows, of which 73 `scanned_not_ocr` totalling **1,943 recorded pages**. On-box GPU OCR output (`c:/tmp/gpu_ocr/scanned_agendas.jsonl`) = 55 docs: Wicklow 26, Galway County 25, Galway City **only 4** — the Galway City backlog is mostly untouched.

### (ii) Named-vote expansion — the coverage CSV tiering is wrong in both directions

**The single biggest finding of this lens: Cork City was missed.** It is tiered `proposer_seconder / has_votes=False`, but its born-digital minutes (already extracted, on disk) contain full named prose votes:

> `FOR: Comhairleoirí J. Maher, J. Kavanagh, … (21)` / `AGAINST: Comhairleoirí T. Tynan, … (9)` / `ABSTAIN: (0)`
> (`corpus/cork_city/minutes_council_meeting_08_09_25_pdf.txt` lines 1640–1650)

Per-council evidence:

| Council | Evidence | Blocker | Effort | Expected yield |
|---|---|---|---|---|
| **Cork City** | Named FOR/AGAINST blocks in **26 of 73** corpus files, **~149 blocks** (Jan 2024–Apr 2026). Includes **named budget-amendment votes** (`051_minutes_budget_meeting_04122024_pdf.txt`: "A vote was taken where there appeared as follows: FOR: … (23) AGAINST: … (3)") and 54 in the Jun 2024 AGM (Lord Mayor/committee elections) | None — text already on disk | ~1 day: prose-list regex parser + roster name-fold | **~40–60 divisions/yr × 31 members ≈ 1,200–1,900 member-vote rows/yr**, backfillable to Jan 2024 immediately. Biggest single prize found |
| **Fingal** | **Confirmed named roll-calls** — downloaded and parsed the real Feb 2026 "Printed minutes" from ModernGov (`meetings.fingal.ie/documents/g6247/Printed minutes…pdf?T=1`): Part 8 Fortlawn housing vote, FOR 18 councillors named / AGAINST 19 named | Harvest — minutes live on ModernGov (CId=129); sandbox only has agenda frontsheets | 1–2 days incl. the ModernGov scraper (shared with Dublin City) | ~40 members; roll-calls on contested Part 8s etc. Born-digital |
| **Laois** | **Confirmed Carlow-style ✓ tables.** Ran `fitz.find_tables` on `corpus/laois/minutes_council_apr2026.pdf`: `COUNCILLOR | FOR | AGAINST | ABSENT` grid, ✓ marks extract (`['Paddy','BRACKEN','','✓']`) | None — Carlow parser generalises; handle split FIRST NAME/SURNAME header | Hours | Sparse: 2 roll-calls in Apr 2026 alone (live-streaming NoM, no-confidence statement); ~10–20 divisions/yr × 19 members |
| **Kilkenny** | Prose named votes: "Four (4) voted in favour: Cllrs. Maria Dollard, … / Thirteen (13) voted against: …" (`minutes_february_plenary_meeting_17022025_pdf.txt`); 3 roll-calls found Feb–Mar 2025 | None — prose parser variant | Hours | Sparse but high-salience (standing-orders fights) |
| **South Dublin** | **FALSE POSITIVE.** The `named_rollcall=True` flag came from sampling the *Local Government Act 2001 PDF* (`minutes_sources.csv` sample_url; corpus dir contains only `local_government_act_2001_pdf.txt`). No SDCC minutes harvested | Custom portal `meetings.southdublin.ie` (1,601 meetings, `/Home/Agenda/{id}` HTML, `/Home/ViewReply/{id}` docs); named-vote practice **unknown** until harvested | 1 day scraper | Unknown |
| **Galway City** | **NOT named.** Post-OCR minutes record per-motion aggregate tallies only: "In Favour: 16 Against: 0 Abstain: 0 Absent: 2" (show of hands) | — | — | No per-councillor record exists; aggregates are still a contested-ness signal |
| **Dublin City** | **NOT named.** Parsed real minutes PDFs: May 2026 (88pp) and Nov 2025 (76pp) — 34× "put and carried", **zero** divisions/roll-calls | — | — | The biggest council does not publish named votes in monthly minutes |
| **Waterford** (bonus) | AGM roll-calls on Mayor/Deputy Mayor/SPC-chair elections with per-councillor `Name: Cllr X` lists (`1d_minutes_of_plenary_council_agm_21st_june_2024_pdf.txt`) | Own parser format | Hours | Once-a-year political-alignment signal (who backed whom for Mayor) |

**What gets roll-called (observed):** budget amendments (Cork City), Part 8 housing approvals (Fingal), development-plan submissions (Carlow), standing-orders amendments (Kilkenny), no-confidence/contentious NoMs (Laois), AGM elections (Cork City, Waterford). Sparse but uniformly high-salience reserved functions — confirms the brief's premise.

**Realistic named-vote map: Carlow (live) + Cork City + Fingal + Laois + Kilkenny (+Waterford AGMs), SDCC unknown. Not 31/31, and not Dublin City.**

### (iii) Scanned-backlog sizing & OCR economics

From `quarantine.jsonl` (n_pages field): 73 docs / 1,943pp — Galway City 40/510pp, Louth 7/**986pp**, Galway County 11/242pp, Leitrim 9/171pp, Kilkenny 2/14pp, Laois 2/11pp, Westmeath 2/9pp.

- **Excluding Louth books: 957pp ≈ 1.6 GPU-hours** at ~6s/page (proven on-box, RTX 3060, one process, from c:/tmp). Trivial.
- **Galway City full archive**: goes back to **2014**, ~400+ docs listed (harvester found 112 PDFs; sampled avg ~4–8pp) → roughly 700–2,500pp ≈ **1–4 GPU-hours** for a decade.
- **Marginal value of full minutes text** (agendas already in gold): per-motion aggregate vote tallies (In Favour/Against counts — Galway City's format), motions+proposers, attendance, s183 disposals, substantive resolutions. **Not named votes** — set expectations accordingly.
- **Louth reclassified**: current signed minutes **do exist** (`louthcoco.ie/media/…/signed-minutes-*.pdf`, 2025–26) but are scanned — verified Jan 2026 = 11pp, **0 text chars**. That's a small rolling OCR job (~120pp/yr), currently garbled in gold. The 986pp backlog is the **minute-books digital archive** (`apps.louthcoco.ie/DUDCMinuteBooks/…`, single 150MB PDFs, last-modified 2020) — historical, likely handwriting-era, an HTR problem not an OCR problem, and low current-accountability value. The "honest gap" applies to the *books*, not to current Louth minutes — this distinction was blurred in the coverage story.
- Wicklow 2026: already done (25pp OCR'd, 25 agenda rows in gold).

### (iv) ModernGov / Dublin City

- `councilmeetings.dublincity.ie` = confirmed ModernGov: `ieDocHome.aspx` lists 50+ committees; Monthly Council = **CId 142**; `ieListMeetings.aspx?CId=142&Year=YYYY` works **2014–2026**; minutes at `documents/g{MId}/Public minutes ….pdf?T=11` (the `?T=` suffix is required — bare URL 404s/misroutes). `mgWebService.asmx` is 404 (disabled); plain HTML scraping is easy.
- **New: Fingal is also ModernGov** (`meetings.fingal.ie`, council CId=129) — the sandbox's mystery "frontsheet" PDFs came from there. One scraper covers both councils. Fingal publishes born-digital "Printed minutes" **with named votes** (verified above).
- DLR = Civica CMIS (`ecouncil.dlrcoco.ie`), separate lane. SDCC = own portal (above).
- **Dublin City prize re-scoped**: it is *not* the named-vote prize (evidence: two full minutes parsed, zero divisions). It *is* the largest born-digital corpus in the country — 13 years of agendas/minutes plus **per-item PDFs**: "Monthly Management Report", Part 8 reports, and s183-type disposals as individual documents (e.g. `documents/s52889/103. Proposed grant of a 5-year lease at Rivermount Hall.pdf` on MId=5836). Machine-harvestable accountability content, just not votes.

### (v) What else lives in minutes (corpus-evidenced, ranked value/effort)

1. **Section 183 land disposals** — 44 files / 164 mentions. Structured statutory notices with parcel, area, counterparty (Donegal: "0.0050 hectares … Milford … to Mr. Garret Orr"; lease to Uisce Éireann). Money+property accountability; regex-extractable; DCC serves them as standalone PDFs. **Best value/effort. Privacy caveat**: disposals to named private individuals → apply the personal-privacy domain rule before surfacing.
2. **Budget/AFS adoption** — 20 files AFS, 23 budget-adoption; in Cork City these come with **named votes on amendments** (direct tie to the AFS fact and council-spending lanes).
3. **Part 8 approvals** — 63 files; they're what triggers roll-calls (Fingal); ties to the planning corpus.
4. **Conference/travel approvals** — 100 files ("Attendance at Conference, Seminar or Event" as standing agenda items); ties to S142 expenses.
5. **CE/Management reports** — 67 files + DCC monthly PDFs; bulk text, moderate value, cheap to index once harvested.

Also noted: Kerry explicitly states its minutes are "edited for Data Protection purposes" before web publication — councils themselves treat minutes as PII-bearing.

### (vi) Historical depth

Available: DCC ModernGov 2014+, Galway City archive 2014+ (~400 docs), Kerry corpus already holds 2017 minutes, Louth books go back ~a century (HTR territory). **Verdict: current-term-first (2024+).** The one worthwhile backfill is **Cork City named votes to ~2014** (a decade of budget-amendment divisions, born-digital, cheap). Do not OCR deep scanned history — cost without product.

### (vii) Ranked recommendations

1. **Cork City named-vote parser** (prose FOR/AGAINST blocks; data already on disk) — ~1 day for ~150 divisions incl. budget amendments; then fix the coverage CSV tier and the page's "currently Carlow" copy.
2. **Laois via the existing Carlow table parser** (verified parseable) — hours.
3. **Kilkenny prose variant** — hours.
4. **ModernGov scraper for meetings.fingal.ie + councilmeetings.dublincity.ie** (shared code): Fingal named votes + DCC 13-year agenda/minutes/s183-item corpus.
5. **Gold DQ fixes**: 8 malformed meeting_date rows; garbled Louth agendas; wrong tier flags (Cork City, and false-positive provenance on South Dublin/Fingal flags).
6. **OCR the 957 non-book scanned pages** (~1.6 GPU-hrs) for Galway×2/Leitrim/Westmeath full minutes — yields aggregate tallies, s183, resolutions.
7. **s183 disposal extractor** across the clean corpus — new accountability lane.
8. **Louth current signed minutes**: small rolling OCR (~120pp/yr).

**Do NOT:**
- OCR the Louth minute books (986pp/150MB files; HTR problem; near-zero current-accountability value).
- Build/promise named-vote coverage for Dublin City or Galway City — their minutes verifiably don't contain named votes ("put and carried" / aggregate tallies). Never present the councillor voting record as 31-council.
- Trust `minutes_sources.csv` `named_rollcall` flags (SD/Fingal flags came from the LG Act 2001 and standing-orders PDFs respectively; Cork City was missed) — re-derive from actual minutes.
- Surface s183 individual-counterparty names or votes-of-sympathy content without the privacy pass.

Key files: `pipeline_sandbox/council_minutes/{FINDINGS.md,QUALITY_ASSESSMENT_ULTRA.md,quarantine/quarantine.jsonl,corpus/}`, `data/_meta/la_*.csv`, `utility/pages_code/your_councillors.py`, `c:/tmp/gpu_ocr/scanned_agendas.jsonl`; verification downloads in scratchpad (`fingal_min3.pdf`, `dcc_min3.pdf`, `dcc_min_nov25.pdf`, `louth_jan26.pdf`).

## Appendix C — Wider council-source sweep

**Fetch environment notes:** `gov.ie` returns HTTP 403 to the datacenter fetcher (known WAF; `GOVIE_HEADERS` works locally but NOT from datacenter IPs). `data.gov.ie`, `revenue.ie`, and council sites all fetch fine.

### 1. Section 142 registers — payments/expenses to councillors
- Statutory register of every payment (representational payment, annual allowance, travel/subsistence, conference & training, chair/mayor allowances) to each named councillor. Basis: LGA 2001 s.142 + S.I. 37/2010; rates set centrally by S.I. 313/2021 as amended by S.I. 283/2024.
- **No central collection exists.** Structured beachhead: SDCC publishes quarterly S142 registers as **CC-BY 4.0 CSV via ArcGIS Hub** (verified: data.gov.ie "s142-register-q1-2024-sdcc" with CSV endpoint; fields include representational payment, annual allowance, conference expenses, **meeting attendance**, mayor/SPC-chair allowances). **Dublin City** also on data.gov.ie. Others = web pages/PDFs: DLR, Waterford, Meath, Fingal, Cork City (quarterly, foreign-travel broken out).
- Value **HIGH** (named money to named politicians; joins the 916-councillor roster; carries an attendance signal). Effort S (SDCC+DCC CSV) → M (~6–10 HTML/PDF publishers) → L (31). **Verdict: PURSUE phased — never 31-at-once.**

### 2. Part 15 LGA 2001 ethics registers
- Annual declarations of interests per councillor (land, business, directorships, contracts with the LA) — the local SIPO-equivalent (LGA 2001 s.171 et seq.). No central publication; online publication patchy: Dublin City publishes **scanned** declarations; Galway County per-councillor filled Form A PDFs; Cork City/County + Galway City = inspection in person only.
- Value **HIGH** (councillor property interests × planning/zoning votes is the killer cross-ref) but scanned handwritten forms → OCR; PII care (home addresses → redact per SIPO donations precedent). **Verdict: PURSUE as a gated pilot** (Dublin City + Galway County only).

### 3. CE monthly management reports (s.136)
- Widely published born-digital 20–60pp narrative PDFs (Fingal, Louth, Meath, Cork City, SDCC, DCC). Quantitative content better sourced centrally; unique content is narrative — poor fit for the data-anchored provenance rule. **Verdict: PARK.**

### 4. Adopted annual budgets
- Covered by the AFS lens; central DHLGH "Local Government Finance"/local-authority-budgets publications are the source (also carries the ARV answer for family 6).

### 5. Development contribution schemes + collections (s.48 PDA 2000)
- **No central dataset** (data.gov.ie: 97 results, all spatial). But per-LA tables recur in **PQ answers** (2023–25 waiver-scheme stream) — and the project holds the PQ corpus in-house. The ingested AFS divisions do **not** capture this money lane. Value **HIGH** (developers → councils, live political story). **Verdict: PURSUE via in-house PQ-corpus mining first; annual reports (s.48(15) disclosure) as fallback.**

### 6. Commercial rates / LPT LAF
- Rates: income already in AFS; collection % in NOAC; ARVs in budget books/DHLGH pub → **DROP standalone**.
- **LPT local adjustment factor: PURSUE** — Revenue publishes an official HTML table for all 31 LAs (verified: e.g. Carlow +15%, Dublin City 0%, DLR −15% for 2026); prior years via archived Revenue pages/Wayback. 31 rows/yr, effort S, each row a whole-council money vote.

### 7. LGAS statutory audit reports
- **CENTRAL** (gov.ie DHLGH: all LA audit reports since 2012 + VFM reports; per-council mirrors as fallback). ~4–8pp templated born-digital PDFs: opinion → findings → CE responses. ~400 files. Value **HIGH** — the accountability layer the AFS numbers lack; "qualified/emphasis-of-matter per council-year" is a clean derived metric. Effort M (WAF → local-run with GOVIE_HEADERS or mirrors). **Verdict: PURSUE — top of list.**

### 8. Audit-committee & council annual reports
- Audit-committee reports = thin governance narrative → **DROP**. Council annual reports (s.221) = **PARK** (only as the s.48 development-contribution fallback).

### 9. FOI disclosure logs
- Per-council, no central source; inconsistent granularity; no money content; OIC aggregates already pulled in the 06-28 sandbox run. **DROP.**

### 10. Derelict sites + vacant sites
- Derelict: registers per-council (several structured on data.gov.ie) but the money is **CENTRAL** — DHLGH "Annual Returns under the Derelict Sites Act 1990" per LA (sites, levies imposed/collected/outstanding; €20.45m outstanding nationally end-2023; 10 councils have never levied). **PURSUE (central return; spatial registers optional garnish).**
- Vacant sites levy: superseded by Revenue-administered RZLT (from 1 Feb 2025). **DROP.**

### 11. Housing delivery/voids/HAP per council
- Central DHLGH sources dominate (Social Housing Construction Status Report quarterly per-LA CSV/XLSX on data.gov.ie; voids in NOAC; HAP central). **DROP as a council-scrape family.**

### 12. RPS / CPOs / s.183 disposals
- RPS: heritage, not money → **DROP**. s.183: no register anywhere; notices are in the already-ingested agenda corpus → **PARK with in-house mining hook**. CPOs: An Coimisiún Pleanála publishes confirmed CPOs centrally → **PARK**.

### 13. Also found
- **Local election donation/expenditure statements** (1999 Act): filed with each council, not SIPO; patchy, scanned, donor-address PII → **PARK**.
- **Members' allowance rate circulars** (assets.gov.ie/AILG): central validation table for s.142 amounts — free rider on family 1.

### RANKED TOP-5 PURSUE
1. LGAS statutory audit reports (central, templated, pairs 1:1 with AFS).
2. s.142 councillor payments, phased (SDCC + DCC CC-BY CSVs first).
3. Derelict Sites Act annual returns (central DHLGH).
4. LPT local adjustment factor (Revenue HTML).
5. Part 15 ethics registers — gated 2-council pilot (off-box OCR, address redaction).
**Zero-ingestion bonus:** development contributions from the PQ corpus; s.183 disposals from the agenda corpus.

### DROP LIST
FOI logs · vacant sites levy (RZLT) · RPS · rates-standalone · per-council housing scrapes · audit-committee reports.
**PARKED:** CE s.136 reports; council annual reports (s.48 fallback only); local-election donations (PII care); CPO central pickup.
**PII flags:** Part 15 declarations + local-election donations — statutory public registers of public officials/candidates, publishable in principle, but redact home/donor addresses (SIPO precedent).

## Appendix D — Adversarial skeptic (opportunity cost + BI fit)

### (i) Where council data surfaces today, and per-family deltas

**The app has effectively ONE citizen-facing council surface.** Nav "Your Area" shows only **Your Council** (`utility/pages_code/your_council.py`, hub + triptych). The three old pages — `local_government.py` (CE + NOAC + choropleth), `your_councillors.py`, `council_spending.py` — are `visibility="hidden"` but routable (app.py L147–186). Every proposed family lands as a sub-section of a section of one page: none opens a new surface; they decorate an existing one.

Verified current state (gold, not the stale brief numbers): `procurement_payments_fact` carries **31 distinct LA publishers** — 29 councils COMMITTED (~€17.78bn, 160,751 rows) + 2 SPENT (~€1.26bn); councils-as-buyers is essentially complete in gold. AFS 22/31. Councillors: 916 roster, agendas 221 rows, SO 8/31, votes Carlow-only.

| Family | What a citizen would actually see change |
|---|---|
| AFS tail (9 councils) | The teal audited-accounts chart + traceability callout + by-division grid appear on 9 more council dossiers, replacing an honesty note |
| Fresher AFS years | Existing bars extend rightward; honesty improvement, not new capability |
| Adopted budgets | A new planned-vs-actual lane — a **fourth money grain** on a page already policing three |
| Named votes beyond Carlow | Best case ~5 more councils get sporadic vote chips; ~25 councils structurally decide by agreement |
| S142 expenses | The existing Pay card gains **actual** per-councillor payments for ~5 open-data councils |
| LGAS audit reports | A new auditor's-findings block per council; overlaps NOAC framing |
| Ethics registers | A councillor-interests card; 31 bespoke, scanned, PII-adjacent |

**Named-vote structural ceiling:** voice-vote-by-default means most decisions have no named record anywhere; the 2026-06-22 probe's "poor effort/yield" conclusion for a *31-council OCR* approach stands. *(Synthesis note: the minutes lens subsequently showed the born-digital subset — Cork City/Laois/Kilkenny/Fingal — was mis-tiered, so the bounded expansion in §5 Tier 1/2 is consistent with this ceiling.)*

**On the AFS dead-end:** argued "not reopened by GPU OCR" from the recorded miss taxonomy (Playwright/discovery/layout, only Wexford scanned; Kildare+Sligo recoverable via a camelot re-run priced and declined). *(Synthesis note: the recorded taxonomy itself proved stale — see Appendix A — but the skeptic's core point survives: OCR is not the reopening argument.)*

### (ii) Per-family verdict table

| Family | Citizen value | Commercial value | Verdict |
|---|---|---|---|
| AFS tail | Real but bounded; ~1.1m people (~22% of state) in the 9 counties — strongest pro-completeness argument | **OUT** of paid (budget grain beside awarded/paid € = never-sum liability, no buyer) | PARK *(superseded on facts → reopen-with-evidence, §5)* |
| Fresher AFS | Honesty gain | OUT | PARK *(largely solved by the camelot fix)* |
| Adopted budgets | Marginal; 4th-grain risk | OUT | DROP *(superseded by the DHLGH central pub — pursue with grain guard)* |
| Named votes >Carlow | Low-to-real for the ~5 roll-call councils | **OUT permanently** — diary precedent a fortiori; the only commercial meaning of "who voted against rezoning" is lobbying-targeting | Civic-only, bounded probe |
| S142 (open-data 5) | Genuine; Pay card slot exists | OUT (named-individual payments) | **NOW (capped at open-data councils)** |
| LGAS | Moderate; NOAC overlap | OUT | DROP *(sweep + AFS lenses rate it higher; synthesis: PURSUE Tier 2)* |
| Ethics registers | Low usage, high sensitivity | OUT | DROP *(sweep: gated pilot; synthesis: Tier 4 gated)* |
| Councils as buyers in dossiers | n/a | **IN — already done** (31 LA publishers in gold; paid sample has zero council mentions, nothing blocked) | NOW = do nothing |
| Councils as customers (P2 #14) | n/a | **IN — a sales task, not a data task** (competition/single-bid views, none of it AFS/minutes) | NOW as sales motion |

**Load-bearing point:** "target county-council data" conflates two unrelated things. Councils as a *data subject* is a civic-completeness question. Councils as a *customer segment* is the commercial question — and it requires **zero new council data**.

### (iii) Effort envelope
Ceiling ~3 focused days of civic council work this quarter, none before the BI pre-sell window has an answer: 0 days AFS *(pre-revision)*; 1–2 days S142 open-data councils; ≤1 day fitz roll-call probe over already-cached clean minutes ("if tables don't fall out, stop"). Commercial: fold one council/OGP contact into the P0 pre-sell conversations; hand-build a single-council benchmark from existing views only if one bites.

### (iv) Failure-mode warnings
1. **Dead-end re-litigation via the OCR pretext** — any plan saying "the OCR win reopens AFS" is contradicted by the miss taxonomy. *(Validated: the actual reopening case is code+discovery, not OCR.)*
2. **The 31-bespoke-parsers trap** — S142-beyond-open-data, ethics, LGAS-as-scrape, minutes-at-scale all have the AFS-Phase-1 shape; the repo has run this experiment.
3. **OCR-for-sporadic-yield** — ruled out 2026-06-22 for named votes; Galway City is also a *source* limit.
4. **Rebuild-what-exists** — NOAC (6th false positive), councillor-data-still-sandbox, amalgamated-AFS premise; this very tasking arrived with stale coverage numbers (22 councils/€12bn vs actual 31 publishers/~€19bn), stale in the direction that *understates* how done councils-as-buyers is.
5. **Commercial scope creep past the firewall** — councillor-keyed data is worse than diaries: elected-member personal data sold into a bid/lobbying context.
6. **Fourth-grain contamination** — the AFS parser auto-rejects budget pages *by design*; reintroducing budget data deliberately invites the mislabel the safety gate exists to prevent.

### (v) The single highest-leverage council move
Put one Irish council (or OGP/LGMA) on the P0 pre-sell interview list, armed with a one-off hand-built competition/single-bid benchmark drawn entirely from existing views — and ingest nothing new. If the civic side must get one thing: S142 actuals for the five open-data councils into the existing Pay card.
