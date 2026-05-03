# Iris Oifigiúil — Discovery Notes

**Date**: 2026-05-01 (original) · **v2 update**: 2026-05-02
**Status**: Research phase — not yet in pipeline. **v2 probes complete** — read `iris_oifigiuil_probe_findings.md` for the data-driven verdict; this doc is preserved as the original scoping.

---

## Status update — what changed after the probes (2026-05-02)

The probes (`iris_oifigiuil_probe.py`, `_probe2.py`, `_probe3.py`, `_evidence.py`, `_evidence2.py`, `_value_probe.py`) ran across 436 valid PDFs (2022-2026) and overturned several claims below. Read this delta first, then the original notes for context.

### Corrections to original claims

| Original claim | Correction |
|---|---|
| Format "consistent enough across the full range" since 2016 | **Format breaks 2022 → late-2023.** 2022/early-2023 issues have ~5 underscore-delimiters per issue; modern issues have 50–80. Hybrid splitter required (split on `_{6,}` then re-split on strong heading anchors). Modern issues also carry `[C-N]`/`[G-N]`/`[L-N]` block codes that older issues lack. |
| Use **pdfplumber** for extraction | **fitz / PyMuPDF only** (per current project standard). pdfplumber works but adds a dependency we don't need. |
| Section classifier splits on `\n_{5,}` (horizontal rule) | Use `_{6,}` (5 was over-permissive); see hybrid-splitter recommendation in v2 findings §2. |
| `IRISH_STANDARDS` is "noise — skip" | **Partially wrong.** 49 specific I.S. code references in 22 PDFs — these are revocation/supersession events for native Irish I.S. standards (fire, gas, electrical). Worth a slim structured parse. Blanket EN/CEN adoption boilerplate is still skip. |
| `CENTRAL BANK / ICAV` is "noise — skip" | **Partially wrong.** ~198 CB authorisation revocations are a useful **regulated-entity attrition feed** (KYC-adjacent). ICAV migrations are real but tiny (12 hits / 5 years). Source jurisdictions are Guernsey + Cayman, **not London** (no Brexit story; this is AIFMD substance-driven). |
| Skip-list mentions `PROCESS_ADVISER_SCARP` and `PLANNING` | Confirmed correct for SCARP (boilerplate). Planning is more nuanced — see new `FORESHORE_LICENCE` category below. |

### New value categories surfaced (not in original)

| Category | Hits | Volume class | Why it matters |
|---|---|---|---|
| **MEMBER_INTEREST_SUPPLEMENT** (Sections 6 & 29 Ethics Acts) | 34 blocks / 27 PDFs | low-volume, high-value | ⭐⭐ **the big find** — captures *mid-year register changes* + *§29 conflict-of-interest disclosures tied to specific votes*. Materially different from `member_interests.py` annual register output. 27 of 40 declarations parsed have NO matching row in `silver/dail_member_interests_combined.csv`. Concrete cases: Robert Troy 2022 directorships (the resignation-trigger event), Christopher O'Sullivan 2025 voluntary directorship, Senator Ollie Crowe Crowe's Bar declarations, Josepha Madigan 2023 Russia/Ukraine sanctions §29 statement. |
| **FORESHORE_LICENCE** | 856 / 71 PDFs | high-volume, mid-value | offshore wind / port / aquaculture permits; relevant to Ireland's 10GW-by-2040 offshore wind target |
| **TAX_DEFAULTERS_LIST** | 43 / 17 PDFs | quarterly, high-value | statutory §1086 publication, **publicly nameable by law**, newspaper-grade content |
| **STATE_BOARD_APPOINTMENT** | within 393 APPOINTMENT hits | irregular, mid-value | An Bord Pleanála chair, Horse Racing Ireland board, EPA, Pensions Authority, etc. |
| **PENSION_SCHEME_EVENTS** | 102 / 36 PDFs | steady, mid-value | scheme registrations + terminations |
| **CHARITY_COOP_LIFE_EVENTS** | 366 / 40 PDFs | steady, niche | Industrial and Provident Societies Acts — co-ops, water schemes; only public record |
| **BYELECTION_WRIT** | 1 / 1 PDF | rare, very high signal | political event |
| **REVENUE_FORFEITURE** | 10 / 8 PDFs | low, niche | Revenue §142 vehicle seizures (count only) |

### Findings worth surfacing in UI

- **>50% of SIs are amendments** to existing SIs — the Statute Book is being constantly patched.
- **~30% of SIs are EU-aligned** — measurable answer to "how much Irish secondary legislation is downstream of Brussels".
- **Election effect on SI volume**: 90 days before the 2024 GE = 234 SIs published; 90 days after = 165 SIs. **30% drop** post-election. Irish equivalent of "midnight regulations" / "wash-up".
- **Top-5 SI-issuing departments** (5-year): Finance (409), Enterprise (222), Agriculture (188), Housing (173), Justice (89).

### Schema — superseded

The original `si_register` single-table schema (§ "Silver table schema" below) has been superseded by a multi-table design in `iris_oifigiuil_probe_findings.md` §11. Summary:

- `iris_blocks` — primary (one row per parsed block, all categories)
- `iris_si` — SI metadata (no full body; link out to eISB)
- `iris_member_interests` — Section 6/29 declarations
- `iris_tax_defaulters`, `iris_state_appointments`, `iris_foreshore_licences`, `iris_pension_events` — Tier-1 specialised
- `iris_fishing_quotas` — optional second-pass via `fitz.find_tables()`

### Build plan (small, focused)

1. **v1 extractor** — `pipeline_sandbox/iris_oifigiuil_extract.py` (~200 lines). Two outputs: `iris_si.csv` + `iris_member_interests.csv`. Skip everything else for v1.
2. **v2 value categories** — add Tier-1 parsers for tax defaulters, state appointments, foreshore licences.
3. **v3 resolvers** — fuzzy-match member/minister names to existing silver tables.
4. **v4 UI** — Streamlit pages per the UX framing in v2 findings §12.

### What NOT to do (deliberately)

- Don't render full SI text in-tracker (eISB does this canonically; duplication creates encoding-error maintenance liability)
- Don't editorialise which SIs matter (directory + filters lets users find their own)
- Don't summarise SI bodies with NLP (legal jargon doesn't summarise; any attempt will be wrong sometimes)
- Don't merge SIs and Acts in UI (keep legal hierarchy visible — citizens often confuse "the law" with "an Act")
- Don't republish bankruptcy individual names (per user direction; counts + petition type only)
- Don't pull this into Phase 1 of the short-term plan — it's a Phase 2 / Week 7+ Track A candidate

---

## Original notes (preserved for context)


## What it is

The official State gazette of Ireland, published **Tuesday and Friday** by the Government Publications Office. Certain notices are *legally required* to appear here (SI commencements, company winding-up petitions, government appointments). Published since at least 2002, archive online.

## Archive URL pattern

```
https://www.irisoifigiuil.ie/archive/{year}/{month}/{filename}.pdf
```

Filename format: `IR{DDMMYY}.pdf` (sometimes `Ir{DDMMYY}.pdf` — mixed case in archive).  
Current issues: `https://www.irisoifigiuil.ie/currentissues/IR{DDMMYY}.pdf`  
Archive index: `https://www.irisoifigiuil.ie/archive/{year}/{month}/`

Caveats on filenames: some issues have suffixes (`IR050416-2.pdf`, `Ir190416-1.pdf`).  
Edge case: `Ir280420 AMENDED.pdf` (space in name). Need to scrape the archive index rather than guess filenames.

## Format consistency scan

Sampled: 5 Apr 2016, 8 Apr 2016, 3 Apr 2018, 6 Apr 2018, 3 Apr 2020, 7 Apr 2020, 28 Apr 2026.

| Year | Pages | Key observation |
|------|-------|----------------|
| 2016 | 14–20 | More Central Bank / investment scheme content; some issues bilingual (Irish/English side-by-side) |
| 2018 | 8–18  | Standard SI + company notices; Friday issues include quarterly **Exchequer Statement** |
| 2020 | 12–38 | COVID emergency SIs dominate; COVID publishing notices prepended; otherwise same structure |
| 2026 | 12    | Cleaner layout; SIs fully prose-described |

**Verdict: consistent enough across the full range.** The SI block format hasn't changed meaningfully since 2016. Company notice boilerplate is identical year to year.

## Section taxonomy

### High-value for Dáil Tracker

**Statutory Instruments (SIs)**  
Most reliable section. Every block starts with `S.I. No. \d+ of \d{4}\.` on its own line, followed by the regulation title in ALL CAPS, then a prose description naming the Minister and the parent Act. Published Tuesday *and* Friday.

Example patterns for minister extraction:
- `"The Minister for Transport, in exercise of the powers conferred on him..."` (no name, just title)
- `"The Minister for Housing, Planning and Local Government, Mr Eoghan Murphy T.D., has made..."` (name + title)
- `"Darragh O'Brien, Minister for Transport, in exercise of..."` (name-first)
- `"The Minister of State at the Department of Education and Skills has made..."` (junior minister, no name)

All three patterns need handling. Name extraction will need fuzzy match against the member register.

**Government Appointments**  
Irregular but valuable. Format: `APPOINTMENT AS A [ROLE]` header, then prose naming the person and appointing authority. Not always a Minister — sometimes a state body.

**Commissions of Investigation / Terms of Reference**  
High-value policy events. Appear rarely. Identified by headers like `COMMISSION OF INVESTIGATION`.

**Presidential Bill Signings**  
Found in 2018: bilingual notice when President signs a Bill into law. Includes Act number. Useful for linking to legislation tracker.

**Exchequer Statement**  
Found in Friday issues near end of quarter (Q1 in April issues). Full quarterly government revenue/expenditure breakdown by department. Would be a standalone gold table if extracted. Not always present.

**Referendum / Election Notices**  
Polling day orders, register of political parties amendments. Niche but linkable to member data.

### Noise (skip)

- **Company winding-up petitions** (Revenue Commissioner pursuing tax debts) — pure legal boilerplate, no parliamentary angle
- **Voluntary liquidations / MVL notices** — same
- **Process adviser appointments** (SCARP rescue process) — same
- **Central Bank authorisations** — investment fund registrations, ICAV migrations, credit union cancellations
- **Irish Standards (NSAI/CEN/CENELEC)** — standards adoption notices, no parliamentary relevance
- **Planning notices** from local authorities — county development plan consultations etc.

## Extraction approach

1. **Scrape archive index** per month to get exact filenames (avoid guessing due to suffixes/case)
2. **Download PDFs** — pdfplumber extracts cleanly (machine-generated, not scanned)
3. **Section classifier** — split full text on `\n_{5,}` (horizontal rule) or double-newline + ALL-CAPS header; label each block by type
4. **SI parser** — regex `S\.I\. No\. (\d+) of (\d{4})\.` to anchor each block; extract title, minister text, parent act
5. **Minister resolver** — fuzzy match extracted minister names against gold member table
6. **Skip list** — drop blocks matching company/liquidation/ICAV/NSAI patterns

## Silver table schema (proposed)

`si_register`
| column | type | notes |
|--------|------|-------|
| si_number | int | e.g. 172 |
| si_year | int | e.g. 2026 |
| title | str | regulation title |
| gazette_date | date | publication date |
| gazette_issue | int | issue number within year |
| minister_raw | str | extracted text before resolution |
| minister_uri | str | matched Oireachtas member URI (nullable) |
| department_raw | str | |
| parent_act_raw | str | |
| description | str | full prose block |
| source_pdf | str | filename |

## Next steps

- [ ] Write archive scraper (month index → PDF filename list)
- [ ] Write bronze downloader (parallel, skip already-downloaded)
- [ ] Write SI extractor + section classifier
- [ ] Build minister name resolver against existing member data
- [ ] Decide scope: 2020-present only (matches pipeline quality threshold) vs full archive
