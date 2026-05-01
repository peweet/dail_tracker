# Iris Oifigiúil — Discovery Notes

**Date**: 2026-05-01  
**Status**: Research phase — not yet in pipeline

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
