# sipo_expenses_fact.parquet — source citation

Companion to the silver (pre-promotion) fact
`data/silver/sipo/sipo_expenses_fact.parquet`, promoted to gold
`data/gold/parquet/sipo_expenses_fact.parquet` by `extractors/sipo_promote_to_gold.py`.
One row per candidate
per party, from each party's **National Agent — Election Expenses Statement** for
the 2024 general election (the 34th Dáil), as published by the Standards in Public
Office Commission (SIPO). These are **election expenses** (money spent
campaigning), NOT donations — see the editorial note at the bottom.

Built by `extractors/sipo_expenses_paddle_etl.py` (PaddleOCR re-OCR of the
scanned returns → geometry + 43-constituency closed-set anchor; see
`doc/SIPO_OCR_INVESTIGATION.md`). OCR-derived → every figure must carry a
"verify against the official SIPO PDF (page N)" caveat; the `flag` column marks
rows needing review.

## Statutory spending limits (GE2024) — the validity bound used in the ETL

Per-candidate spending limit by constituency size. Verified **verbatim** against
the SIPO guidelines PDF (figures quoted exactly as printed):

| Constituency size | Statutory spending limit |
|---|---|
| 3-seat | €38,900 |
| 4-seat | €48,600 |
| 5-seat | €58,350 |

"Under no circumstances can total spending on a candidate exceed the statutory
spending limit for the candidate" (Electoral Act 1997 as amended, s.32). The ETL
uses these as `statutory_limit_eur` and flags any extracted expenditure above the
limit as `over_limit_verify` (an impossible value ⇒ OCR misread or wrong
constituency match).

**Cross-check:** Fianna Fáil's "amount assigned to the party" column is a flat
**40%** of these limits (€15,560 / €19,440 / €23,340 = 0.40 × 38,900 / 48,600 /
58,350) — an internal corroboration of both the figures and the assignment
reading. Assignment proportions differ per party, which is why the ETL anchors on
the statutory limit (universal) rather than any party's assigned amounts.

### Other thresholds on the same page (for the future donations dataset)

- €11,200 — maximum reimbursement of election expenses to a candidate.
- €1,000 — max donation a candidate may accept (single source, per year).
- €2,500 — max donation a political party may accept.
- €1,500 — donation declaration threshold (party).

## Source

- **SIPO, _Guidelines for the General Election to the 34th Dáil, 29 November
  2024_** — spending limits in §"Relevant dates & spending limits"; assignment
  mechanism in §1.2 "Assignment of a portion of a candidate's spending limit to
  his/her political party."
  - PDF: <https://assets.sipo.ie/media/283883/b6e53676-bb38-4bfd-8773-565b4cd95135.pdf>
  - Collection: <https://www.sipo.ie/en/collection/30d00-2024-general-election-guidelines/>
- **Per-party expenses returns** (the scanned forms this fact is OCR'd from) —
  SIPO _Dáil General Election 2024_ collection:
  <https://www.sipo.ie/en/collection/2e0c0-dail-general-election-2024/>
  - Fianna Fáil <https://assets.sipo.ie/media/283955/6261d302-2a56-49af-abfc-57e9364c13fe.pdf>
  - Fine Gael <https://assets.sipo.ie/media/283936/7be18f2f-5cdc-4333-9f77-86830b14615c.pdf>
  - Sinn Féin <https://assets.sipo.ie/media/283935/30d4c805-b333-44d4-ac5a-cf45756b0ea5.pdf>
  - Labour <https://assets.sipo.ie/media/283939/d62f2116-9ad3-47ad-b564-415de4f043cb.pdf>
  - Green Party <https://assets.sipo.ie/media/285734/0a5f2c08-0fe3-4b0f-ab73-1c012820c90e.pdf>
  - Social Democrats <https://assets.sipo.ie/media/283937/e6e5a11f-9186-4bc6-928a-12454c9690a3.pdf>
  - People Before Profit/Solidarity <https://assets.sipo.ie/media/285690/859abebd-2f42-4a0e-8c34-045cbebf00ac.pdf>
  - Aontú <https://assets.sipo.ie/media/285737/f2c55c0f-9c08-4b5a-b7c4-48e2f2bf0c87.pdf>
  - (full field incl. Independent Ireland, Independents4Change, Right to Change,
    100% Redress, Irish Freedom, National Party, Ireland First, The Irish People,
    Centre Party also on the collection page)
- Background: [Citizens Information — Election expenses](https://www.citizensinformation.ie/en/government-in-ireland/elections-and-referenda/running-for-office/election-expenses/)

## Part 4 — itemised expenses (`sipo_expense_items_fact.parquet` + `sipo_expense_categories_fact.parquet`)

A second extractor, `extractors/sipo_expense_items_paddle_etl.py`, reads
**Part 4** of the same National-Agent returns (the candidate-summary above is
Part 3). Two outputs, both sandbox:

- **Line items** (`sipo_expense_items_fact.parquet`): one row per `Ref | Item |
  Cost` line under each heading — e.g. `A16 | Meta ads | €24,853.05`. The Ref
  prefix letter deterministically encodes the heading (A=Advertising …
  H=Campaign Workers), so categorisation needs no header OCR.
- **Category totals** (`sipo_expense_categories_fact.parquet`): the 8 heading
  totals + Overall total from the "Expenses Review" page.

Part 4 headings (standard SIPO National-Agent form):

| Section | Heading |
|---|---|
| 4A | Advertising (incl. social-media / Meta ads) |
| 4B | Publicity |
| 4C | Election Posters |
| 4D | Other Election Material |
| 4E | Office and Stationery |
| 4F | Transport and Travel |
| 4G | Market Research |
| 4H | Campaign Workers |

**Validator (engine-independent):** Σ(line items in a heading) is reconciled to
that heading's review-page total (`reconciles` column); a mismatch flags an OCR
miss or a page-split item. This is the Part-4 analogue of the constituency/cap
anchor used in Part 3.

**Granularity caveat:** these line items are at **party / National-Agent level**
("NATIONAL LEVEL" on each heading page) — e.g. *Fianna Fáil → Meta ads €24,853*,
NOT *per-candidate → vendor* (e.g. a specific TD paying the Galway Advertiser).
The per-candidate vendor detail lives in each candidate's **own** election-expense
statement, a separate document not held here.

## Per-TD / Senator / MEP annual donation disclosures (future track, NOT yet sourced)

Individual Oireachtas members file an **annual Donation Statement** (separate from
the election-period returns above). These are the "who personally gave money to
this TD" records.

- Source collection: <https://www.sipo.ie/en/collection/76651-annual-disclosures/#reports-of-donations-to-tds-senators-and-meps>
- Status: **not downloaded / not extracted.** Candidate for a later sourcing pass.

## Full collection census (2026-06-03) — all 18 national-agent statements + the candidate tier

Verified against the collection's raw HTML (`2e0c0-dail-general-election-2024`), not the
lossy summariser. The collection has **three sections**:

1. **National Agent Election Expenses Statements — exactly 18 PDFs** (party-level spend;
   this is the tier the ETL targets). Full map below.
2. **Candidates Election Statements — 43 constituency sub-pages**, each listing ~10–16
   individual candidate sub-pages, each holding that candidate's **own** expense
   statement ⇒ ≈400–600 per-candidate PDFs. This is the granular per-candidate→vendor
   detail referenced above as "not held here." **Now being sourced** — see
   *Per-candidate corpus* below.
3. **Other Persons Election Expenses Statements — empty.**

Donations are **not** in this collection (separate Election-reports `5b104` /
Annual-disclosures `76651`).

| media-id | Party / org | pp | size | layer | in repo? |
|---|---|---|---|---|---|
| 283955 | Fianna Fáil | 45 | 3.2 MB | OCR'd→extracted | ✅ |
| 283935 | Sinn Féin | 30 | 1.0 MB | **TEXT (born-digital)** | ✅ |
| 285737 | Aontú | 45 | 1.0 MB | **TEXT (born-digital)** | ✅ |
| 283936 | Fine Gael | 37 | 11.9 MB | scanned | ✅ (OCR in progress) |
| 285734 | Green Party | 26 | 7.7 MB | scanned | ✅ |
| 283939 | Labour | 33 | 8.7 MB | scanned | ✅ |
| 285690 | People Before Profit/Solidarity | 30 | 7.1 MB | scanned | ✅ |
| 283937 | Social Democrats | 28 | 7.1 MB | scanned | ✅ |
| 286561 | National Party | 24 | 0.4 MB | **TEXT (born-digital)** | ❌ |
| 283933 | Independent Ireland | 17 | 4.1 MB | scanned | ❌ |
| 284004 | 100% Redress | 24 | 6.1 MB | scanned | ❌ |
| 284005 | Independents4Change | 35 | 6.9 MB | scanned | ❌ |
| 285402 | Right to Change | 24 | 6.2 MB | scanned | ❌ |
| 285787 | Irish Freedom Party | 25 | 1.8 MB | scanned | ❌ |
| 286560 | The Centre Party of Ireland | 29 | 7.3 MB | scanned | ❌ |
| 286559 | The Irish People | 24 | 15.9 MB | scanned | ❌ |
| 286562 | Ireland First | 24 | 6.4 MB | scanned | ❌ |
| 283923 | Aontú (2nd entry) | 12 | 3.8 MB | scanned | ❌ — distinct from 285737; likely earlier/partial/superseded, verify |

Asset URL pattern: `https://assets.sipo.ie/media/<id>/<uuid>.pdf`. The 10 not-in-repo
PDFs are cached at `c:/tmp/sipo_missing/`. **3 of 18 are born-digital text (SF, Aontú
285737, National Party) → no OCR, fitz `get_text` direct.** 14 scanned → PaddleOCR.

## Per-candidate corpus (`data/bronze/sipo_candidate_expenses/`)

Sourced by `extractors/sipo_candidate_expenses_crawl.py`, which crawls the
collection two levels deep (root → 43 constituencies → candidate pages) over the
server-rendered HTML (no JS/OCR needed to discover links). **Each candidate page
publishes up to two documents** — a *GE 2024 Election Expense Statement* and a
*GE 2024 Donation Statement* (label variants incl. the abbreviations `EES`/`EDS`).
The crawler classifies every document (`doc_type` = `expense_statement` /
`donation_statement` / `other`).

- **Manifest:** `data/bronze/sipo_candidate_expenses/_manifest.csv` — one row per
  published document: constituency, candidate, candidate-page URL, `doc_type`,
  `doc_label`, `pdf_url`, `media_id`, `local_path`, `bytes`, `sha256`,
  `duplicate_of`, `status`. This is the OCR-ingestion work list.
- **PDFs:** downloaded to `<constituency_slug>/<candidate_slug>__<media_id>.pdf`.
  By default **only `expense_statement` docs are downloaded** (the manifest still
  lists donation statements as `status=FOUND` so the donation corpus is
  discoverable); pass `--doc-types all` (or `expense_statement,donation_statement`)
  to fetch both. Re-runs are resumable (existing files → `CACHED`).
- These per-candidate expense statements are the **scanned forms** that still need
  the same PaddleOCR + geometry/anchor extraction pipeline as the party-level
  returns — *not yet parsed*; the manifest is the queue for that pass.

## Scope constraint

**GE2024 / 2024 only.** Do not ingest SIPO years beyond 2024 (no 2025+ returns or
disclosures) until explicitly extended.

## Editorial note (no-inference rule)

These figures are *spending* by a party's national agent on behalf of its
candidates. A high or low spend is not evidence of anything beyond the spend
itself. Donations (money *received*) are a separate SIPO register
(`2024_election_donations.pdf`, not yet processed) — do not conflate the two, and
never imply influence from either.
