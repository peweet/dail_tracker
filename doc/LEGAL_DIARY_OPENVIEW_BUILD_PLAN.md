# Legal Diary — OpenView source promotion plan

**Status:** PROMOTED 2026-06-16 (Phases 1+2 built & tested). Decisions taken: docx→High-Court-only,
panels show on every member (capped at 9 — larger strings are roster notices). A `--limit 250`
backfill (1,000 docs) produced 77,487 anonymised cases (Circuit 4,442); judge profiles now populate
(Circuit 1,723 rows / 29 judges). Source span (live): higher courts back to **2018**, Circuit to
**2022**, plus forward scheduling to Dec 2026 — OpenView queries the persistent Domino document DB,
not the daily-overwritten `.docx` snapshot. Source-health CANARY added
(tools/legal_diary_openview_health.py + .github/workflows/legal_diary_openview_health.yml). Full
backfill (~5,750 docs) run 2026-06-16.
**Probe:** `pipeline_sandbox/legal_diary_openview_probe.py` · report `data/_meta/legal_diary_openview_probe.json`
**Origin:** "the cases for circuit court judges are missing" — they were never in the source we ingest.

---

## 1. Problem

The production diary path ([pdf_infra/legal_diary_poller.py](../pdf_infra/legal_diary_poller.py) →
[extractors/legal_diary_extract.py](../extractors/legal_diary_extract.py)) downloads the single
`/download` **.docx**, which is the **Four Courts current court day only** (Supreme / Court of
Appeal / Central Criminal / High). Consequences:

- **Circuit Court is entirely absent** — 0 rows in gold, raw audit included. 46 Circuit Court
  judges on the bench roster have a permanently empty "Before the court".
- The higher courts are **capped at one day** — no history, no forward schedule.

## 2. Discovery — the OpenView source

### 2a. How it was found (so it can be re-derived if the site changes)

The history was a side-discovery — the goal was *Circuit Court* data; the archive fell out of
*how* the source turned out to work. The trail, reproducible:

1. **Confirmed the gap is source-side, not ours.** Querying the gold parquet showed 0 Circuit
   rows even in the raw audit; grepping the archived `.docx` files showed "Circuit" only ever as a
   *list-type inside the High Court*, never its own court section. So the `.docx` genuinely omits it.
2. **Read the site's own links.** Fetching the `legaldiary.courts.ie` landing page and listing every
   `href` surfaced per-jurisdiction pages (`/circuit-court`, `/supreme-court`, …) that the `.docx`
   poller never follows — it only ever takes the `/download` link.
3. **Followed the JavaScript.** Those pages are JS-rendered (no document links in the HTML). The
   loader `legaldiary.nsf/JavaScript/legal-diary.min.js` contained the tell: the fragment
   `?OpenView&Jurisdiction=`. Its `doSearch()` assembles
   `dbPath + '/' + searchView + '?OpenView&Jurisdiction=' + searchView + '&dateType=…&dateFrom=…&dateTo=…&text=…'`,
   and the page HTML defines `dbPath = "/legaldiary.nsf"` and `searchView = "circuit-court"`.
4. **Recognised the technology.** Three signatures point to an **IBM Domino** (Lotus Notes) backend:
   the **`.nsf`** database file, the **`?OpenView`** command (Domino's built-in "list every document
   in this view"), and **`?OpenDocument`** ("fetch one document"). A Domino `.nsf` is a *document
   database*, so `OpenView` lists the whole persistent store — not a daily file. The presence of
   **`dateFrom`/`dateTo`** range fields was the clincher: you only build a date-range search over
   data you actually keep.
5. **Tested it directly.** Hand-built the `OpenView` URL → got the index table (`<tr
   class="clickable-row" data-url="…/<UNID>?OpenDocument">`); fetched one `OpenDocument` → a real
   case list (judge, parties, record number).
6. **Measured the age.** Each index row carries a sortable `data-text="YYYYMMDD"` on its Date cell.
   Parsing those across all jurisdictions returned the span below — the history was simply every
   document still held in the Domino DB, which the daily `.docx` snapshot never exposes.

**Re-derivation recipe if the ingest breaks:** open a jurisdiction page in DevTools, find the
`searchView` / `dbPath` vars and the `?OpenView` call in `legal-diary.min.js`, rebuild the index
URL, and read `data-url` for the `?OpenDocument` detail links. The poller/extractor encode exactly
this; the canary (`tools/legal_diary_openview_health.py`) asserts each step still holds.

### 2b. The source structure

The site exposes a Domino **OpenView** index per jurisdiction with the **full party-level archive**,
one HTML detail document per sitting:

- Index: `https://legaldiary.courts.ie/legaldiary.nsf/<slug>?OpenView&Jurisdiction=<slug>&area=&type=&dateType=Date&dateFrom=&dateTo=&text=`
  → rows `<tr class="clickable-row" data-url="/legaldiary.nsf/<slug>/<UNID>?OpenDocument">`.
- Detail: `<div class="ld-content">`, `<br/>`-separated lines.

| Jurisdiction (`slug`) | Index rows (live 2026-06-16) | Use |
|---|---|---|
| `supreme-court` | 1,512 | promote (panel court) |
| `court-of-appeal` | 2,939 | promote (Civil + Criminal, panel court) |
| `central-criminal-court` | 1,607 | promote (single-judge) |
| `circuit-court` | 697 | **promote — the reported gap** (single-judge) |
| `high-court` | `OpenView` 500s | keep on the **.docx** path |
| `district-court` | no case view (schedule page only) | source gap — honest note in UI |
| Circuit Court – Family | in-camera | **never fetch** |

Index dates span **deep past and near future** (verified live across all four indexes): higher
courts back to **2018-07/08**, Circuit Court to **2022-01**, forward to **2026-12** — real history
**and** forward-scheduled sittings (which may not yet carry final lists). This is the persistent
Domino document store, not the daily-overwritten `.docx`, which is why old records exist at all.

## 3. What the probe proved

Stress run (`--limit 25`, 100 detail docs): **8,057 case lines → 7,910 anonymised, 100 in-camera
dropped (55 minor / 45 child-and-family), privacy gate PASSED, 77 judges.** Re-uses the docx
anonymiser verbatim (`parties` / `protected_reason` / `category_of` / `residual_name_tokens`) and
the same `PrivacyInvariantError` gate. Anonymisation correct across every layout: individuals →
initials, orgs/State named, criminal defendants reduced even where the **source self-anonymises
inconsistently** (`DPP -v- J G` vs `DPP -v- Muris Coco`) — so our anonymiser stays mandatory.

Three detail layouts, all handled:
1. **Higher-court packed:** `Before Mr. Justice X in Courtroom 06 at 10:15 (For Mention)` + tab cells `idx⇥record#⇥DPP -v- A H`.
2. **Circuit on-circuit packed:** `…in Galway at 10:30 (For Hearing)` — location is a **town**, not a courtroom.
3. **Circuit civil bare:** `Before Judge X` / `At 10:30 Am` on separate lines; case line has a **solicitor tail** after ` : ` that is dropped.

Case cells are TAB-delimited → the party cell (the one with the v-separator) isolates cleanly,
dropping record# **and** solicitors. **Supreme + Court of Appeal sit as PANELS** (consecutive
`Mr./Ms. Justice X` lines, no "Before") — the probe joins them with ` & `.

## 4. Promotion architecture (reuse, don't duplicate)

```
pdf_infra/legal_diary_openview_poller.py   NEW  walk index per jurisdiction → archive each
                                                OpenDocument HTML to bronze (gitignored, raw)
extractors/legal_diary_openview_extract.py NEW  parse archived HTML → SAME anonymise+gate →
                                                gold parquet rows (shares helpers w/ docx extract)
extractors/legal_diary_extract.py          EDIT factor the anonymiser/gate into a shared module
                                                so both extractors call one privacy implementation
sql_views/judiciary/*.sql                  EDIT union OpenView + docx into the existing
                                                v_judiciary_legal_diary_* / judge_sittings views
extractors/judiciary_diary_link.py         EDIT feed Circuit/Central-Criminal judges into the
                                                diary→roster surname-within-court join
utility/pages_code/judiciary.py            EDIT panel rendering; District honest-coverage note
```

The gold **schema is unchanged** — OpenView rows carry the same columns plus `venue` (Circuit) and
a `panel`/`is_panel` signal. The privacy gate must be the **single shared implementation** both
paths call.

## 5. Open design decisions (need a call before building)

1. **Overlap precedence (Supreme/Appeal/Central Criminal are in BOTH sources).**
   Recommended: split by court — High Court from docx; Circuit + the three overlap courts from
   OpenView (richer history, same content). Drop those three courts from the docx extract to avoid
   double counting, leaving the docx path as **High-Court-only**. Alternative: keep both and dedup
   on `(court, diary_date, case_anonymised)` preferring OpenView.
2. **Panel courts.** Supreme & Court of Appeal hear matters as 3–5-judge panels. The single-judge
   "Before the court" profile section doesn't fit them. Options: (a) attribute to every panel
   member (a Supreme matter shows on all 5 profiles), (b) store the panel but don't surface it per
   judge, (c) a distinct "panel sitting" presentation. Recommended (a) for the roster join, with a
   panel chip in the UI.
3. **Backfill volume & cadence.** ~6,755 detail docs across the four courts, against a flaky
   Domino server (`RemoteDisconnected`). One-time backfill + a daily **incremental** keyed on the
   index `Updated` column (only refetch rows whose Updated changed). Future-dated rows refetched
   until their date passes (lists firm up over time).
4. **District Court.** No party-level source exists here — add the honest coverage note in
   `_render_profile_diary` so Circuit/District judges aren't implied to be a *match* failure.

## 6. Privacy (non-negotiable)

- Same model as the docx pipeline: persons → initials, orgs/State in clear, statutory in-camera
  categories dropped, residual-name gate refuses to write gold.
- Bronze HTML archive names raw parties **and solicitors** → gitignored, off-box only.
- "Circuit Court – Family" jurisdiction never fetched.
- Tidy at promotion: the `[formerly …]` bracket case over-anonymises org words (`… and Skills` →
  `… and S.`) — harmless (safe direction) but cosmetically worth fixing in `strip_refs`/`parties`.

## 7. Tests

- Parser fixtures for all three layouts + a panel sitting (reuse the existing
  `test/extractors/test_judiciary_privacy.py` gate pattern).
- A privacy contract test asserting `residual_name_tokens` is empty across the OpenView gold.
- An overlap test asserting no `(court, diary_date)` is double-sourced after the precedence rule.

## 8. Validation already done

- `--limit 25` × 4 courts: gate PASSED on 7,910 rows; venues, panels, in-camera drops all correct.
- Outputs gitignored (audit holds raw names); report JSON is aggregates-only.
