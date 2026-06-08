# SIPO OCR — extraction backlog & scope (2026-06-06)

Scope floor: **2022 and later** (do not ingest pre-2022). Companion to
`doc/SIPO_PIPELINE.md`, `doc/SIPO_OCR_INVESTIGATION.md`, the census in
`data/_meta/sipo_ge2024_expenses_sources.md`, and the memory `project_sipo_ocr`.

## What is DONE (in gold)

- **GE2024 National-Agent expenses** — `data/gold/parquet/sipo_expenses_fact.parquet`,
  399 rows, **9 parties** (FF, FG, SF, Lab, Green, SocDem, PBP, Aontú, National Party).
  ⚠️ Known open bug: FF total reads €3.44M, should be ≈€375k (×100 OCR decimal-loss,
  currently flagged not repaired — cap-repair queued in `doc/SIPO_CONSOLIDATION_PLAN.md`).
- **GE2024 election donations** — `data/gold/parquet/sipo_donations.parquet`,
  74 donations, €161,578, 7 parties.

## Crash-safety rules (READ BEFORE RUNNING ANY OCR)

PaddleOCR on this machine is unstable: intermittent **SEGFAULTs** and multi-minute
**HANGs**. Hard rules (from `project_sipo_ocr`):

1. **NEVER run two OCR processes at once** — it has crashed the machine + GPU driver.
2. Always launch via the **watchdog** (`extractors/_sipo_watchdog.py`), never the ETL
   directly — it kills a run stalled >200s and resumes. A bash restart-loop cannot
   bound a HANG.
3. Per-page **checkpoints** (`data/silver/sipo/by_party/_ckpt/<key>/pNNN.json`) +
   DPI retry ladder (2×300 → 1×200 → skip) mean a crash loses one page, not the run.
4. **Cache raw cells** so parser fixes need re-parse only, never re-OCR.
5. Born-digital returns (text layer) skip OCR entirely — instant, exact.
6. Throughput ≈ **33–40 s/page** on CPU. Plan one-shot, overnight if large.

## TIER 1 — immediate, low-risk (reuse the working expenses ETL)

The 8 remaining GE2024 party national-agent returns, now in
`data/bronze/scan_pdf/` (moved from `c:/tmp/sipo_missing/` 2026-06-06). All scanned.

| key | file | pp |
|---|---|---|
| centre_party | centre_party_sipo_ge_2024_expenses.pdf | 29 |
| i4c | i4c_sipo_ge_2024_expenses.pdf | 35 |
| indep_ireland | indep_ireland_sipo_ge_2024_expenses.pdf | 17 |
| ireland_first | ireland_first_sipo_ge_2024_expenses.pdf | 24 |
| irish_freedom | irish_freedom_sipo_ge_2024_expenses.pdf | 25 |
| irish_people | irish_people_sipo_ge_2024_expenses.pdf | 24 |
| redress100 | redress100_sipo_ge_2024_expenses.pdf | 24 |
| right_to_change | right_to_change_sipo_ge_2024_expenses.pdf | 24 |

**Total ≈ 202 pages ≈ 2 hours** single-process OCR. Work: add 8 keys + party labels
to `PARTY_JOBS` in `extractors/sipo_expenses_paddle_etl.py`, run them through the
watchdog ONE AT A TIME, then re-run `sipo_promote_to_gold.py`. Roster-layout parsing
(numbered list vs FF/SF table) may need the `roster_fix` band handling — validate
per party. Adds **party-affiliated** candidates only.

- **national_party** — born-digital, ALREADY in gold (1 row). Skip.
- **aontu2_283923_..._VERIFY-DUP.pdf** (12pp scanned) — a 2nd Aontú filing distinct
  from the 45pp born-digital one already extracted. Eyeball before deciding; likely
  earlier/partial/superseded. Do NOT queue until confirmed.

## TIER 2 — the genuine-Independents gap (NEW extractor, big OCR load)

Genuine **non-party Independent TDs** (Lowry, the Healy-Raes, Verona Murphy, Harkin,
Fitzmaurice, etc.) file no national-agent return — they exist ONLY in the
**Candidates Election Statements** tier: 43 constituency sub-pages → ~10–16 candidate
sub-pages each ⇒ **≈400–600 per-candidate PDFs**, untouched. This is the only place
independents are captured, and also holds per-candidate→vendor detail.

Scoping notes:
- New extractor + new anchors (candidate name / constituency / Part-3+Part-4 per
  candidate). Constituency is fixed per sub-page (free anchor).
- OCR load: ~7,840 expense pages + ~570 donation pages. On CPU this was ~2 weeks of
  nights (~55 s/page); **on the machine's RTX 3060 GPU it's ~0.76 s/page → the whole
  corpus in ~3 hours.** GPU validated 2026-06-08 (equivalent fidelity, 0 errors) — see
  `doc/SIPO_CANDIDATE_OCR_HOWTO.md` "⚡ GPU fast path".

**STATUS UPDATE 2026-06-08:** this tier is no longer "untouched" — the corpus is SOURCED
(607 expense + 428 donation PDFs, `extractors/sipo_candidate_expenses_crawl.py`) and the
**OCR is RUNNING on GPU** into `data/silver/sipo_candidate/_ckpt/`. The scout step is done
(per-doc census in the manifest). Remaining work after OCR = the per-candidate **form
parser** (cells → structured rows; the genuine-Independents payoff). Full how-to +
GPU setup in `doc/SIPO_CANDIDATE_OCR_HOWTO.md`; corpus details in memory
`project_sipo_candidate_expenses_corpus`.

## TIER 3 — corpus missed by the first pass (2022→2025), NOT yet downloaded

Found by re-searching SIPO 2026-06-06. The first pass only sampled 2024 + one 2022 file
(samples now preserved in `data/bronze/sipo_annual/`).

**Annual Disclosures** (`collection/76651-annual-disclosures/`) — full series exists:
- Party donation statements: **2022, 2023, 2024, 2025**
- TD annual returns: **2022, 2023, 2024, 2025** (+ individual late filers)
- Senator annual returns: **2022, 2023, 2024, 2025** (+ individuals)
- MEP annual returns: **2022, 2023, 2024, 2025** (+ individuals)
- Format reality: scanned, LARGE, mostly NIL (TDs_2024 ≈ 201pp/56MB, ~1 form/member,
  most "no donations to declare"). High page count, sparse signal.

**Election Reports** (`collection/5b104-election-reports/`) — other events ≥2022:
- **Seanad Bye-Election 2022** — Report + Donation Statements ("senator spend")
- **European Election 2024** — national-agent + candidate expenses + candidate donations
- **Limerick Mayoral 2024** — national-agent + candidate expenses + candidate donations
- **Presidential Election 2025** — (document types TBC)
- **Dáil Bye-Elections 2026** — Dublin Central, Galway West

**Privacy:** donation statements name private donors incl. home addresses → suppress
addresses on any UI; no-inference rule applies (see `feedback_personal_insolvency_privacy`,
`feedback_no_inference_in_app`).

## Tier-1 OCR run + parser diagnosis (2026-06-07)

OCR run (PaddleOCR, watchdog) cached cells for `indep_ireland` (17pp), `redress100`
(24pp), `ireland_first` (17→23pp); the other 5 not yet OCR'd. **All parsed to 0 rows.**
Read-only diagnosis (`c:/tmp/sipo_parse_diag.py`, imports the live parser, writes
nothing) shows the constituency-anchored Part-3 parser does not fit these minor-party
returns — **three distinct failure modes:**

1. **`indep_ireland` — real data, parser drops it (RECOVERABLE).** Page 2 *is* a
   candidate summary (`parse_page` returns 3 rows), but `parse_party`'s stricter
   "≥3 rows carrying BOTH money columns" gate rejects it, AND real constituencies on
   later per-candidate pages are OCR-garbled below `match_constituency`'s threshold
   (`OFFAY`→Offaly 0.91, `Linerick Gity`→Limerick City 0.83, `ConK EAST`→Cork East
   0.88). Fix = relax the parse_party gate for small returns + fuzzy-repair / lower the
   constituency cutoff with the cap cross-check as backstop.
2. **`redress100` — near-NIL return.** p12 "Expenses Review" = categories 4A–4H all
   `€ NIL`; the only "money" cells are NIL template totals; no candidate table. 0 rows
   is likely substantively correct — needs a NIL-confirmation path, not row extraction.
3. **`ireland_first` — individual-level itemized, mostly €0.** p16 "Election Posters
   (INDIVIDUAL LEVEL)" Ref L1–L15 all `€ 0`. Per-candidate template, near-NIL.

**Implication:** these returns need either (a) a small-return/NIL detector that records
"filed, nil/near-nil" rather than forcing the constituency-summary anchor, plus (b) OCR
constituency fuzzy-repair for `indep_ireland`. This is parser work on the SHARED
`extractors/sipo_expenses_paddle_etl.py` — coordinate before editing (the other context
runs it). All cells are cached, so every iteration is re-parse only, **no re-OCR**.

## Recommended order

1. **Tier 1** now (2 hrs, pure reuse) → adds 8 small/independent-leaning parties incl.
   Independent Ireland & Independents4Change.
2. **Tier 2 scout** (no OCR) → size the per-candidate corpus before committing.
3. Decide Tier 2 vs Tier 3 by value: Tier 2 captures genuine independents; Tier 3
   broadens to annual + other-election donations.
