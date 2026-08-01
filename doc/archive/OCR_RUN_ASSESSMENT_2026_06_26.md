---
tier: RECORD
status: LIVE
domain: sources
updated: 2026-06-27
supersedes: []
read_when: before re-running or re-scoping OCR jobs, or checking what OCR/GPU extraction work has already been done (SIPO, ministerial diaries, council minutes)
key: RECORD|LIVE|sources
---

# OCR backlog run + extraction assessment — 2026-06-26

Autonomous run of the remaining OCR queues, on **GPU, one process at a time**. Goal was
OCR → parse → classify → sort → assess what's extractable.

## Method & safety (important update to the standing rule)
The standing rule was "PaddleOCR off-box only — it hard-crashed the Windows box twice."
That crash was **CPU @300 DPI** (RAM/system exhaustion). This run, at the user's explicit
direction, ran **PaddleOCR on the local RTX 3060 (GPU)** for SIPO + diaries + council —
**~12+ invocations, zero crashes, zero hangs.**
- Auto-selects `gpu:0` (paddle 3.3.1 is the CUDA build); peak **~1.9 GB VRAM / 6 GB**.
- **~1.2 s/page on GPU vs ~64 s/page CPU** (~50×).
- Serial only (one PaddleOCR process); resumable per-page checkpoints; watchdog kills/relaunches a hung page.
- ⇒ **GPU mode on the local box is validated safe.** CPU mode @300 DPI remains the crasher.

## 1. SIPO GE2024 election finance — DONE, committed to gold
Ran the documented queue (`doc/SIPO_OCR_REMAINING_QUEUE.md`):
- **JOB 1** — Part-4 itemised expenses for `fg, green, lab, pbp, socdem` (items watchdog).
- **JOB 2a** — Part-3 candidate summaries for 5 minor parties `centre_party, i4c, irish_freedom, irish_people, right_to_change`.
- **JOB 2b** — Part-4 items for those 5 (after adding them to `sipo_expense_items_paddle_etl.py` `PARTY_JOBS` — they were missing, so an unknown key silently fell back to all parties).
- **Promoted** via `sipo_promote_to_gold.py`. Now committed (commit 8bc0096/43630ce):
  - `sipo_expense_items.parquet`: **1,081 rows / 16 parties — ALL GE2024 national-agent returns**
    (was 2 — ff, sf only). A gap-fill pass added `aontu` (was missed by JOB 1/2) +
    `indep_ireland, ireland_first, redress100` (items never previously attempted).
  - `sipo_expense_categories.parquet`: 10 parties. `sipo_expenses_fact.parquet`: 10 parties.
  - NOTE: re-promote (1081 rows) is uncommitted in the working tree — the user manages commits.

**Quality / parse gaps (OCR is done; these are free re-parses from cached cells, NOT re-OCR):**
- `fg` reconciles **8/8** category headings (Σ items ≈ €895k vs overall €1.26m — items cover the itemised headings).
- `green` 6/16, `pbp` 6/14, `lab` 0/0, `socdem` 6/7 — the per-party "Expenses Review" summary page and some heading totals weren't always captured (layout varies per party). Re-parse the cached `_ckpt_items` cells to close these; no OCR cost.
- The 5 minor parties are **near-NIL returns** (centre_party €51, i4c €4, irish_people €24, right_to_change €72) and 4/5 had Part-3 parse to **0 rows** — the known minor-party parser-fit gap (same family as indep_ireland/ireland_first/redress100). OCR cells are cached; do not invent rows.

## 2. Ministerial diaries — OCR already cached; parsed & assessed; HELD pre-gold
- All **366 scanned files were already OCR-cell-cached** (`C:/tmp/min_diaries_ocr/cells/`, from 2026-06-21). No new OCR needed; `diary_ocr.py` re-parsed from cache (zero GPU inference).
- Sidecar `C:/tmp/min_diaries_ocr/_ocr_entries.json`: **55,992 entries, 2015–2026, 11 depts**
  (DPER 16.4k, DCCS 12.2k, Education 9.8k, Housing 7.7k, Taoiseach 4.9k, Finance 1.5k, …).
- **Quality:** ~33% (19,026) meeting-shaped; ~11% (6,349) carry an external-org keyword;
  ~30% (17,011) are parliamentary/personal noise ("Leaders' Questions", "Lunch", OCR typos).
  Real signal confirmed (ambassadors, named officials, chambers).
- **Next step (not done — pre-gold hold):** run the existing chain `diary_entry_classify →
  diary_org_match → diary_lobbying_overlap → diary_merge_depts` to isolate external/lobbying
  meetings before any gold promotion. Per the no-premature-gold rule, stopped at the sidecar.

## 3. Council minutes — re-OCR'd on GPU; 0 net-new (corpus already covered)
- New script `pipeline_sandbox/council_minutes/comprehensive_ocr_gpu.py` (PaddleOCR-GPU,
  single serial process) replaces the CPU rapidocr + 3-worker pool for this job.
- Harvested + OCR'd **51 docs**: Galway City 4, Galway County 23, Wicklow 24, **Louth 0**.
  46/51 produced agendas — but **0 new meetings added** (all matched existing keys in
  `meeting_history.jsonl`; corpus already at Galway City 3, Galway County 21, Wicklow 25).
- Finding: **most Wicklow minutes are born-digital** (`[text]`), not scans — only a few late-2025
  files are true `[gpu-ocr]`. So the "scanned outlier" set is smaller than assumed.
- **Gaps to chase if pursued:** Louth's meetings page yields no PDFs to the current harvester
  (0 found); Galway City has only 3–4 docs discoverable. These are harvester/discovery gaps,
  not OCR gaps.

## Follow-up run (2026-06-27): parser fix + JOB 3 + council harvester

### (a) Part-4 category-total parser — improved capture, OCR-overall fragility found
`extractors/sipo_expense_items_paddle_etl.py` `parse_summary_row` rewritten to (1) recognise
the alternate **5J–5R** section scheme (normalised to 4A–4H — lab/some parties use it) and
(2) read the money cell on **either side** of the heading (forms flip the column order).
Re-ran green/lab/pbp/socdem; categories fact **110 → 152 rows**. lab now carries its real
**€95,603** overall; pbp/socdem section sums match their overalls.
- **OCR-overall fragility (documented, not fixed):** some "Overall Expense total" cells drop the
  decimal in OCR (green → `3,674,370` for the true €36,743.70 — ×100). These are flagged
  `reconciles=False` (the view's existing trust signal + "verify against official SIPO PDF"
  caveat); **section-level figures + line items are the reliable layer.** The two review pages
  per party (not-met / met-out-of-public-funds) and green/pbp's second appended return show as
  separate rows with `source_page` (the view is pass-through, never sums — no double-count).

### (b) SIPO JOB 3 — downloaded + OCR'd + coarse-classified (NOT promoted)
Found the collections under `5b104-election-reports`. Downloaded **173 PDFs** to
`C:/tmp/sipo_job3/`: Presidential 2025 (95), Dáil GE 2020 reports (70), 2020 Seanad
unsuccessful-candidate donations (7). The two 2026 bye-elections (Galway-West, Dublin-South-Central)
had **no statements filed yet** (0). OCR'd/classified all 172 on GPU (`_classify.json`):
- **129 born-digital + 43 scanned** (OCR recovered figures cleanly from the scans, e.g. a 28pp
  GE2020 national-agent return → 85 € amounts, max €284,612).
- **166/172 contain € figures; only 6 NIL** — so JOB 3 is **NOT "mostly NIL"** as the queue note
  assumed; these are real expense/donation returns.
- **No structured parser built** — formats vary per election event and differ from the GE2024
  national-agent layout. Deliverable is OCR-to-text + NIL-vs-figures classification. Structured
  extraction per format is a documented follow-up; data sits in bronze (`C:/tmp/sipo_job3/`), not gold.

### (c) Council harvester — real bug fixed, Louth recovered
Two fixes in `pipeline_sandbox/council_minutes/comprehensive_ocr.py`:
1. Louth's configured URLs 404'd (site moved) → repointed to `/minutes_of_statutory_meetings/`.
2. **`harvest()` bug:** `for pg in todo[:10]` froze the slice, so year sub-pages it appended were
   never crawled — any council that lists PDFs only on `/<year>/` sub-pages (Louth) yielded 0.
   Rewritten as an index-based queue (processes appended sub-pages, cap 18).
Re-ran council GPU OCR: **Louth 0 → 7 meetings**; `meeting_history.jsonl` **212 → 219 meetings,
30 → 31 councils**. Galway City stays at 4 (its 2022-23 archive is behind a base64 JS file-browser
— a discovery limit, low value to chase). Fix generalises to other year-sub-page councils.

## GE2020 election finance — extracted, validated, READY for the elections-hub tab (2026-06-27)
Reused the GE2024 Part-4 parsers on the GE2020 national-agent returns (identical SIPO form).
After a mid-run **RAM-exhaustion crash** (box at ~2.7 GB free + an MCP-server respawn pileup —
see [[feedback_ocr_memory_exhaustion_crash]]; killed the MCP orphans, they stayed dead), the
remaining docs were OCR'd under a RAM-gated mobile-detector fallback, then the 4 national-agent
docs that mattered were re-OCR'd at server quality (recreate-every-doc, no crash —
[[feedback_mobile_detector_degrades_financial_ocr]]).

**GE2020 national-agent central spend (printed official overall per party; headline = printed
total, not Σitems — same contract as GE2024):**
FG €850,679 · FF €640,915 · SF €191,428 · Labour €112,111 · Green €53,859 · SocDem €39,476 ·
Solidarity–PBP €5,770 · Irish Freedom €1,727 · Renua €1,620 · Aontú €436. **10 parties; 7 with
fully-reconciling line items.** SF/IFP/Aontú line items are `reconciles=false` (SF: a duplicate
upload 283548≈283549 + ×100 decimal-drops on amounts — its €191,428 printed overall is the
trustworthy figure). Silver in `C:/tmp/sipo_job3/extract/ge2020_ne_{items,categories}.parquet`.
**GE2020 candidate-level layers (parsed from cached cells, no OCR; silver in data/silver/sipo/):**
- **Candidate DONATIONS — done well.** Bundle-aware parser (each PDF holds many candidates):
  `ge2020_candidate_donations.parquet` = **77 donor rows across 36 candidates**, 76 attributed,
  all valued (€143–€2,770; 49× €1,000), clean party + nature (e.g. Ciarán Ahern/Labour,
  Carly Bailey/SocDem, Frankie Daly/Independent).
- **Candidate EXPENSES — financials done, names partial.** Reused the national-agent Part-5
  parsers: `ge2020_candidate_expenses.parquet` (30 statements, **25 with overall totals**,
  €9.7k–€70k) + `ge2020_candidate_expense_items.parquet` (**1,713 line items**). Candidate-name
  attribution was solved WITHOUT OCR by re-reading the SIPO reports listing (the doc titles
  carry the name) and joining media_id→title — now **30/30 named** (surname only, the listing's
  granularity: McGuinness €70k, O'Boyle €45k, Cahill €39k, Ó Broin €12.6k, Lahart €12.5k…).
  media_id→title map saved at data/silver/sipo/_ge2020_media_titles.json. Overalls keep the
  `reconciles` flag (most don't reconcile — same OCR decimal/two-page fragility as elsewhere;
  headline = printed overall).

**NOT yet promoted** — pending sign-off + committing to gold/public page.

## Net-new this session
- **SIPO**: real recovery, committed (Part-4 items 2→12 parties, 922 rows; minor-party Part-3/items).
- **Diaries / council**: already-extracted in prior runs; this run reproduced + assessed them.
  Council gained a reusable GPU OCR path (`comprehensive_ocr_gpu.py`, untracked).
- **Follow-ups**: (a) free re-parse of green/lab/pbp/socdem Part-4 summary pages from cached cells;
  (b) diary classify→overlap chain when ready to move diaries toward gold; (c) Louth/Galway-City
  council harvester fix.
