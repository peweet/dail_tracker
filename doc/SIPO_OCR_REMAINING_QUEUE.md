# SIPO OCR — remaining work queue (run OFF-BOX, one at a time)

> ✅ **JOB 1 + JOB 2 COMPLETE — 2026-06-26.** Ran on the local **GPU** (single process, zero
> crashes; see doc/OCR_RUN_ASSESSMENT_2026_06_26.md + [[feedback_paddleocr_crashes_local_box]]).
> Promoted to gold (commit 8bc0096/43630ce): `sipo_expense_items.parquet` now **922 rows / 12
> parties** (was 2). Remaining: **JOB 3 still PARKED** (needs go/no-go). Open follow-up (NOT OCR,
> free re-parse from cached `_ckpt_items`): green/lab/pbp/socdem Part-4 "Expenses Review" summary
> pages + some heading totals weren't captured; minor parties are near-NIL and 4/5 Part-3 parsed
> to 0 rows (parser-fit gap — do not invent rows).

Lined up 2026-06-18. State verified against `data/silver/sipo/by_party/` checkpoints +
`data/bronze/sipo_candidate_expenses/_manifest.csv`.

## ⛔ Hard constraint — DO NOT run on the local Windows box
PaddleOCR @300 DPI has hard-crashed this machine **twice**. Run every job below on a
Linux box / CI runner / cloud GPU, then copy the produced parquet back and promote.
Never run two PaddleOCR processes at once (per-page checkpoints make every job resumable).

Per-job recipe:
1. Run the command on the off-box machine (one job, one process).
2. Copy `data/silver/sipo/by_party/*.parquet` (incl. `*_items.parquet`, `*_categories.parquet`) back.
3. After all OCR jobs: `python extractors/sipo_promote_to_gold.py` → rebuilds gold + views.

---

## ✅ Already done — DO NOT re-OCR
- **Candidate tier** (the big corpus): 1,021 OCR checkpoints cached, 607 expense + 426
  donation statements. Gold = 473 candidates + unquantified surface. The only 2 missing
  (`O'Flynn, Ken`, `Kennedy, Pat` donation PDFs) are **HTTP 403 on SIPO's server** —
  cannot be downloaded; not OCR-recoverable.
- **Born-digital parties** (`sf`, `aontu`, `national_party`): text layer, never need OCR.
- **`ff`, `socdem` Part-4**: OCR cells already cached.
- **`indep_ireland`, `ireland_first`, `redress100`**: OCR cells cached but parser returns
  0 rows → **PARSER fix, not OCR**. Do not re-OCR (re-running the same model gives the
  same cells). Tracked separately.

---

## JOB 1 — Part-4 itemised backfill (highest value, ~96pp)
Parties have Part-3 candidate summaries but no Part-4 line-item / category breakdown.
`socdem` Part-4 cells are already cached (parse-only — included so the watchdog parses it).

```
.venv/Scripts/python.exe extractors/_sipo_items_watchdog.py fg green lab pbp socdem
```
Produces `by_party/{fg,green,lab,pbp,socdem}_items.parquet` + `_categories.parquet`.

## JOB 2 — Minor party national-agent returns never OCR'd (~137pp, Part-3 + Part-4)
No checkpoints and no parquet for these 5. Part-3 first, then Part-4.

```
# Part-3 candidate summaries
.venv/Scripts/python.exe extractors/_sipo_watchdog.py centre_party i4c irish_freedom irish_people right_to_change
# Part-4 itemised
.venv/Scripts/python.exe extractors/_sipo_items_watchdog.py centre_party i4c irish_freedom irish_people right_to_change
```
Page counts: centre_party 29 · i4c 35 · irish_freedom 25 · irish_people 24 · right_to_change 24.
NOTE: these are minor/independent-coalition parties; expect small returns and possible
parser-fit issues (same family as indep_ireland/ireland_first/redress100). OCR first, then
assess parse separately — do not invent rows.

## JOB 3 — PARKED (not scoped, large, not yet downloaded)
Annual-disclosure series 2022–2025 (party/TD/Senator/MEP donations) + non-GE2024
Election-Reports events (Seanad byes, European 2024, Limerick mayoral 2024, Presidential
2025, Dáil byes). Hundreds of mostly-NIL scanned pages. Out of current GE2024 scope —
needs an explicit go/no-go before downloading or OCR'ing.

---

## After OCR
```
python extractors/sipo_promote_to_gold.py     # rebuild gold + auto-registered views
```
The Election 2024 page (`v_sipo_party_national_*`) already consumes these — only the OCR
moves off-box.
