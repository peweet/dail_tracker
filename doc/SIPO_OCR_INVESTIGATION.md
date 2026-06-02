# SIPO political-finance OCR extraction — investigation kickoff brief

> **Status:** investigation, planning/prototyping only. Sandbox rule applies — all
> new extraction code goes in `pipeline_sandbox/`, nothing wired into
> `pipeline.py` / `enrich.py` yet. This brief is written so a fresh context
> window can resume cold.

## Goal

Recover **usable, structured, source-linked** data from scanned SIPO
political-finance PDFs (general-election candidate **expenses** and party
**donations**), good enough to consider a future `pipeline_sandbox/` extractor
and eventual gold view. SIPO publishes these only as **scanned image PDFs**, so
OCR is mandatory. The user has already tried this (installed Tesseract; found it
"very complicated").

Output we ultimately want, per the review plan (Section F-B / Phase 4):
- candidate / party, constituency, expense or donation amount, year/election,
  heading (advertising / publicity / etc.), source PDF + page.
- Heavy caveats: OCR-derived → "verify against the official SIPO PDF"; never
  imply influence from a donation.

## What already exists in the repo (verified 2026-06-02)

**Source PDFs** — `data/bronze/scan_pdf/`
- `2024_election_donations.pdf` (17 MB) — **donations** data. **NOT yet OCR'd.**
  Arguably the higher-value prize (who gave money to whom).
- `fg_sipo_ge_2024_expenses.pdf` (12 MB) — Fine Gael GE2024 expenses. Source only.
- `target/` — held an `ff_sipo_ge_2024_expenses.pdf` (Fianna Fáil) that WAS OCR'd.

**OCR output** — `data/bronze/scan_pdf/output/`
- `ff_sipo_ge_2024_expenses-ocr.pdf` (3.3 MB) — **already has a text layer**
  (ocrmypdf ran previously). This is the artifact to read FROM; you do NOT need
  to re-run OCR to make progress.
- `ff_sipo_ge_2024_expenses-tables1.csv … tables34.csv` — the prior extraction
  output. **These are the failed approach** (see below).
- `scan_pdf-ocr.pdf` (186 KB) — a tiny earlier test, ignore.

**Extraction code** — `experimental/test_read_scan_pdf.py`
- Pipeline: `ocrmypdf.ocr(deskew=True, force_ocr=True)` → `fitz.open()` →
  `page.find_tables(strategy="text")` → `tab.to_pandas()` → one CSV per table.

## Why the prior approach failed (the key lesson)

OCR itself **worked** — the signal is present. But `find_tables(strategy="text")`
**shattered the text into per-character cells**. Evidence from the output CSVs:

- `tables1.csv`: `P l e a s e c o m p le t e,i n B L O C,K C A P I T A L S` — form
  boilerplate, character-fragmented.
- `tables5.csv` (this one has REAL DATA, just garbled):
  - `fNi orma Foley,,~ Kerry,,ee,34723`  → *Norma Foley, Kerry, 34723*
  - `52. Pat the Cope,Gallagher,,,ao,"3,844.03"` → *Pat the Cope Gallagher, €3,844.03*
  - `Ba.Imelda Goldsb,oro,Tipper,ary South,"(615,560","€6,597.48"` → *Imelda
    Goldsboro, Tipperary South, €6,597.48*

So: candidate name + constituency + € amount **are recoverable**, but word-split
and with OCR noise (`fNi orma`, `Goldsb oro`, `Gallag her`). The table-detector
was the wrong tool for these scanned forms.

## The next thing to try (already scaffolded, NOT yet run)

`pipeline_sandbox/probe_sipo_ocr_text.py` is **already written** and ready. It
skips `find_tables` and reads the OCR'd PDF's **text layer directly**
(`page.get_text("text")`), then measures:
- text-layer coherence (% of char-fragmented lines) — is `get_text` cleaner than
  `find_tables`?
- monetary signal (count of `€`/money lines)
- samples the € lines and the raw first lines to judge OCR quality by eye.

**First action for the new window:** run it and read the verdict.
```
./.venv/Scripts/python.exe pipeline_sandbox/probe_sipo_ocr_text.py
```
- If the text layer is coherent (low fragmentation, many clean € lines): build a
  text-layer extractor (regex name / constituency / € amount per section), not a
  table extractor.
- If it's still fragmented: the OCR quality is the ceiling — consider re-OCR with
  better settings (PSM/segmentation tuned for forms, higher DPI rasterisation),
  or per-cell OCR of detected table regions.

## Environment notes (Windows)

- `./.venv/Scripts/python.exe` is the interpreter. **PyMuPDF (`fitz`) IS
  installed.** **`ocrmypdf` is NOT installed** in the venv (import fails) — fine,
  because the expenses PDF is already OCR'd. To OCR the donations PDF you'll need
  `ocrmypdf` + a Tesseract binary on PATH (the painful part the user hit).
- Force UTF-8 stdout in any probe (`sys.stdout.reconfigure(encoding="utf-8")`) —
  Windows cp1252 console chokes on accented names / €.

## Constraints / project rules that apply

- **Sandbox rule:** new Python extraction → `pipeline_sandbox/` only. SQL views →
  `sql_views/` only. Never touch `pipeline.py` / `enrich.py` in a probe.
- **No inference in app UI:** SIPO data, if surfaced, is verifiable facts +
  source link only. A donation is not evidence of influence.
- **Source-first + caveat:** OCR-derived numbers must carry a "verify against the
  official SIPO PDF (page N)" caveat and confidence.
- **Parquet writers** (if/when you persist): `compression="zstd"`,
  `compression_level=3`, `statistics=True`.

## Open questions for the investigation

1. Does the text layer beat `find_tables`? (run the probe)
2. What % of candidate-expense rows are cleanly recoverable from GE2024 expenses?
3. Is the donations PDF (`2024_election_donations.pdf`) worth OCR'ing — what's its
   structure (per-donor rows? thresholds)?
4. Can OCR'd candidate names be matched to `v_member_registry` / unique_member_code
   (link to Member Overview)? Surnames are OCR-noisy → fuzzy match + confidence.
5. Is single-election (2024) coverage enough to ship anything, or does it need the
   multi-year SIPO back-catalogue to be useful?

## Related plan context

This is **Phase 4** of the SI/Corporate review (`see this session's review`).
Rated value High / cost Medium / risk Medium. It is a *separate* track from the
corporate/SI hardening probes (`pipeline_sandbox/probe_cro_corporate_join.py`,
`probe_corporate_actors.py`, `probe_entity_quality.py`,
`probe_cross_enrichment.py`) run in the same session.
