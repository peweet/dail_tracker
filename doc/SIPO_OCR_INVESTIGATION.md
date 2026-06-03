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

## ⚠️ Headline correction (2026-06-03): the OCR ENGINE is the bottleneck, not the scan

A later check (rendering the scanned page to a high-DPI PNG and looking at it)
**overturns the "OCR quality is the hard ceiling" conclusion below.** The source
scan is *crisp*: the amount column on p.3 reads cleanly — `€ 17,844.78`,
`€ 856.51`, `€ 2,492.07`, `€ 889.10`, `€ 368.16` — every decimal point and comma
plainly present and legible. Yet the Tesseract/ocrmypdf text layer recorded those
same cells as `feireaa7e`, `85651`, `249207`. The killer example: the cell my
pipeline filed as unrecoverable garbage `feireaa7e` (Catherine Ardagh) is a
perfectly readable **`€ 17,844.78`** in the image.

**✅ VALIDATED (2026-06-03): PaddleOCR recovers what Tesseract destroyed.**
Installed PaddleOCR 3.6.0 (free, Apache-2.0, local) into `.venv`, rendered p.3 with
fitz, re-OCR'd the amount column (`pipeline_sandbox/probe_sipo_ocr_paddle.py`).
Result: **4/4 known-bad cells recovered**, decimals/commas intact, with per-cell
confidence — `€17,844.78` (conf 0.99; Tesseract gave `feireaa7e`), `€ 856.51`
(0.92; was `85651`), `€2,492.07` (0.94; was `249207`), `€ 889.10` (0.96). Every
other amount on the page read clean too. **Decision: build the enrichment on
PaddleOCR.** Two Windows gotchas locked in: (1) `PaddleOCR(..., enable_mkldnn=False)`
is REQUIRED — paddle 3.3.1's oneDNN/PIR path throws
`ConvertPirAttribute2RuntimeAttribute not support`; the plain CPU path works.
(2) The paddleocr/paddlepaddle dependency is ~heavy ML → it MUST live in a
pipeline-only extra, NEVER core deps (Streamlit Cloud syncs core from `uv.lock`).

**Implications — the rest of this doc UNDERSTATES what's recoverable:**
- The 29% "corrupt" and 15% "unrecoverable" amount buckets are **mostly OCR-engine
  failure on legible data, NOT a data floor.** All my cap-bound `/100` repair and
  the "irreducible decimal-loss" caveats are working around a *bad OCR pass*.
- **Yes, the tool is wrong/mis-configured.** Tesseract drops punctuation
  inconsistently and garbles some clean cells entirely. A stronger engine should
  push recovery from ~85% toward ~98–100% because the ink is all there.
- **Recommended OCR engines, ranked for this task (crisp scanned tabular form):**
  1. **Table-aware Document-AI** — AWS Textract / Google Document AI / Azure
     Document Intelligence. Return table CELLS + per-cell confidence; on a clean
     form like this they'd likely hit ~99% and make the geometry row-reconstruction
     AND the `/100` repair heuristics unnecessary. Data is public (SIPO publishes
     it) so cloud is acceptable; ~187 pages ≈ a few dollars. Needs an API key.
  2. **Local / free / private:** PaddleOCR (PP-Structure does table cells) or
     Surya — modern DL OCR, far better than Tesseract on this, no data leaves the
     box. pip install (pulls torch); no system binary, so arguably *easier* than
     the Tesseract install that proved painful.
  3. **Cheapest diagnostic:** re-run Tesseract properly — rasterise ≥400 DPI, DROP
     the aggressive deskew/clean, numeric whitelist on the amount column
     (`tessedit_char_whitelist=0123456789.,€`, PSM 6/7). Tells you settings-vs-engine,
     but turning `17,844.78` into `feireaa7e` suggests even tuned Tesseract lags.
  4. **Vision-LLM (Claude/GPT-4o/Gemini):** can read the page to structured JSON
     and apply cap/closed-set constraints in-prompt — but HALLUCINATION risk (it may
     "tidy" a number to a plausible-wrong value). Only with per-cell confidence +
     mandatory PDF verify; best as a second-engine cross-check, not sole source.
- **Keep the anchors as a validation layer.** The 43-constituency closed set and
  the cap-bound check are engine-independent QA — run them on top of *whatever*
  OCR engine to catch its residual errors. So the work below isn't wasted; it
  becomes the verifier, not the extractor.
- **De-risks the donations PDF:** if its scan is similarly crisp (spot-check it),
  the 105-page donations file is likely cleanly extractable with a good engine —
  better odds than implied below.

Everything below predates this correction; read it as "what the *current bad OCR
pass* yields + how to squeeze it", with the engine swap as the real #1 lever.

## Findings (2026-06-02) — investigation worked end-to-end

Four throwaway probes added (all `pipeline_sandbox/`, read-only, nothing wired in):
`probe_sipo_ocr_text.py` (text-layer coherence), `probe_sipo_ocr_geometry.py`
(row reconstruction), `probe_sipo_ocr_columns.py` (column-banded recovery),
`probe_sipo_ocr_repair.py` (cap-bounded amount repair — see "Reducing data loss").

**Q1 — Does the text layer beat `find_tables`? YES, decisively.**
`page.get_text("text")` on the OCR'd FF expenses PDF (45pp, 2,524 non-empty
lines) is **0.0% char-fragmented** vs the per-character shatter from
`find_tables(strategy="text")`. `find_tables` was the wrong tool, confirmed.
*But* the line stream de-aligns rows (name / constituency / € on separate lines).
The fix is **word geometry**: `get_text("words")` → cluster by y-coordinate →
rows reconstruct left-to-right as `Index Name Constituency Cap Expenditure`.

**Q2 — What % of candidate-expense rows are cleanly recoverable?**
The candidate-summary table is **pages 3–10** (~64 rows). With two anchors:
- **Constituency: ~100%.** A *closed set of 43 constituencies* (from
  `data/gold/parquet/ec_constituency_pop_2022.parquet`) + difflib fuzzy match
  repairs even brutal OCR: `way West`→Galway West, `javan Monaghan`→
  Cavan-Monaghan, `jun Laoghaire`→Dún Laoghaire. This is the hero of the method.
- **Candidate name: ~85–90% legible** but OCR-noisy (`Charlle McGonalogue`=
  McConalogue, `Maicoim Byrne`=Malcolm). No closed set → needs registry match (Q4).
- **Expenditure €: 66% clean / 29% corrupt / 5% missing.** When the 2-decimal
  survives it parses perfectly (`9,414.48`, `17,043.88`). The OCR ceiling is
  **decimal-point loss**: `€ 85651` is really €856.51, `€249207`→€2,492.07,
  `1558062788`=garbage. The **statutory cap** (3-seat €15,560 / 4-seat €19,440 /
  5-seat €23,340, derivable from `td_seats_2024`) gives a validity bound — any
  spend > €23,340 is provably corrupt and gets flagged. The repeating round
  figures in the data ARE these caps (the "amount assigned to the party"
  column), not noise. **Update: the cap does more than flag — it also *validates
  a repair*, see "Reducing data loss" below, which cuts the amount loss to ~15%.**

### Reducing data loss — cap-bounded amount repair (`probe_sipo_ocr_repair.py`)

The 29% "corrupt" amount bucket is mostly *recoverable*, once you split it by
failure mode (per-token, NOT regex over the joined row — spaces merge the cap and
spend columns and break parsing):

- **Parser-hidden decimals** — a real 2-dp figure buried under leading OCR noise
  (`e3560.63`→3560.63, `‘927.04`→927.04, `e9,166.52`→9166.52). Deterministic
  strip → these become *clean*. This is the bulk of the recovery and is safe.
- **Dropped/misplaced decimal** — `85651`=€856.51, `249207`=€2,492.07, `4538`=€45.38,
  `8.41487`=€8,414.87. SIPO always reports to the cent, so `digits/100`
  reconstructs it. **Two hard guards make this safe-ish:** (a) the value must land
  in `(0, €23,340]` — the legal cap — or it's rejected; (b) require ≥4 digits, so
  stray garbage like `feireaa7e`→`7` can't masquerade as `€0.07`.
- **Genuine garbage** (`jeast25`, `SATS OT`, `1558062788`) — unrecoverable; the
  only lever left is a higher-DPI / form-tuned re-OCR (blocked: no Tesseract).

Measured on pages 3–10 (n=67 constituency-anchored rows):

| bucket | rate | display policy |
|---|---|---|
| clean (explicit 2-dp, deterministic) | **58%** | show as-is |
| reconstructed (`/100`, cap-validated, FLAGGED) | **27%** | show only with "≈, OCR-reconstructed — verify vs PDF p.N", or suppress the number |
| unrecoverable (true OCR loss) | **15%** | suppress; link to source PDF |

**Net: expenditure data loss falls from ~34% (29% corrupt + 5% missing) to ~15%.**

Honesty caveats on the *reconstructed* band (why it must stay flagged, never a
bare figure):
- ~2/18 reconstructions are probably wrong — the real spend token was garbage so
  the parser fell back to a stray leftover number (`<3340 eioaeaoa`→33.4). The
  flag is the safety net.
- Even a *plausible* `/100` repair assumes OCR dropped ONLY the decimal. If it also
  dropped a digit, the magnitude is wrong. This uncertainty is irreducible.
- Project posture (verifiable-facts-only): the safe shippable surface is the **58%
  clean** figures as real numbers + the 27% reconstructed shown as "amount on
  record — verify, p.N" (number optional/approximate). Reconstruction is best
  treated as a completeness/QA aid, not a licence to publish uncertain € figures.

### Do the recovered numbers make sense? (`probe_sipo_ocr_analysis.py`)

Ran sanity cross-checks on the 57 recovered rows. They *mostly* hold up, but the
checks surfaced three real correctness issues — proof the data still needs the
"verify vs PDF" posture, not blind trust:

1. **Spend ≤ per-constituency cap: 56/57 pass.** The one failure is a *clean*
   read: Tipperary North (a 3-seat seat, €15,560 cap) shows €17,043.88 — over the
   legal limit. So even an explicit 2-dp read isn't guaranteed correct; it's
   flagged. (The repair guard uses the GLOBAL €23,340 ceiling; tightening it to
   the per-constituency cap — available whenever the constituency match is
   confident — would catch this at extraction time.)
2. **Independent cap cross-check: 88% agree (23/26).** *Framing correction:*
   column 1 is the amount the candidate **assigned to the party** (≤ their cap),
   NOT the statutory cap itself — many candidates simply assign their full limit,
   which is why it usually equals a cap. So assigned < max is VALID (Dublin
   Mid-West assigned €15,560 of a €23,340 max). Only assigned **>** max is
   impossible, and that pattern (a row reading €23,340 in a €15,560 seat) reliably
   flags a mis-assembled row.
3. **Constituency matching is high-RECALL but imperfect-PRECISION.** Match-score
   distribution over 65 rows: 40 exact (1.00) / 6 high (0.90–0.99) / 19 low
   (0.80–0.89). Errors live in the low band. **Confirmed bug:** the common word
   **"South"** (in Dublin South-X, Cork South-X, Kildare South, Tipperary South…)
   fuzzy-matches the short name **"Louth"** at exactly ratio 0.80, so "Louth"
   absorbed 9 rows when FF ran ~2 candidates there — ~7 are misread South-
   constituencies (`jublin South`, `ork South`, `Pubie South`). This is the single
   biggest correctness gap; it revises the earlier "~100%" claim to ~100% recall
   but lower precision on garbled rows. **The precision fix (tested):**
   - **DO:** skip a *single-token* window whose normalized text is a generic
     component word shared by many names — `{north, south, east, west, central,
     city, county, bay, mid}`. Keep the 0.80 cutoff. Verified: Louth 9→6,
     removes 2–3 clear errors, introduces **zero** false negatives.
   - **DON'T** also raise the single-token cutoff to 0.90 (the obvious first
     instinct). Tested: it kills 3 *correct* one-letter-OCR-slip matches —
     `Pouth`→Louth, `Ponegal`→Donegal, `Sligo-Loitim`→Sligo-Leitrim. The right
     discriminator is "is the query a generic shared word?", not the score.
   - **Remaining gap (deeper fix):** the blacklist is partial — ~3 deeply-garbled
     "South" rows still mis-snap because the cell itself is unreadable (`jublin
     south` can't reliably reach `Dublin South-Central`). The complete fix isolates
     the constituency COLUMN by x-band and fuzzy-matches the WHOLE cell at once
     (`jublin south` vs `dublin south central`, not `south` vs `louth`), and/or a
     token-SET match (row clustering scrambles order — `Limerick`/`County` land
     non-adjacent), with the cap cross-check (assigned > constituency max =
     impossible) as a backstop validator.
4. **Magnitudes are plausible.** Recovered spends €19–€17,160; total ≈€275k across
   57 (~€4,800 avg) — reasonable for national-agent candidate expenditure. The
   *reconstructed* band skews small (median €886 vs clean €5,099); benign
   explanation: sub-€1,000 values have no thousands-comma, so a dropped decimal
   destroys them entirely → they fall into reconstruction, while ≥€1,000 values
   keep the comma as a redundant cue → stay clean. So the skew is a sampling
   effect, though a dropped non-decimal digit can't be ruled out without the PDF.

### Analysis of the remaining 15% (the truly unrecoverable amounts)

All 10 rows dumped in full. The candidate NAME and constituency are still legible
in most (`Peter 'Chap' Cleere`, `Robbie Gallagher`, `Darragh O'Brien`,
`Sandra Murphy Kelleher`); it is ONLY the amount column that is garbled:
`feireaa7e`, `jeast25`, `jesaets2`, `SATS OT`, `\eto3tat`, `eer`, `e7at2 8s`,
`(es o7t92`, `EanteTS500`, `1558062788`. After removing the statutory-cap digits,
**no genuine spend digits survive** — the only ≥3-digit runs left are tails of the
cap itself (`440`, `340`, `560`), so a looser regex would *invent* numbers, not
recover them. The lone concatenation (`1558062788`) could *speculatively* be cap
15,560 + €627.88, but that is a guess that needs the PDF.

**Key reframing:** the 15% are NOT lost rows — they are rows with an unreadable
amount. Candidate + constituency + source link remain, so they ship as
*"amount unclear — see official SIPO PDF p.N"*, not dropped. The only lever to
recover the actual € figures is a higher-DPI / form-tuned **re-OCR** (blocked:
no Tesseract in the venv). This 15% is therefore the hard OCR-quality floor for
the current scan.

**Q3 — Is the donations PDF worth OCR'ing?** Structure unknown — it has **zero
text layer** (105 pages, never OCR'd). The FG expenses PDF (37pp) is also un-OCR'd.
Only the FF expenses PDF was ever OCR'd. OCR'ing either needs `ocrmypdf` +
Tesseract (NOT installed; the painful part). Donations is the bigger prize but
the bigger lift, and will likely hit the same decimal-loss ceiling.

**Q4 — Can names match the member registry?** Not yet attempted (no
`v_member_registry` join in a probe). The names are legible enough that
difflib/rapidfuzz against `unique_member_code` surnames + a confidence score is
very likely to work — same closed-set trick as constituencies, but the set is
all members not 43, so confidence thresholds matter more. Next concrete step.

**Q5 — Is single-election coverage enough to ship?** No. Only **one party, one
election** (FF GE2024 expenses) is OCR'd. A useful civic feature needs at least
all parties for GE2024, ideally the back-catalogue — each PDF = an OCR + verify
pass at the 66%-clean amount rate.

### Bottom-line judgement
- **Structure & linkage are highly recoverable** (candidate → constituency →
  source PDF+page ≈ 100% on the summary table). The closed-set anchor is robust.
- **The € figures are the gating risk.** A third are wrong (often by ~100×) if
  shown raw. Given the no-inference + source-first rules, the **only safe MVP**
  is: display candidate + constituency + source link + *clean amounts only*
  (explicit 2dp AND ≤ statutory cap), suppress/flag the rest as
  "amount unclear — verify against official SIPO PDF p.N". Never publish a raw
  OCR magnitude. A donation/expense figure is a fact to be verified, never
  evidence of influence.
- **Effort is Medium-High** because the amount ceiling forces either a better
  re-OCR pass (higher DPI / form-tuned PSM to save decimals) or per-figure manual
  verification, multiplied across parties × elections.
- **Recommended next steps, in order:** (1) prototype the name→`v_member_registry`
  fuzzy match with confidence (Q4) — cheap, high value for linkage; (2) try a
  higher-quality re-OCR of pages 3–10 to see if the decimal-loss rate drops
  before committing to manual verification; (3) only then decide whether to OCR
  the 105-page donations PDF.

## Related plan context

This is **Phase 4** of the SI/Corporate review (`see this session's review`).
Rated value High / cost Medium / risk Medium. It is a *separate* track from the
corporate/SI hardening probes (`pipeline_sandbox/probe_cro_corporate_join.py`,
`probe_corporate_actors.py`, `probe_entity_quality.py`,
`probe_cross_enrichment.py`) run in the same session.
