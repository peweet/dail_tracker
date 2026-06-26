# Council minutes — quality assessment & recovery plan (synthesis)

Status: **assessment, sandbox only — NOTHING promoted to gold** (per owner instruction 2026-06-22).
Hand-synthesised from the deterministic consolidation (`QUALITY_ASSESSMENT.md`) + recovery sources
already web-located. (The Ultracode multi-agent pass was launched but every agent failed on the
account session limit — reset 22:50; the verification/recovery findings below are therefore the
deterministic equivalents, not agent output. Re-run `quality_workflow.js` after the reset to layer
LLM verification on top.)

## Headline
- **308 unique recent (2024+) docs** harvested across 31 councils; **150 clean (49%)**, **158 quarantined**.
- **~15 councils have usable clean minutes now**; **3 are scanned (OCR-pending)**; **~13 need a corrected
  seed or a CMIS scraper**.
- **Per-member named votes: Carlow only, validated** — 185 vote rows (dedicated `member_votes.jsonl`);
  72 in the clean-only consolidated set. All other councils minute votes as proposer/seconder + outcome.

## Council tiers (what we have / what's needed)

**Tier 1 — clean minutes extracted now (born-digital, fitz; HTML for Clare):**
Cork City 34, Donegal 20, Monaghan 19, Kerry 16, Kilkenny 14, Kildare 13, Waterford 12, Laois 10,
Carlow 3(+votes), Clare 3, Leitrim 2, Cavan 1, Cork County 1, Offaly 1, South Dublin 1.
→ Agenda items, attendance, motions (proposer/seconder), decisions, resolutions, dates all extract.

**Tier 2 — scanned, recoverable via OFF-BOX GPU PaddleOCR (the 73 `scanned_not_ocr`):**
Galway City 40, Galway County 11, Louth 12, + scanned Cork City LCDC committee docs. Source is fine;
only OCR is missing. Highest-volume single recovery.

**Tier 3 — ModernGov / CMIS portals (need a bespoke scraper, not a PDF crawl):**
Dublin City (`councilmeetings.dublincity.ie` / `dublin.moderngov.co.uk`) and Dún Laoghaire-Rathdown
(`ecouncil.dlrcoco.ie`). Minutes sit behind `mgConvert2PDF.aspx?ID=` / `ieListDocuments.aspx?CId=&MId=`
links. Moderate effort; unlocks the two biggest urban authorities.

**Tier 4 — known PDF/HTML source, just need a corrected seed + harvest (0 docs today):**
Limerick (`limerick.ie/.../meetings` → `/sites/default/files/media/documents/*.pdf`), Meath (per-year
`council-meetings` pages), Mayo (`mayo.ie/.../meetings-agendas`), Tipperary (`tipperarycoco.com/sites/...`),
Westmeath, Wexford, Wicklow (HTML), Sligo (per-year `MeetingMainBody` pages), Longford, Fingal,
Roscommon. Mostly trivial — the v2 crawler's "filename must contain 'minute' + recent year" filter was
too strict for these.

## Quarantine triage (158 — for later review)
| reason | docs | disposition |
|---|---|---|
| scanned_not_ocr | 73 | **RECOVERABLE** — off-box GPU OCR (Galway×2, Louth, Cork LCDC) |
| low_text | 40 | **MIXED** — short genuine minutes (recover by lowering threshold/OCR) vs notices/stubs (exclude); needs per-doc inspection |
| not_minutes_standing_orders | 23 | correctly excluded (not minutes) |
| not_minutes_report_or_plan | 12 | correctly excluded (LECP/annual reports/financial statements) |
| extract_err_ConnectionError | 5 | **RECOVERABLE** — transient, retry |
| unrecognised_doctype | 4 | inspect |
| extract_fetch_fail | 1 | retry |
→ ~**80–100 of 158 are recoverable**; ~35 are correctly excluded non-minutes.

## Vote-attribution reliability
- **Carlow — reliable.** Roll-call mark is √ (U+221A); `fitz.find_tables()` recovers the
  `Member|For|Against|Abstain|Absent` grid. Deterministically validated: Feb-2026 motion = 4 For /
  10 Against / 4 Absent = the minutes' own "Result" line. 18 councillors, 185 attributed votes,
  motion text attached. Names roster-folded.
- **Candidates not yet extracted:** Kilkenny, Laois, South Dublin, Fingal show roll-call language but
  produced no per-member table (different layout or scanned) — needs a per-council parser check.
- **Structural ceiling:** Galway County, Kerry, Monaghan, Cork City record proposer/seconder + AGREED
  only — no named tally exists to extract.

## What this corpus can / cannot support
- **CAN now** (~15 councils, 2024–2026): meeting agendas, attendance lists, motions with proposer/
  seconder, decisions (AGREED/NOTED/ADOPTED/DEFEATED), substantive resolutions, dates.
- **CAN (Carlow only, reliable):** per-councillor named voting record.
- **CANNOT yet:** Galway/Louth content (OCR pending); 9–11 CMIS/unseeded councils; per-member votes for
  proposer/seconder councils (not in the source).

## Top 5 fixes to maximise quality (priority order)
1. **Off-box GPU PaddleOCR** on the 73 scanned docs → unlocks Galway City+County and Louth in full.
2. **Corrected-seed harvest** for the ~11 Tier-4 councils (loosen the "filename contains 'minute'"
   filter; use the per-council seeds in `council_seeds.csv`) → fast breadth, +7–9 councils.
3. **ModernGov scraper** (`mgConvert2PDF`) for Dublin City + DLR → the two biggest urban authorities.
4. **Extend vote attribution** to Kilkenny/Laois/South Dublin/Fingal; deepen Cork County archive
   (only 1 doc found — its monthly minutes live on a deeper year archive).
5. **Encoding + low_text** — run text through `shared/text_encoding.py` (fixes "O�Donoghue" mojibake);
   re-triage the 40 `low_text` (many are 1–2pp genuine minutes).

## Artifacts (all sandbox: `pipeline_sandbox/council_minutes/`)
- `corpus/<council>/*.txt` — extracted full text (clean docs).
- `meetings_clean.jsonl` / `quarantine/quarantine.jsonl` — classified docs (+ reason codes).
- `member_votes.jsonl` (185, Carlow full) / `member_votes_all.jsonl` (72, clean-only consolidated).
- `QUALITY_ASSESSMENT.md` (mechanical metrics) + this file.
- Extractors: `council_minutes_v2.py`, `council_minutes_consolidate.py`, `cork_precise.py`,
  `council_votes_extract.py`, `council_minutes_pipeline.py` (production fitz + PaddleOCR-GPU, off-box).
