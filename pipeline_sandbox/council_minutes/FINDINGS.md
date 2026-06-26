# Council minutes → OCR → structure: findings

Status: **EXPERIMENTAL probe (2026-06-22).** Goal: from council meeting minutes, get an idea of
**agenda, voting history, and what is going on** in meetings, across all 31 local authorities.

## TL;DR
- Minutes are **published and harvestable** for most councils, but Galway's (and many others') are
  **scanned image PDFs with no text layer** → need OCR.
- OCR works well. A safe ONNX engine (CPU) already extracts attendance, full agendas, motions
  (proposer/seconder), and decisions cleanly. Production fidelity uses **PaddleOCR on GPU**, run
  **off-box** (PaddleOCR crashes this Windows box — `feedback_paddleocr_crashes_local_box`).
- **"Voting history" ceiling:** Irish council minutes record **motion + proposer + seconder +
  outcome (AGREED/NOTED/ADOPTED)** — NOT TD-style named for/against tallies. Named roll-call votes
  occur only when specifically requested (rare). So the realistic product is *what was decided and
  who moved it*, not a per-councillor voting record.

## Pipeline (built)
- `council_minutes_pipeline.py` — per PDF: **fitz** if the page has a text layer (no OCR), else OCR
  the page image. `--engine paddle-gpu` (production, off-box) or `--engine rapidocr` (safe local).
  Parses agenda items / motions / decisions / vote markers.
- `council_minutes_harvest.py` — discovers each council's minutes page, collects PDF links,
  classifies scanned-vs-text, and samples one recent meeting. Writes `minutes_sources.csv`.
- `council_domains.csv` — the 31-LA seed.

## Galway County — worked example (plenary, 27 April 2026, 32pp, fully scanned)
OCR'd to 48,328 chars. Extracted:
- **Attendance**: Leas-Cathaoirleach + members present + members online + named officials (CE
  L. Conneally, Directors of Services, Head of Finance, Meetings Administrator…).
- **Agenda — 14 items**, incl.: confirm minutes; **adopt the Unaudited Annual Financial Statement
  2025**; a **report to An Coimisiún Pleanála under s.37(E)(4) PDA 2000**; adopt **Annual Report
  2025**; conference attendance/authorisations; **Management Report April 2026**; **N6 Galway City
  Ring Road update**; CE business & correspondence; votes of sympathy/congratulations.
- **15 motions** in `On the PROPOSAL of Cllr X SECONDED by Cllr Y … AGREED/NOTED` form.
- **34 decision markers** (AGREED/NOTED/ADOPTED).
- **Substantive content surfaced**, e.g. a political resolution declaring the constituency a
  **"CETA-Free Zone"** opposing the Investor Court System (council to write to Ministers/Taoiseach);
  Grant Thornton named as the County/City Economic Development Strategy consultants.
- **Votes**: no named for/against roll-call in this meeting — only the procedural quorum line
  ("not less than 20 Members vote in favour, if required") and decisions recorded as AGREED. This is
  the typical pattern (see ceiling above).

Format note: every Galway minutes PDF sampled (county plenary, county MD, city, 2021–2026) is **100%
scanned images, zero text layer** → OCR mandatory for Galway.

## All-councils source map
See `minutes_sources.csv` (one row per LA: meetings page, # minutes PDFs found, sample classification
born-digital vs scanned, and sample agenda/motion/decision/vote counts). Summary table below.

**Full classify+extract run (`council_minutes_extract_full.py` → `meetings.jsonl` +
`council_classification.csv`, 2026-06-22).** Three groups:

**A. Minutes published + extractable NOW (born-digital / mixed → fitz, no OCR):** Carlow, Cavan,
Cork City, Cork County, Fingal, Kerry, Kilkenny, Laois, Leitrim, Longford, Monaghan, Offaly,
Roscommon, South Dublin (14).

**B. Scanned → need OCR (off-box PaddleOCR-GPU for the full pass):** Galway County, Galway City
(112 PDFs), Louth (old minute-books) (3).

**C. Auto-discovery miss → need a manual page seed (likely JS/ModernGov portals):** Clare, Donegal,
Dublin City, Dún Laoghaire-Rathdown, Kildare, Limerick, Mayo, Meath, Sligo, Tipperary, Waterford,
Westmeath, Wexford, Wicklow (14).

**Vote style (where actual minutes were sampled):**
- **NAMED roll-call votes** (the valuable case): **Carlow** — full tallies extracted (e.g. Feb 2026
  *4 For / 10 Against → Defeated*; Apr 2026 *11 For / 7 Against → Carried*). Also flagged: Fingal,
  Kilkenny, Laois, South Dublin, Galway City (roll-call language present).
- **Proposer/seconder + AGREED/NOTED** (no named tally): Galway County, Kerry, Monaghan, Cork City.
- **Unknown** = the auto-sample grabbed a non-minutes PDF (standing orders / agenda / plan): Cavan,
  Cork County, Leitrim, Longford, Offaly, Roscommon — re-sample real minutes to classify.

**Per-member attribution caveat:** Carlow's roll-call TABLE lists every councillor with a ✓/✗ per
column, but the mark glyphs render as `?` in the text layer → the aggregate result (N For / M
Against) extracts cleanly, but assigning each *named member's* For/Against needs a cell-geometry
parse (table cell x-coords) or OCR of the mark column. That's the next step to turn "vote tallies"
into a true per-councillor voting record.

**Extracted so far:** ~200 minutes PDFs across groups A+B → `meetings.jsonl` (one row per PDF:
status, agenda_items, motions, decisions, rollcall_votes, named_vote_results, motion_outcomes).

## CONSOLIDATED RESULT (2026-06-22) — see QUALITY_ASSESSMENT_ULTRA.md
All 31 councils harvested (recent/2024+), full-text extracted, classified, quarantined, quantified.
**308 docs · 150 clean (49%) · 158 quarantined · Carlow per-member votes validated.** ~15 councils
usable now (Cork City 34, Donegal 20, Monaghan 19, Kerry 16, Kilkenny 14, Kildare 13, Waterford 12,
Laois 10, Carlow, Clare, Leitrim, Cavan, Cork County, Offaly, South Dublin). Recovery tiers: 73 scanned
→ off-box GPU OCR (Galway×2, Louth); Dublin City + DLR → ModernGov CMIS scraper; ~11 councils → corrected
seed harvest. Full triage + per-council tiers + top-5 fixes in **QUALITY_ASSESSMENT_ULTRA.md**.
(The Ultracode verification workflow was launched but all agents failed on the account session limit
[reset 22:50]; re-run `quality_workflow.js` after reset to add LLM verification.)

## Per-member vote attribution — ACHIEVED (Carlow, validated)
`council_votes_extract.py` → `member_votes.jsonl` / `.csv`. The roll-call mark is **√ (U+221A)**,
not a "?" (that was console encoding) — so `fitz.find_tables()` recovers the full
`Member | For | Against | Abstain | Absent` grid and we attribute **each named councillor's vote**.
No OCR (born-digital).
- **18 councillors** (= Carlow's full membership) × **185 attributed votes** across 4 meetings;
  by vote: 129 for / 44 against / 6 abstain / 6 absent.
- **Validated**: Feb-2026 defeated motion resolves to exactly 4 For / 10 Against / 4 Absent =
  the recorded result. Every row carries the **motion text** (e.g. development-plan submission
  approvals — reserved-function votes).
- Cleanups applied: names folded to the council roster (fixes split-cell fragments); motion text
  carried across page-breaks. **Remaining nit**: apostrophe mojibake ("O�Donoghue") — run through
  the repo's `shared/text_encoding.py` on promotion.
- **Generalises**: the parser keys on a `For/Against` header, so it works for any roll-call council.
  Next: feed Kilkenny / Laois / South Dublin / Fingal minutes URLs (they show roll-call language).
  This is a genuine TheyWorkForYou-style councillor voting record for the councils that hold named
  votes — a real, unique dataset.

## Realistic build path
1. **Harvest** minutes PDFs per council (this harvester, generalised + paginated).
2. **Extract** with `council_minutes_pipeline.py`: fitz for born-digital councils (free, fast),
   **PaddleOCR-GPU off-box** for scanned councils (the bulk).
3. **Parse** to a structured record per meeting: date, attendance, agenda items, motions
   (proposer/seconder/outcome), substantive resolutions. *Not* a per-councillor vote tally — state
   that limitation honestly.
4. Yield is high for **agenda + decisions + who-moved-what + substantive resolutions**; low for
   **named individual votes** (structurally rare in Irish council minutes).
