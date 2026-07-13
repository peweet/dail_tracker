# OCR scoping backlog — scanned sources awaiting a go/no-go

Candidate scanned-only sources that *would* need OCR to extract. OCR runs **off-box only**
(PaddleOCR @300 DPI has hard-crashed the local Windows box twice — see
[SIPO_OCR_REMAINING_QUEUE.md](SIPO_OCR_REMAINING_QUEUE.md)). Each entry is scoped, not
committed: do the value/cost call before downloading or OCR'ing.

Already-scoped OCR queues live elsewhere — don't duplicate them here:
- **SIPO** election-finance returns → [SIPO_OCR_REMAINING_QUEUE.md](SIPO_OCR_REMAINING_QUEUE.md)
- **Ministerial diaries** historic scans → `data/_meta/ministerial_diaries_ocr_queue.csv`
  (built by `extractors/diary_build_ocr_queue.py`; historic scan layouts largely don't parse).

---

## STAGED & READY: council GPU-OCR batch (AFS + minutes)
Added 2026-07-13 (council-targeting execution). **Everything is downloaded and queued in
bronze/the sandbox quarantine — the only missing piece is the GPU run itself**, held for an
attended session because (a) the c:/tmp/gpu_ocr paddle venv no longer exists (multi-GB
rebuild), and (b) the AFS slice needs the SERVER detector (the mobile detector garbles
€-tables per the 2026-06 finding) which is precisely the RAM-heavy configuration that
crashed the box before. Rails when running: rebuild venv at c:/tmp (outside the project so
pip-sync can't strip paddle-gpu), ONE process, DPI 150, instance-reset-every-8, resume
support; server det for the AFS tables, mobile det acceptable for minutes prose.

**Queue (all cached on disk already):**
1. **Wexford AFS 2017–2024** — `data/bronze/pdfs/la_afs/wexford/*.pdf` (8 files, ~46pp each;
   2024 verified still scanned — Wexford did NOT go born-digital like Waterford/Laois did).
   The last AFS-dark county: OCR → text → the existing `parse_ie`/camelot chain + reconcile
   gate. Highest value of the batch.
2. **Louth AFS 2020–2024** — `data/bronze/pdfs/la_afs/louth/` (5 scanned files, 38–72pp) —
   un-stales Louth from 2017.
3. **Kildare (2018/20/22), Sligo (2018/20), Leitrim ("2026–28"-misnamed)** AFS backfill years.
4. **Scanned minutes, non-book: 957pp** — `pipeline_sandbox/council_minutes/quarantine/
   quarantine.jsonl` (Galway City 510pp, Galway County 242pp, Leitrim 171pp, Kilkenny/Laois/
   Westmeath small) → full minutes text (aggregate tallies, s.183 notices, resolutions — NOT
   named votes; Galway City records tallies only).
5. **Louth current signed minutes** — rolling ~120pp/yr (louthcoco.ie signed-minutes PDFs).
**Explicitly excluded:** the Louth minute-books archive (986pp/150MB, handwriting-era = HTR,
near-zero current-accountability value) and pre-2016 AFS years.

## Briefing Document for the Minister for Justice (incoming-minister briefs)
Added 2026-06-26.

**Source:** gov.ie publication page
<https://www.gov.ie/en/department-of-justice-home-affairs-and-migration/publications/briefing-document-for-the-minister-for-justice/>
→ 4 editions: `october-2021`, `november-2022`, `may-2023`, `january-2025`
(`https://assets.gov.ie/static/documents/briefing-document-for-the-minister-for-justice-<edition>.pdf`).

**Verified facts:**
- Jan 2025 edition = **108 pages, ~40 MB, fully scanned/image-only** (0/108 pages carry a
  text layer; 20 raster-image pages). So extraction = OCR, off-box.
- gov.ie pages **403 WebFetch** (datacentre-IP WAF); `assets.gov.ie` PDFs are fetchable via
  `curl` with a browser UA but **rate-limit to HTTP 403** intermittently — pull slowly/retry.
- Other departments' incoming-minister briefs (DETE, Education) appear **born-digital** in
  search — Justice's scan is the outlier.

**Assessment (recommendation: low priority / likely no-go for this doc):**
- Structured facts are **duplicated by cleaner born-digital sources we already have/scoped**:
  Vote allocations → REV + C&AG Appropriation Accounts; agencies-under-aegis → State Boards
  register + gov.ie org structure; spend → payments/procurement facts; legislative priorities
  → Government Legislation Programme + existing bills data.
- The genuinely **unique** content is qualitative (candid risk register, narrative framing) —
  context, not panel-ready facts, for a 108pp × 4-edition OCR job.
- Trips two known traps at once: scanned-layout OCR (fragile, like council-minutes/diary
  scans) **and** figures-better-sourced-elsewhere.

**Only framing worth revisiting:** the **cross-department class** of incoming-minister briefs
as a corpus (many born-digital, no OCR needed) mined for one thin artifact — a
`department → agencies-under-aegis` map — but only after confirming it fills a gap the State
Boards register doesn't already cover.
