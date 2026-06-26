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
