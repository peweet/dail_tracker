# Session resume prompts

**Written 2026-06-13** (Opus took over the in-flight threads after Fable was blocked by the US government).

Each block below is a self-contained prompt. Paste **one** into a fresh Claude Code session in this repo. Auto-memory (`MEMORY.md` + topic files) loads automatically, so the prompts stay tight and point at the canonical memory + `doc/` handoffs rather than repeating them. **Run threads in separate sessions** — the SIPO and diaries threads have documented multi-context collision / crash traps and must not share a context with each other.

> Coordination rule (learned the hard way — see `project_sipo_ocr`): only **one** context runs any PaddleOCR/heavy-crawl job at a time. Before launching OCR or a full crawl, confirm no other session is mid-run.

---

## 1. Ministerial diaries build  *(actively being resumed by Opus 2026-06-13)*

```
Resume the ministerial diaries build. Read doc/MINISTERIAL_DIARIES_BUILD_PLAN.md and
memory project_enrichment_round2_2026_06_12 first.

State: sandbox v1 shipped — data/sandbox/enrichment/ministerial_diary_entries.parquet
(14,935 DETE engagements) + ministerial_diaries_index.parquet (220 files). Registry
data/_meta/ministerial_diaries_sources.csv seeded (131 rows, 13/18 depts publish).
Extractor = pipeline_sandbox/ministerial_diaries_extract.py (single entrypoint, DETE
born-digital parse, DPER scans queued for off-box OCR).

DONE 2026-06-13 (this thread): Phase 5.1 entry classification module + golden test
(see pipeline_sandbox/diary_entry_classify.py — confirm before re-doing).

Recommended v1 cut = Phases 1-2-4-5-6 + Minister Activity page on born-digital depts,
Experimental badge. Next concrete steps in priority order:
  - Phase 2: formalise the 4 DETE regex layouts into a named layout registry; burn down
    the 16 text_layout_unrecognised files; add golden-file parser tests.
  - Phase 1: extend the crawler for the ~3 new born-digital depts (Housing 153 PDFs,
    Finance publication pages, Justice/Transport/Climate) — hand-seed from the registry
    CSV, never URL-template (Health/Education use custom slugs). Add content_sha256 +
    page_kind ③ (publication-page→asset hop).
  - Phase 4: minister identity spine (member_code). Check what backs the MCP
    who_was_minister / current_cabinet tools FIRST and reuse if it's a gold table.

TRAPS: suffix-stripping collapses "Insurance Ireland"/"Chambers Ireland" to one generic
token → matches minister surnames + placenames (need surname+placename guards before
display); surname-only minister join collides (Higgins/Burke fan-out) → identity spine
mandatory pre-ship; coverage asymmetry is a fairness risk → never cross-dept "most
meetings" rankings while coverage is partial; diaries are self-curated + non-exhaustive
(carry the DETE caveat verbatim); no-inference (a diary meeting is NOT a lobbying return).
DPER OCR is OFF-BOX only (PaddleOCR crashes this Windows box — feedback_paddleocr_crashes_local_box).
```

---

## 2. Enrichment → gold promotion + views

```
Resume the enrichment-round-2 gold promotion. Read memory project_enrichment_round2_2026_06_12
and doc/IDEAS.md §9b first.

State: 4 extractors shipped to data/sandbox/enrichment/ (NOT yet gold/views/UI):
  - eu_tam_ireland_awards.parquet     15,593 IE state-aid awards 2016-2026 (cro_company_num
                                      6-digit, ~36% → clean CRO join 99% valid)
  - cbi_enforcement_actions.parquet   140 actions, 112 fines (BOI €100.5m / AIB €83.3m validated)
  - isif_portfolio.parquet            213 investee cards (~28% state an amount)
  - ministerial_diary_entries.parquet (owned by the diaries thread — leave it)
Extractors: pipeline_sandbox/{eu_tam_ireland,cbi_enforcement,isif_portfolio}_extract.py.
Matching probe pipeline_sandbox/probe_enrichment_matching.py proves the join rates.

Task = Phase 8-style gold promotion for TAM/CBI/ISIF:
  - Move/promote to data/gold/parquet via a pipeline chain (save_parquet convention,
    --dry-run support, freshness.json entry, source-health registry rows).
  - LOCK the value taxonomy before any merge: TAM value_kind=grant_awarded (a CEILING,
    NEVER summable across the realised-spend public_payments_fact — see
    project_procurement_ted_overlap). CBI fine_amount = realised penalty (distinct kind).
  - Registered SQL views (dependency-first registration — feedback_sql_view_dependency_order)
    + SQL-contract tests (never-negative, date bounds).
  - REFINE the TAM individual-flag heuristic BEFORE any display — it over-flags at 46%.
  - PII: TAM/CBI name private individuals in places — apply the leak-guard + personal-
    insolvency-privacy posture (feedback_personal_insolvency_privacy).

No UI yet — gold + views + contracts only. /shape the page separately afterwards.
```

---

## 3. State Boards register — identity re-curation

```
Resume State Boards identity curation. Read memory project_stateboards_register first.

State: register BUILT (extractors/stateboards_roster_extract.py → silver
stateboards_roster.parquet 2,061 seats + stateboards_boards.parquet 196 boards; gold =
silver + curated identities; views v_stateboards_roster / v_stateboards_boards;
stateboards chain). Automated Wikidata name-matching was REMOVED (~25% wrong person,
"looks ridiculous" per user) — the ONLY identity source is the hand-curated CSV
data/_meta/stateboards_wikidata_curated.csv (66 names / 70 seats).

Task = work the re-curation candidate queue WITHOUT re-introducing auto-matching to gold:
  1. Run wikidata/stateboards_wikidata_enrich.py (un-wired, WDQS-only) to refresh
     data/bronze/wikidata/stateboards_candidates.csv (149 uncurated matched/ambiguous remain).
  2. HAND-REVIEW candidates; append only verified rows to the curated CSV with a
     curation_note reason; verify against gov.ie/press, not Wikidata alone.
  3. Re-run the extractor so gold picks up the new identities. NULL qid = "not curated",
     never "no match".

TRAPS: anon MediaWiki API 429s instantly → use bulk WDQS SPARQL (VALUES against
rdfs:label, 80/query); match preferred label ONLY, never skos:altLabel (alias collisions);
strip stacked honorifics iteratively; accent-exact labels lose recall (Seamus vs Séamus).
Do NOT let any automated match flow into gold/view unreviewed — that was the whole lesson.
```

---

## 4. SIPO OCR

```
Resume SIPO OCR work. Read memory project_sipo_ocr + doc/SIPO_EXTRACTION_BACKLOG.md +
doc/SIPO_PIPELINE.md FIRST (this thread runs in its own context window).

CRITICAL crash rules (non-negotiable):
  - NEVER run two PaddleOCR processes at once (crashed the machine + GPU driver).
  - Run OCR ONLY via the watchdog drivers, never the ETL directly; per-page checkpoints
    cache raw cells so parser fixes need NO re-OCR.
  - Before launching, confirm NO other context is running SIPO OCR (multi-context
    collision proven — a system-python twin appears in <50s). Only ONE context owns OCR.
  - PaddleOCR(enable_mkldnn=False) required on Windows; text_det_limit_side_len=1280
    (full A4 @300DPI segfaults the detector); paddle is a pipeline-only extra, never core deps.

State: SIPO graduated to production (extractors/sipo_*.py; gold
data/gold/parquet/sipo_{expenses_fact,donations}.parquet via sipo_promote_to_gold.py).
GE2024 party expenses + donations extracted. Remaining backlog (doc/SIPO_EXTRACTION_BACKLOG.md):
  - TIER 1: 8 scanned party returns in data/bronze/scan_pdf/ — add keys to PARTY_JOBS,
    run via watchdog, re-promote (pure reuse). Parser diagnosis 2026-06-07: indep_ireland
    3-row summary dropped by ≥3-row gate + constituency OCR garble below cutoff —
    RECOVERABLE with a small-return/NIL detector + constituency fuzzy-repair.
  - TIER 2: per-candidate Election Statements (~400-600 PDFs, sourced not OCR'd —
    data/_meta/sipo_candidate_expenses_sources.csv is the queue). Huge OCR load, scout first.
  - TIER 3: 2022→2025 annual-disclosure series (scanned, large, mostly-NIL). Floor = 2022.
  - Constituency precision fix (whole-cell match + cap cross-check) still partial.

PII: donation statements name private donors (legally published but personal data —
feedback_personal_insolvency_privacy + no-inference posture). Tell the user before any
OCR launch (crash risk) and get a go-ahead.
```

---

### Threads NOT included (already shipped / no open obligation)
State of the rest as of 2026-06-12, for reference: procurement redesign, commercial uplift,
GE2024 election hub, atomic-parquet migration, freshness canaries — all shipped. Pick those
back up only if you hit a regression.
