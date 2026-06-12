# Ministerial Diaries — Build Plan (full extraction → gold → surfaces)

**Written:** 2026-06-12, immediately after the sandbox v1 ingest (`pipeline_sandbox/ministerial_diaries_extract.py`).
**Goal:** take ministerial diaries from a 2-department sandbox corpus to the **complete published record of who Irish ministers met**, joined to the lobbying register and member identities, surfaced as the spine of the planned **Minister Activity** page (IDEAS.md §12) and a new Member Overview tab.
**Why it matters:** the lobbying register records what lobbyists *say* they did; the diaries record what ministers' offices *say* happened. Nobody in Irish civic tech joins the two. This is exactly the cross-source-join ground `COMPETITIVE_LANDSCAPE.md` says we own.

Two inviolable rules apply throughout (IDEAS.md §1):
- **No inference in app UI** — a diary meeting is NOT a lobbying return; co-occurrence wording only, never "X lobbied Y".
- **Diaries are self-curated and non-exhaustive** — DETE's own header: *"The diary is used for planning purposes. It may not be a true reflection of all activities."* Every surface must carry this caveat verbatim or near-verbatim.

---

## 0. What exists today (sandbox v1, 2026-06-12)

| Artefact | Contents |
|---|---|
| `data/sandbox/enrichment/ministerial_diaries_index.parquet` | 220 diary PDFs from 13 listing pages (DETE + DPER), per-file `has_text_layer` / `parse_status` |
| `data/sandbox/enrichment/ministerial_diary_entries.parquet` | 14,935 engagements 2017-06→2026-02, grain = (date, time_slot, subject); DETE only |
| Parse state | 131/147 text-layer files parsed (4 layout generations); 16 `text_layout_unrecognised`; 73 DPER scans → `scanned_needs_offbox_ocr` |
| Top ministers | Varadkar 2,540 · Humphreys 2,079 · English 1,644 · Burke 1,338 · Calleary 1,236 |

Known v1 gaps: `minister` is a filename-derived surname string (1,094 entries null); no department dimension beyond DETE/DPER; no entry classification; no org matching; no identity join; no freshness automation.

---

## 1. Source landscape (probe-verified 2026-06-12)

There is **no central hub**. Each department publishes on its own page with its own conventions.

**Full 18-department sweep completed 2026-06-12** (URL-pattern probes × 4 paths + 5-query × 12-page gov.ie search sweep; raw in `c:/tmp/enrich_probe/dept_diary_*.json`). Result: **13 of 18 departments publish.** The registry (§3.1) is **already seeded** with the findings: `data/_meta/ministerial_diaries_sources.csv` (131 rows: 19 collections, 8 listings, 2 hubs, 97 publication pages, 5 documented non-publishers).

| Dept | Where | Scale | Format verdict |
|---|---|---|---|
| **DETE** | `enterprise.gov.ie` listing + gov.ie `collections/ministers-diaries/` mirror | ~150 PDFs, 2016→current | **born-digital**, ingested (≥4 layouts, all but 16 parsed) |
| **Housing** | gov.ie `collections/ministers-diaries/` | **153 PDFs** (largest single collection) | Phase 1 format probe |
| **Finance** | **97 per-month publication pages** (Donohoe back to 2018, MoS Troy series) + 4 collections (Jack Chambers etc.) | ~100 files | Phase 1 format probe |
| **DPER** | org-info hub + 10 year collections + 3 MoS collections | ~70 PDFs | **image scans** (every sample) → OCR queue |
| **Culture/Comms/Sport** | org-info `ministers-diaries/` | 48 PDFs | Phase 1 |
| **Rural/Gaeltacht** | `collections/ministers-diaries/` | 46 PDFs | Phase 1 |
| **Justice** | `collections/ministerial-diaries/` | 29 PDFs | Phase 1 |
| **Transport** | org-info `ministers-diaries/` | 25 PDFs | Phase 1 |
| **Climate/Energy/Env** | `collections/ministers-diaries/` | 21 PDFs | Phase 1 |
| **Health** | `collections/department-of-health-ministers-diaries/` (**custom slug** — URL guessing missed it) | unknown | Phase 1 |
| **Education** | `collections/department-of-education-ministers-diaries/` (custom slug) + publication pages from 2021 | unknown | Phase 1 |
| **Social Protection** | publication pages (Calleary, Humphreys, **Varadkar back to May 2016**) | ≥3 series | Phase 1 |
| **FHERIS** | org-info `ministers-diary/` | unknown | Phase 1 |
| **Non-publishers** (documented in registry as `none_found`): Agriculture, Children/Disability/Equality, Defence, Foreign Affairs, Taoiseach | — | — | absence is a displayable finding |

Corpus estimate revised: **~700–900 files** (≈3-4× the sandbox v1 corpus). Lesson encoded in the registry: Health/Education use custom slugs, so **hand-curated seeding from search, never URL templating**.

**Publication conventions worth recording now:**
- gov.ie asset URLs are non-templatable (`assets.gov.ie/static/documents/<hash?>/<name>.pdf`) — same as LGAS; crawl by href, never construct.
- Three *page* shapes feed PDFs: ① collection page → direct asset links (DPER), ② single listing page → relative `/publication-files/` links (DETE), ③ per-month publication pages (Finance, DSP) → needs a publication-page→asset hop.
- Filenames carry minister + period sometimes; page titles carry them more reliably (`Minister of State Robert Troy's Diary - June 2025`). **Harvest the link/page title alongside the URL** — v1 already stores `link_text`.

---

## 2. Target data model

Three tables, two grains, one identity bridge. All money-free; no value taxonomy needed. Privacy note: subjects occasionally name private individuals ("Meeting with [citizen]" is rare — diaries are pre-redacted by departments under FOI/GDPR before publication) — but add a leak-guard regex sweep (phone numbers, emails, home addresses) as a belt-and-braces gate before gold, mirroring the CPO/FOI guard pattern.

### 2.1 `ministerial_diaries_index` (file grain) — exists, extend
```
department, listing_url, file_url, file_name, link_text,
minister_guess, period_month_guess, period_year_guess,
n_pages, has_text_layer, n_entries_parsed,
parse_status ∈ {parsed, text_layout_unrecognised, scanned_needs_offbox_ocr,
                ocr_done, download_failed, unreadable},
layout_id (NEW — which registered layout parsed it),
content_sha256 (NEW — detect silent in-place re-issues, the DAIL-160s trap)
```

### 2.2 `ministerial_diary_entries` (engagement grain) — exists, extend
```
entry_date, time_slot, subject,                      -- as parsed, verbatim
department, minister_label,                          -- raw surname/title string
minister_member_code (NEW, nullable),                -- → member_registry
minister_role (NEW: minister | minister_of_state | taoiseach | tanaiste),
entry_class (NEW, §7.1), source_pdf_url, layout_id,
extraction_method (NEW: text_layer | offbox_ocr),
ocr_confidence (NEW, nullable)
```

### 2.3 `diary_org_mentions` (mention grain — NEW, §7.2)
One row per (entry × matched organisation). **Explosion-prone by design** — same trap as
`procurement_lobbying_overlap` (never count entries via this table without DISTINCT).
```
entry_id, matched_org_name, match_source ∈ {lobbying_register, cro, stateboards, public_body},
match_method ∈ {exact_norm, token_set, alias}, match_confidence ∈ {high, medium},
gazetteer_key
```

---

## 3. Phase 1 — discovery registry + crawler hardening (the "fully extract" core)

Reuse the **`publishers_seed` pattern** from procurement: a hand-curated, git-tracked seed CSV is the source of truth; the crawler only follows it.

1. **`data/_meta/ministerial_diaries_sources.csv`** — ✅ **DONE 2026-06-12** (131 rows from the full sweep, incl. 5 `none_found` departments; gitignore negation verified). Columns: `department, page_kind ∈ {listing, collection, hub, publication_page, none}, url, status, first_seen, notes`.
2. ~~Systematic sweep~~ — ✅ done (results in §1). Residual sweep work: re-run quarterly to catch new collections (the canary, §10); per-org gov.ie publications search proved useless (client-side rendering) — the global-search + URL-probe combination is the working method.
3. **Crawler generalisation** (`ministerial_diaries_extract.py` stays the single entrypoint):
   - Add page_kind ③ handling: publication page → asset link (one extra hop, gov.ie publication pages embed the asset `<a>` directly).
   - Per-source politeness unchanged (0.3s); cache-by-content-name in `C:/tmp/min_diaries_pdfs/` (already resumable).
   - `content_sha256` per download; if a cached URL's remote content changes, mark superseded and reparse (in-place re-issue trap).
   - Move the dept list out of code into the seed CSV. **Gitignore-negation check** — `data/_meta/*.csv` already has the negation rule (project_curated_meta_reference_files) but verify the new file is actually tracked.
4. **Acceptance gate:** registry covers every department of state (18 + Taoiseach's); each has `status ∈ {seeded, none_found}`; crawl produces an index where `download_failed = 0` on two consecutive runs.

Effort: ~1 day incl. probing Finance/Justice/DSP formats.

## 4. Phase 2 — born-digital parse completion

1. **Layout registry**: formalise the current 4 regex generations into named layouts (`dete_time_subject_2022`, `dete_dateheader_2025`, `dete_ordinal_dotted`, `dete_inline_subject`, …) each with a tiny matcher + parser; `layout_id` lands in both tables. New depts get new layout entries, not edits to old ones.
2. **Burn down the 16 `text_layout_unrecognised`** DETE files; expect 1-2 more generations (q1-2022 Troy files show date-only rows with no engagements — possibly legitimately empty days, needs eyeballing, may be `parsed_empty` not unrecognised).
3. **Finance/Justice/DSP parsers** once Phase 1 probes their formats. If Finance is Word-export tabular like DETE, reuse; if scanned, queue.
4. **Golden-file tests** (the repo's parser convention): one fixture PDF per layout in `test/fixtures/diaries/` + expected-entries snapshot; plus property checks (entry_date within the file's period_guess month/quarter; time_slot well-formed; no entry without date). Wire into the SQL-contract/pytest CI job.
5. **Quality counters in the index**: `n_entries_parsed` per file already exists; add a per-file `n_lines_unconsumed` so parser regressions show as a number, not silence.
6. **Acceptance gate:** ≥95% of text-layer files `parsed`; every parsed file's entry dates fall inside its labelled period; golden tests green.

Effort: ~1-2 days.

## 5. Phase 3 — the scanned tail (off-box OCR queue)

DPER (~73 files) + any scanned files other depts contribute. **Never OCR locally** (feedback_paddleocr_crashes_local_box — two hard crashes).

1. The index *is* the queue: `parse_status = scanned_needs_offbox_ocr` + git-tracked source CSV export (mirror `sipo_candidate_expenses_sources.csv` so the off-box worker needs only the repo).
2. Off-box PaddleOCR run (same harness as SIPO expenses; diaries are typed-text scans like the SIPO forms — Paddle handled those at ~98%); emit per-page text + confidence to silver parquet; copy back; a `diaries_promote_ocr.py` step merges OCR text into the same layout parsers with `extraction_method=offbox_ocr`, `ocr_confidence` carried per entry.
3. Closed-set validation analogues: day-numbers must be calendar-valid for the period; time-ranges must parse; reject pages whose date sequence is non-monotonic (OCR scramble detector).
4. **Acceptance gate:** DPER 2017-2026 entries exist with confidence ≥ threshold; flagged tail quarantined, counted, and visible in provenance (the SIPO verified-vs-flagged honesty model).

Effort: ~½ day on-box prep + one off-box OCR session.

## 6. Phase 4 — minister identity resolution

The whole feature's join value depends on `minister_member_code`.

1. **Build/extend a minister-tenure spine**: `(member_code, role_title, department, date_from, date_to)`. Check what already backs the MCP `who_was_minister` / `current_cabinet` tools first — if that's already a gold table, reuse it; if it's API-driven, materialise it (Oireachtas API has office-holder data; IDEAS §16 notes office-holders are partly outside RoMI, but cabinet membership itself is well-recorded).
2. Resolution order: `(department, period, minister_guess/link_text)` → tenure spine lookup → `unique_member_code` via `normalise_df_td_name` rules (project_td_name_join_key). Surname-only guesses disambiguate through the dept+date window, which is almost always unique.
3. Keep a hand-curated alias CSV for the stragglers (`Mitchell O'Connor`, hyphenations, MoS vs senior minister collisions within a dept-quarter).
4. **Acceptance gate:** ≥95% of entries carry `minister_member_code`; the unresolved residue is listed, not dropped.

Effort: ~1 day (less if the cabinet spine already exists as a table).

## 7. Phase 5 — enrichment (the analytical payoff)

### 7.1 Entry classification (deterministic, pipeline-side)
`entry_class` via ordered keyword rules — **pipeline-owned, tested, never UI-side** (logic firewall):
`govt_business` (Cabinet/Government Meeting/CC/pre-CC) · `oireachtas` (Dáil/Seanad/LQ/QPL/PQ/votes/committee) · `media` (interview/radio/doorstep/photocall) · `constituency` · `travel` · `external_meeting` (the residual that contains the interesting meetings) · `internal_dept` (officials/diary meetings/briefings). Rules in one module with a golden test set of ~200 hand-labelled lines. Misclassification is cosmetic (filtering), not factual — but publish the rule list in provenance.

### 7.2 Org-name matching (the user-requested fuzzy feature, IDEAS §9b 💡)
Match `subject` free text against a **gazetteer**, not open-ended fuzzy search:
1. Gazetteer tiers: ① lobbying-register org names (lobbyist_name + exploded `clients`) — the highest-value tier because a hit means *both sides of the record exist*; ② CRO company names (suffix-stripped); ③ state bodies/stateboards + public-body publishers (to *exclude* state-internal meetings from "company met minister" counts); ④ curated alias map (Shein, Wyeth, IBEC, etc. as encountered).
2. Matching: normalise (lowercase, NFD, strip punctuation/suffixes) then **exact-contains and token-set only, conservative thresholds, no edit-distance in v1**. Short names (≤4 chars) and dictionary-word org names (e.g. "Vision", "Focus") require word-boundary + a context cue ("meeting with", "visit to", "launch of") to match.
3. Output `diary_org_mentions` with `match_confidence`; **high only = display tier**, medium = export/research tier.
4. **Validation protocol before any display**: hand-score a random 150-mention sample; require ≥90% precision on the high tier; publish the measured precision in provenance. (Recall will be poor — fine; say so: "matches are indicative, not exhaustive".)
5. **The corroboration join** (flagship novelty): lobbying returns carry `dpo_lobbied` + `lobbying_period` + `clients`. Window-join: same org (gazetteer key), same minister (member_code ↔ dpo name via existing most_lobbied join keys), diary entry_date inside the return's period → `diary_corroborated` flag on the *lobbying* side and `lobbying_context` flag on the *diary* side. Strictly descriptive: "a return and a diary entry coincide", never "this meeting was the lobbying".

Traps: explosion counting (always `COUNT(DISTINCT entry_id)`); nil returns excluded (already quarantined upstream); collective-DPO returns excluded (existing filter).

Effort: ~2 days incl. validation scoring.

## 8. Phase 6 — gold promotion + registered views

1. Promote the three tables to `data/gold/parquet/` via a `diaries` pipeline chain (sandbox → extractors/ move, `save_parquet`, `--dry-run` support, freshness.json entry, source-health registry rows for each listing URL).
2. Registered views (dependency-first registration — feedback_sql_view_dependency_order):
   - `v_minister_diary_activity` — per minister × month: counts by entry_class (the page workhorse)
   - `v_minister_diary_entries` — drill-down grain with class + org-mention enrichment joined on
   - `v_diary_org_mentions` — high-confidence tier only, with DISTINCT-safe counts
   - `v_diary_lobbying_overlap` — the corroboration join (both directions)
3. SQL-contract tests: never-negative counts, date bounds, DISTINCT-entry invariants, view builds in CI fixture mode (two-layer fixture pattern, gitignore negation for fixture parquets).

Effort: ~1 day.

## 9. Phase 7 — uplift (surfaces)

Run **/shape first** for the page design — this section is scope, not design.

1. **Minister Activity page** (IDEAS §12 — diaries finally give it a spine): per-minister header (tenure from the spine), month-pill timeline of activity mix, "external meetings" ranked list with org chips where high-confidence matched, lobbying-context panel (returns naming this minister in the same window — existing data), SIs signed (existing SI actor work, still the weak join). The §12 consolidation note stands: build this once, absorb Policy-to-Action/Lobbying-to-Regulation framing later.
2. **Member Overview tab** ("Ministerial diary", ministers only): reuse the two-stage member flow; card-based per the no-dataframes rule (feedback_member_overview_no_dataframes).
3. **Highlights / nuggets**: "most-met organisations this quarter", "diary-corroborated lobbying returns" — both DISTINCT-counted, both badged Experimental.
4. **MCP tool** `minister_diary` (who did minister X meet in period Y) + `/v1/data` export of entries + mentions (the newsroom product per CITATION_AND_DATA_PLAN).
5. **Provenance everywhere**: coverage table (which depts publish, which don't, which quarters are OCR-pending), the self-curated caveat, match-precision number, "diary ≠ lobbying register" explainer. Feature badge: **Experimental** until OCR tail + ≥3 more departments land.

Effort: ~2-3 days after /shape.

## 10. Automation & freshness

- Quarterly-in-arrears cadence → a **period-start canary** exactly like the lobbying one (project_scheduled_canaries): in the first week of each quarter, check each registry listing for a file newer than the last indexed; alert on silence > 1 quarter past expectation.
- Add each listing URL to the source-health registry (~107 sources today).
- `content_sha256` re-issue detection runs on every refresh (DAIL-160s class of trap).

## 11. Risks & anti-goals

- **Coverage asymmetry is a fairness risk**: DETE ministers will look "busiest" simply because their dept publishes parseable diaries. Every ranking must be within-department or carry the coverage caveat inline — never a cross-government "most meetings" league table while coverage is partial.
- **Machinery-of-government renames** (DPER's slug already includes "infrastructure-…-digitalisation") — registry rows are per-URL with dept_alias_key, renames become new rows, not edits.
- **Anti-goals**: no sentiment/importance scoring of meetings; no "access index" per company (defamation-adjacent league table of who "has access"); no private-citizen names surfaced in subjects (leak-guard quarantines, §2).
- **OCR tail honesty**: DPER quarters stay visibly "pending OCR" rather than silently absent.

## 12. Sequencing & effort summary

| Phase | What | Effort | Unblocks |
|---|---|---|---|
| 1 | discovery registry + crawler (3 new depts probed) | ~1d | everything |
| 2 | parse completion + golden tests | ~1-2d | gold |
| 4 | minister identity spine | ~1d | all joins (can run parallel to 2) |
| 5 | classification + org matching + corroboration | ~2d | the payoff views |
| 6 | gold + views + contracts | ~1d | UI |
| 3 | off-box OCR (DPER tail) | ½d + off-box session | full coverage (not a blocker for v1 surfaces) |
| 7 | /shape → Minister Activity page + member tab + MCP/export | ~2-3d | ship |

**Recommended cut for v1 ship:** Phases 1-2-4-5-6 + the Minister Activity page on born-digital departments only, Experimental badge, OCR tail and remaining departments following as data-only updates. Total ≈ 7-9 working days.
