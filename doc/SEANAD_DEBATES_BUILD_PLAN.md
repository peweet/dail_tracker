# Seanad/Dáil Debates (Speeches) + Commencement Matters — Build Plan

Status: **PLAN** (2026-06-08). Sibling to `doc/SEANAD_PARITY_BUILD_PLAN.md`
and `doc/SEANAD_APP_PARITY_PLAN.md`. Reuse-based, additive; existing Dáil/Seanad
paths untouched.

## Why this plan exists
User asked for Seanad **questions + votes + debates** at parity with the Dáil.
On audit (2026-06-08):

| Type | Dáil | Seanad | Member-attributed? |
|---|---|---|---|
| Voting record | ✅ full | ✅ full (verified today) | ✅ |
| Questions | ✅ PQs | ❌ → Commencement Matters (unbuilt) | ✅ |
| Debates | index only | index only | ❌ **no speech text either house** |

**Votes are already done** (`seanad` pipeline chain → `current_seanad_vote_history`,
`v_vote_base` chamber-union; verified: Seanad 41,956 member-votes / 1,208 divisions /
172 senators; Kyne resolves 645 Dáil + 612 Seanad). See [[project_seanad_parity]].

There are **zero member-attributed speeches anywhere in the app**. The only debate
data is the structural *index* (`debate_listings.parquet`, 36,219 rows, both chambers).

## Key insight: two asks, one pipeline
"Seanad questions" (**Commencement Matters**) and "debates with speeches" come from
the **same source — the AKN XML transcript harvest**. Commencement Matters is just a
`debate_type` filter on the resulting speech data. Build one speech pipeline, surface
it two ways.

## Pre-computed inputs (already present)
- `data/silver/parquet/debate_listings.parquet` — 36,219 sections (Dáil 33,864 /
  Seanad 2,355), each with `akn_xml_url`, `debate_type`, `speech_count`,
  `parent_section_title`, `debate_url_web`. **442,482 speeches indexed across 32,092
  sections with speech_count>0** = the harvest target list.
- AKN XML confirmed to carry machine-readable attribution: `<speech>` blocks with
  `<from>Deputy Erin McGreehan</from>` + `by="#ErinMcGreehan"` ref + `<recordedTime>`.
  Probe samples in `data/bronze/debates/probe/*.xml`.

## Acceptance test (ties to the originating question)
*"Show Seán Kyne's Seanad speeches, flagged where he spoke as Gaeilge."* returns real
results at the end.

## Phases

### Phase 0 — Votes ✅ DONE (verified 2026-06-08)

### Phase 1 — AKN transcript harvest → bronze ✅ BUILT (2026-06-08)
`debates/akn_harvest.py` + `services.http_engine.fetch_all_text` (text sibling of
`fetch_all`, same session/retry). Harvests **day-level `main.xml`** (NOT per-section
`dbsect_N.xml`) — worklist = distinct day-level `debateRecord.formats.xml.uri` read
straight from the listings bronze; chamber taken from the transcript URI path so
committees drop cleanly. Writes `data/bronze/debates/akn/<chamber>_<date>_main.xml`,
incremental (skips existing). Flags: `--limit`, `--since`, `--chamber`, `--overwrite`.
Validated: 4 Dáil days → 4 files, 0 failures. ~1,205 day-candidates in the current
(stale-to-2018) bronze; refresh the `debates_listings` scenario for recent days.

### Phase 2 — Parse transcripts → silver ✅ BUILT (2026-06-08)
`debates/speech_parse.py` — `parse_akn(xml)` pure transform → one row per
contribution: `(date, chamber, debate_section_id, section_heading, contribution_type,
contribution_order, akn_eid, unique_member_code, speaker_raw, recorded_time,
speech_text)` → `data/silver/parquet/speeches.parquet`. Namespace-agnostic (handles
`/CSD13`), multi-section attribution via child→parent climb, captures speech +
question + answer. Deterministic member resolution via `<TLCPerson>` href tail.
Firewall-clean (no inference). Tests: `test/debates/test_speech_parse.py` (5 cases).
Validated: 4 Dáil days → 1,601 contributions / 45 sections / **99% resolved**;
1 Seanad day → 65 speeches / 12 sections / **100% resolved**.

### Phase 3 — Member resolution → gold `speeches_fact` ✅ BUILT (2026-06-08)
`debates/speeches_gold.py` — `build_speeches_fact()` joins silver→member registries
(Dáil+Seanad union) for member_name/party/constituency; adds `house`, `word_count`,
`is_irish`/`irish_score`, `year`. Irish detector = fada + Irish-FUNCTION-WORD density
(≥0.25, ≥2 function words, ≥10 words) — rejects fada proper nouns ("Dáil Éireann",
"Ó Murchú") that a naive fada-only flag false-flagged. Parser also gained `business`
(outermost debateSection heading) so Commencement Matters group correctly. Tests:
`test/debates/test_speeches_gold.py`. Validated: Dáil 1601 + Seanad 432 contributions,
99.4% identity-resolved, 70 Irish-flagged (precise), 59 Commencement Matters.

### Phase 5/6 — Views + member-overview UI ✅ BUILT (2026-06-08)
Single unified parquet (has `house`) → ONE placeholder `{SPEECH_FACT_PARQUET_PATH}`,
not a 2-file union. Views: `sql_views/speech_base.sql` + `speech_member_detail.sql`
(`v_member_speeches`) + `speech_member_summary.sql` + `speech_member_business.sql`,
registered as Phase 5 in `dail_tracker_core/connections.py` (+ api_conn glob/subs).
Core queries: `speech_summary/speech_years/speech_business/member_speeches` in
`dail_tracker_core/queries/member_overview.py` (retrieval-only). UI: rewrote
`_section_debates` in `utility/pages_code/member_overview.py` — was a question-derived
proxy, now the real floor-contribution feed (stat_strip header w/ contributions·words·
As-Gaeilge·Commencement, year pills + type segmented + As-Gaeilge toggle + business
selectbox + full-text search; transcript cards w/ excerpt + "Read full" expander +
As-Gaeilge green badge). Seanad Questions empty-state now cross-links to Debates.
CSS `.signal-gaeilge`/`.mo-speech-*` in shared_css.py. Lint clean; firewall clean;
debates+member-overview+sql_views tests green. NOT yet visually verified on a running
server (do per [[feedback_validate_fresh_server]]). Original Phase 3 design below:

### Phase 3 (design) — Member resolution → gold `speeches_fact`
Resolve `by="#ErinMcGreehan"` → `unique_member_code` + `house`; `normalise_join_key`
fallback ([[project_td_name_join_key]]). Key on `(member, chamber)` for the cross-house
code collision (Kyne) — same pattern as the votes union. Add an **Irish-language flag**
per speech. Atomic `save_parquet` + zstd ([[feedback_parquet_write_convention]]).
Probe needed: member-ref coverage on pre-2016 debates.

### Phase 4 — Surface "Seanad questions" = Commencement Matters
Filter `speeches_fact` where `debate_type = commencement` (Seanad). Populate the
existing member-overview "Questions → Commencement Matters" empty-state (already
stubbed in Seanad parity) + a feed mirroring the questions page. Confirm the exact
`debate_type` label first (not in listings top-25).

### Phase 5 — Surface debates (both chambers)
member-overview "Spoke N times" + per-member searchable speech feed (Irish-language
flag), for TDs and Senators. Debates page: browse by date/chamber/topic, full text +
attribution + link-out. Classification stays in pipeline; UI display-only
([[feedback_no_inference_in_app]]).

### Phase 6 — MCP + views + tests + freshness
MCP tools `get_member_speeches`, `search_debates`. `sql_views/speech_base.sql`
chamber-union mirroring `vote_base.sql`; register dependency-first
([[feedback_sql_view_dependency_order]]). Tests mirror `test_sql_views.py`;
source-health registry entry; extend the `debates` pipeline chain.

## Sizing
| Phase | Effort |
|---|---|
| 1 Harvest | M (bounded 32k fetch, reuses poller) |
| 2 Parse | S–M |
| 3 Resolve+gold | M |
| 4 Commencement surface | S |
| 5 Debates UI | M–L |
| 6 MCP/views/tests | M |

Heavy lift = Phases 1–3. Once `speeches_fact` exists, Phase 4 is nearly free.

## De-risk spike — RESULTS (2026-06-08)
1. ✅ **Commencement Matters label** = `parent_section_title == 'Commencement Matters'`
   (NOT a `debate_type`; type is `'debate'`). 788 Seanad sections; `show_as` holds the
   specific topic. Phase-4 filter confirmed.
2. ✅ **Member-ref resolution = 31/31 (100%)** on local probe AKN, via the `by="#FirstLast"`
   ref normalised against the combined Dáil+Seanad registry (470 keys). Deterministic —
   no fuzzy matching needed on modern AKN. Caveat: probes are all modern; **pre-2016
   `by=` coverage still unprobed** (do during Phase 1 harvest).
3. ⚠️ **Irish detection works but is naive** — the heuristic fired on procedural Irish
   ("An Cathaoirleach Gníomhach" = Acting Chair title) rather than a substantive Gaeilge
   speech. Phase 3 must refine: exclude chair/procedural turns, require sustained Irish
   density over N words, or run `langdetect` on the speech body.

### Phase-1 harvest — ACCESS DIAGNOSED (2026-06-08): NOT blocked/throttled
The earlier "403" was a **key-construction bug, not a block**. Full diagnosis:
- The 403 body is S3 `<Error><Code>AccessDenied</Code></Error>` (Server: AmazonS3 /
  CloudFront, no Retry-After, no WAF challenge). **S3 returns 403 AccessDenied — not 404 —
  for non-existent keys** when ListBucket is denied.
- `dbsect_listings_flatten.py:101` *constructs* `…/mul@/dbsect_<n>.xml` URLs as a fallback,
  and those per-section S3 objects **frequently do not exist** → 403.
- The **authoritative key is `…/mul@/main.xml`** (the whole sitting day), taken from the
  API's `formats.xml.uri`. PROOF: the exact known-good probe URL (dail/2026-03-26/dbsect_30)
  → 200/11,850 B; a live Seanad day `seanad/2025-12-18/.../main.xml` → 200/129 KB/65 speeches/
  25 TLCPerson refs. Access is completely fine.
- **HARVEST DESIGN CORRECTION:** harvest **`main.xml` per (chamber, sitting-day)** via the
  API's authoritative `formats.xml.uri` — NEVER the constructed `dbsect_N.xml`. ~one fetch
  per sitting day (~few thousand total), not 32,092 per-section fetches. Then split sections
  out of `main.xml` by their `<debateSection eId="dbsect_N">` structure and map to the
  listings index. Get day-level xml uris from `/v1/debates` (200, works).
- **Listings bronze is STALE** — `debate_listings.parquet` only runs to ~2018. Phase 1
  must first re-run a fresh + wider `debates_listings` bronze harvest before the AKN sweep,
  else recent speeches (incl. current-term Seanad) are missing.

## Reuse audit (2026-06-08) — avoid duplication
- **Member resolution is FREE (direct join, no fuzzy match).** AKN `<references>` carries
  `<TLCPerson eId="ErinMcGreehan" href="/ie/oireachtas/member/id/Erin-McGreehan.S.2020-06-29"/>`.
  The href tail = `unique_member_code` exactly. So each speech's `by="#ErinMcGreehan"` →
  eId lookup → href → `unique_member_code` → join member registry, IDENTICAL to how
  `questions.py` joins on `question.by.memberCode`. Cross-house collision auto-handled
  (code carries .D/.S + date). My spike's normalise fallback is a safety net, not needed.
- **Harvest framework ~80% reusable, with one real gap.** `run_member_scenario(name, urls)`,
  the worklist→URL idiom (`_load_debates_worklist`), `output_exists` freshness, and the
  `fetch_json` retry/`fetch_all` ThreadPool machinery are all reusable in shape. GAP:
  `fetch_json` calls `response.json()` (line 57) and `save_json` dumps JSON — **AKN is XML**,
  so reuse needs a sibling `fetch_all_text` (returns `response.text`) + a per-file XML saver
  under `bronze/debates/akn/`, reusing the same `session`/retry constants. ~40 LOC parallel
  path, NOT a refactor of the JSON hot path.
- **Silver/gold/view/UI all follow existing templates (pattern reuse, new content):** speech
  flattener mirrors `dbsect_listings_flatten.py` (but XML, not json_normalize); gold mirrors
  `enrich._build_*` arg-parameterised helpers; `speech_base.sql` clones `vote_base.sql`'s
  2-placeholder chamber-union; Commencement-Matters surface reuses the questions page +
  member_overview section; `save_parquet` atomic+zstd throughout.
