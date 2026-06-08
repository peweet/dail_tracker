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

### Phase 1 — AKN speech harvest → bronze
Fetch AKN XML for the 32,092 sections where `speech_count > 0`, driven by
`akn_xml_url` in `debate_listings.parquet`. Both chambers, incremental by date.
Reuse the poller/scenario framework (`services/oireachtas_api_main`). Polite
rate-limit; delta-only refresh (mirror `seanad`/`debates` chains).

### Phase 2 — Parse speeches → silver (firewall-clean, structural)
Parse `<speech>` → one row per speech:
`(date, chamber, debate_section_id, parent_section_title, debate_type, speaker_raw,
member_ref, recorded_time, speech_order, speech_text, akn_url)`. Deterministic XML
extraction, no inference.

### Phase 3 — Member resolution → gold `speeches_fact`
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
