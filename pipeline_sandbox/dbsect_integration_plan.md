# Debates integration plan — pipeline + UI

Standalone planning doc for graduating the debates work from sandbox
into the main pipeline and onto the UI. Builds on
[dbsect_harvest.py](dbsect_harvest.py),
[dbsect_harvest_migration.md](dbsect_harvest_migration.md), and the
empirical findings from [dbsect_probe.py](dbsect_probe.py) /
[dbsect_probe_findings.md](dbsect_probe_findings.md).

This doc is intentionally separate from the harvest-migration doc
because it covers the *next-stage* work (debates API + content layer +
UI). The harvest migration doc covers Stage 0 only.

No code changes pending — read once, then plan a sprint against the
"Recommended first slice" section at the bottom.

---

## 1. Volume reality

Real numbers from harvest + probe runs against current bronze:

| Layer | Count | Size |
|---|---|---|
| Distinct dbsects in index | 3,008 | — |
| Distinct (date, chamber) pairs | ~700 | — |
| Day-window JSON per call | ~35 KB | ~25 MB total backfill |
| AKN XML per dbsect | 4–15 KB (avg ~10) | ~30 MB total backfill |
| `<speech>` rows after parse | ~6 per debate × 3,008 | ~50 k rows |
| Per-member aggregate | 175 TDs × ~50 debates | ~9 k rows |

The corpus looks "huge" because of request count (3,008 AKN calls
during first backfill), not data size. ~55 MB of raw bronze, ~5–10 MB
of silver speech parquet, <1 MB of gold per-TD aggregate. The 3,008
calls are a one-time cost — debates are immutable, so cache forever.

## 2. The four insights that make this tractable

1. **Debates are immutable.** Once published, a dbsect's XML never
   changes. Fetch-once-cache-forever is correct, not a hack. Single
   biggest cost saver.
2. **`dbsect_index.parquet` already carries date + chamber.** No need
   to call `/v1/debates` per dbsect — one day-window call returns
   *every* dbsect for that day. Worklist for the structural layer is
   ~700, not 3,008.
3. **`/v1/debates?member_id=…` is dead** (probe returned 0 results
   even on dates the TD demonstrably contributed). Don't build a
   member-filtered listing. Contributor → debate join must come from
   parsing `<speech by>` out of XML. The XML pool *is* the join key.
4. **The UI never reads raw XML.** UI reads a tiny gold parquet keyed
   on member_uri. Streamlit Cloud only sees the per-TD aggregate. The
   XML pool stays on the build machine.

## 3. Two-stage pipeline shape

### Stage 1 — cheap structural layer (runs every refresh)

```
bronze JSON (existing pipeline)
  → harvester (already built)         → silver/parquet/dbsect_index.parquet
  → day-window fetcher                → bronze/debates/listings/<date>__<chamber>.json
  → listings flattener                → silver/parquet/debate_listings.parquet
```

`debate_listings.parquet` schema, one row per `(date, chamber, dbsect)`:

```
debate_section_id, date, chamber,
parent_section_id, parent_section_title,
bill_ref, debate_type,
speaker_count, speech_count,
akn_xml_url, debate_url_web
```

Stage 1 alone enables three useful joins **without ever calling AKN**:

- `bill ↔ debate` (already partly works via legislation_debates.sql)
- `question ↔ debate parent` (parent section reveals whether a written
  question rolled into a Topical Issue debate, etc.)
- `vote ↔ debate ↔ debate.speakerCount` (was this division on a
  heavily-debated section or a procedural rubber-stamp?)

Cost: ~700 polite parallel calls via existing
[services/http_engine.py](../services/http_engine.py) — ~30 sec total.
Refresh going forward: ~5 new days/week → 5 calls.

### Stage 2 — content layer, lazy + content-addressed

```
silver/parquet/debate_listings.parquet
  → akn_fetcher (cache-aware)         → bronze/debates/akn/<chamber>/<date>/<dbsect>.xml
  → speech parser                     → silver/parquet/speeches.parquet
  → member resolver                   → silver/parquet/speech_member_link.parquet
  → aggregator                        → gold/parquet/member_debates.parquet
```

Cache discipline: `if xml_path.exists(): skip`. First full run ≈ 3,008
calls × 250 ms throttle ≈ 12 min. Every subsequent refresh: only the
dbsects added since last run, typically 10–50/week → 5–10 sec.

`speeches.parquet` schema (one row per `<speech>`):

```
debate_section_id, date, chamber, speech_index,
speaker_token,           # raw '#FirstnameLastname' from XML
paragraph_count, char_count, first_words   # NOT full text
```

Full text stays on oireachtas.ie. We carry `char_count + first_words`
(~200 chars) for any UI snippet — keeps silver small and avoids any
question of caching attributable speech corpora locally.

## 4. Storage layers + deployment

The deployment-safety part. Only the bottom row ships to the cloud.

| Layer | Path | Git status | On Cloud? | Refreshed when |
|---|---|---|---|---|
| bronze AKN XML | `bronze/debates/akn/**/*.xml` | gitignored | no | first run + new dbsects |
| bronze listings | `bronze/debates/listings/*.json` | gitignored | no | every refresh |
| silver `debate_listings.parquet` | `silver/parquet/` | gitignored | yes (~3 MB) | every refresh |
| silver `speeches.parquet` | `silver/parquet/` | gitignored | **no** | when XML pool grows |
| silver `speech_member_link.parquet` | `silver/parquet/` | gitignored | **no** | when speeches change |
| **gold `member_debates.parquet`** | `gold/parquet/` | committed to `data` branch | **yes (<1 MB)** | when speech_member_link changes |

Streamlit reads <1 MB indexed by member_uri — instant page loads, no
risk of OOM on Cloud's small dyno. Mirrors the
[unitedstates/congress cache/data split](../doc/REACHITECTURE_NEW.MD#L662-L670)
and the [data branch pattern](../doc/SHORT_TERM_PLAN.md#L45-L49) from
week 1.

## 5. Integration into `services/`

Stage 1 fits the existing scenario pattern cleanly — one new entry, no
new abstractions:

- [services/dail_config.py](../services/dail_config.py): add
  `DEBATES_DIR = BRONZE_DIR / "debates"` and
  `AKN_DIR = DEBATES_DIR / "akn"` to the directory list.
- [services/urls.py](../services/urls.py): new
  `build_debates_day_urls(date_chamber_pairs)` that takes a
  deduplicated `(date, chamber)` set sourced from
  `dbsect_index.parquet`.
- [services/storage.py](../services/storage.py): `debates_listings`
  scenario branch in `result_file_path()`.
- [services/oireachtas_api_main.py](../services/oireachtas_api_main.py):
  one new `run_member_scenario(...)` call after the existing three.

Stage 2 does **not** fit `run_member_scenario` (single-record fetches,
cache-by-existence, custom headers, throttle). Don't force it. It
belongs in either:

- a new `services/akn_fetcher.py` with its own
  `fetch_akn_pool(dbsect_index_df)` function, **or**
- the upcoming polite HTTP helper in
  [SHORT_TERM_PLAN §6.3](../doc/SHORT_TERM_PLAN.md#L166-L171). Time it
  to land with that work and re-use the throttle + retry +
  robots-respecting helper.

The headers requirement (`User-Agent: Mozilla/5.0 (compatible; …)` +
`Referer: https://www.oireachtas.ie/`) is a Stage-2-only concern.
Keep it out of `services/http_engine.py` — that session is for
`api.oireachtas.ie`, which doesn't need the dance.

## 6. Speech parsing + member resolution

XML carries `<speech by="#FirstnameLastname">…</speech>`. Parse with
lxml or `xml.etree`, one pass per file.

Member resolution is a separate enrichment:
`speaker_token` (`#ErinMcGreehan`, `#AodhánÓRíordáin`) → `member_uri`.
Two-pass match:

1. Exact token match against `flattened_members.full_name_no_spaces`
   (or equivalent slug derived for this purpose).
2. Fuzzy fallback by surname + chamber + sitting on that date —
   re-uses the [normalise_join_key](../enrich.py) pattern. Memory
   rule: don't touch `enrich.py` directly. Fold into a new
   `pipeline/enrich/debate_speech.py` when this graduates.

Unmatched tokens get logged + written to a
`speech_member_link_unresolved.parquet` for manual triage. Expect
1–3% miss rate, mostly fada / initial edge cases. Surface the
unresolved count in the provenance footer of any debate-related UI
page.

## 7. UI surface (gold table shape)

`gold/member_debates.parquet`, keyed on member_uri, one row per
(member, debate_section):

```
member_uri, year, debate_section_id, date, chamber,
debate_title, parent_section_title, bill_ref, debate_type,
speech_count, char_count_total, oireachtas_debate_url
```

That's everything the existing card components need. Per memory rules:

- Card list, no `st.dataframe` (per
  `feedback_member_overview_no_dataframes`).
- Year pills filter (per `project_design_principles`).
- Two-stage flow: TD → debates list → card → external oireachtas.ie
  link in new tab.

The high-value derived feature on top of this — the
[td + bills + debates scrutiny join](../doc/REACHITECTURE_NEW.MD#L38) —
is one inner-join in SQL: `gold/member_debates × silver/sponsors`.
New view `sql_views/v_member_scrutiny.sql`: "for each bill this TD
sponsored, did they speak in any of its debate sections?" Cheap, no
new fetch.

## 8. Refresh / incremental story

**Local dev / first run** (`python pipeline.py --refresh`):

- Stage 1: ~30 sec
- Stage 2 first time: ~12 min (mostly polite throttle on AKN)
- Total ~13 min — paid once.

**Subsequent refresh (CI cron from week 3):**

- Stage 1: 5 new (date, chamber) pairs → 5 calls, ~2 sec.
- Stage 2: 10–50 new dbsects → cached-skipped + new fetches,
  ~5–15 sec.
- Total <30 sec added to the existing pipeline run.

**CI cache:** the AKN XML pool is the thing to persist. Two options:

- GitHub Actions cache keyed on a hash of `dbsect_index.parquet`
  (cheap, fits free tier well under 10 GB cap).
- A separate orphan `xml-cache` branch in the repo (durable, free,
  same pattern as the `data` branch in
  [SHORT_TERM_PLAN §1.4](../doc/SHORT_TERM_PLAN.md#L45-L49)).

Recommendation: orphan branch. Survives runner rotations, doesn't
expire, and serves as a cite-able snapshot of the corpus at any
commit.

## 9. Risks to flag in DATA_LIMITATIONS.md before shipping

Add to [DATA_LIMITATIONS.md](../doc/DATA_LIMITATIONS.md):

- **AKN access is not contractual.** Works today with UA + Referer; if
  `data.oireachtas.ie` tightens that, new debates can't be fetched.
  Cached XML keeps existing data working. Add a Tier 1 issue alarm on
  AKN-403 streaks of >5.
- **Speaker token resolution is a heuristic**, not a join key. Fada-
  stripped tokens are the failure mode. Surface unresolved counts in
  the provenance footer.
- **Speech count is not a quality metric.** A TD with 200 short
  procedural interjections beats a TD with 5 substantive speeches
  under naive count. `char_count_total` is the column the UI should
  switch to when "minutes of contribution" becomes the right framing.
- **`debate_type='question'` floor sections vs `writtens` sections**
  look identical in the dbsect_index. The day-window listing only
  surfaces the floor type. Writtens reach `member_debates` only via
  direct AKN fetch — and writtens typically have one minister speech,
  not the TD's, so they should *not* be attributed to the questioning
  TD as a "debate contribution". Filter on `speaker_token !=
  minister-of-the-day` rather than counting writtens as TD speech
  events.
- **dbsect ids are per-day, not global.** `dbsect_2` recurs every
  Dáil sitting day. Composite identity is `(date, chamber, dbsect)`.
  All downstream views must respect this — never join on dbsect alone.

## 10. Recommended first slice

If only one slice gets built before the user-signal exists, do
**Stage 1 end-to-end, no Stage 2 yet**:

1. Day-window fetcher in `services/` (mirrors the existing legislation
   scenario, ~700 calls).
2. Listings flattener in `pipeline_sandbox/` (graduates with the
   harvester later).
3. New view `sql_views/v_debate_listings.sql`.
4. Three new card panels on
   [member_overview.py](../utility/pages_code/member_overview.py):
   - "Bills with debate sessions" (already half-built via
     `legislation_debates.sql`)
   - "Questions that landed in a debate parent section"
   - "Votes that came out of high-debate sittings"

Cheapest, lowest-risk slice. Answers most of the structural questions
without parsing a single speech. Stage 2 graduates only after:

- Stage 1 is shipped and stable in production.
- The polite HTTP helper from
  [SHORT_TERM_PLAN §6.3](../doc/SHORT_TERM_PLAN.md#L166-L171) lands.
- There's a real user signal that speech-level content (not just
  counts) is what the dashboard is missing.

## 11. Out of scope for this plan

Worth listing explicitly so future-us doesn't drift into them
prematurely:

- Topic / keyword classification of debates (NLP layer).
- Speech sentiment / stance extraction.
- Backfilling pre-2020 debate XML (per project's general 2020 cut-off
  rule in [DATA_LIMITATIONS.md §1.3](../doc/DATA_LIMITATIONS.md)).
- Cross-Oireachtas-period analysis (33rd Dáil questions reach into
  this corpus; treat as pre-current-Dáil and don't surface in primary
  views).
- Real-time debate ingestion. Daily-cron refresh is sufficient for
  parliamentary monitoring; live-streaming is not the use case.
- A standalone debates browser UI page. The data lives where the user
  already is — on the TD page, on the bill page, on the question
  page. Don't build a fourth navigation surface.
