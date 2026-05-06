# Benchmark — oireachtas-explorer.ie full comparison

Status: feature-gap audit, not yet planned.
Authored: 2026-05-06.
Reference: https://oireachtas-explorer.ie — open repo at https://github.com/oireachtas-explorer/oireachtas-explorer

This document expands the bill-page benchmark in [legislation_benchmark_oireachtas_explorer.md](legislation_benchmark_oireachtas_explorer.md) into a full feature comparison. Each gap is cross-referenced against existing pipeline parquet files and SQL views so we can flag what's a cheap SQL view edit vs. real new pipeline work vs. permanently out of scope.

## Stack comparison

| Layer | Dáil Tracker | Oireachtas Explorer |
|---|---|---|
| Runtime | Python / Streamlit | React 19 SPA in browser |
| Language | Python + SQL | TypeScript 6 (strict) |
| Build | none (Streamlit serves) | Vite 8 |
| Routing | Streamlit pages | Hash routing `#/<houseNo>/<view>` |
| Data layer | pipeline → parquet → DuckDB views | live `api.oireachtas.ie` per request |
| Caching | none (DuckDB views are virtual) | in-memory + IndexedDB |
| Backend | none (Streamlit only) | none (Cloudflare Worker optional, for XML proxy + KV) |
| Charts | Plotly / Altair | likely native canvas / lightweight charts |
| Mobile | responsive web only | native Android (Kotlin/Compose) and iOS (Swift/SwiftUI) |
| Deploy | TBD | GitHub Pages + GitHub Actions |
| Open source | private repo | public repo, **proprietary licence** |

## ⚠ Licensing & IP

> "The application source code in this repository is proprietary. All rights reserved." — README

The code is publicly *visible* but **not free to copy or fork**. Any feature parity work must be independent reimplementation, not a port. Their data licence (CC-BY 4.0 via the Oireachtas Open Data PSI Licence) governs only the parliamentary data they render — it does not extend to their code.

Implication: this doc is for **inspiration and feature-gap identification**, not for lifting components.

## Architectural philosophy — the fundamental split

These are different products built on the same data:

**Oireachtas Explorer** is a **live thin client**. It hits `api.oireachtas.ie` from the browser at request time, paginates voting records on demand, caches in IndexedDB, runs no metrics. Everything is "what does the API say right now." Strength: zero data-staleness, broad coverage of API endpoints, all 34 Dálaí for free. Weakness: no analytical depth — you can't say "who attended the most plenaries in 2024" because that's a multi-year aggregate the API doesn't precompute.

**Dáil Tracker** is a **pipeline-baked analytical archive**. We ingest, normalise, fuzzy-match, and pre-compute metrics into parquet → DuckDB views, then render through Streamlit. Strength: cross-source joins (lobbying × interests × payments × attendance), pre-computed leaderboards, editorial framing. Weakness: scope-bounded by what the pipeline has ingested, currently Dáil 34 only; staleness depends on rebuild cadence.

The right benchmark question isn't "match their features" — it's "which of their features address user questions Dáil Tracker should also answer, given our different posture?"

## Feature gap matrix

Effort key:

- **🟢 Easy** — SQL view edit only, data already in silver parquet (`sql_views/`, no `pipeline.py` touch)
- **🟡 Moderate** — needs `pipeline_sandbox/` work or new ingestion, but data source exists
- **🔴 Hard** — requires rebuilding scope (e.g. historical Dálaí) or new external data source
- **⚫ Out of scope** — different product posture; not worth matching

### 1. Voting record donut (Tá / Níl / Staon) 🟢

**Their feature:** per-member donut chart showing complete voting record for a Dáil term, paginated fetch of every division.

**Our state:** [sql_views/vote_td_summary.sql](sql_views/vote_td_summary.sql) already returns `yes_count`, `no_count`, `abstained_count`, `division_count`, `yes_rate_pct` per member. The data is exactly what a donut needs.

**Gap:** no chart in [utility/pages_code/votes.py](utility/pages_code/votes.py) or in the votes tab of [utility/pages_code/member_overview.py](utility/pages_code/member_overview.py) — `member_overview.yaml` lists only the aggregate numbers as text in the headline.

**Effort:** Streamlit-only. Add a Plotly donut to the Votes tab using existing `td_vote_summary` columns. Fold into the `branded_chart` component from [chart_export_branding.md](chart_export_branding.md).

### 2. Member comparison workspace 🟢 → 🟡

**Their feature:** compare up to 3 TDs side by side with custom date ranges and term-aware presets.

**Our state:** all per-domain views support `WHERE unique_member_code = ?` filtering. Three parallel queries with three parameters is permitted retrieval SQL.

**Gap:** no comparison page exists. Would be a new `utility/pages_code/member_compare.py` with a corresponding contract.

**Effort:** Streamlit-only if we limit to current pipeline metrics (attendance, payments, interests count, vote yes-rate). 🟡 if we want per-domain time-series comparison — payments has `v_payments_yearly_evolution` (good), attendance has `v_attendance_member_year_summary` (good), but votes only has aggregate `td_vote_summary` (no per-year). For a time-series vote comparison we'd need a new pipeline view.

**Editorial value:** high. "Compare A vs B" is a press / opposition-research workflow that the existing single-member page can't do. theyworkforyou ships this.

### 3. Questions analytics 🟡

**Their feature:** per-member parliamentary questions browser, department bar chart (which ministries received the most questions), monthly line chart with total + oral series, click-to-filter cross-filtering.

**Our state:** `data/silver/parquet/questions.parquet` **already exists** ([questions.py](questions.py:1) ingests it from the bronze Oireachtas API dump). Columns include `unique_member_code`, `td_name`, `ministry`, `topic`, `question_type`, `question_date`, `house`, `question_text`, `question_ref`. **No SQL views, no Streamlit page, no contract.**

**Gap (pipeline):** four new SQL views in `sql_views/`:

```text
TODO_PIPELINE_VIEW_REQUIRED: v_questions_index — one row per question, columns:
  unique_member_code, td_name, ministry, topic, question_date, question_type, question_ref, uri
TODO_PIPELINE_VIEW_REQUIRED: v_questions_member_summary — aggregate per member:
  unique_member_code, total_questions, oral_questions, written_questions, ministry_count
TODO_PIPELINE_VIEW_REQUIRED: v_questions_by_ministry — aggregate per (member, ministry)
  for the department bar chart
TODO_PIPELINE_VIEW_REQUIRED: v_questions_monthly — one row per (member, year_month, type)
  for the monthly line chart
```

All are pure aggregates over a single parquet — no cross-source joins, no enrichment. SQL-view-only work.

**Gap (Streamlit):** new `utility/pages_code/questions.py` page **and** a Questions tab on `member_overview.py` (the contract already names a Votes/Interests/Payments/Lobbying/Legislation tab structure — a sixth tab fits).

**Editorial value:** very high. Parliamentary questions are a primary accountability mechanism (a TD asking the Minister for Health 50 times about hospital waiting lists is a story). Currently invisible in Dáil Tracker.

### 4. Cabinet overview per Dáil 🟡

**Their feature:** collapsible cabinet section on each Dáil home page; current vs former office-holders deduplicated; multiple offices grouped per person.

**Our state:** `v_member_registry.is_minister` is a boolean. The underlying `flattened_members.parquet` has a `ministerial_office` field but its schema for *which* office and *when* is unverified.

**Gap:** schema check on `flattened_members.parquet`. If it has office name + start/end dates, this is a 🟢 SQL view (`v_cabinet_history`). If it only has a current-snapshot boolean, this becomes 🟡 — needs additional ingestion (Department of Taoiseach office holder lists, or oireachtas.ie `/members/{id}/office_holders` endpoint).

**Action:** dump the `flattened_members.parquet` columns to confirm before estimating.

**Editorial value:** medium. Useful context but not a primary user question for Dáil Tracker's accountability frame.

### 5. 34 Dálaí, back to 1919 🔴

**Their feature:** global Dáil session selector, switch between any of 34 historical Dálaí.

**Our state:** [sql_views/member_registry.sql:3](sql_views/member_registry.sql#L3) explicitly notes "Dáil 34, 174 members." Pipeline is scoped to current term. Attendance, payments, lobbying are all current-term only.

**Gap:** historical data ingestion across all surfaces — members per term, votes per term, questions per term, debates per term, sponsors per term. The Oireachtas API supports this; the pipeline doesn't yet.

**Effort:** real. Multi-week pipeline expansion. Some surfaces (lobbying.ie, payments) only exist for recent years anyway and have no pre-2010 equivalent.

**Editorial value:** medium-high for journalists doing historical comparison ("how does this Dáil's question volume compare to the 33rd?"). But Dáil Tracker's strength is contemporary accountability with cross-source joins — historical scope dilutes that.

**Recommendation:** ⚫ defer indefinitely. If we add it, scope to a single dataset first (e.g. votes back to Dáil 30) rather than all-domains.

### 6. Joint committee pages (Dáil + Seanad) 🟡

**Their feature:** committee membership pages merge Dáil and Seanad members for joint committees, with chamber-specific labels (`T.D.`, `Senator`).

**Our state:** `data/silver/parquet/flattened_seanad_members.parquet` **already exists** — Seanad members are pipeline-ingested. No Streamlit surface uses them.

**Gap:** [utility/pages_code/committees.py](utility/pages_code/committees.py) is Dáil-only. No `v_seanad_members` view. No committee membership view connecting the two chambers.

**Effort:** SQL view to expose Seanad members + extend committees view to flag chamber. 🟡 because committee membership data itself needs verification — is it on `flattened_members.parquet`, or does it need its own ingestion?

**Editorial value:** medium. Joint committees do real accountability work (PAC for instance). Worth doing once questions analytics are shipped.

### 7. Saved research workspace + public collections ⚫ → 🟡

**Their feature:** save bills, debates, members locally; publish read-only public collections via Cloudflare Workers KV with persistent short links.

**Our state:** none. Streamlit has session state but no persistent user storage.

**Gap:** this is a Streamlit anti-pattern at scale — Streamlit's session state isn't designed for cross-session bookmarking. Doing it properly means either (a) a real backend with auth, or (b) a Cloudflare Worker + KV setup like theirs (which they got from being an SPA).

**Effort:** ⚫ as a feature on Streamlit. 🟡 if reframed as **deep linking** — the chart-export branded permalink work in [chart_export_branding.md](chart_export_branding.md) covers 80% of the user value (a journalist sharing "this specific filtered view") without needing storage.

**Recommendation:** absorb into the permalink work, don't build saved-items.

### 8. Constituency browsing as primary entry 🟢

**Their feature:** type-ahead constituency picker, browse-by-geography surface on home page.

**Our state:** `v_member_registry.constituency` exists. The current `member_overview.py` lists constituency as text but no browse-by-constituency entry point exists.

**Gap:** would need a `v_constituencies` view (SELECT DISTINCT constituency, COUNT(*) AS member_count, party_breakdown_array …) — but multi-column GROUP BY is forbidden in Streamlit per contract, so it's a pipeline view. SQL-view-only work.

**Effort:** 🟢. New view + small constituency picker on the home page sidebar.

**Editorial value:** medium. UK theyworkforyou treats geography as a primary entry; readers know their MP by constituency. Worth doing for the same reason.

### 9. Party composition bar chart on home page 🟢

**Their feature:** party composition with party colours and seat counts on the Dáil home page.

**Our state:** `v_member_registry.party_name` is available. Multi-column aggregation belongs in pipeline.

**Gap:** new `v_party_composition` view: `party_name, seat_count, government_status` (one row per party for current Dáil).

**Effort:** 🟢. SQL view + Plotly horizontal bar chart on home page. Use the established pill palette from `member_overview.yaml` (blue for government, amber for opposition).

**Editorial value:** medium. Sets context for first-time visitors. Currently the home/landing experience is sparse.

### 10. Inline transcript reading ⚫

**Their feature:** click into any debate to read the official XML transcript inline; IndexedDB-cached parsed transcripts.

**Our state:** no transcript surface. Debates aren't a core dataset; we link out for transcripts.

**Effort:** ⚫. XML transcript ingestion + viewer is a separate product. The Oireachtas Explorer's whole value prop on debates is reading them; Dáil Tracker's value prop is metric-shaped accountability across sources.

**Recommendation:** stay focused. Link out to oireachtas.ie debate pages where transcripts already render natively.

### 11. Cross-filter navigation (chart bar → list) 🟢

**Their feature:** click a department bar → questions list filters to that department; click a month → list filters to that month.

**Our state:** none. Streamlit charts emit no events natively.

**Gap:** Streamlit's `on_select` parameter on `st.plotly_chart` (recent addition) supports chart-as-filter. We just haven't used it.

**Effort:** 🟢. Pattern-based — establish in the questions analytics surface (when built), reuse on payments yearly-evolution and attendance timeline.

**Editorial value:** high once charts exist. Without questions analytics it has nowhere to land first.

### 12. Term-aware date defaults 🟢

**Their feature:** date pickers default to the selected Dáil term's start/end dates.

**Our state:** [sql_views/member_registry.sql:18-19](sql_views/member_registry.sql#L18-L19) has `membership_start_date` and `membership_end_date` per member. A Dáil-term boundary view would be a `MIN(membership_start_date), MAX(membership_end_date)` aggregate.

**Gap:** small. Pipeline view + a helper in `utility/ui/` for term-aware date defaults.

**Effort:** 🟢.

**Editorial value:** medium. UX polish, not a content feature.

## Where Dáil Tracker is stronger

Worth naming explicitly so we don't lose sight of these in feature-comparison anxiety:

- **Cross-source accountability joins.** Lobbying revolving-door × member identity, member interests × company holdings × CRO data, payments × attendance ratio. The Explorer is a thin live API client; it has no equivalent because the API doesn't expose these joins. This is *the* Dáil Tracker differentiator.
- **Pre-computed leaderboards.** Hall of Fame / Hall of Shame attendance, payments leaderboards, lobbying intensity rankings. Requires pipeline-baked metrics; the Explorer can't do these without server-side compute.
- **Editorial framing.** theyworkforyou-style accountability tone, not "explorer" framing. We make the editorial choice that some metrics matter; they leave the user to find their own narrative.
- **Lobbying.ie integration.** Entirely absent from the Explorer. This is the most journalism-relevant dataset Dáil Tracker has.
- **Members' Interests (SIPO register).** Absent from the Explorer.
- **CSV exports with provenance.** The Explorer has saved collections but no comma-separated artefacts for spreadsheet workflows.
- **Committee Iris Oifigiúil parsing.** Statutory instruments and government appointments; absent from the Explorer.
- **Future: branded chart export with permalink** ([chart_export_branding.md](chart_export_branding.md)) — provenance baked into the exported artefact. The Explorer's saved-collections sharing is the closest analogue but is private-collection-shaped, not artefact-shaped.

## Suggested rollout

If we adopt anything from this audit, sequence by editorial value × effort:

1. **🟢 Voting donut** (gap 1). Pure Streamlit chart, one afternoon. Wire into existing `member_overview.py` Votes tab.
2. **🟡 Questions analytics** (gap 3). Highest editorial value. Four SQL views + a new Streamlit page + a tab on `member_overview.py`. The data is already in silver — pipeline work is SQL-only.
3. **🟢 Party composition + constituency picker** (gaps 8, 9). Home-page polish; sets context.
4. **🟢 Cross-filter chart pattern** (gap 11). Establish on questions analytics, reuse elsewhere.
5. **🟢 Member comparison workspace** (gap 2). High journalistic utility. Limit V1 to current pipeline metrics; defer time-series vote comparison until vote per-year view exists.
6. **🟡 Cabinet overview** (gap 4). Schema check first. Defer if `flattened_members.parquet` doesn't already have office history.
7. **🟡 Joint committees** (gap 6). Bundle with the broader committee page bold redesign.
8. **🟢 Term-aware date defaults** (gap 12). UX polish; absorb into other work.
9. **⚫ Saved collections** — fold into chart-export permalinks instead.
10. **⚫ Historical Dálaí, transcript reader** — stay out.

## Open questions

- Does `flattened_members.parquet` carry office-holder history (start/end per office) or just a current-snapshot `ministerial_office` boolean? (Schema dump needed for gap 4.)
- Are committee memberships in `flattened_members.parquet` or a separate parquet? (Gap 6 estimate depends.)
- Does the Oireachtas API expose Cream List membership directly, or is it Order Paper scraping only? (Carried over from [legislation_benchmark_oireachtas_explorer.md](legislation_benchmark_oireachtas_explorer.md).)
- Is questions data deduplicated across joint askers in `questions.parquet`, or is one question with three askers three rows? (Affects v_questions_member_summary semantics.)

## Cross-references

- [legislation_benchmark_oireachtas_explorer.md](legislation_benchmark_oireachtas_explorer.md) — bill page specifics (sponsors, timeline, Cream List)
- [chart_export_branding.md](chart_export_branding.md) — permalink + branded export feature, replaces saved-collections value
- [dail_tracker_improvements_v4.md](dail_tracker_improvements_v4.md) — broader improvements log; fold validated items here once decided
