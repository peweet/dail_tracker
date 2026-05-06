# Minister Activity page — feature idea

Status: idea / not yet scoped for sprint
Drafted: 2026-05-06

## User question this page answers

> "What was this minister doing in the last six months — who lobbied them,
> what statutory instruments did they sign, and where do those two threads meet?"

The page exists for a specific civic suspicion: lobbying contacts cluster
around the topics of secondary legislation a minister later signs. The page
presents the evidence side by side without asserting causation.

## Why this is greenfield

No existing page answers this. The closest neighbours are:

- `lobbying_2.py` — politician-targeted lobbying with no minister filter and
  no SI overlay
- `member_overview.py` — cross-domain TD profile with a known per-member
  lobbying gap and no SI section at all

Neither surfaces statutory instruments. Neither defaults to a rolling window.
Neither links lobbying topics to SI topics.

## Bold redesigned layout

```
┌─ HERO (editorial) ────────────────────────────────────────────────────┐
│  MINISTER ACTIVITY                                                    │
│  What was the Minister for Health doing for the last 6 months?        │
│  · Period: 06 Nov 2025 → 06 May 2026 · 47 lobbying contacts ·         │
│    12 statutory instruments signed                                    │
└───────────────────────────────────────────────────────────────────────┘

┌─ COMMAND BAR ─────────────────────────────────────────────────────────┐
│  [Minister: Stephen Donnelly ▾]   [Window: 6 months ▾]   [Custom ▾]   │
└───────────────────────────────────────────────────────────────────────┘

┌─ IDENTITY STRIP ──────────────────────────────────────────────────────┐
│  Stephen Donnelly · Minister for Health · Fianna Fáil ·               │
│  Dáil  · Government                                                   │
└───────────────────────────────────────────────────────────────────────┘

┌─ HEADLINE STATS (4-up) ───────────────────────────────────────────────┐
│  47 lobbying contacts · 12 SIs signed · 18 distinct lobbyists ·       │
│  4 overlap topics                                                     │
└───────────────────────────────────────────────────────────────────────┘

┌─ DUAL TIMELINE (the hero chart) ──────────────────────────────────────┐
│  ◆ lobbying contacts (top track)                                      │
│  ▲ SIs signed         (bottom track)                                  │
│  Coloured by topic cluster — same hue links lobbying ◆ to SI ▲        │
│  Vertical guides at month boundaries                                  │
└───────────────────────────────────────────────────────────────────────┘

┌─ TOPIC CONFLUENCE (ranked card list) ─────────────────────────────────┐
│  1. Health insurance regulation                                       │
│     14 lobbying contacts · 2 SIs signed · 8 lobbyists                 │
│     [→ open evidence]                                                 │
│  2. Pharmacy & medicines pricing                                      │
│     9 lobbying contacts · 3 SIs signed · 5 lobbyists                  │
│     [→ open evidence]                                                 │
│  ...                                                                  │
└───────────────────────────────────────────────────────────────────────┘

┌─ STAGE 2 — TOPIC EVIDENCE (when a card is opened) ────────────────────┐
│  Topic: Health insurance regulation        [← back]                   │
│                                                                       │
│  ◆ Lobbying contacts (chronological)                                  │
│    table: date · lobbyist · client · description · source             │
│                                                                       │
│  ▲ Statutory instruments signed (chronological)                       │
│    table: date · SI no · title · parent act · EISB link               │
│                                                                       │
│  No claim of causation — sequence shown for inspection.               │
└───────────────────────────────────────────────────────────────────────┘

┌─ TWO PARALLEL EVIDENCE TABLES (always visible below confluence) ──────┐
│  Lobbying contacts in window (CSV ↓)                                  │
│  Statutory instruments signed in window (CSV ↓)                       │
└───────────────────────────────────────────────────────────────────────┘

┌─ ABOUT & DATA PROVENANCE (collapsed expander) ────────────────────────┐
└───────────────────────────────────────────────────────────────────────┘
```

## Interaction model

- **Primary view** is the dual timeline + topic confluence ranked list. The
  user lands and immediately sees two streams of activity coloured by topic.
- **Stage 1 → Stage 2** uses the established two-stage member flow pattern but
  on **topic** as the navigation key, not on a person. The minister is fixed
  by the command bar; the topic card click drills in.
- **Entry points**: sidebar selectbox of ministers; URL query param
  `?minister=<unique_member_code>&window=6m`; deep link from any lobbying or
  legislation page row tagged with the minister.
- **Notable chips** in the sidebar: An Taoiseach, Minister for Finance,
  Minister for Health, Minister for Justice — match the lobbying.yaml
  convention.
- **Back button** at the top of the main content area when in Stage 2 topic
  view; clears the topic query param.

## Temporal behaviour

- **Mode**: `event_date` rolling window. **Default: trailing 6 months from
  today.**
- **Control**: `st.segmented_control` with options `3m / 6m / 12m / Custom` —
  when Custom is selected, reveal `st.date_input` range.
- Window applies to **both** datasets: `period_start_date BETWEEN :start AND
  :end` for lobbying, `si_signing_date BETWEEN :start AND :end` for SIs.
- No global year pill — this is a rolling-window page, not a year-scoped one
  (mirrors lobbying.yaml's date-range pattern).

## Source-link behaviour

- Source links **enabled**, rendered with `render_source_links()` in Stage 2
  topic-evidence view and inline at the end of each row in the two parallel
  evidence tables.
- Approved URL columns: `source_url`, `oireachtas_url` (lobbying); `eisb_url`
  (SI — links to electronic Irish Statute Book).
- **Provenance footer**: collapsed expander at bottom — required, this page
  mixes two distinct sources (lobbying.ie and Iris Oifigiúil) and the user
  must be able to inspect both.

## Chart and table strategy

| Question | Visualisation |
|---|---|
| When did things happen, and do streams overlap? | **Dual-track timeline** (Altair `mark_circle` + `mark_triangle` on two y-rows, x = date). Single chart, two glyphs, topic colour. |
| Which topics dominate the window? | **Ranked card list** (`int-rank-card` CSS reused) — counts on each card, no horizontal bar chart. |
| What are the actual contacts and SIs? | **Two `st.dataframe` tables** in Stage 2 topic evidence. Native row-click navigation off. Date column sorts default DESC. |
| Volume by month? | **Not shown on primary view.** The timeline already encodes density visually. Adding a stacked-bar would duplicate. |

No charts in Stage 2 topic evidence — by then the user is reading rows.

## Empty state copy

- **No minister selected**:
  `st.info("Select a minister from the sidebar to see their activity in the chosen window.")`
- **No lobbying contacts in window**:
  `empty_state("No lobbying contacts on the public register for this minister between {start} and {end}.", "This is what the register shows — it is not a claim of zero activity.")`
- **No SIs signed in window**:
  `empty_state("No statutory instruments attributed to this minister in this window.", "Iris Oifigiúil only attributes SIs signed under the minister's own hand. Departmental orders signed by officials are not included.")`
- **No topic overlap**:
  `st.info("Lobbying contacts and SIs in this window do not share a topic cluster. The two activity streams below remain visible.")`
- **TODO_PIPELINE_VIEW_REQUIRED**: `todo_callout()` for any view backed by a
  missing pipeline view.

## Design differentiators (greenfield)

This page is materially different from every existing Dáil Tracker page along
at least these dimensions:

1. **Minister-as-subject, not TD** — first page where ministerial office is
   the navigation primitive.
2. **Rolling-window default** — every other page defaults to "all data" or a
   fixed year pill; this one defaults to trailing 6 months.
3. **Dual-track timeline** as the hero chart — no other page renders two
   event streams together.
4. **Topic confluence cards** as Stage 1 — collapses two datasets into a
   single ranked list.
5. **Topic-keyed Stage 2** — first time a non-person, non-bill entity drives
   the drill-down.
6. **Cross-source provenance footer** — required because two sources are
   mixed; most pages have a single provenance source.

## TODO_PIPELINE_VIEW_REQUIRED items

The page cannot be fully built without these. Each must be flagged with
`todo_callout()` until the pipeline exposes them.

| View | Status | What's needed |
|---|---|---|
| `v_minister_register` | required | One row per current minister: `unique_member_code`, `minister_name`, `office_title` (e.g. "Minister for Health"), `department`, `appointed_date`, `term_end_date`. Source: pipeline-side join of `flattened_members.parquet` `ministerial_office` / `office_1_name` columns. **Already noted as a gap in `lobbying_index.sql`.** |
| `v_minister_lobbying_contacts` | required | Per-minister lobbying contacts. Columns: `unique_member_code`, `minister_name`, `period_start_date`, `lobbyist_name`, `client_name`, `public_policy_area`, `topic_cluster_id`, `description`, `source_url`. Filterable by `unique_member_code` and `period_start_date BETWEEN`. The `topic_cluster_id` is the cross-source linker. |
| `v_statutory_instruments` | required | Per-SI row from `out/iris_si_taxonomy.csv` promoted to silver/gold. Columns: `si_number`, `si_year`, `si_signing_date`, `signing_minister_unique_member_code`, `signing_minister_name`, `title`, `parent_legislation`, `policy_domain_primary`, `topic_cluster_id`, `eisb_url`. Filterable by `signing_minister_unique_member_code` and `si_signing_date BETWEEN`. **Critical pipeline gap — `si_responsible_actor` in the sandbox CSV is a free-text actor name, not a `unique_member_code`. Pipeline must resolve "Minister for Health (Stephen Donnelly TD)" → `unique_member_code` via the same `LOWER(strip_accents(TRIM()))` pattern lobbying uses, plus an office-name map for unsigned-by-name SIs.** |
| `v_minister_topic_confluence` | required | Per-(minister, topic_cluster_id) summary in window. Columns: `unique_member_code`, `topic_cluster_id`, `topic_label`, `lobbying_contact_count`, `si_count`, `distinct_lobbyists`, `first_event_date`, `last_event_date`. Filterable by `unique_member_code` and a window. |
| `v_minister_activity_sources` | required | Per-(minister, return_id, si_id) source-link rows for `render_source_links()`. Columns: `unique_member_code`, `event_type` (lobbying/SI), `source_url`, `eisb_url`, `oireachtas_url`. |

**The hardest gap — topic clustering.** Lobbying uses `public_policy_area`
(free-text from lobbying.ie); SIs use `si_policy_domain_primary` (derived in
the sandbox ETL). They do **not** share a vocabulary. The pipeline must
produce a single `topic_cluster_id` mapping that covers both. Two acceptable
approaches, both pipeline work:

- **Pragmatic crosswalk**: hand-curated mapping table in
  `data/_meta/topic_crosswalk.csv` keyed by both vocabularies →
  `topic_cluster_id` + `topic_label`. Cheap, auditable, reviewable, easy to
  extend.
- **Embedding-based clustering**: `pipeline_sandbox/` script that embeds
  lobbying descriptions and SI titles, clusters them, and emits the same
  crosswalk.

The crosswalk approach is the right starting point — auditable beats clever
for civic transparency.

## Implementation plan

### Files to create

- `utility/page_contracts/minister_activity.yaml` — new contract following
  the `member_overview.yaml` + `lobbying.yaml` patterns, using `mode:
  event_date` with `default_window: trailing_6_months`.
- `utility/pages_code/minister_activity.py` — new page file. No backend
  imports.
- `dail_tracker_bold_ui_contract_pack_v5/page_runbooks/minister_activity.md`
  — runbook following the `lobbying.md` template.

### Files to modify (UI helpers only — never backend)

- `utility/shared_css.py` — add three new class families:
  - `min-confluence-*` — topic confluence card (rank, label, dual-count
    badges, → button)
  - `min-timeline-*` — dual-track timeline container chrome (axis labels,
    legend swatch)
  - `min-window-*` — segmented-control wrapper for the rolling-window picker
  - All cards use `background: #ffffff` (per `feedback_css_surface_trap.md`);
    use `:has()` to collapse the `stHorizontalBlock` row alongside the
    `dt-nav-anchor` button (per `feedback_css_card_pattern.md`).
- `utility/ui/components.py` — extend `breadcrumb()` helper to take a topic
  label; add `dual_track_timeline(lobbying_df, si_df)` function returning an
  Altair chart bound to `chart_theme.py` palette.
- `utility/ui/temporal_controls.py` — add `rolling_window_picker(default="6m")`
  returning `(start_date, end_date)`.
- `utility/ui/source_links.py` — add `eisb_url` to the approved URL columns
  list.
- `utility/ui/export_controls.py` — already supports
  `current_displayed_view_only`, no change needed.

### Helpers to reuse, not rewrite

- `empty_state()`, `todo_callout()` from `components.py`
- `render_source_links()` from `source_links.py`
- `export_button()` from `export_controls.py`
- `int-rank-card` CSS family for the confluence list
- `dt-hero`, `dt-kicker`, `dt-dek` for the editorial hero
- `dt-provenance-box` for the footer expander

### Build order

1. Land the page contract YAML and runbook with all six views marked
   `TODO_PIPELINE_VIEW_REQUIRED`.
2. Build the page shell — hero, command bar, identity strip, headline stats —
   entirely on `todo_callout()`s. Verify boldness against
   `feedback_iteration_process.md`.
3. Pipeline work parallel: SI silver/gold promotion, minister-name resolution,
   topic crosswalk, four registered views in `sql_views/`.
4. Wire the views into the page section by section as they ship; remove
   `todo_callout()`s as they go live.
5. Civic UI review — `civic-ui-review` skill.

### Constraints

- No new metrics, joins, or aggregations defined in Streamlit.
- No JavaScript.
- No `st.markdown(unsafe_allow_html=True)` — `st.html()` only.
- No `var(--surface)` for cards — `#ffffff`.
- No edits to `pipeline.py`, `enrich.py`, `normalise_join_key.py`.
- New Python/Polars enrichment lives in `pipeline_sandbox/`.
- New SQL views land directly in `sql_views/`.
