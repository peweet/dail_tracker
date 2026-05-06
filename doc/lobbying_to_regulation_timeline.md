# Lobbying-to-Regulation Timeline — feature idea

Status: idea / not yet scoped for sprint
Drafted: 2026-05-06

## The pitch

Lobbying.ie tells you: lobbyist X lobbied Minister Y about topic Z in period P.
Iris Oifigiúil tells you: Minister Y signed Statutory Instrument SI-N about
topic Z in issue dated D.

Joining them gives you the line nobody can write today without weeks of manual
work:

> "In the 6 months before SI 2025/123 on pharmaceutical pricing was published,
> the Department of Health appeared on 47 lobbying returns covering Health
> policy — 31 of them filed by the four companies most affected by the new
> rules."

Automated, this is a story-generation engine. The Ditch's investigation into
Robert Troy is exactly this kind of pairing, done case-by-case. This page does
it across the full corpus.

## User question this page answers

**Primary (Stage 2):** *In the [3 / 6 / 12] months before this Statutory
Instrument was published, which organisations were lobbying the responsible
department about this topic, and during which periods?*

**Secondary (Stage 1):** *Which Statutory Instruments published this year saw
the most concentrated lobbying activity in the lead-up?*

The page does **not** claim causation. It surfaces a temporal coincidence
between two public registers and lets the reader judge.

## Page architecture

Greenfield page (`utility/pages_code/regulation_timeline.py`). New YAML
contract at `utility/page_contracts/regulation_timeline.yaml`.

Two-stage flow keyed on the SI ID (e.g. `2025_142`) — a structural first for
the app. Every other ranked page is keyed on a member or organisation; this
one is keyed on a regulatory event.

### Stage 1 — primary view

```
┌─ EDITORIAL HERO ───────────────────────────────────────────────┐
│ Kicker:   STATUTORY INSTRUMENTS · LOBBYING TIMELINE            │
│ Title:    Who was lobbying before this regulation appeared?    │
│ Dek:      One sentence. Two-register join. Coincidence,        │
│           not causation.                                        │
│ Caveat strip: TOPIC-MATCH CONFIDENCE · SEE METHODOLOGY          │
└─────────────────────────────────────────────────────────────────┘

[ Year pills:    2026 · 2025 · 2024 · 2023 · 2022 · 2021 · 2020 ]
[ Lookback:      3 mo · 6 mo · 12 mo  (segmented_control)        ]
[ Department:    selectbox — all · Health · Finance · Justice... ]
[ Match floor:   any · likely · strong  (segmented_control)      ]

EVIDENCE HEADING — "Most-lobbied SIs published in 2025"
   sub-line: ranked by overlapping returns in the [6-month] window

┌─ rg-si-card  (compact card + adjacent → button) ────┐ →
│ S.I. 142/2025 · Iris 14 Apr 2025                     │
│ Mortgage Credit (Amendment) Regulations 2025         │
│ Responsible: Minister for Finance                    │
│ ── 47 returns · 12 orgs · 6-month window ──          │
│ pills:  [ likely match ] [ Department of Finance ]   │
└──────────────────────────────────────────────────────┘
… one card per SI, ranked by `returns_in_window` DESC, top ~50

[expander] About & data provenance  (collapsed at bottom)
```

### Stage 2 — single SI profile

```
← Back to all regulations   (top of main content, not sidebar)

┌─ IDENTITY STRIP ───────────────────────────────────────────────┐
│ S.I. 142 of 2025                                                │
│ Mortgage Credit (Amendment) Regulations 2025                    │
│ Iris published: 14 Apr 2025  →  [PDF]                           │
│ Responsible: Minister for Finance                               │
│ Topic-match confidence:  ●●○ likely  (see methodology)          │
└─────────────────────────────────────────────────────────────────┘

[ Lookback:  3 mo  · ●6 mo · 12 mo ]   ← drives all panels below

┌─ LOBBYING-PERIOD STRIP  (Altair, the one chart) ───────────────┐
│ ▌shaded lookback window                ▼ Iris published         │
│ ▬▬▬▬▬       ▬▬▬▬▬       ▬▬▬▬▬                                  │
│  ▬▬▬▬▬▬▬▬▬▬▬       ▬▬▬▬▬▬▬▬▬▬▬                       ▼         │
│ Oct 2024 ───────────────────────────────────── Apr 2025         │
│ x: date (left-to-right chrono); y: stacked bars per return      │
│ each bar = one return's lobbying period (start→end)             │
└─────────────────────────────────────────────────────────────────┘
caveat:  47 returns · 12 unique organisations · 6 of them filed
         ≥3 returns whose period overlaps this window

EVIDENCE HEADING — "Top organisations lobbying in this window"
[ rg-org-card · rg-org-card · rg-org-card · rg-org-card · rg-org-card ]
five compact cards · each → existing lobbyist Stage 2 profile

EVIDENCE HEADING — "Returns overlapping this window"
secondary view — st.dataframe with column_config:
  Period · Lobbyist · DPO targeted · Subject · lobbying.ie link
[ Download as CSV ]  ← directly under the table

[expander] Methodology & data provenance  (collapsed at bottom)
```

## Interaction model

- Two-stage flow (matches `member_overview` and `lobbying_2` patterns).
- Selection state: `rg_selected_si` (e.g. `"2025_142"`).
- Back button at top of main content. Sidebar back is missed.
- Sidebar: SI search → year-scoped selectbox → notable-department chips.
- Cross-page links:
  - Each org card → existing `lobbyist_poc` Stage 2.
  - Each DPO mention → `member_overview?member=…` only when
    `unique_member_code` is present (name fallback is forbidden — see
    `feedback_streamlit_api_patterns` and existing lobbying TODOs).
  - Each Iris PDF link → official Iris PDF.

## Temporal model

Two event-anchored controls. **No calendar date range** — the unit of interest
is the lead-up to a publication event, not absolute calendar time.

| Control          | Anchor                          | Default       | Widget                 |
|------------------|----------------------------------|---------------|------------------------|
| Year pills       | Iris `issue_date` year           | most-recent   | `st.pills`             |
| Lookback window  | offset back from `issue_date`    | 6 months      | `st.segmented_control` |

Year pills follow the established pattern (DESC, default newest, never "All").
Stage 2 keeps the lookback control but drops the year pill.

## Data feasibility — verified 2026-05-06

**Verdict: implementable. No main-pipeline rework. Three concrete sandbox gaps
to fill.**

### What already exists

**Iris (`pipeline_sandbox/out/iris_si_taxonomy.csv`)** — produced by
`pipeline_sandbox/iris_oifigiuil_etl_polars.py`:

```
source_file · issue_date · issue_number · si_number · si_year · title ·
si_form · si_operation_primary · si_policy_domain_primary ·
si_responsible_actor · si_effective_date_text · si_parent_legislation ·
si_eu_relationship · si_taxonomy_confidence · eisb_url
```

**Lobbying (`data/silver/lobbying/returns.csv`)**:

```
primary_key · lobbyist_name · public_policy_area · relevant_matter ·
specific_details · dpo_lobbied · clients · lobbying_period_start_date ·
lobbying_period_end_date · lobby_url · date_published_timestamp …
```

The `dpo_lobbied` column carries the department per DPO in delimited form,
e.g.:
```
Jennifer Carroll MacNeill|Minister|Department of Health::
Mary Butler|Government Chief Whip|Department of the Taoiseach
```
That's the keystone field. It makes the department-side join feasible.

### Two corrections to the original sketch

1. **Iris has no parsed `signing_date`.** The realistic anchor is `issue_date`
   (when the issue was published). The actual signing/making date sits in body
   text as `si_effective_date_text` ("from 1 January 2025") and is not parsed.
   Iris publication typically lags signing by 1–7 days, so `issue_date` is a
   defensible proxy provided the methodology footer says so. Don't pretend we
   have signing dates.

2. **Lobbying returns are quarterly-period rows, not point-in-time contacts.**
   Periods are typically 4-month windows (Jan–Apr, May–Aug, Sep–Dec). The
   honest unit is "returns whose period overlaps the lookback window", not
   dots on a daily timeline. The Altair strip shows period bars
   (start→end), not points. Phrasing must say "47 overlapping returns", not
   "47 contacts on these dates".

### Gaps to fill (all sandbox, not main pipeline)

| Gap | Lives at | Why it's needed |
|---|---|---|
| Promote `iris_si_taxonomy.csv` → `data/silver/iris_oifigiuil/si_taxonomy.parquet` | new sandbox script | SQL views can't read sandbox output |
| Parse `dpo_lobbied` into long-form `(return_id, dpo_name, role, department)` | new sandbox script | Department-side join key |
| `data/_meta/department_aliases.csv` — Iris `si_responsible_actor` ↔ canonical dept name; lobbying department string ↔ same canonical | hand-seeded CSV | Iris extracts "The Minister for Health"; lobbying says "Department of Health". Without alignment, no join. |
| `data/_meta/policy_area_to_si_domain.csv` — SIPO area (~19) ↔ Iris policy domain (17) | hand-seeded CSV | Topic-side confidence axis |
| Windowed join sandbox script → `data/gold/parquet/si_lobbying_window_contacts.parquet` | new sandbox script | The page reads this via SQL view |

### Coverage realism

- **`si_responsible_actor` extraction quality is unaudited.** Run a coverage
  audit on the existing taxonomy CSV first. If <70% of SIs have a parseable
  responsible actor, the page needs a first-class "department unknown"
  bucket rather than dropping those SIs silently.
- **Iris parser known gaps** (see `project_iris_oifigiuil_parser_gaps.md`):
  - 151 valid issues fail the page-1-only `valid_iris_issue` check
  - 247 split-failure giants (fisheries / appointments / catalogues)
  - 5,021 unclassified-other quarantined records
  These reduce SI corpus completeness. Methodology must surface this — not
  hide it.
- **Lobbying pre-2020 floor**: `_LOBBYING_INGEST_FROM_YEAR = 2020`. Stage 1
  must hide or label SIs published before 2020 as "no lobbying register
  coverage".

## What does NOT need rework

- `pipeline.py`, `enrich.py`, `normalise_join_key.py` — untouched.
- Iris ETL — already does the heavy lift; output just needs promotion.
- Lobbying ingestion — already produces the silver returns.
- All existing registered views — no breakage.

## TODO_PIPELINE_VIEW_REQUIRED items

The page cannot be built until each of these lands:

### Iris promotion

- `v_iris_si_index` registered view with: `si_id` (`si_year || '_' || si_no`),
  `si_no`, `si_year`, `si_title`, `issue_date`, `issue_number`,
  `responsible_actor_canonical`, `made_by_department_canonical`,
  `iris_pdf_url`, `si_operation_primary`, `si_policy_domain_primary`,
  `si_taxonomy_confidence`, `coverage_caveat` (nullable flag for known
  parser gaps).

### Joining

- `v_regulation_si_index` — one row per SI with `returns_in_window_3mo`,
  `returns_in_window_6mo`, `returns_in_window_12mo`, `distinct_orgs_6mo`,
  `top_match_confidence`. Approved filters: `issue_date BETWEEN`,
  `made_by_department_canonical =`, `top_match_confidence >=`.

- `v_regulation_si_returns` — per-(si_id, lookback_months) overlapping
  returns. Columns: `si_id`, `return_id`, `lobbyist_name`, `dpo_name`,
  `department`, `lobbying_period_start_date`, `lobbying_period_end_date`,
  `relevant_matter`, `public_policy_area`, `match_confidence`, `lobby_url`.
  Approved filters: `si_id =`, `lookback_months IN (3,6,12)`.

- `v_regulation_si_top_orgs` — per-(si_id, lookback_months) org leaderboard.

- `v_regulation_sources` — per-`si_id` source URLs (`iris_pdf_url`, parent
  Act `oireachtas_url` where extractable from `si_parent_legislation`).

### Coverage caveats the UI must surface

- `lobbying_coverage_year_floor` exposed to the page (currently `2020`) so
  the pre-2020 empty state is data-driven, not hardcoded.
- Iris pre-2020 issue completeness check before relying on Stage 1 ordering.

## Implementation plan

### New files only — nothing modified in main pipeline

1. `pipeline_sandbox/iris_promote_to_silver.py` — promote
   `out/iris_si_taxonomy.csv` to `data/silver/iris_oifigiuil/`.
2. `pipeline_sandbox/lobbying_dpo_explode.py` — parse `dpo_lobbied`
   delimited string into long-form parquet.
3. `data/_meta/department_aliases.csv` — hand-seeded.
4. `data/_meta/policy_area_to_si_domain.csv` — hand-seeded.
5. `pipeline_sandbox/si_lobbying_match.py` — windowed join, emits
   `data/gold/parquet/si_lobbying_window_contacts.parquet`. Polars (per
   `project_polars_vs_pandas_split.md`).
6. `sql_views/iris_si_index.sql`
7. `sql_views/regulation_si_index.sql`
8. `sql_views/regulation_si_returns.sql`
9. `sql_views/regulation_si_top_orgs.sql`
10. `sql_views/regulation_sources.sql`
11. `utility/pages_code/regulation_timeline.py`
12. `utility/page_contracts/regulation_timeline.yaml`
13. `dail_tracker_bold_ui_contract_pack_v5/page_runbooks/regulation_timeline.md`

### CSS classes (added to `utility/shared_css.py`, never page-local)

| Class | Purpose |
|---|---|
| `rg-si-card` | Stage 1 SI card — `inline-flex` + `width: fit-content` per `feedback_css_card_pattern.md` |
| `rg-si-card-rank` | Rank badge inside card |
| `rg-si-card-meta` | "S.I. 142/2025 · Iris 14 Apr 2025" line |
| `rg-si-card-title` | SI title line |
| `rg-si-returns-pill` | "47 returns" badge inside card |
| `rg-org-card` | Stage 2 top-org card (same compact pattern) |
| `rg-confidence-pill-strong` / `-likely` / `-possible` | Match-confidence pill — blue / amber / grey (deuteranopia-safe per `project_design_principles.md`) |
| `rg-caveat-strip` | Hero caveat band |
| `rg-timeline-shade` | Altair lookback shading (set in `chart_theme.py`) |

### Helpers — reuse, don't recreate

Reuse from `utility/ui/`: `back_button`, `breadcrumb`, `empty_state`,
`evidence_heading`, `hero_banner`, `pill`, `todo_callout`, `clickable_card_link`
(`components.py`); `render_source_links` (`source_links.py`);
`provenance_expander` (`source_pdfs.py`); `export_button` (`export_controls.py`);
`chart_theme.py`.

Add:
- `lookback_window_control(key)` in `temporal_controls.py` — `segmented_control`
  returning `int` months. Reused for any future event-window page.
- `confidence_pill(level)` in `components.py` — three-state pill.

### Build order

1. **First: responsible-actor coverage audit** on the existing taxonomy CSV.
   If poor (<70%), the design needs a "department unknown" bucket as a
   first-class state — adjust the brief before plumbing.
2. Iris promotion sandbox + `v_iris_si_index`.
3. Department alias + policy-area mapping seeds.
4. `dpo_lobbied` parser sandbox.
5. `si_lobbying_match.py` sandbox + four `regulation_*` SQL views.
6. Page contract + runbook.
7. Page file + CSS.
8. Chart styling.
9. Methodology expander content (load-bearing — provenance is the whole
   pitch).

Rough effort: ~2 days sandbox + SQL views, ~1 day page + CSS. Each step is
independently shippable — Stage 1 can stand up against `todo_callout()`
placeholders before the join data exists.

## Why nobody has built this

- Lobbying.ie consumers don't parse Iris.
- Iris readers don't have the lobbying register joined.
- SI-to-topic classification is genuinely fiddly — `si_policy_domain_primary`
  is the existing-pipeline answer to it.
- Department-string alignment across the two registers is unglamorous data
  janitorial work that journalists don't have time for.

The Iris ETL has already absorbed most of the difficulty. The remaining work
is alias-table + windowed-join + UI — none of which is novel; it just hasn't
been done in one place.
