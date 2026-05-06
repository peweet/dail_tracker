# Highlights page — feature idea

Status: idea / not yet scoped for sprint
Drafted: 2026-05-06

## User question this page answers

> "I'm a journalist landing on Dáil Tracker for the first time. What are the most newsworthy patterns in the data **right now**, and where do I dig deeper?"

The page is the **front door** — a curated editorial spine, not a dashboard. Each
story is one paragraph long, evidence-led, and ends with a single deep link into
the page that sourced it. The journalist hook.

## Page architecture

Greenfield page (`utility/pages_code/highlights.py`). A `home_overview.yaml`
contract exists but no implementation file does. Highlights replaces Attendance
as the default route in `utility/app.py`.

A vertical magazine spine. **One column on mobile, two columns on desktop with a
wide hero on top.** No sidebar filters — the page is consumed top-to-bottom like
a longread. Stories are not modals or expanders — every story is fully visible
without interaction.

```
┌───────────────────────────────────────────────────────────────┐
│  KICKER:  DÁIL TRACKER · HIGHLIGHTS · 6 May 2026              │
│  HERO HEADLINE: "What the parliamentary record showed in 2025"│
│  DEK: One sentence summarising the "year in numbers" stat.    │
│  YEAR PILLS:  [2022] [2023] [2024] [2025*]                    │
└───────────────────────────────────────────────────────────────┘
┌────────────────────────┐  ┌────────────────────────┐
│  STORY 1               │  │  STORY 2               │
│  Most-lobbied TDs 2025 │  │  Top earners 2025      │
└────────────────────────┘  └────────────────────────┘
┌────────────────────────┐  ┌────────────────────────┐
│  STORY 3               │  │  STORY 4               │
│  Property declarations │  │  Hall of Shame: skipped │
│  in 2025               │  │  sitting days 2025     │
└────────────────────────┘  └────────────────────────┘
┌───────────────────────────────────────────────────────────────┐
│  STORY 5 (full-bleed, evergreen)                              │
│  Former officials now lobbying — the revolving door          │
└───────────────────────────────────────────────────────────────┘
┌───────────────────────────────────────────────────────────────┐
│  HOW THIS PAGE IS BUILT — provenance footer                  │
└───────────────────────────────────────────────────────────────┘
```

A story card is **kicker → headline → dek → 3-5 stacked rows of evidence (rank
pill + member name + meta + numeric badge) → "See the data →" link**. Looks like
a New York Times morning-briefing item, not a dashboard tile.

## The five stories

| # | Story | Source view | Year-aware? | Status |
|---|---|---|---|---|
| 1 | **Top 10 most-lobbied TDs of {year}** | `v_lobbying_index` | ⚠ all-time only today | needs new SQL view |
| 2 | **Top 10 highest-paid TDs of {year}** (PSA / TAA) | `v_payments_yearly_evolution` (`rank_high`, `payment_year`) | yes | feasible today |
| 3 | **Property & landlord declarations in {year}** | `v_member_interests` filtered by `landlord_flag` / `property_flag` and `declaration_year` | yes | feasible today |
| 4 | **Hall of Shame: TDs with the lowest sitting-day attendance, {year}** *(excludes ministers — caveat in dek)* | `v_attendance_year_rank.rank_low_exc_ministers` | yes | feasible today |
| 5 | **The revolving door: former officials now on lobbying returns** *(evergreen, all-time)* | `v_lobbying_revolving_door` ranked by `return_count` | no — all-time | feasible today |

### Stories considered but deferred

**"TDs with declared property interests who voted on housing bills"** —
editorially the strongest framing but not buildable from current views. It needs:

- a `policy_area` tag on `v_legislation_index` (today there is bill_type / status only)
- a `member_id × bill_id × vote_position` view (today votes are keyed by vote_id ≠ bill_id)

These are real pipeline tasks, not view-level fixes. Move forward without it;
revisit when those views land.

## Interaction model

- **Primary view:** the magazine spine itself.
- **Year pills** at the top retune the four year-aware stories simultaneously.
  Story 5 ignores the pill (it's all-time).
- **No filters, no search, no sidebar.** This page is a hook, not a tool. Users
  who want filters click through.
- **CTA per story:** a single `→` button under each card that deep-links to the
  relevant page with year and rank already applied via `?year=2025&sort=…` so
  the journalist lands inside the same data context, no re-filtering needed.
- **Member name in any row** is a clickable card that routes to
  `?member={join_key}` on member-overview (existing pattern).
- **No drilldown view on this page itself** — Highlights only refers out. This
  is the rule that keeps the page magazine-shaped.

## Temporal behaviour

Single year-pill control at the top. Mode = `latest_completed_year`, so on
2026-05-06 the default is **2025**. Pill list derived once from the union of
available years across the four year-aware views. Story 1 gets a yellow caveat
strip "All-time, year filter pending — see provenance" until
`v_lobbying_index_year` exists.

## Source-link behaviour

Per the existing `home_overview.yaml` contract, source links are required.

- Each row that names an entity (TD / lobbyist / DPO) gets a
  `source_link_html(...)` to the canonical Oireachtas / lobbying.ie page.
- Each story card gets a small footer line: *"Source: lobbying.ie register,
  Oireachtas payments PDFs"* — italicised meta line in the existing
  `pay-name-body-pos` style.
- Page-level **provenance footer** (one collapsed expander at the bottom) lists
  every view the page reads, every parquet file behind those views, and the
  latest fetch timestamp from each domain's `*_summary` view
  (`v_lobbying_summary`, `v_payments_summary`, `v_member_interests_summary`,
  `v_attendance_summary`).

## Chart and table strategy

**No charts.** A magazine front page does not get charts; it gets ranked lists
with one big number per row. Charts live on the destination pages.

**No `st.dataframe` calls** — this is a primary view, and the project rule bans
dataframes on primary views. Every row is a card in the existing
`att-list-row` / `pay-name-row` family.

Per row: rank pill (`att-list-rank`-style), member card (avatar + name +
party·constituency in `pay-identity-card`-style), then a single right-aligned
numeric badge (`pay-amount-badge`-style) that varies per story:

| Story | Badge content |
|---|---|
| 1 most-lobbied | `124 returns` |
| 2 highest-paid | `€48,200` |
| 3 property | `5 declarations` |
| 4 attendance | `38 / 87 days` |
| 5 revolving door | `12 returns · 4 firms` |

## Empty state copy

| Story | Empty copy |
|---|---|
| 1 | "No lobbying returns recorded for {year} yet. Lobbying.ie publishes returns three times a year — check back after the next deadline." |
| 2 | "No payment records for {year}. The Oireachtas publishes the PSA register annually after year-end." |
| 3 | "No property declarations on file for {year}. Members file annual declarations under SIPO; the {year} register may not yet be published." |
| 4 | "Sitting day data for {year} not yet available." |
| 5 | "No former-DPO records currently on the lobbying register." (this should never fire) |

If all five stories empty, render a single dt-callout: *"No highlights to show
yet for {year}. Try {previous_year}."* with the year pill pre-set.

## Design differentiators

- The Dáil's "front page" today is Attendance — a working list, not a hook.
  Highlights establishes the editorial register.
- No other page in the app is read top-to-bottom in one pass. Highlights is the
  only **passive** page; the rest are filtering tools.
- Year pills retune **multiple stories at once** — no other page does
  multi-section retuning from a single control.
- Rank pills get colour-coded by story type (lobbying = blue, payments = green,
  attendance = amber, interests = purple, revolving door = red) so the spine
  reads as five distinct sections at a glance.

## TODO_PIPELINE_VIEW_REQUIRED

```
TODO_PIPELINE_VIEW_REQUIRED: v_lobbying_index_year
  Per-year version of v_lobbying_index. Same columns plus a `period_year`
  partition. Story 1 ("Top 10 most-lobbied TDs of 2025") cannot be honestly
  rendered from v_lobbying_index because total_returns is all-time. Aggregate
  from silver/lobbying/returns_master.parquet (period_start_date) joined to
  politician_returns_detail.parquet. Underlying data is present — view-only.

TODO_PIPELINE_VIEW_REQUIRED: v_legislation_index.policy_area
  Bill-level policy tagging (housing, immigration, justice, climate, etc.).
  Today v_legislation_index has bill_type / status only. Required for the
  deferred Story "TDs with property interests who voted on housing bills".

TODO_PIPELINE_VIEW_REQUIRED: v_member_bill_votes
  member_id × bill_id × vote_position (yes/no/abstain). Currently votes are
  keyed by vote_id, and vote_id ≠ bill_id. Needs a join from divisions to
  bills via debate/title resolution. Required for the same deferred Story.

TODO_PIPELINE_VIEW_REQUIRED: v_highlights_year_axis
  Helper view returning the union of available years across attendance,
  payments, interests and (eventually) per-year lobbying. Used to populate
  the single year pill row. Could be done in retrieval SQL but cleaner as a
  one-line view so all four stories agree on the pill set.

TODO_PIPELINE_VIEW_REQUIRED: latest_fetch_timestamp_utc on v_payments_summary
  Already flagged in payments_summary.sql as TODO. Required for the
  provenance footer to honestly show "data current as of X".
```

## Implementation plan

**Files to create**

- `utility/pages_code/highlights.py` — page body, one function per story
- `utility/data_access/highlights_data.py` — DuckDB connection registering the
  five views the page uses (mirrors `member_overview_data.py`)
- `utility/page_runbooks/highlights.md` — runbook for token discipline
- `utility/page_contracts/highlights.yaml` — driving contract (clone shape from
  `home_overview.yaml`, swap in the five approved views and the year-aware
  temporal mode)
- `sql_views/lobbying_index_year.sql` — new SQL view backing Story 1 (Day-1
  ship blocker if Story 1 must be year-aware on launch)

**Files to update**

- `utility/app.py` — register `highlights_page` as the new default route at the
  top of `st.navigation([...])`, demote Attendance from `default=True`
- `utility/shared_css.py` — add a `hl-*` class family: `hl-spine`, `hl-story`,
  `hl-kicker`, `hl-headline`, `hl-dek`, `hl-cta`, `hl-rank-lobby`,
  `hl-rank-pay`, `hl-rank-attendance`, `hl-rank-interests`,
  `hl-rank-revolving`. Reuse existing `att-list-row`, `pay-amount-badge`,
  `pay-identity-card` for the row components.
- `utility/ui/components.py` — one new helper
  `story_card(*, kicker, headline, dek, rows, cta_href, cta_label, empty_copy)`
  that takes a list of pre-rendered row HTML strings.
- `utility/ui/entity_links.py` — add `PAGES["highlights"]` slug if missing

**Helpers to reuse, not duplicate**

`member_card_html`, `clean_meta`, `source_link_html`, `clickable_card_link`,
`empty_state` — all already in `utility/ui/components.py` / `entity_links.py`.

**Approved registered views the page reads**

- `v_lobbying_index` (Story 1, all-time caveat) or `v_lobbying_index_year` once available
- `v_payments_yearly_evolution` (Story 2)
- `v_member_interests` (Story 3)
- `v_attendance_year_rank` (Story 4)
- `v_lobbying_revolving_door` (Story 5)
- `v_lobbying_summary`, `v_payments_summary`, `v_member_interests_summary`,
  `v_attendance_summary` (provenance footer only)

**Retrieval SQL shape** (one query per story, no joins, no GROUP BY) — example
for Story 2:

```sql
SELECT member_name, unique_member_code, party_name, constituency, total_paid
FROM v_payments_yearly_evolution
WHERE payment_year = ? AND rank_high <= 10
ORDER BY rank_high ASC
```

## Open questions before kickoff

1. Confirm the slate above (Stories 1–5) is acceptable, or substitute an
   alternative for any slot.
2. Default landing page — confirm Highlights replaces Attendance as the
   `default=True` route, or stay as a sibling page.
3. Acceptable to ship Story 1 with the all-time caveat banner until
   `v_lobbying_index_year` lands? Or hold the page until that view exists?
