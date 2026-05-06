# Chart idea — Branded, provenance-linked chart export

Status: design brief, not yet implemented.
Authored: 2026-05-06.
Source: shape skill brief, this branch.

## User question this feature answers

"I want to share this Dáil Tracker chart on Twitter / in a report / in a slide. How do I credit the source and let my reader verify the data behind it?"

This is not a page — it's a cross-cutting **chart affordance** that lives on every page that already renders a chart. Treat it as a shared component, not a route.

## Current UI problems

- **No chart export exists today.** `utility/ui/export_controls.py` handles CSVs only. For images, users currently screenshot, which strips all provenance.
- **Chart provenance is invisible once the image leaves the app.** A screenshot of `utility/pages_code/attendance.py` is just a chart with no source, no date, no link back.
- **Filter state is partially in the URL but not consistently.** 11 pages call `st.query_params`, but year pill / member / topic filters aren't all reflected there. Even if you copy the URL, it may not reproduce what you saw.
- **Brand identity is absent from charts.** The `--accent` token (warm terracotta, oklch(51% 0.130 62)) is used in CSS chrome but doesn't appear on charts, so screenshots feel ownerless.

## Bold redesigned layout — the chart frame pattern

A single reusable component: `branded_chart(fig, *, page_id, chart_id, data_source_label, permalink_params)`.

Visual structure of every exported chart:

```
┌──────────────────────────────────────────────────┐
│  Plenary attendance — 2024                       │  ← chart title (existing)
│                                                  │
│      [the chart itself, unchanged]               │
│                                                  │
│  ───────────────────────────────────────────     │
│  DÁIL TRACKER  ·  dailtracker.ie/attendance?…    │  ← brand footer band
│  Data: Oireachtas Commission Annual Report 2024  │     (rendered as Plotly
│  Generated 6 May 2026                            │      annotations, in-chart)
└──────────────────────────────────────────────────┘
```

The footer band is **rendered into the chart itself** as Plotly annotations — not overlaid post-hoc, not a separate image step. The on-screen chart and the downloaded PNG are byte-for-byte the same. One render path.

Below the chart, a `st.popover("Share this chart", icon=":material/share:")` with three actions — Copy link, Download PNG, Download SVG — and a one-line explainer that the chart includes attribution and a link back to the live filter.

## Interaction model

- **Primary view:** chart renders with the footer band already visible. What you see is what you share.
- **Share action:** popover opens beneath the chart. No tabs, no settings panel.
- **Permalink:** the URL in the popover *is* the current page URL with the chart's filter state encoded. Copy and paste into a fresh tab → reproduces the chart exactly.
- **Navigation flow:** entirely in-place. No new pages, no modals.

## Temporal behaviour

Charts already control their own temporal scope via `st.pills`. The export inherits whatever year is selected when Share is clicked. The permalink encodes `?year=2024`. The footer band's "Generated <date>" is today's date in `Europe/Dublin`, formatted as `"6 May 2026"` (matches the editorial-newspaper tone).

## Source-link behaviour

This feature **is** a source-link mechanism, projected onto the chart medium. Three layers of provenance, in the order the consumer encounters them:

1. **Brand band** — "DÁIL TRACKER" wordmark answers *who*.
2. **Permalink** — `dailtracker.ie/attendance?year=2024` answers *where to verify, with the same filters*.
3. **Data source line** — "Data: Oireachtas Commission Annual Report 2024" answers *what underlies the chart*.

This text comes from the existing per-page `source_links` contract section (e.g. `attendance.yaml`'s `per_year_source.note`). It does not introduce new metadata — it surfaces existing pipeline metadata into the image. Document this in `_shared_ui_policy.yaml` as a new `chart_provenance_export` section.

For pages where the contract says "no provenance footer" (e.g. `member_overview.yaml`, which spans 5–6 sources), the share affordance still works but the **data source line is omitted** from the band. Only the permalink + brand wordmark appear. Same rationale as the page-level decision: a merged data source attribution would mislead.

## Chart and table strategy — what gets the export first

Phased rollout, ordered by share-worthiness:

**Phase 1 — flagship charts (must ship together):**
- `attendance.py` — calendar heatmap on member profile (the most screenshot-worthy single chart in the app)
- `payments.py` — yearly evolution line/bar chart per member
- `lobbyist_poc.py` — topic timeline (e.g. the Immigration & Asylum 2016–2026 view)

**Phase 2:**
- `committees.py`
- `legislation_poc.py` / `legislation_si_poc.py`
- `attendance_overview.py`
- `vote_explorer.py`

**Phase 3:**
- Each chart inside `member_overview.py` domain tabs (with the no-provenance-line rule above)

Tables are out of scope. CSV export already covers them.

## Empty state copy

- **Chart has no data after filtering** (existing): unchanged. No share popover renders — there is nothing to share.
- **Permalink couldn't be constructed** (filter state not yet wired to query_params): show in the popover, replacing the URL block:

  > "Sharing this chart requires the page filters to be in the URL. We're rolling this out page by page — this chart isn't there yet."

  This is honest about phased delivery and prevents silent broken links.

## Visual differentiators

- **The on-screen chart already shows the band.** Most BI tools add branding only on export, so the user can't verify what their audience will see. We render the band on screen.
- **No watermark over the data.** The band sits below the plot area, never on top of it. Chart legibility is sacred.
- **Brand uses `--accent` token** (terracotta), not Streamlit purple. First place the editorial brand reaches the chart medium.
- **Permalink shown as text, not a QR code.** QR codes are for posters; this is for digital sharing. Keep V1 simple.
- **Generation timestamp appears only in the exported image, not on screen.** It's a property of the artefact, not of the live chart.

## TODO_PIPELINE_VIEW_REQUIRED items

- **Canonical chart filter-state schema per page.** Each chart-bearing page needs an explicit, contract-declared list of filters that participate in the permalink. Belongs in each page contract under a new `permalink_filters` section. Without this, the permalink is fragile and can drift silently.
- **`data_source_label` scalar on chart-bearing views.** Today, source attribution lives in YAML prose. For chart export it needs to be a single short string ("Oireachtas Commission Annual Report 2024") returned with the data, parameterised by year/scope. Hardcoding it in Streamlit is permitted as a stopgap (qualifies under `permitted_hardcoding` — immutable historical fact, official document) but the long-term home is the view.
- **Per-chart canonical short URL** (optional). A short link service (`dailtracker.ie/c/abc123`) so the band stays under one line on narrow charts. Not required for V1.

## Implementation plan

**New files:**
- `utility/ui/branded_chart.py` — `branded_chart(fig, *, page_id, chart_id, data_source_label, permalink_params, on_screen_band=True) -> None`. Adds annotations to the Plotly figure, renders it via `st.plotly_chart`, then renders a `st.popover` underneath with the share controls.
- `utility/ui/permalink.py` — `build_permalink(page_id, params: dict) -> str` and `sync_query_params(params: dict) -> None`. Pushes filter state to the URL on every change so the popover URL is always live.

**Files modified:**
- `utility/shared_css.py` — add `.dt-share-popover` styling (white card, `--accent` border on copy button). No new colour tokens; reuse `--accent`, `--text-primary`, `--text-meta`.
- `utility/ui/export_controls.py` — leave CSV export as-is. Add sibling helper `chart_export_button(fig, filename)` that wraps `kaleido` PNG/SVG bytes into `st.download_button`.
- `dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/_shared_ui_policy.yaml` — add `chart_provenance_export` section declaring band format, on-screen requirement, and the `member_overview` carve-out.
- Each Phase 1 page contract (`attendance.yaml`, `payments.yaml`, `lobbying.yaml`) — add `permalink_filters: [...]` listing which filters participate.
- Each Phase 1 page file — replace `st.plotly_chart(fig, ...)` calls with `branded_chart(fig, ...)`. Wire filter widgets to `sync_query_params`.

**CSS classes to add (in `shared_css.py`):**
- `.dt-share-popover` — popover container, `background:#ffffff`
- `.dt-share-link` — monospace, line-wrap-anywhere, `color: var(--text-primary)`
- `.dt-share-copy-btn` — `--accent` border, label "Copy"
- `.dt-share-actions` — flex row for PNG / SVG buttons

**Helpers to reuse:**
- `st.html` for popover internals (per `_shared_ui_policy.yaml` `streamlit_api_rules`)
- `st.popover`, `st.download_button`, `st.plotly_chart` (all in `library_policy.native_streamlit_first`)
- `html.escape` on every dynamic value inserted into the band annotation text

**Dependency:**
- Add `kaleido` to `pyproject.toml`. Pure-Python Plotly companion, no network calls — clears the `forbidden_in_streamlit: api_calls` rule. No JavaScript involved.

**Acceptance tests:**
- `branded_chart_renders_band_on_screen`
- `downloaded_png_byte_matches_visible_chart_band`
- `permalink_in_popover_reproduces_filter_state_when_pasted`
- `member_overview_charts_omit_data_source_line`
- `no_unsafe_allow_html_in_branded_chart`
- `no_kaleido_call_during_normal_page_load` (only fires on download click)

## Open question

**Generation date in the band — include it?** Newspapers do this; BI tools don't. The argument for: a chart shared in 2027 should announce that it was generated in 2026 so the reader knows the data may have moved on. The argument against: it ages the artefact and discourages reuse. **Recommend including it** — matches editorial-accountability tone, honest about freshness, cost is one line of text.
