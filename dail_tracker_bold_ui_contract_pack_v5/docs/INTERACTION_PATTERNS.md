# Shared Interaction Patterns

Use these patterns across pages instead of inventing new controls each time.

---

## Year pills selector

Use this pattern on any page that has reporting-year data. It was developed on the Attendance page
and is the standard year-navigation control across the app.

**When to use:** the data is sliced by year (reporting year, not event date) and the user needs to
navigate between years without leaving the page.

**Control:** `st.pills` — horizontal pill buttons, no dropdown.

```python
year_options = [str(y) for y in opts["years"]]  # DESC from SQL: ORDER BY year DESC
selected_year_str = st.pills(
    "Year",
    options=year_options,
    default=year_options[0],   # most recent year is the default — never "All years"
    key="<page>_year",
    label_visibility="collapsed",
)
selected_year = int(selected_year_str) if selected_year_str else int(year_options[0])
```

**Rules:**
- Years must be ordered newest first: 2026, 2025, 2024, 2023 …
- Default to the most recent year — do not default to "All years"
- Place in the **main content area**, not the sidebar
- Use the same pattern in both the primary view and the profile/secondary view (with a different key)
- Handle deselection (pills allows clicking again to deselect) by falling back to `year_options[0]`

**Why:** Users care most about current-year data. Past years are available for curious users but
should not be the first thing seen. This approach was validated on the Attendance page.

---

## Sidebar layout order (all pages)

When a page has both member search and notable member shortcuts in the sidebar, the order is always:

```
1. Text search input  (placeholder "e.g. Name", label_visibility="collapsed")
2. Browse all members selectbox  (filtered by search text, label_visibility="collapsed")
3. st.divider()
4. Notable members  (chip buttons in 2 columns)
```

**Rule:** Search and browse are primary navigation tools — they go at the top. Notable members are shortcuts — they go below. Never put notable members above the search.

```python
member_search: str = st.text_input(
    "", placeholder="e.g. Mary Lou McDonald",
    key="<page>_sidebar_search", label_visibility="collapsed",
)
sq = member_search.strip().lower()
filtered = [n for n in opts["members"] if sq in n.lower()] if sq else opts["members"]
chosen = st.selectbox(
    "Browse all members",
    ["— select a member —"] + filtered,
    key="<page>_member_sel", label_visibility="collapsed",
)
if chosen != "— select a member —" and st.session_state.get("<key>") != chosen:
    st.session_state["<key>"] = chosen
    st.rerun()

st.divider()
st.markdown("**Notable members**")
chip_cols = st.columns(2)
for i, name in enumerate(_NOTABLE_TDS):
    if chip_cols[i % 2].button(name, key=f"chip_<page>_{name}", use_container_width=True):
        st.session_state["<key>"] = name
        st.rerun()
```

**No main-area search input:** The sidebar search replaces any `st.text_input` that was previously in the main content area below the year pills. The main content area shows the full ranked list or good/bad view — no filtering in the main area.

---

## Two-stage member flow

Many pages show a list of members and allow drilling into one member's record.

**Stage 1 — Primary / browse view:**
- Year pills (if applicable) → member list (always full, unfiltered)
- Member navigation via sidebar search + selectbox or notable member chips
- Clean, minimal, no charts between year pills and the ranked list
- Clicking a row navigates to Stage 2

**Stage 2 — Secondary / profile view:**
- Back button at the **top of the main content area** — not only in the sidebar
- Member identity strip: name, party, constituency
- Summary stats for selected year (or all time)
- Detail sections: timeline/calendar, year breakdown, source links
- Year pills (if applicable) — allows switching between years without returning to Stage 1

**Back navigation rule:** The "← Back to all members" button must appear at the top of the main
content in Stage 2. A sidebar-only back button will be missed. Example:

```python
if st.button("← Back to all members", key="<page>_back"):
    st.session_state["selected_td_<page>"] = None
    st.rerun()
```

**Navigation triggers for Stage 1 → Stage 2:**
- Row click on the member table (`on_select="rerun"`, `selection_mode="single-row"`)
- Sidebar selectbox
- Sidebar notable-member chips

---

## ISO date data

If a column is ISO date formatted like `2015-01-01`, treat it as event-date data.

Preferred controls:
- `st.date_input` date range
- left-to-right timeline when showing change over time

Avoid:
- radio buttons for individual dates
- long dropdowns of dates
- sorting timelines by value when the purpose is chronological evolution

---

## Reporting year data

If the data is yearly, treat it as reporting-year data.

Preferred control: **year pills** (see above). Use `st.pills` horizontal.

Rules:
- Newest year first
- Default to most recent year
- Oldest-to-newest left to right in charts (time flows left to right)
- No "All years" as default — it produces a low-information aggregate view

---

## Data provenance footer

Every page must expose its data source and freshness. Use a single collapsed expander at the
**bottom** of the page — not between the hero and the content.

```python
def _render_provenance(summary: pd.Series, year: int | None = None) -> None:
    source   = str(summary.get("source_summary") or "—")
    fetch_ts = str(summary.get("latest_fetch_timestamp_utc") or "—")[:19]
    mart_v   = str(summary.get("mart_version") or "—")
    code_v   = str(summary.get("code_version") or "—")
    with st.expander("About & data provenance", expanded=False):
        st.markdown(_CAVEAT)   # plain-language explanation of what the data shows and doesn't show
        if year:
            st.caption(f"Showing data for: {year}. {_YEAR_SOURCE_NOTE}")
        st.caption(f"Source: {source}  ·  Fetched: {fetch_ts}  ·  Mart: {mart_v}  ·  Code: {code_v}")
```

**Per-year source note:** For pages where data is sourced from annual documents (e.g. Oireachtas
attendance PDFs), include a `_YEAR_SOURCE_NOTE` that explains the year label and flags the future
per-year PDF link:

```python
_YEAR_SOURCE_NOTE = (
    "Each year's data will link to the official Oireachtas [document name] for that year "
    "(e.g. 2025, 2024, 2023) once the pipeline exposes per-year source URLs."
)
```

Use `TODO_PIPELINE_VIEW_REQUIRED: per-year source PDF URL` until the pipeline exposes it.

**Rules:**
- One expander, not two ("About" and "Data provenance" must be merged)
- Always collapsed by default
- Always at the bottom of the page or profile view
- Show the current year label when the view is year-scoped
- Do not show raw file paths as the source — flag them with TODO_PIPELINE_VIEW_REQUIRED

---

## CSV export

CSV export should export the current displayed view:
- active filters
- date/year scope
- visible table rows
- selected columns if column visibility exists

Do not recompute an export dataset in Streamlit.

Use `export_button()` from `utility/ui/export_controls.py`. It disables automatically when the
dataframe is empty.

---

## Member drilldown

Many pages should allow a user to focus on one member:
- row selection
- member search
- member identity strip
- detail table
- source links
- export selected member view

Do not join across pages in Streamlit to create a 360-degree member profile. If needed:

```text
TODO_PIPELINE_VIEW_REQUIRED: member_profile_view
```

---

## Timeline strip (sitting/attendance dates)

Use when showing individual dated records (attendance days, votes, events) for one member
across a calendar year. Clearer than a heatmap for sparse event data.

**Key decisions:**
- Gray background band spanning the full domain so recess gaps read as empty space, not
  missing data
- Green `mark_tick` on top — tall and thick enough to be legible (`size=72, thickness=6`)
- Smart domain clipping: current/future year clips right edge to today; past years show
  full year (Dec 31) so recess gaps are preserved and informative
- Month labels on x-axis, no grid lines
- Collapsed date table below the strip for users who want the exact list

**When to add a `#` row column to the date table:**
If the underlying view may produce duplicate rows for the same date (pipeline deduplication
gap), add `tl_table["#"] = range(1, len(tl_table) + 1)` as the first column so rows are
distinguishable. Always accompany with a `TODO_PIPELINE_VIEW_REQUIRED` callout.

See `CHART_AND_TABLE_STYLE_GUIDE.md` for the full Altair code pattern.

---

## Hall of Fame / Hall of Shame primary view

Use this pattern when the primary user question is about extremes — who is at the top, who is at the bottom — rather than browsing all members. Validated on the Attendance page.

**When to use:**
- Page question has a natural good / bad polarity (attendance, expenses, compliance)
- Data for the selected year is complete enough for a meaningful ranking (see partial-year caveat below)
- The ranked set is small enough for cards (top 3 / bottom 3 per side)

**Structure:**
```
year pills (newest first)
────────────────────────────
[HALL OF FAME col]   [HALL OF SHAME col]
  🥇 Name             💀 Name
  🥈 Name             👻 Name
  🥉 Name             😴 Name
────────────────────────────
[last-initial navigation buttons]
────────────────────────────
name search → ranked card list for matching member(s)
────────────────────────────
export button
about & provenance expander (collapsed)
```

**Medal convention:**
```python
_GOOD_MEDALS = ["🥇", "🥈", "🥉"]   # top performers
_BAD_MEDALS  = ["💀", "👻", "😴"]   # lowest performers
```

**Rendering — single HTML block per side:**
```python
html = "".join(_hall_card(row, medal, "good") for row, medal in zip(top_rows, _GOOD_MEDALS))
st.markdown(html, unsafe_allow_html=True)
```

Never call `st.markdown` once per card — Streamlit's inter-element padding breaks the visual grouping.

**Tie-breaking sort** (prevents the same person appearing on both sides when many members share the same count):
```python
top3 = df.sort_values(["rank_high", "attended_count"], ascending=[True, False]).head(3)
bot3 = df.sort_values(["rank_low",  "attended_count"], ascending=[True, True ]).head(3)
```

**Navigation button alignment:**
Place all navigation buttons (both sides) in a single `st.columns(n * 2)` row **outside** both column contexts — not inside each `with col_*:` block. Cards on the two sides often differ in height (longer names, longer party strings), so buttons placed inside each column will be at different vertical positions. A shared row outside the columns guarantees alignment:

```python
all_rows = list(top.iterrows()) + list(bottom.iterrows())
keys     = [f"att_good_{i}" for i in range(n)] + [f"att_bad_{i}" for i in range(n)]
btn_cols = st.columns(n * 2)
for col, (_, row), key in zip(btn_cols, all_rows, keys, strict=False):
    if col.button(str(row["member_name"]).split()[-1], key=key, use_container_width=True):
        clicked = str(row["member_name"])
```

**Partial / in-progress year:**
When the current year is incomplete (same day count across most members), the good/bad split is misleading. Detect and fall back:
```python
import datetime
if year >= datetime.date.today().year:
    st.info("Attendance data for the current year is still being recorded. ...")
    st.dataframe(df.sort_values("rank_high"), ...)
else:
    _render_good_bad(df, year)
```

**Back button rule:** The "← Back to all members" button must be at the top of the main content area in the secondary/profile view, not only in the sidebar. Clear the selection key with:
```python
if st.button("← Back to all members", key="att_back"):
    st.session_state.pop("att_member_sel", None)
    st.rerun()
```

Use `st.session_state.pop("key", None)` — do not assign directly after a widget has been instantiated (raises `StreamlitAPIException`).

---

## Ranked leaderboard (interests / payment volume)

Use this pattern when the primary view is a ranked list of members (not a flat browse table).

**When to use:**
- The user question is "who has the most X?" (declarations, payments, lobbying contacts)
- One aggregated row per member fits on screen without overwhelming the user
- The ranked set is 10–50 members (for larger sets, use a sortable `st.dataframe` instead)

**Card structure per member:**
```
[rank #]  Name                              [→]
          Party · Constituency
          [pill: 42 declarations] [pill: landlord]
          "…highlight quote excerpt…"
```

Amount badges and secondary stats go **inside the card HTML**, not in a separate third column.

**Navigation trigger — always `st.columns([5, 1])` with `:has()` CSS collapse:**

```python
for i, (_, row) in enumerate(df.iterrows()):
    c1, c2 = st.columns([5, 1])
    c1.markdown(card_html(row), unsafe_allow_html=True)
    c2.markdown('<div class="dt-nav-anchor"></div>', unsafe_allow_html=True)
    if c2.button("→", key=f"row_{i}"):
        st.session_state["selected_td"] = str(row["member_name"])
        st.rerun()
```

The card outer div must be `display: inline-flex; width: fit-content`. Without the `:has()` CSS rule below, the card shrinks but the Streamlit column row still spans full page width, stranding the `→` button at the far right.

**Required CSS in `shared_css.py` (once per unique card class):**
```css
[data-testid="stHorizontalBlock"]:has(.my-card-class) {
    width: fit-content !important; max-width: 100% !important; gap: 0.4rem !important;
}
[data-testid="stHorizontalBlock"]:has(.my-card-class) [data-testid="stColumn"] {
    width: auto !important; flex: 0 0 auto !important; min-width: 0 !important;
}
.dt-nav-anchor { margin-top: 1.1rem; }   /* vertical centering shim — already in shared_css.py */
```

Do not use `st.columns([11, 1])` or `st.columns([7, 1, 2])` for card+button rows.

**CSS classes:** `.int-rank-card`, `.int-rank-num` / `.int-rank-num-top`, `.int-rank-body`, `.int-rank-name`, `.int-rank-meta`, `.int-rank-stats`, `.int-stat-pill` / `.int-stat-pill-accent`, `.int-highlight-quote`. See `CSS_REUSE_GUIDE.md` for the full table.

---

## Government source links

Official source links are evidence. Render them clearly:
- "Official PDF"
- "Oireachtas record"
- "Source document"
- "Government source"

Do not show raw long URLs unless necessary.
Do not construct URLs in Streamlit unless the contract explicitly gives a safe URL template.
Prefer pipeline-provided URL columns.
