# Chart and Table Style Guide

## Charts

Charts must answer a user question.

Good:
- attendance over time
- yearly payment change
- vote result breakdown
- interests by year
- lobbying contacts by period

Bad:
- decorative charts
- pie charts with too many categories
- random gradients
- charts that duplicate a table without insight

## Temporal charts

- X-axis left to right
- chronological order
- show missing periods if the data exposes them
- do not interpolate missing data

## Ranking cards vs tables

Use **ranking cards** (HTML rendered via `st.markdown`) instead of a `st.dataframe` when:
- The primary question is about extremes (top / bottom N)
- Each record carries a strong identity element (name + medal + hero statistic)
- The rendered set is intentionally small (≤ 15 cards per side)

Use **`st.dataframe`** when:
- The user needs to browse or sort a full list (> 15 rows)
- The question is "find member X" rather than "who is top/bottom?"
- The partial-year fallback is active (all members at the same count — cards add no value)

**Never replace a full browse table with cards.** Card rendering at 100+ rows is slow and buries the data.

---

## Tables

Tables are evidence.

Use:
- clear column labels
- type-aware column config
- compact but readable density
- search/filter controls outside the table
- row selection where useful
- current-view CSV export

Avoid:
- raw snake_case labels
- unreadable long text columns without configuration
- hidden source/provenance
- exporting data different from what the user sees

### Row numbering for ambiguous tables

When a table may contain rows that share the same visible values (e.g. same date appearing
twice due to a pipeline deduplication gap), add a `#` row-number column as the first column
so rows are at minimum distinguishable:

```python
tl_table["#"] = range(1, len(tl_table) + 1)
```

Use `st.column_config.NumberColumn("#", width="small")`. This is a UI stopgap — the
underlying duplicate issue must be flagged as `TODO_PIPELINE_VIEW_REQUIRED`.

---

## Timeline strip chart (attendance / sitting dates)

Use a layered Altair `mark_tick` chart when showing a member's individual attendance days
across a calendar year. This is clearer than a heatmap or bar chart for sparse event data.

**Pattern:**

```python
today = datetime.date.today()
domain_end = today.isoformat() if year >= today.year else f"{year}-12-31"
domain_start = f"{year}-01-01"

# Gray background band — makes recess gaps obvious
bg_df = pd.DataFrame({"start": [domain_start], "end": [domain_end]})
background = (
    alt.Chart(bg_df)
    .mark_rect(color="#e5e7eb", opacity=1.0, cornerRadius=3)
    .encode(
        x=alt.X("start:T", scale=alt.Scale(domain=[domain_start, domain_end]), axis=None),
        x2=alt.X2("end:T"),
    )
)

ticks = (
    alt.Chart(df)
    .mark_tick(size=72, thickness=6, opacity=1.0)
    .encode(
        x=alt.X(
            "sitting_date:T",
            title=None,
            axis=alt.Axis(format="%b", tickCount="month", labelAngle=0,
                          labelFontSize=12, grid=False, domain=False, tickSize=0),
            scale=alt.Scale(domain=[domain_start, domain_end]),
        ),
        color=alt.value("#16a34a"),
        tooltip=[alt.Tooltip("date_str:N", title="Date attended")],
    )
)

chart = alt.layer(background, ticks).properties(height=100).configure_view(strokeWidth=0)
```

**Rules:**
- Always layer a gray background rect first so the green ticks pop and gaps are visible
- Smart domain clipping: current/future year clips right edge to `today.isoformat()`; past
  years use `{year}-12-31` so the full calendar is shown and recess gaps are informative
- Month labels on x-axis (`format="%b"`, `tickCount="month"`) — no date grid lines
- Tooltip shows the formatted date string, not the raw ISO value
- `configure_view(strokeWidth=0)` removes the chart border
