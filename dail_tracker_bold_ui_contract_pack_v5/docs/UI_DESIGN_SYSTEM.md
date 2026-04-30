# Dáil Tracker UI Design System

## Mandatory API rules

These apply to every page. Do not use the deprecated alternatives.

| Task | Correct | Forbidden |
|---|---|---|
| Insert HTML/CSS | `st.html(...)` | `st.markdown(..., unsafe_allow_html=True)` |
| Button fill width | `width="stretch"` | `use_container_width=True` |
| 2–5 option toggle | `st.segmented_control(...)` | `st.radio(..., horizontal=True)` |
| Multi-select pills | `st.pills(...)` | dropdown for year navigation |
| Vertical spacing | `st.space(n)` | `st.write("")` spacer |
| Status chips | `st.badge(label, color=...)` | verbose markdown badge syntax |
| Icons | `:material/icon_name:` | emoji strings in headers/page config |
| Card background | `background: #ffffff` | `var(--surface)` (resolves to warm beige) |
| Dynamic text in HTML | `html.escape(value)` | bare f-string into HTML strings |
| Card + right button | CSS grid unit or `st.columns([6,1], vertical_alignment="center")` | ghost HTML, JS, `position:absolute` |

---

## Style

Editorial civic data reference.

The UI should feel like:
- investigative newspaper
- public evidence database
- parliamentary accountability reference
- serious research tool

It should not feel like:
- generic Streamlit
- fintech SaaS dashboard
- AI demo
- glassmorphism
- decorative analytics wallpaper

## Layout language

Use:
- strong editorial hero
- compact command/filter bar
- clear evidence summary
- dense but readable tables
- source/provenance panels
- focused detail areas
- left-to-right timelines for time evolution

## Reusable components

Prefer adding reusable helpers under:

```text
utility/ui/
```

Useful helpers:
- `civic_page_header`
- `evidence_summary_panel`
- `filter_bar`
- `provenance_box`
- `source_link_list`
- `current_view_export`
- `member_identity_strip`
- `empty_state`
- `section_heading`

---

## Ranking and leaderboard components

### When to use ranked cards instead of a table

A table is the default. Switch to ranked cards when:
- the primary question is "who is top / who is bottom?" (not "show me all members")
- a strong emotional or competitive dimension exists (attendance extremes, most declarations)
- the record set is intentionally small (top 3 / bottom 3, top 10 leaderboard)

Never replace a full browse table with cards — card rendering at 100+ rows is too slow and buries the data.

### Hall of Fame / Hall of Shame card pattern

Used when the page's primary view is a good-cop / bad-cop split (e.g. attendance best vs worst).

**Layout:**
- Two `st.columns` — left column for "good" (green), right column for "bad" (red/amber)
- Three cards per side (top 3 / bottom 3) rendered as a **single `st.markdown` HTML block** — do not call `st.markdown` per card or Streamlit's inter-element padding breaks the visual grouping

**Medal convention:**
```python
_GOOD_MEDALS = ["🥇", "🥈", "🥉"]   # top attenders / best performers
_BAD_MEDALS  = ["💀", "👻", "😴"]   # lowest attenders / worst performers
```

**Card Python helper:**
```python
def _hall_card(row, medal: str, side: str) -> str:
    name  = row["member_name"]
    meta  = f"{row['party_name']} · {row['constituency']}"
    days  = int(row["attended_count"])
    pct   = row.get("rate_pct", "")
    pct_s = f" ({pct}%)" if pct else ""
    return (
        f'<div class="att-hall-card-{side}">'
        f'  <div class="att-hall-medal">{medal}</div>'
        f'  <div class="att-hall-body">'
        f'    <p class="att-hall-name">{name}</p>'
        f'    <p class="att-hall-meta">{meta}</p>'
        f'    <p class="att-hall-days"><strong>{days}</strong> days attended{pct_s}</p>'
        f'  </div>'
        f'</div>'
    )
```

**Rendering all cards as one block:**
```python
html_block = "".join(_hall_card(r, medal, "good") for r, medal in zip(top_rows, _GOOD_MEDALS))
st.html(html_block)   # st.html — never st.markdown(..., unsafe_allow_html=True)
```

**CSS classes (in `shared_css.py`):**
- `.att-hall-card-good` — green gradient background, left green border
- `.att-hall-card-bad` — red gradient background, left red border
- `.att-hall-heading-good` / `.att-hall-heading-bad` — colored heading above each side
- `.att-hall-medal` — 2rem emoji, fixed width, flex-shrink 0
- `.att-hall-name` — 1rem bold member name
- `.att-hall-meta` — 0.76rem muted party/constituency
- `.att-hall-days` — key stat; `strong` inside uses 1.25rem font-size, font-weight 800

**Partial / in-progress year:** When the current year has too little data for a meaningful good/bad ranking (e.g. all members tied at the same day count), display `st.info` with an in-progress notice and fall back to a flat `st.dataframe` ranked by `rank_high ASC`. Detect with:
```python
import datetime
if year >= datetime.date.today().year:
    # partial year — show flat list, not Hall of Fame/Shame
```

**Navigation buttons below cards:** After the card groups, render last-initial navigation buttons so users can jump to a member's profile without scrolling through the flat table:
```python
cols = st.columns(len(buttons))
for col, label in zip(cols, sorted_initials):
    with col:
        if st.button(label, key=f"att_nav_{label}"):
            st.session_state["att_member_sel"] = label_to_name[label]
            st.rerun()
```

### Leaderboard ranking card pattern

Used for ranked member leaderboards (e.g. interests page "most declarations").

- Render one card per member showing: rank number, name, party, top statistic pills, highlight quote
- Rank number uses `.int-rank-num` (large, muted); top-10 use `.int-rank-num-top` (primary blue)
- Stat pills use `.int-stat-pill` (neutral) or `.int-stat-pill-accent` (burnt-orange accent) for highlighted figures
- Highlight quote uses `.int-highlight-quote` (italic, left border, ellipsis overflow)
- Use `→` navigation triggers in an `st.columns([5, 1])` split (never `[11, 1]`) to link to the member's profile

**CSS classes (in `shared_css.py`):**
- `.int-rank-card` — flex row card with border, shadow
- `.int-rank-num` — 1.5rem, weight 800, muted color
- `.int-rank-num-top` — overrides color to `--dt-primary`
- `.int-rank-body` — flex: 1, min-width: 0
- `.int-rank-name` — 1.02rem, weight 700
- `.int-rank-meta` — 0.8rem, muted
- `.int-rank-stats` — flex row of pills
- `.int-stat-pill` — pill chip (muted)
- `.int-stat-pill-accent` — pill chip (accent color, for key figures)
- `.int-highlight-quote` — italic quote line with left border

### Large typography convention for key statistics

When a single number is the hero of a card (days attended, total declarations, amount), use:
```css
font-size: 1.25rem;
font-weight: 800;
letter-spacing: -0.02em;
```

The surrounding label text should be 0.82rem or smaller. This contrast creates a clear visual hierarchy between the number and its label without needing color.

## Primary vs secondary view complexity

### Primary view (browse / index)

The user has not made a selection yet. Lead with the index table immediately after the filter bar.

Do **not** add before the table:
- charts (bar, heatmap, or otherwise)
- a stat strip that duplicates data already in the hero badges
- multiple stacked expanders

These bury the table and overwhelm the user before they have context for what they are looking at.

Accepted structure:
```
hero (kicker + title + dek + badges)
command bar (filters)
────────────────────────────
member / record table         ← primary content
export button
────────────────────────────
about & provenance expander   ← collapsed, at bottom (if contract requires it)
```

### Secondary view (detail / profile / drilldown)

The user selected a record. Complexity is acceptable because scope is narrow. Accepted structure:
```
identity strip (name, party, constituency)
summary stats (3 columns max)
────────────────────────────
section 1: timeline or calendar chart + export
section 2: year breakdown table + export
────────────────────────────
about & provenance expander   ← collapsed, at bottom (if contract requires it)
```

**Provenance rule:** A provenance footer is standard on most pages. Pages that merge
many disparate sources (e.g. a cross-domain member profile) may omit it where a combined
footer would be misleading. Check the page contract's `source_links` section.

A third content section requires explicit justification.

## CSS

Shared CSS belongs in:

```text
utility/shared_css.py   ← inject_css() — loaded by every page at startup
```

Do not add new styles to `utility/styles/base.css` — it is a legacy file loaded only by `lobbying_2.py`.
Do not create page-local CSS systems.
Read `utility/shared_css.py` before adding any class — it may already exist.

Add reusable classes such as:

```text
dt-page-shell
dt-hero
dt-command-bar
dt-evidence-grid
dt-evidence-card
dt-section
dt-section-header
dt-table-toolbar
dt-member-strip
dt-source-list
dt-empty-state
dt-provenance-box
```
