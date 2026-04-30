# CSS Reuse Guide

**Primary CSS file — add all new classes here:**
`utility/shared_css.py` — the `inject_css()` function is called by every page at startup.

**Legacy file — do not add new styles here:**
`utility/styles/base.css` — loaded only by `lobbying_2.py` for backwards compatibility.

Do not put a new CSS system inside a page file. No inline `style=""` attributes.

**Before writing any CSS, read `utility/shared_css.py` first** — the class you need may already exist.

---

## Card background rule

Always use `background: #ffffff` for cards and pills. **Never `background: var(--surface)`.**

`var(--surface)` resolves to `oklch(94% 0.007 75)` — a warm beige matching the page background. Cards set to `var(--surface)` are invisible (zero contrast against the page).

---

## Compact card + adjacent → button (ranked-list rows)

The standard pattern for any ranked list with a navigation button. Used on interests and payments pages.

**Why this pattern:** `st.columns()` always expands to full page width. Without the `:has()` collapse rule, even a `fit-content` card leaves the `→` button stranded at the far right of the page.

**Streamlit — 2 columns `[5, 1]`:**
```python
for i, (_, row) in enumerate(df.iterrows()):
    c1, c2 = st.columns([5, 1])
    c1.html(card_html(row))                          # st.html — never unsafe_allow_html
    c2.html('<div class="dt-nav-anchor"></div>')
    if c2.button("→", key=f"row_{i}"):
        st.session_state["selected_td"] = str(row["member_name"])
        st.rerun()
```

**Card outer div — must use `inline-flex` + `fit-content`:**
```css
.my-card-class {
    display: inline-flex;
    width: fit-content;
    max-width: 100%;
    background: #ffffff;   /* never var(--surface) */
    ...
}
```

**`:has()` collapse rule — paste this into `shared_css.py` for each new card class:**
```css
[data-testid="stHorizontalBlock"]:has(.my-card-class) {
    width: fit-content !important;
    max-width: 100% !important;
    gap: 0.4rem !important;
}
[data-testid="stHorizontalBlock"]:has(.my-card-class) [data-testid="stColumn"] {
    width: auto !important;
    flex: 0 0 auto !important;
    min-width: 0 !important;
}
```

**Button vertical centering:**
```css
.dt-nav-anchor { margin-top: 1.1rem; }
```

**Rules:**
- Amount badges and extra metadata go **inside the card HTML** — not in a third column
- The trigger class (e.g. `.int-rank-card`, `.pay-name-row`) must be unique per page so `:has()` selectors don't collide
- Do not use 3-column layouts (`[7, 1, 2]` etc.) for card + button rows — always 2 columns `[5, 1]`

---

---

## Core layout classes (`dt-*`)

- `dt-page-shell`
- `dt-hero`
- `dt-kicker` — all-caps label above a hero title
- `dt-dek` — subtitle/deck below a hero title
- `dt-badge` — small inline badge chip
- `dt-command-bar`
- `dt-evidence-grid`
- `dt-evidence-card`
- `dt-section`
- `dt-section-header`
- `dt-table-toolbar`
- `dt-member-strip`
- `dt-source-list`
- `dt-empty-state`
- `dt-callout`
- `dt-provenance-box` — sourcing box with left accent border

---

## Attendance Hall of Fame / Hall of Shame (`att-hall-*`, `att-cop-head-*`)

Used on the Attendance page primary view good-cop / bad-cop split. Two columns: top attenders (green) on the left, lowest attenders (red) on the right.

CSS classes for layout (flex, borders, backgrounds) work correctly when injected via `inject_css()` in `shared_css.py`. Do not use inline `style=""` dicts — use the named classes below.

| Class | Purpose |
|---|---|
| `att-hall-card-good` | Card container — green border-left, green tint background |
| `att-hall-card-bad` | Card container — red border-left, red tint background |
| `att-hall-rank` | `#1` rank number inside card |
| `att-hall-medal` | Medal emoji inside card |
| `att-hall-body` | Name + meta flex body |
| `att-hall-name` | Member name inside card |
| `att-hall-meta` | Party · constituency meta line |
| `att-hall-badge-good` | Days badge — green variant |
| `att-hall-badge-bad` | Days badge — red variant |
| `att-hall-badge-num` | The large number inside the badge |
| `att-hall-badge-label` | The label below the number in the badge |
| `att-hall-heading-good` | Section heading — green text + green underline |
| `att-hall-heading-bad` | Section heading — red text + red underline |
| `att-cop-head-good` | Compact heading variant (0.68rem uppercase, green) |
| `att-cop-head-bad` | Compact heading variant (0.68rem uppercase, red) |

**Card structure:** flex row — `#rank` · medal emoji · name + meta body · days badge (right-aligned).

**Medal convention:**
```python
_GOOD_MEDALS = ["🥇", "🥈", "🥉"]
_BAD_MEDALS  = ["💀", "👻", "😴"]
```

Render the heading and all cards for a side as **one** `st.markdown()` call — never one call per card, and never a separate call for the heading vs the cards.

---

## Interests leaderboard (`int-rank-*`, `int-stat-pill*`, `int-highlight-quote`)

Used on the Interests page for the "most declared interests" leaderboard and on any future ranked-member primary view.

| Class | Purpose |
|---|---|
| `int-rank-card` | Flex row card with border + shadow |
| `int-rank-num` | 1.5rem weight-800 rank number (muted color) |
| `int-rank-num-top` | Overrides `int-rank-num` color to `--dt-primary` for top-10 |
| `int-rank-body` | Flex-1 content area |
| `int-rank-name` | 1.02rem bold member name |
| `int-rank-meta` | 0.8rem muted party · constituency |
| `int-rank-stats` | Flex row wrapping area for stat pills |
| `int-stat-pill` | Neutral pill chip (muted background) |
| `int-stat-pill-accent` | Accent pill chip (`--dt-accent` border/color, warm background) — use for the single most important figure |
| `int-highlight-quote` | 0.8rem italic quote with 2px left border; ellipsis overflow |

---

## Diff badges (`int-diff-badge-*`)

Used on declaration items to mark changes between years.

| Class | Purpose |
|---|---|
| `int-diff-badge-new` | Green pill — "NEW" declaration added this year |
| `int-diff-badge-removed` | Red pill — "REMOVED" declaration no longer present |

Always add a trailing space after the badge so the declaration text is not flush against it:
```python
badge = '<span class="int-diff-badge-new">NEW</span> '
```

---

## Interests category heading (`int-category-section`)

Uppercase label above a group of declarations within a member's profile view.

```html
<div class="int-category-section">Property &amp; Land</div>
```

---

## Typography convention for hero statistics

When a single number is the main point of a card (days attended, count of declarations, amount), use large bold numerals inside the surrounding label text:

```html
<p class="att-hall-days"><strong>94</strong> days attended</p>
```

The `<strong>` inside `.att-hall-days` picks up `font-size: 1.25rem; font-weight: 800; letter-spacing: -0.02em` from the CSS rule. Surrounding label text stays at 0.82rem. The contrast creates visual hierarchy without color changes.
