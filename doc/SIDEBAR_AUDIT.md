# Sidebar — cross-page impeccable audit (2026-05-26)

> **Status update (2026-05-27):** Full uplift SHIPPED — score lifted
> **12/20 → ~17/20**. P0-1 (sidebar-fold squeeze) closed via
> `st.navigation(position="hidden")` + a horizontal top-nav strip in
> the dark banner. P1-3 (sidebar grammar) closed via `sidebar_shell`
> + `sidebar_subtitle` + `sidebar_provenance` + `sidebar_divider`
> helpers + 11-page migration. P0-2 / P1-1 / P1-2 / P1-4 / P1-5 / P2-1
> were already shipped 2026-05-26. Open: P1-6 chip handler
> (pipeline-blocked); P2-2 session-key naming (memo); P2-3 chip
> visibility (design decision); P3 polish (Streamlit-framework /
> Playwright-only). See [[project-sidebar-audit-2026-05-26]] for the
> full diary.

Captured via Playwright across all 11 pages of the Dáil Tracker app at
desktop (1440×900) and mobile (390×844). 29 screenshots in
[audit_screenshots/sidebar/](../audit_screenshots/sidebar/) cover
every page's sidebar in its default state, plus stateful interactions
(typed search, expander opened, view toggle).

Unlike the per-page audits, this one is **cross-cutting**: the goal is
to understand how the sidebar behaves AS A SYSTEM across the app, not
to grade any one page. The deliverable is a list of consistency
problems + a proposed sidebar grammar that every page should follow.

This document has three parts:
1. **Audit findings** — what's inconsistent or broken, ranked by severity.
2. **Proposed sidebar grammar** — a single template every page can adopt.
3. **The uplift prompt** — a self-contained handoff for the rework.

Capture script: [audit_screenshots/_sidebar_capture.py](../audit_screenshots/_sidebar_capture.py).

---

## Part 1 — Audit findings

### Health score

| # | Dimension          | Score | Headline finding                                                                              |
|---|--------------------|-------|------------------------------------------------------------------------------------------------|
| 1 | Accessibility      | 2/4   | Sidebar collapsed by default; per-page content sits below page-nav fold; mobile drawer unverified |
| 2 | Performance        | 3/4   | Helpers cached; sidebar widgets cheap; one minor `value_counts` style bug in lobbying chip handler |
| 3 | Theming            | 3/4   | `sidebar-label` / `page-kicker` / `page-title` classes consistent; one `st.divider` + one `st.error` leak  |
| 4 | Responsive         | 2/4   | Mobile drawer state not deterministic; lobbying chip text wraps ("An Taoiseac h"); no compact layout |
| 5 | Anti-patterns      | 2/4   | 7 distinct sidebar grammars across 11 pages; payments missing page header; Enter-trap in shared helper |
| **Total** |            | **12/20** | **Acceptable — needs a unifying grammar** — the sidebar is the most inconsistent surface in the app |

### Anti-patterns verdict

**Pass** on AI-slop in any individual sidebar. Every page's sidebar
looks like part of Dáil Tracker — no glassmorphism, no gradient
accents, no fintech chrome.

**Fail on cross-page coherence.** A user navigating from
`/rankings-attendance` to `/rankings-lobbying` to `/rankings-legislation`
sees three fundamentally different sidebar shells: a member-picker on
attendance, a combined politician+org typeahead with two collapsed
expanders on lobbying, a date-range + status + title-search panel on
legislation. The page-navigation list stays the same; the per-page
content reorganises wholesale every time. No documented vocabulary
governs what can appear, in what order, or how it should look.

---

### Executive summary

- **1 P0 systemic** — sidebar `<p>`-as-heading + `st.markdown(unsafe_allow_html=True)`
  in the shared `sidebar_page_header` helper, and the per-page content
  sits **below the page-navigation fold** on a default 1440×900
  viewport (the user must scroll the sidebar to find the page header).
- **6 P1 cross-page issues** — payments missing the helper entirely;
  `sidebar_member_filter` still has the Enter-trap (same family as
  the just-fixed `main_member_jump`); 7 different sidebar layout
  grammars; `st.error` leaks register voice on committees; "Notable
  Targets" chip text wraps mid-word on lobbying; lobbying's
  fuzzy-match notable-chip handler.
- **6 P2 polish** — `st.divider` in attendance_overview, inconsistent
  session-state key naming, notable-chip visibility inconsistency,
  lobbying's `[Org]` prefix is jargon, sidebar caption hierarchy
  varies, label semantics inconsistency.
- **3 P3 nice-to-haves** — sidebar collapse animation feedback,
  mobile drawer interaction patterns, sidebar provenance
  prominence.

The single highest-leverage move is **codifying a sidebar grammar**
([Part 2](#part-2--proposed-sidebar-grammar)) — every page conforms
to one structure, and the helpers enforce it. Doing this lifts
nearly every finding here at once.

---

### Code-level inventory

The current sidebar shape per page (from
[audit_screenshots/sidebar/A_*_desktop.png](../audit_screenshots/sidebar/)):

| Page | `sidebar_page_header` | Subtitle | Filter widgets | Member picker | Notable chips | Notes |
|---|---|---|---|---|---|---|
| member-overview | ✓ override kicker | – | – | – | – | date-range (profile only) |
| attendance | ✓ | – | – | `sidebar_member_filter` | inline | – |
| attendance_overview | ✓ | – | – | `sidebar_member_filter` | inline (after `st.divider`) | – |
| votes | ✓ | "Data covers …" caption | View toggle + Outcome + Party selectboxes | `sidebar_member_filter` (TDs view) | – | reflows per view |
| interests | ✓ | – | Chamber `segmented_control` | – | inline (sidebar) | cross-chamber state cleanup; main-panel typeahead |
| **payments** | **✗ MISSING** | – | – | `sidebar_member_filter` | inline | identity orphan |
| lobbying | ✓ | – | – | combined search + selectbox with `[Org]` prefix | inside expander | + policy-area expander |
| lobbying-poc | ✓ | – | – | same combined pattern as lobbying | – | (PoC variant) |
| legislation | ✓ | "Source: Oireachtas Open Data API" / "Bill detail" | date-range + status selectbox + title search | – | – | the most filter-heavy sidebar |
| statutory-instruments | ✓ | "Secondary legislation · Iris Oifigiúil" | – | – | – | header + caption only |
| committees | ✓ | committee count stat caption | – | – | – | header + caption only |
| glossary | ✓ | descriptive `st.caption` | – | – | – | header + caption only |

**7 distinct layouts** for 11 pages. The shared helpers are good but
the per-page composition isn't governed.

---

### P0 — Systemic (fix before next deploy)

**[P0-1] Per-page sidebar content sits below the page-nav fold by default**

- **Location**: every page. The Streamlit multipage navigation
  (`st.navigation([...]).run()` in [`app.py`](../utility/app.py))
  auto-renders an 11-item page list at the top of the sidebar; the
  per-page `with st.sidebar:` content appends below.
- **Evidence**: every `A_*_desktop.png` shot. The visible sidebar
  contains the page-nav list ("Member Overview / Attendance / Votes /
  Interests / Payments / Lobbying / Lobbying (PoC) / Legislation /
  Statutory Instruments / Committees / Glossary"), and the per-page
  content (header / filters / chips) is **off-screen at the bottom of
  the sidebar** on a default 1440×900 viewport. Only when the user
  scrolls the sidebar — or focuses a widget below the fold — does
  the page-specific content appear.
- **Stateful proof**: [`C_attendance_search_typed.png`](../audit_screenshots/sidebar/C_attendance_search_typed.png) — when Playwright `.fill("McDonald")` focused the
  sidebar search box, Streamlit auto-scrolled the sidebar to reveal it.
  The bottom strip shows "DÁIL TRACKER / Plenary Attendance / BROWSE
  ALL MEMBERS / [McDonald typed]" — proof the content exists, just
  hidden by default.
- **Impact**: this is the **single biggest sidebar UX problem in the
  app**. Citizens who don't think to scroll the sidebar never see
  notable-member chips, year filters, chamber toggles, or member
  search. On 4 of 11 pages the page header itself is hidden — the
  user can't tell what dimension they're on except via the
  highlighted nav item.
- **Why it's P0**: the audit on every individual page assumed the
  sidebar content was visible. None of those audits flagged this
  because each captured the sidebar with state already engaged. A
  cross-page sweep makes it obvious.
- **Recommendations** (ranked by tractability):
  - **A (cheapest)**: change `app.py` to use `st.navigation(...)`'s
    `position="hidden"` mode and render a custom compact nav inside
    each page's main column header instead. The sidebar becomes 100%
    per-page content. The trade-off is losing Streamlit's built-in
    accessible nav widget.
  - **B**: collapse the page-nav into a single dropdown / kebab menu
    at the top of the sidebar so it takes ~40px instead of ~440px.
    Requires custom Streamlit component or restructure.
  - **C (cheapest with zero framework risk)**: order the sidebar
    content so it's at the TOP via `st.sidebar.empty()` or a
    placeholder reservation pattern. Then `app.py` does `st.navigation(
    ..., position="bottom")` (this option may exist in newer
    Streamlit — verify).
  - **D**: leave as-is and add a `[scroll for filters]` chevron at
    the bottom of the page-nav so the affordance is at least
    discoverable. The weakest fix.

**[P0-2] `sidebar_page_header` uses `st.markdown(unsafe_allow_html=True)` instead of `st.html`**

- **Location**: [`components.py:60-63`](../utility/ui/components.py#L60-L63):
  ```python
  def sidebar_page_header(title: str, kicker: str = "Dáil Tracker") -> None:
      st.markdown(f'<p class="page-kicker">{kicker}</p>', unsafe_allow_html=True)
      st.markdown(f'<p class="page-title">{title}</p>', unsafe_allow_html=True)
  ```
- **Impact**: violates `feedback_streamlit_api_patterns` ("st.html
  over unsafe_allow_html"). Cross-page leak: every page that calls
  the helper inherits the violation. Compounding: the
  `class="page-title"` element is rendered as a `<p>` — also fails
  the `<p>`-as-heading a11y concern that the votes + interests
  audits flagged for `evidence_heading`. The page-title should be
  an `<h2>` or `<h3>` so screen readers can navigate by heading.
- **Recommendation**: rewrite as
  ```python
  st.html(
      f'<p class="page-kicker">{_h(kicker)}</p>'
      f'<h2 class="page-title">{_h(title)}</h2>'
  )
  ```
  and update `shared_css.py:.page-title` to style an `<h2>` instead
  of a `<p>`. Also adds `_h()` escaping (currently the helper does
  not escape — a malformed page that passed user input through here
  would XSS itself).

---

### P1 — Major (next pass)

**[P1-1] Payments sidebar has no `sidebar_page_header` call**

- **Location**: [`payments.py:582-596`](../utility/pages_code/payments.py#L582-L596). `with st.sidebar:` starts directly with `sidebar_member_filter`.
- **Evidence**: [`A_05_payments_desktop.png`](../audit_screenshots/sidebar/A_05_payments_desktop.png) — sidebar has no "TD Payments" identity. Even when the user
  scrolls past the page nav, the first thing they see is "BROWSE ALL
  MEMBERS" — no page name above it.
- **Audit trail**: flagged in
  [`project-payments-audit-2026-05-26`](../../../.claude/projects/c--Users-pglyn-PycharmProjects-dail-extractor/memory/project_payments_audit_2026_05_26.md)
  as P1-5; still open.
- **Recommendation**: add `sidebar_page_header("TD<br>Payments")` (or
  "Public<br>Spending" to match the hero kicker) as the first call
  inside `with st.sidebar:`.

**[P1-2] `sidebar_member_filter` has the same Enter-trap as the (now-fixed) `main_member_jump`**

- **Location**: [`components.py:985-1008`](../utility/ui/components.py#L985-L1008). The helper renders a `st.text_input` followed by a `st.selectbox`
  filtered by the input's value. The text input shows Streamlit's
  red-border "Press Enter to apply" hint, but pressing Enter only
  re-filters the selectbox; the user must additionally click an
  option to commit. Interests P1-4 fixed this in `main_member_jump`
  by replacing the pair with a single `st.selectbox` (which has
  built-in type-to-search).
- **Affected pages**: attendance, attendance_overview, payments,
  votes (TDs view).
- **Evidence**: [`C_attendance_search_typed.png`](../audit_screenshots/sidebar/C_attendance_search_typed.png) — sidebar shows "McDonald" typed; below is the un-filtered
  selectbox dropdown placeholder. The user has no visible
  confirmation that Enter does or does not commit; in fact, it
  re-filters the dropdown but doesn't pick an option.
- **Recommendation**: collapse `sidebar_member_filter` to a single
  `st.selectbox` (mirror the interests P1-4 fix). Removes the
  misleading Enter-to-apply hint, retires the redundant text input,
  and uses Streamlit's built-in type-to-search.

**[P1-3] Seven distinct sidebar layout grammars across 11 pages**

- **Evidence**: the table in the code-level inventory above. No two
  pages share the same sidebar composition; navigating between pages
  feels like switching apps even though the page-nav stays put.
- **Impact**: violates GOV.UK service standard #4 (simple to use) —
  users build an expectation of sidebar contents from one page and
  find none of it on the next. The cognitive load is borne by the
  citizen.
- **Recommendation**: codify a sidebar grammar (see
  [Part 2](#part-2--proposed-sidebar-grammar)). Every page conforms
  to one ordering. Helpers enforce it (a `sidebar_shell(...)`
  function takes named slots for `header`, `subtitle`, `filters`,
  `member_picker`, `notable_chips`, `secondary` and renders them in
  a fixed order — pages can omit slots they don't need but can't
  reorder them).

**[P1-4] `st.error` in committees sidebar when data is missing**

- **Location**: [`committees.py:950`](../utility/pages_code/committees.py#L950):
  ```python
  if df_long.empty:
      st.error(f"No committee data found for {chamber}.")
  ```
- **Evidence**: not visually captured (the data is always loaded in
  practice), but the code path renders a red Streamlit error box
  INSIDE the sidebar — off-register with the otherwise calm sidebar
  voice.
- **Recommendation**: use `empty_state(...)` per
  `feedback_streamlit_api_patterns`, OR move the empty-state to the
  main panel (where it's contextually relevant) and leave only the
  page header in the sidebar.

**[P1-5] "Notable Targets" chip text wraps mid-word in lobbying**

- **Location**: [`lobbying_2.py:531-543`](../utility/pages_code/lobbying_2.py#L531-L543). The expander renders 4 buttons in a 2-column layout: "An
  Taoiseach", "Minister for Finance", "Tánaiste", "Minister for
  Health".
- **Evidence**: [`C_lobbying_notable_open.png`](../audit_screenshots/sidebar/C_lobbying_notable_open.png) — "An Taoiseac h" splits the word across two lines. "Minister for
  Finance" / "Minister for Health" wrap to multi-line.
- **Impact**: makes the sidebar look broken on its strongest
  accountability surface. The chip labels are political-office
  identifiers; wrapping mid-word is the worst possible failure mode.
- **Recommendation**: either
  - reduce chips to single-word labels ("Taoiseach" / "Finance" /
    "Tánaiste" / "Health") with `help=` tooltips for the full title, OR
  - drop the 2-column layout and stack the chips vertically (one per
    row), OR
  - widen the sidebar (not possible in Streamlit without custom CSS
    override).

**[P1-6] Lobbying notable-chip handler uses `str.contains` fuzzy match**

- **Location**: [`lobbying_2.py:535-543`](../utility/pages_code/lobbying_2.py#L535-L543):
  ```python
  if chip_cols[i % 2].button(chip, key=f"lob_chip_{i}", width="stretch"):
      idx = fetch_politician_index()
      if not idx.empty and "position" in idx.columns:
          m = idx[idx["position"].str.contains(chip, case=False, na=False)]
  ```
- Already flagged in
  [`project-lobbying-audit-2026-05-26`](../../../.claude/projects/c--Users-pglyn-PycharmProjects-dail-extractor/memory/project_lobbying_audit_2026_05_26.md)
  as P2-5; still open. Mild logic-firewall erosion (fuzzy matching
  in Streamlit).

---

### P2 — Minor

**[P2-1] `st.divider()` in attendance_overview sidebar**

- [`attendance_overview.py:506`](../utility/pages_code/attendance_overview.py#L506) — heavy rule between member filter and chip block. Removed
  elsewhere this session (attendance, payments). Removed here for
  consistency with no functional loss.

**[P2-2] Inconsistent session-state key naming for "selected TD"**

- `selected_td` (interests, member_overview),
  `selected_td_att` (attendance),
  `selected_td_pay` (payments),
  `_ov_td` (attendance_overview),
  `v_sel_member_id` (votes),
  `lob_selected_politician` (lobbying).
- The `selected_td` key on interests is intentionally shared with
  member_overview (it's the cross-page handshake). Other pages
  invent their own — no convention.
- **Recommendation**: settle on `<page>_selected_td` everywhere
  except the canonical handshake key; document the rule in
  `feedback_streamlit_api_patterns` or a new memory note.

**[P2-3] Notable-chip visibility varies wildly**

- Inline (attendance / attendance_overview / payments / interests),
  hidden inside expander (lobbying), absent (votes / committees / SI
  / legislation / glossary / member_overview / lobbying-poc).
- No documented rationale for inclusion / exclusion. Some pages have
  notable-member shortcuts; some don't. Citizens learn the pattern
  on one page and lose it on the next.
- **Recommendation**: either make chips a documented standard slot
  (present on every member-keyed page) or drop them entirely in
  favour of the main-panel typeahead.

**[P2-4] Lobbying's `[Org]` prefix is jargon**

- [`lobbying_2.py:516`](../utility/pages_code/lobbying_2.py#L516)
  emits selectbox options like `[Org] Ibec` to distinguish
  organisations from politicians in the combined search. The square-
  bracket prefix is developer notation.
- **Recommendation**: use a small typographic distinction (faded
  prefix, or icon + label) and a screen-reader-friendly aria-label.

**[P2-5] Sidebar subtitle / caption hierarchy varies**

- Votes: "Data covers 2016-01 to 2026-04" (`st.caption`)
- Legislation: "Source: Oireachtas Open Data API" / "Bill detail"
  (`<div class="page-subtitle">`)
- SI: "Secondary legislation · Iris Oifigiúil" (`<div
  class="page-subtitle">`)
- Glossary: descriptive paragraph (`st.caption`)
- Committees: stat caption (`<p class="page-subtitle">`)
- Other pages: nothing.
- Five distinct presentations for the same conceptual slot ("what
  is this page about?").
- **Recommendation**: a single `sidebar_subtitle(text)` helper that
  every page calls (or omits) with the same `.page-subtitle` class.

**[P2-6] Sidebar member-filter label semantics inconsistent**

- "Browse all members" is the label on attendance / attendance_overview / payments — that's a navigation affordance.
- "Browse all members" is also the label on lobbying — but there
  it's the COMBINED politicians+organisations selectbox below a
  "Search" text input. Same label, different behaviour.
- **Recommendation**: standardise on "Search by name" (input) +
  "Select a member" (dropdown) — verbs match action; cross-page
  consistency.

---

### P3 — Polish

**[P3-1] Sidebar collapse animation feedback**

- Streamlit auto-collapses the sidebar below ~960px viewport. On
  desktop, the user can manually collapse via the «  button at the
  top. The animation is Streamlit-default — no feedback for what's
  inside the collapsed sidebar.

**[P3-2] Mobile drawer state is not deterministic**

- Phase B captures (mobile) all show the sidebar collapsed. The
  Playwright `ensure_sidebar_open` helper couldn't reliably open the
  drawer in headless mode — needs investigation. A real mobile user
  would tap the hamburger to open the drawer; the drawer
  behaviour-and-styling on mobile is untested in this audit.

**[P3-3] Sidebar provenance prominence**

- Votes' "Data covers …" caption is genuinely useful provenance.
  Other pages have similar provenance in the main-panel expander but
  not the sidebar. Promote a one-line provenance footer to every
  sidebar.

---

### Patterns and systemic issues

1. **The page-navigation list is the sidebar's enemy.** It eats most
   of the vertical real estate by default. Until that's fixed (P0-1),
   every per-page sidebar fix is rearranging deck chairs on the
   below-the-fold deck.

2. **The sidebar grammar is undefined.** 7 distinct compositions for
   11 pages. No helpers govern composition order; pages drop
   whatever helpers they need in whatever order they pleased. Part 2
   proposes a fix.

3. **`sidebar_member_filter` is the most common helper but carries
   the same Enter-trap bug** the interests audit just fixed in
   `main_member_jump`. One follow-up edit retires it across 4 pages.

4. **Payments is the only page missing the page header.** Pure
   oversight; one line of code fixes it. But noteworthy as
   evidence that the helpers are NOT mandatory — there's no system
   enforcing them.

5. **Notable chips have no documented contract.** Six pages have
   them inline, one hides them, four omit them. Citizens get
   inconsistent shortcuts depending on which page they land on.

6. **`sidebar_page_header` is the only `unsafe_allow_html=True`
   leak left after the recent sweep** — and the `<p class="page-title">`
   underneath fails a11y heading-level discoverability. Two-bug fix
   in one place.

---

### Positive findings — preserve these

1. **`sidebar_page_header` and `sidebar-label` CSS classes are
   consistently applied** — `page-kicker`, `page-title`,
   `sidebar-label`, `page-subtitle` are the right vocabulary, just
   not consistently composed.
2. **`render_notable_chips`** got the surname-collision fix today
   (`D. Healy-Rae` / `M. Healy-Rae`) — works across all consumers.
3. **`sidebar_date_range`** is a clean single-purpose helper, used
   correctly on legislation + votes + member_overview.
4. **Votes' view-aware sidebar** (Outcome+Party filters on Dáil
   view; member search + date range on TDs view) is a good example
   of context-aware sidebar composition.
5. **Interests' chamber-switch cleanup** clears all four interests-
   scoped session keys on chamber change. Defensive and correct.

---

## Part 2 — Proposed sidebar grammar

Codify a single composition every page conforms to. Slots are
optional but ordering is fixed.

```
┌─ SIDEBAR ──────────────────────────┐
│                                    │
│  1. page_header   (always)         │
│  2. subtitle      (optional)       │
│  3. provenance    (optional)       │
│                                    │
│  ─── divider ───────────────────   │
│                                    │
│  4. global_filters (optional)      │
│     (chamber, view, year, etc.)    │
│                                    │
│  5. member_picker  (optional)      │
│     (single searchable selectbox)  │
│                                    │
│  6. notable_chips  (optional)      │
│                                    │
│  ─── divider ───────────────────   │
│                                    │
│  7. secondary      (optional)      │
│     (date range, status, search)   │
│                                    │
└────────────────────────────────────┘
```

A new helper `sidebar_shell(slots: dict)` takes named slots and
renders them in this order. Pages either pass values for the slots
they use or call individual helpers manually (the legacy escape
hatch).

The dividers between slot groups (1-3, 4-6, 7) ARE intentional and
match the design-skill's "divider semantics: section boundary, not
section-of-one separator" rule — they're page-skeleton dividers, not
decoration.

A starter sketch:

```python
def sidebar_shell(
    *,
    page_header: tuple[str, str | None] = None,    # (title, kicker)
    subtitle: str | None = None,
    provenance: str | None = None,
    global_filters: list[Callable[[], None]] = (),
    member_picker: Callable[[], str | None] | None = None,
    notable_chips: tuple[list[str], list[str], str, str] | None = None,
    secondary: list[Callable[[], None]] = (),
) -> str | None:
    """Render the canonical sidebar shell.

    Returns the picked member name if a member_picker is provided and
    a selection has been made this rerun, else None.
    """
    with st.sidebar:
        if page_header:
            sidebar_page_header(*page_header)
        if subtitle:
            sidebar_subtitle(subtitle)        # new helper, .page-subtitle
        if provenance:
            sidebar_provenance(provenance)    # new helper, st.caption
        if global_filters or member_picker or notable_chips:
            sidebar_divider()                 # new helper, light rule
        for fn in global_filters:
            fn()
        picked = None
        if member_picker:
            picked = member_picker()
        if notable_chips:
            names, available, key_prefix, session_key = notable_chips
            if render_notable_chips(names, available, key_prefix, session_key):
                picked = st.session_state.get(session_key)
        if secondary:
            sidebar_divider()
            for fn in secondary:
                fn()
        return picked
```

---

## Part 3 — Uplift prompt

```
We're rewiring the Dáil Tracker sidebar across all 11 pages based on
the 2026-05-26 cross-page sidebar audit at doc/SIDEBAR_AUDIT.md.
Read that file first — it has screenshots and the proposed grammar.

Context to read first:
- doc/SIDEBAR_AUDIT.md (this audit)
- audit_screenshots/sidebar/*.png (29 screenshots; A_*_desktop.png
  shows the default state for each page; C_*.png shows stateful
  interactions)
- utility/ui/components.py — sidebar_page_header, sidebar_member_filter,
  sidebar_date_range, render_notable_chips
- The cross-cutting memory: [[project-app-design-synthesis-2026-05-26]]
- The interests P1-4 fix (just shipped): main_member_jump was collapsed
  from text_input + selectbox to a single selectbox — same fix needed
  for sidebar_member_filter.

Scope: codify a sidebar grammar (Part 2 of the audit doc) and migrate
every page to it. Fix the 2 P0s and 6 P1s in the process.

Priority order:

1. **P0-2 + P1-1 — `sidebar_page_header` rewrite** (one helper, all pages).
   Convert to `st.html` + `<h2 class="page-title">` for a11y heading
   nav. Add `_h()` escaping. Update shared_css.py:.page-title to style
   an <h2>. Add the missing call to payments.py.

2. **P1-2 — `sidebar_member_filter` Enter-trap fix** (one helper,
   4 pages). Replace text_input + selectbox pair with a single
   st.selectbox using Streamlit's built-in type-to-search. Mirrors
   the interests P1-4 fix already shipped to main_member_jump.

3. **P1-3 — `sidebar_shell` helper** (new component) + grammar.
   Take named slots and render in the fixed order. Migrate the 11
   pages one at a time. Each page should end up looking like:

       sidebar_shell(
           page_header=("Plenary<br>Attendance", None),
           member_picker=lambda: sidebar_member_filter(...),
           notable_chips=(NOTABLE_TDS, opts["members"], "chip_att", "selected_td_att"),
       )

4. **P1-4 — committees sidebar st.error → empty_state**.

5. **P1-5 — lobbying notable-target chip wrapping**. Either
   (a) shorten labels to single words with `help=` tooltips, or
   (b) stack vertically (one chip per row).

6. **P1-6 — lobbying notable-chip handler str.contains fuzzy match**
   → use a pre-computed `notable_target_kind` column in
   v_lobbying_index. (Or block on pipeline; document.)

7. **P0-1 — sidebar real-estate squeeze**. This is the highest-impact
   finding but also the riskiest fix because it touches the
   Streamlit multipage navigation contract. Verify whether
   st.navigation supports `position="bottom"` or `position="hidden"`
   in Streamlit 1.56. If not, ship a custom compact nav in each
   page's main-column header and use position="hidden" on the
   sidebar nav. Worth scoping into a separate session.

After each fix:
- Re-run audit_screenshots/_sidebar_capture.py
- Compare new screenshots against the originals
- Update doc/SIDEBAR_AUDIT.md memory entry with what shipped

Don't:
- Add new sidebar widgets outside the grammar
- Touch pipeline code
- Add custom CSS for individual pages — all sidebar CSS belongs in
  the shared sidebar-* classes

Save findings updates and verification screenshots in the same audit
directory so future audits can diff.
```

---

## Appendix — Screenshot index

Phase A: `A_01..A_11` — every page's default desktop sidebar
Phase B: `B_01..B_11` — every page's mobile sidebar (closed; the open-drawer state
    was not reliably captured — Phase 3 recommendation)
Phase C: `C_*` — stateful interactions (votes view toggle, attendance
    search typed, lobbying notable expander open)
