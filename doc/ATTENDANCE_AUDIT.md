# Attendance page — impeccable audit (2026-05-26)

Captured via Playwright over `/rankings-attendance` on the running Streamlit
app. 30 screenshots in [audit_screenshots/attendance/](../audit_screenshots/attendance/)
covering desktop / tablet / mobile, year-pill interaction (default,
completed year, in-progress year, oldest year), sidebar interaction
(name search + notable chips), card-click redirect, legacy `?att_td=`
deep-link, missing-members expander, provenance expander, empty/edge
states, and keyboard focus.

This document has two parts:
1. **Audit findings** — what's wrong, why it matters, ranked by severity.
2. **The uplift prompt** — a single prompt ready to hand to a coding
   session to drive the rework.

Capture script: [audit_screenshots/_attendance_capture.py](../audit_screenshots/_attendance_capture.py).
Verifier (one-shot DuckDB check on `v_attendance_year_rank`): inlined in
[Part 2](#part-2--the-uplift-prompt).

---

## Part 1 — Audit findings

### Health score

| # | Dimension          | Score | Headline finding                                                                              |
|---|--------------------|-------|------------------------------------------------------------------------------------------------|
| 1 | Accessibility      | 2/4   | Hall headings are `<p>` not `<h2>/<h3>`; sub-heading levels missing; warning ⚠ + medal emoji   |
| 2 | Performance        | 3/4   | Provenance + missing-members expander bodies always render; in-progress mode dumps 129 cards    |
| 3 | Theming            | 2/4   | `att-hall-*` CSS still uses raw Tailwind hex (`#1d4ed8`/`#c2410c` etc.) despite signal-good/bad tokens |
| 4 | Responsive         | 1/4   | Mobile + tablet above-fold is hero + 2 stacked info boxes — zero data visible                  |
| 5 | Anti-patterns      | 3/4   | Civic-editorial register holds; deprecated APIs (`use_container_width`, `st.info`, dividers)   |
| **Total** |            | **11/20** | **Acceptable lower band — significant work needed; one P0 blocker.**                       |

### Anti-patterns verdict

**Pass** on the AI-slop test. The page is recognisably civic-editorial
— side-stripe hero, serif title, ink-on-paper surface, no gradient text,
no hero-metric template. The good-cop / bad-cop split is a genuine
editorial idea, not a generic dashboard.

**The slop risk is in the data, not the design.** Hall-of-Fame cards
showing impossible day counts (see P0-1) read as AI-generated because
no civic-accountability site that respected its source would ship
numbers that exceed the official sitting-day total.

---

### Executive summary

- **1 P0 blocking issue** — leaderboard shows impossible attendance
  counts (max 114 days in a year with 83 sitting days). User-visible
  impact of the long-known `session_type` dedupe TODO.
- **6 P1 major issues** — mobile/tablet above-fold has no data; two
  stacked `st.info` blocks before the first card; in-progress year is
  a 129-card scroll; "Per-td" lowercase callout bug; deprecated
  `use_container_width` x3; duplicate Healy-Rae notable chips.
- **9 P2 minor issues** — `st.divider` x3, `ProgressColumn`
  `format="%.0%"` typo, raw Tailwind hex throughout `att-hall-*` CSS,
  emoji icons, `<p>` heading anti-pattern, dead `_render_year_breakdown`
  dataframe branch.
- **5 P3 nice-to-haves** — sidebar selectbox heavy `st.rerun`,
  contract drift (`name_filter` / `name_search_mode` in primary flow no
  longer implemented), missing 2020–2023 in year pills, double-blue
  visual tie between primary info box and in-progress notice, focus
  rings rely on Streamlit default.

The single highest-leverage move is **fixing the upstream
`v_attendance_member_year_summary` dedupe** so the leaderboard reflects
real sitting days. Doing that one thing makes the page trustworthy;
no UI work can compensate for the data being wrong.

---

### P0 — Blocking (fix before next deploy)

**[P0-1] Hall-of-Fame cards show impossible day counts**

- **Location**: card list in `_render_good_bad()`
  (`utility/pages_code/attendance.py:244-300`). Source view:
  `v_attendance_year_rank` → `v_attendance_member_year_summary`.
- **Evidence**: `B02_year_2024_hall_split.png` — entire "Highest
  recorded attendance" column shows **`114 days`** on every card. The
  contract published in `attendance.yaml:65-71` declares 2024 had
  **83 official sitting days**.
- **Verified in DuckDB** (`v_attendance_year_rank`):

  | Year | Contract sitting days | Data max | Impossible? |
  |------|-----------------------|----------|-------------|
  | 2024 | 83                    | **114**  | yes (+37%)  |
  | 2025 | 82                    | **139**  | yes (+70%)  |
  | 2026 | YTD                   | 53       | (in-progress, n/a) |

- **And 5 members are tied at exactly 114** in 2024 — Matt Carthy,
  Duncan Smith, Louise O'Reilly, John Lahart, John McGuinness —
  confirming the dedupe issue inflates everyone to a similar ceiling.
- **Impact**: a citizen who lands here cannot trust the page. The
  Hall of Fame says "Louise O'Reilly attended 114 days" in a year
  that had 83 sittings. A journalist quoting this number publishes
  a falsehood. The page's editorial purpose collapses.
- **Root cause**: documented in [attendance.py:10-13](../utility/pages_code/attendance.py#L10-L13)
  — the source CSV mixes plenary-sitting rows with committee/other-day
  rows under the same date. `v_attendance_timeline` exposes neither
  the `session_type` label nor a dedupe — so counts roll up to the
  year summary inflated.
- **Fix**: in `sql_views/attendance_member_year_summary.sql` (and the
  upstream gold-layer ETL if needed), dedupe by
  `(unique_member_code, sitting_date)` so each member counts at most
  once per calendar date. Add a `plenary_only` column derived from
  the source's session-type label; have the year summary count only
  plenary rows. After fix, validate `MAX(attended_count) per year ≤
  SITTING_DAYS_BY_YEAR[year]` for every year in the view.
- **Why not in Streamlit**: the page's logic firewall forbids dedupe
  joins / GROUP BY (see `CLAUDE.md` retrieval SQL rule). This must be
  fixed in the pipeline.

---

### P1 — Major (fix this milestone)

**[P1-1] Mobile + tablet above-the-fold has no data**

- **Evidence**: [`H05_mobile_landing_full.png`](../audit_screenshots/attendance/H05_mobile_landing_full.png),
  [`H06_mobile_above_fold.png`](../audit_screenshots/attendance/H06_mobile_above_fold.png),
  [`H02_tablet_above_fold.png`](../audit_screenshots/attendance/H02_tablet_above_fold.png).
  On the 390×844 viewport: hero (side-stripe + kicker + title) +
  three-row glossary strip + year pills + "129 members on record"
  caption + first blue `st.info` ("Plenary attendance only…") +
  second blue `st.info` ("2026 is in progress…") consume every pixel
  of the viewport. **Not one card is visible.**
- **Impact**: a citizen on phone lands on a page that asks them to
  scroll past two near-identical blue boxes before seeing a single
  member. The page's editorial purpose — "who showed up?" — is buried.
  Same pattern that scored P1-2 on the SI audit.
- **Fix**:
  - Collapse the plenary-only caveat into a `<p class="dt-aside">` one-liner under the year pills (or remove from the primary view entirely — it's already in the provenance expander).
  - Replace the "2026 is in progress" `st.info` with a small inline
    `(YTD)` pill next to the selected year, plus a single-sentence
    `st.caption` above the card list. Don't duplicate the in-progress
    message at info-box weight.
  - On `≤640px` collapse the glossary strip behind a small `i` toggle
    or a `<details>`; first-time-visitor onboarding shouldn't push
    every member off-screen.
  - Default the year pill to the **most recent completed year** rather
    than the current YTD year — that way the user sees the real
    hall-of-fame split immediately. (Contract already says
    `default_selection: most_recent` — the page currently picks 2026
    because `skip_current=False` is passed to `year_selector`. Change
    to `skip_current=True` at [attendance.py:752](../utility/pages_code/attendance.py#L752).)

**[P1-2] In-progress year dumps a 129-card flat list**

- **Location**: `_render_good_bad()` in
  [attendance.py:256-265](../utility/pages_code/attendance.py#L256-L265). When `year >= today.year` it
  bypasses the good/bad split and renders every member as a single
  ranked column.
- **Evidence**: [`B03_year_2026_in_progress.png`](../audit_screenshots/attendance/B03_year_2026_in_progress.png)
  shows #1 through #13; the rest scroll on indefinitely. By #129
  Emer Currie has 5 days; the editorial "lowest attenders" moment is
  lost in a long sea of cards.
- **Impact**: the page's editorial idea is the split. Removing the
  split when the year is in-progress trades the strongest UX moment
  for a long table. The user loses the at-a-glance comparison.
- **Fix**: keep the same `top-15` / `bottom-15` split even when the
  year is in progress. Add a small "(YTD)" pill / suffix on the
  year-pill, and a one-sentence caption above the columns explaining
  that the year is not yet complete so the lowest column is provisional.
  If a fairness concern remains about labelling someone "lowest
  attender" mid-year, replace the lowest column with "most-improved
  vs prior year" or "newly elected this term" — but a 129-card scroll
  is the wrong fallback.

**[P1-3] Two stacked `st.info` boxes look near-identical**

- **Location**: [attendance.py:257-261](../utility/pages_code/attendance.py#L257-L261)
  (in-progress notice) and
  [attendance.py:771-776](../utility/pages_code/attendance.py#L771-L776)
  (plenary caveat).
- **Evidence**: [`B01_year_pills_default.png`](../audit_screenshots/attendance/B01_year_pills_default.png),
  [`H05_mobile_landing_full.png`](../audit_screenshots/attendance/H05_mobile_landing_full.png).
  Two blue Streamlit info boxes sit one above the other. They use the
  same colour, the same icon (`:material/info:`), and read as the
  same affordance. The eye treats them as one element that's been
  duplicated.
- **Impact**: page weight without information density. The plenary
  caveat is already on the provenance expander; restating it as the
  first content block above the cards is redundant.
- **Fix**:
  - Replace both `st.info` calls with the existing `dt-callout` or
    `empty_state` helper for civic voice (see
    `feedback_streamlit_api_patterns`). They're cheaper, smaller,
    and don't shout at the user with blue.
  - Move the plenary-only caveat into a single line under the year
    pill or remove it from primary view; the provenance expander
    already contains the long version.
  - Merge the in-progress notice into a year-pill suffix (`2026 ·
    YTD`) rather than a separate alert.

**[P1-4] `Per-td` casing bug in cross-page redirect callout**

- **Location**: [components.py:372](../utility/ui/components.py#L372)
  in `member_moved_callout`:
  ```python
  f'<span style="color:var(--text-meta)">{_h(section_label.capitalize())} '
  ```
  `section_label.capitalize()` lowercases everything after the first
  letter. The attendance page passes `section_label="Per-TD attendance"`
  → renders as "Per-td attendance".
- **Evidence**: [`D03_legacy_att_td_param_redirect.png`](../audit_screenshots/attendance/D03_legacy_att_td_param_redirect.png),
  [`C03_after_notable_chip_redirect.png`](../audit_screenshots/attendance/C03_after_notable_chip_redirect.png),
  [`G01_bogus_member_redirect.png`](../audit_screenshots/attendance/G01_bogus_member_redirect.png).
  All three show the broken casing.
- **Impact**: a civic accountability tool that can't capitalise "TD"
  loses credibility instantly. The page acronym is the single most
  important domain term — and the screen-reader rendering says
  "per-tee-dee" already; the visible lowercase makes it worse.
- **Fix**: change `member_moved_callout` to title-case sentence-style
  with a small protected-acronyms set (TD, TAA, PRA, EU) rather than
  `.capitalize()`. Or simpler: pass the string already correctly
  cased and drop the `.capitalize()` entirely (and uppercase the
  caller's first letter at the source).

**[P1-5] Three `use_container_width=True` deprecations**

- **Location**:
  - [attendance.py:355](../utility/pages_code/attendance.py#L355) — `st.altair_chart(...)` (calendar strip)
  - [attendance.py:484](../utility/pages_code/attendance.py#L484) — `st.dataframe(...)` (sitting dates table)
  - [attendance.py:547](../utility/pages_code/attendance.py#L547) — `st.dataframe(...)` (year breakdown)
- **Impact**: `use_container_width` is deprecated as of Streamlit 1.31+ in favour of `width="stretch"`. The page emits three deprecation warnings to the console on every member-profile render.
- **Fix**: replace `use_container_width=True` with `width="stretch"`. Repeat across the same call sites in `attendance_overview.py:436, 445` while you're in there.
- **Note**: the [attendance.py:547](../utility/pages_code/attendance.py#L547) branch (`as_dataframe=True`) is **unreachable in production** — see [P2-2].

**[P1-6] Notable-chips: two indistinguishable "Healy-Rae" chips**

- **Location**: `render_notable_chips` in [components.py:106-111](../utility/ui/components.py#L106-L111). Chip label is `name.split()[-1]` — "Michael Healy-Rae" → "Healy-Rae", "Danny Healy-Rae" → "Healy-Rae". Same label, different `key`.
- **Evidence**: [`C01_sidebar_default.png`](../audit_screenshots/attendance/C01_sidebar_default.png),
  [`E02_missing_members_no_record_group.png`](../audit_screenshots/attendance/E02_missing_members_no_record_group.png) — two adjacent buttons both reading "Healy-Rae".
- **Impact**: a citizen looking for Michael vs Danny has to hover both to find out. Hover only works on desktop; on mobile the chips are indistinguishable.
- **Fix**: when last-name collides, show `{first_initial}. {last_name}` (M. Healy-Rae, D. Healy-Rae). Or show full name on chips and let CSS truncate.

---

### P2 — Minor

**[P2-1] `ProgressColumn` `format="%.0%"` typo (also in attendance_overview)**

- [attendance.py:556](../utility/pages_code/attendance.py#L556) and [attendance_overview.py:445](../utility/pages_code/attendance_overview.py#L445).
- `%.0%` is malformed printf. Should be `"%.0f%%"` (zero decimal places + literal percent sign) or use Streamlit's `format="percent"` shorthand if available in current version.

**[P2-2] Dead `_render_year_breakdown(as_dataframe=True)` branch**

- The dataframe branch at [attendance.py:542-559](../utility/pages_code/attendance.py#L542-L559) is only reached when `show_member_header=True`. Since the stand-alone profile flow was removed in Phase 6 (page now redirects to `/member-overview` for any TD selection), this branch is unreachable in production. `render_member_attendance` is only called from `member_overview.py` with `show_member_header=False`.
- Either delete the dataframe branch and the `as_dataframe` flag, or document that the page-level entry point doesn't reach it.

**[P2-3] Three `st.divider()` calls — heavy section rules**

- [attendance.py:432](../utility/pages_code/attendance.py#L432),
  [attendance.py:499](../utility/pages_code/attendance.py#L499),
  [attendance.py:721](../utility/pages_code/attendance.py#L721).
- Per the design skill ("Dividers look heavy. Just remove them.") and the 2026-04-30 audit, replace with margin / no divider at all.
- The line 721 divider is inside the sidebar — the lightest case; keep if it helps the notable-chip block visually separate from the member search.

**[P2-4] `att-hall-*` CSS still uses raw Tailwind hex**

- `shared_css.py:1747-1833` — `.att-hall-heading-good` uses `#1d4ed8` + `#3b82f6`; `.att-hall-heading-bad` uses `#c2410c` + `#f97316`; `.att-hall-card-good/bad`, `.att-hall-badge-good/bad` all use the same hex pairs.
- The OKLCH `--signal-good*` / `--signal-bad*` tokens added in the 2026-05-24 audit ([shared_css.py:150-160](../utility/shared_css.py#L150-L160)) map exactly to these hex values. Migration is straightforward and lifts the Theming score.
- Same applies to `_render_attendance_strip` colours hardcoded at [attendance.py:336-352](../utility/pages_code/attendance.py#L336-L352) — `#374151`, `#e5e7eb`, `#d1d5db`, `#16a34a`, `#ffffff`. The green `#16a34a` should be `var(--signal-good)`-equivalent; everything else has a token already in `:root`.

**[P2-5] Emoji icons in production code**

- [attendance.py:197](../utility/pages_code/attendance.py#L197) — `_GOOD_MEDALS = ["🥇", "🥈", "🥉"]` is fine as a deliberate editorial flourish for the top 3 cards; this carries meaning and is OK to keep.
- [attendance.py:625](../utility/pages_code/attendance.py#L625) — `⚠ {total} TDs do not appear...` in the expander label should be `:material/warning:`. Per the 2026-04-30 audit's EMOJI ICONS section, plain Unicode warning emoji in chrome labels should migrate to Material icons for consistency.

**[P2-6] Hall-heading uses `<p>` not `<h2>`/`<h3>`**

- [attendance.py:284, 297](../utility/pages_code/attendance.py#L284): `st.html('<p class="att-hall-heading-good">Highest recorded attendance</p>')` and `<p class="att-hall-heading-bad">...`.
- Screen-reader users cannot navigate by heading: the document outline goes `<h1>` (hero) → nothing → `<p>` styled-as-heading → cards. Same a11y bug flagged in `votes_audit.md`. Use `<h2>` (or `<h3>` if you treat the hero h1 as the page title).

**[P2-7] PDF link labels are date ranges with overlapping spans**

- [`F02_provenance_pdf_links.png`](../audit_screenshots/attendance/F02_provenance_pdf_links.png) shows 7 links: "1 Jan 2026 – 28 Feb 2026", "1 Jan 2026 – 31 Jan 2026", "1 Feb 2025 – 30 Dec 2025", "1 Jan 2025 – 31 Jan 2025", "29 Nov 2024 – 31 Dec 2024", "1 Jan 2024 – 8 Nov 2024", "1 Jan 2023 – 31 Dec 2023".
- The "1 Jan 2026 – 28 Feb 2026" and "1 Jan 2026 – 31 Jan 2026" labels overlap — a user can't tell which is the canonical 2026 source. Either rename ("2026 YTD update" vs "2026 Jan snapshot") or deduplicate at the source list.

**[P2-8] Card width inconsistency in partial-year flat list**

- [`E01_missing_members_open.png`](../audit_screenshots/attendance/E01_missing_members_open.png) shows John McGuinness (#128) card extending visibly further right than every other card around it. The orange `.att-hall-card-bad`/blue `.att-hall-card-good` widths should be uniform via `width: 100%` but something is overriding.
- Inspect via DevTools — likely the `dt-card-link-wrap` `max-width: 80%` rule at [shared_css.py:1815-1819](../utility/shared_css.py#L1815-L1819) being inherited differently for #128. Could be a `display: inline-block` widow.

**[P2-9] Year pill list does not include 2020–2023**

- [`B01_year_pills_default.png`](../audit_screenshots/attendance/B01_year_pills_default.png) shows only 2026, 2025, 2024 pills despite the contract's hardcoded `SITTING_DAYS_BY_YEAR` having entries back to 2020.
- The `_fetch_filter_options()` query at [attendance.py:92-93](../utility/pages_code/attendance.py#L92-L93) pulls years from `v_attendance_member_year_summary` directly — so the gap is upstream (either the gold layer doesn't have pre-2024 data, or `SITTING_DAYS_BY_YEAR` was written aspirationally). Verify and either backfill the data or trim the contract's hardcoded list to match what's actually in the view.

---

### P3 — Polish

**[P3-1] Contract drift — `primary_view_flow` lists `name_filter` and `name_search_mode`**

- `attendance.yaml:158-167` declares a `name_filter` (single text input) and `name_search_mode` in the primary view. Neither exists in the page anymore — name search lives in the sidebar and selecting anyone redirects to `/member-overview` (Phase 6 change).
- Update the contract to reflect the post-Phase-6 reality: name search lives in sidebar; primary view is year-pill + good/bad cop; selecting any TD redirects.

**[P3-2] Expander bodies always render**

- `_render_missing_members` calls `_fetch_missing_members()` unconditionally at the top, then renders inside `st.expander(expanded=False)`. The DataFrame fetch and DOM rendering happen on every page load even when collapsed.
- Same for the provenance expander.
- Per the streamlit-skill performance section: wrap with `if st.toggle(...)` or move the fetch behind a button so the cost is paid only when needed. Low impact on small data, becomes meaningful as the missing-members list grows.

**[P3-3] Sidebar selectbox `st.rerun()` is heavy**

- [attendance.py:717-719](../utility/pages_code/attendance.py#L717-L719) — selecting from the sidebar member dropdown sets state then calls `st.rerun()` immediately. Since the next render fires the `member_moved_callout` (which calls `st.stop()`), the full page goes through one rerun-and-stop cycle.
- Net effect is acceptable; cosmetic finding only. If profiling shows the sidebar redirect feels laggy, set query params and let Streamlit's natural URL listener fire the rerun.

**[P3-4] Focus states use Streamlit defaults**

- Phase I capture crashed (Streamlit body iframe not visible to Playwright). No focus-ring evidence captured, but the page registers no custom focus styles in `att-*` CSS — so card hover states are tinted backgrounds but keyboard focus is the browser default. Same finding as SI P3-3.

**[P3-5] Hero `kicker_and_title_only` matches contract; no dek — fine**

- Contract's `primary_view_flow` says `kicker_and_title_only` and the page complies. No action.

---

### Patterns and systemic issues

1. **Data quality is again the biggest enemy.** P0-1 is the entire credibility of the page. The fix is upstream (silver/gold layer dedupe). No UI improvement saves the page while the leaderboard shows 114-of-83-days.

2. **The page documents three known pipeline gaps** in its module docstring (per-year source URL, `session_type` on timeline, `unique_member_code` on summary views). One of those (`session_type`) is the root cause of P0-1. **Known pipeline-debt items have user-visible blast radius.** Audit doc surfaces what an internal TODO comment does not.

3. **Mobile is an afterthought.** Same finding as SI. Hero + 2 stacked info boxes consume the whole viewport. The page was designed at 1440 and responsive-checked.

4. **In-progress year handling needs a real design.** The flat-list fallback works as a stopgap but it abandons the page's editorial idea. Carry the good/bad cop split into in-progress mode with a "(YTD)" / "(provisional)" marker.

5. **Cross-page redirect callout** is shared with at least Interests / Payments / Lobbying / Committees — the `Per-td` lowercase bug (P1-4) affects all of them. Single fix in `components.py` lifts every page that uses the helper.

---

### Positive findings (keep these)

- **`hero_banner` + `glossary_strip`** — first-time-visitor onboarding pattern is strong. Three acronyms explained inline without burying the title.
- **Side-stripe accent** — applied consistently with PRODUCT.md's documented intentional override. Editorial identity holds.
- **Phase-6 redirect architecture** — sidebar selection, notable chip, card click, and legacy `?att_td=` URL all converge on `member_moved_callout` → `/member-overview`. Clean single source of truth for member navigation.
- **Missing-members expander copy** — "TAA records exclude office-holders by design — they are not absent, they are not recorded." This is exactly the civic-accountability voice the project asks for. Honest about source limitations.
- **`_CAVEAT` and `_MINISTER_NOTE` constants** — long-form caveats are stored as module constants and dropped into `provenance_expander`. Clean separation of editorial content from layout.
- **`@page_error_boundary` decorator** — page-entry-point exception handling renders a calm `dt-callout` instead of a Streamlit red traceback. Civic-voice default for failure.

---

## Part 2 — The uplift prompt

The prompt below is ready to hand to a coding session. It assumes the
person taking it has access to the repo and to the impeccable skill
context (`PRODUCT.md`, project memory). Drop it into a new conversation
or paste it after `/impeccable craft attendance page` to seed
shape-then-build.

> ### Attendance page — comprehensive uplift
>
> Rework `utility/pages_code/attendance.py` and the upstream data it
> consumes (`v_attendance_year_rank` and its parent
> `v_attendance_member_year_summary`, plus the gold layer that feeds
> them) so that the page reads as a trustworthy civic accountability
> tool rather than a leaderboard over broken numbers. Hold to
> `PRODUCT.md` (Direct · Civic · Accountable; editorial accountability
> journalism; ink-on-paper restraint). Stay inside Streamlit constraints;
> honour the project's logic-firewall split — no business metrics in the
> page, no JOIN / GROUP BY / WINDOW in retrieval SQL.
>
> Audit evidence — see [doc/ATTENDANCE_AUDIT.md](ATTENDANCE_AUDIT.md)
> and the 30 supporting screenshots in
> [audit_screenshots/attendance/](../audit_screenshots/attendance/).
>
> #### Goals (in priority order)
>
> 1. **Make the numbers true.** This is the entire page. The Hall of
>    Fame currently shows impossible counts because of an upstream
>    dedupe issue:
>
>    | Year | Sitting days | Max in data | Δ |
>    |------|--------------|-------------|---|
>    | 2024 | 83 | 114 | +37% |
>    | 2025 | 82 | 139 | +70% |
>
>    Five members are tied at exactly 114 in 2024 — Matt Carthy,
>    Duncan Smith, Louise O'Reilly, John Lahart, John McGuinness — the
>    smoking-gun for the dedupe issue. The source CSV mixes plenary
>    sitting-day rows with committee/other-day rows; without a
>    `session_type` column on `v_attendance_timeline` (or a dedupe in
>    the gold-layer aggregate) every member's count rolls up inflated.
>
>    Fix this in `sql_views/attendance_member_year_summary.sql` and /
>    or the gold-layer ETL — never in Streamlit. Dedupe by
>    `(unique_member_code, sitting_date)` so each member counts at
>    most once per calendar date; expose a `plenary_only` column;
>    have the year summary count only plenary rows. Validate after:
>    `MAX(attended_count) per year ≤ SITTING_DAYS_BY_YEAR[year]` for
>    every year in the view. **The page should not redeploy until this
>    holds.**
>
> 2. **Earn the mobile and tablet above-fold.** At 390×844 and
>    820×1180 the page currently shows zero data above the fold —
>    just hero + glossary + year pills + two stacked `st.info` blue
>    boxes. On `≤640px`:
>    - Default the year pill to the **most recent completed year**
>      (`skip_current=True` at attendance.py:752); the YTD year
>      should not be the default. The good/bad split is the page's
>      strongest moment and only works on completed years.
>    - Collapse the plenary caveat into a one-line `<p class="dt-aside">`
>      under the year pill or remove it entirely from primary view (the
>      provenance expander already carries the long version).
>    - Merge the in-progress notice into a year-pill suffix
>      (`2026 · YTD`) plus a small `st.caption` — not a full `st.info`.
>    - Collapse the glossary strip behind a `<details>` on `≤640px`.
>
> 3. **Keep the good/bad cop split in-progress mode.** Currently when
>    `year >= today.year` the page bypasses the split and renders 129
>    cards in a single column ([attendance.py:256-265](../utility/pages_code/attendance.py#L256-L265)). That trades the editorial idea for a long
>    table.
>    - Keep the split. Add a "(YTD)" suffix on the year and a
>      one-sentence caption above the columns: *"This year is in
>      progress — the lowest column is provisional and will change as
>      the Dáil sits."*
>    - If there's a fairness concern about labelling someone "lowest"
>      mid-year, replace the lowest column with "most-improved vs
>      prior year" or "newly elected this term" — but the 129-card
>      scroll is the wrong fallback.
>
> 4. **Fix the cross-page redirect callout typography.** `member_moved_callout`
>    at [components.py:372](../utility/ui/components.py#L372) calls
>    `section_label.capitalize()` which renders "Per-TD" as "Per-td".
>    Pass the string already cased correctly and drop the `.capitalize()`;
>    or expand the helper to title-case with a protected-acronyms set
>    (TD, TAA, PRA, EU, US). This fix lifts every page using the
>    helper — at least Interests, Payments, Lobbying, Committees.
>
> 5. **De-duplicate the notable-chip last-name labels.** `render_notable_chips`
>    at [components.py:106-111](../utility/ui/components.py#L106-L111) labels each chip with `name.split()[-1]`; two TDs share the surname Healy-Rae and render as two visually-identical chips. When last-name collides, show `{first_initial}. {last_name}`.
>
> 6. **Replace `st.info` calls with civic-voice callouts.** Both
>    `st.info` calls on the page (in-progress notice + plenary
>    caveat) should be `dt-callout` HTML or removed entirely. See
>    `feedback_streamlit_api_patterns` and the SI audit P1-6 fix.
>
> 7. **Migrate `att-hall-*` CSS to the signal tokens.** The OKLCH
>    `--signal-good*` and `--signal-bad*` tokens already exist
>    ([shared_css.py:150-160](../utility/shared_css.py#L150-L160)).
>    Replace raw `#1d4ed8`, `#3b82f6`, `#c2410c`, `#f97316`,
>    `#bfdbfe`, `#fdba74`, `#eff6ff`, `#fff7ed` across
>    [shared_css.py:1747-1833](../utility/shared_css.py#L1747-L1833)
>    with the matching `var(--signal-*)` references. Same in
>    `_render_attendance_strip` hex colours at
>    [attendance.py:336-352](../utility/pages_code/attendance.py#L336-L352).
>
> #### Polish (P2 — fold into the same PR if time permits)
>
> - Replace `use_container_width=True` with `width="stretch"` at
>   [attendance.py:355, 484, 547](../utility/pages_code/attendance.py)
>   and the analogous lines in `attendance_overview.py`.
> - `ProgressColumn` `format="%.0%"` is malformed printf — fix to
>   `"%.0f%%"` at [attendance.py:556](../utility/pages_code/attendance.py#L556) and `attendance_overview.py:445`. Or just delete the unreachable dataframe branch (see next).
> - **Delete the dead `_render_year_breakdown(as_dataframe=True)`
>   branch** at [attendance.py:542-559](../utility/pages_code/attendance.py#L542-L559). Since Phase 6 removed the in-page profile flow, this branch only fires from a code path that no longer exists. Either remove the flag or document its embedded-only purpose.
> - Remove the three `st.divider()` calls at
>   [attendance.py:432, 499, 721](../utility/pages_code/attendance.py#L432) — heavy section rules per the design skill.
> - Hall headings: `<p class="att-hall-heading-good">` → `<h2>` (or
>   `<h3>` if you want hero to keep h1 status). Same for `bad`. Fixes
>   the heading-outline a11y issue.
> - `⚠ {total} TDs do not appear...` expander label →
>   `:material/warning: ...` (Streamlit's Material icon spec). Keep
>   the medal 🥇🥈🥉 emojis in `_GOOD_MEDALS` — those carry editorial
>   meaning and are deliberately decorative.
> - Provenance PDF links — the "1 Jan 2026 – 28 Feb 2026" and "1 Jan
>   2026 – 31 Jan 2026" labels overlap; either rename ("2026 YTD
>   update" vs "2026 Jan snapshot") or dedupe at the
>   `ATTENDANCE` constant.
> - Card-width inconsistency in #128 of partial-year flat list (see
>   [`E01_missing_members_open.png`](../audit_screenshots/attendance/E01_missing_members_open.png)) — inspect the `dt-card-link-wrap` `max-width: 80%` rule
>   at [shared_css.py:1815-1819](../utility/shared_css.py#L1815-L1819).
> - Year pills exclude 2020–2023 even though
>   `SITTING_DAYS_BY_YEAR` covers them. Verify the data view actually has rows for those years; if not, trim the constant. If yes, fix the
>   `_fetch_filter_options()` query.
>
> #### Contract update
>
> - `attendance.yaml:158-167` lists `name_filter` and `name_search_mode`
>   in `primary_view_flow`. Neither exists in the post-Phase-6 page.
>   Update the contract to: kicker_and_title_only → year_pills →
>   good_bad_cop_split (with YTD variant) → export_button →
>   missing_members_expander → about_provenance_footer.
>
> #### Non-goals (don't do these in this PR)
>
> - No new Python/Polars enrichment in production paths beyond the
>   dedupe fix; experimental enrichment goes to `pipeline_sandbox/`
>   per `project_pipeline_sandbox_rule.md`. If the dedupe needs
>   Python pre-processing (it shouldn't — SQL `DISTINCT ON` /
>   `ROW_NUMBER OVER` is enough), do it in a sandbox script that
>   writes a clean parquet for the SQL view to read.
> - Don't touch `pipeline.py` / `enrich.py` / `normalise_join_key.py`
>   (sandbox rule).
> - No CSS-architecture split or typography-scale collapse —
>   deferred design debt per `project_impeccable_audit_2026_05_24`.
> - No member-overview drilldown rework — Phase 6 is settled.
>
> #### Acceptance
>
> Verify P0-1 first by running this one-shot DuckDB check:
> ```python
> # python tools/verify_attendance_dedupe.py (create this if needed)
> import sys; sys.path.insert(0, 'utility')
> from data_access.attendance_data import get_attendance_conn
> from config import SITTING_DAYS_BY_YEAR
> con = get_attendance_conn()
> bad = []
> for y, total in SITTING_DAYS_BY_YEAR.items():
>     mx = con.execute(
>         "SELECT MAX(attended_count) FROM v_attendance_year_rank WHERE year=?",
>         [y],
>     ).fetchone()[0]
>     if mx is not None and mx > total:
>         bad.append((y, total, mx))
> print('FAIL' if bad else 'OK', bad)
> ```
> Until this prints `OK`, do not deploy.
>
> Re-run [audit_screenshots/_attendance_capture.py](../audit_screenshots/_attendance_capture.py) after the rework. New screenshots should show:
> - No card with a day-count exceeding `SITTING_DAYS_BY_YEAR[year]` (P0-1 resolved).
> - Mobile above-fold (390×844) shows at least the first hall card or year-pill (P1-1).
> - In-progress year still uses the good/bad cop split (P1-2).
> - No `st.info` boxes on the page (P1-3).
> - `Per-TD attendance` rendered correctly in every redirect callout (P1-4).
> - Zero deprecation warnings in the Streamlit console (P1-5).
> - Two distinguishable Healy-Rae chips in the sidebar (P1-6).
> - All P2 polish issues from the checklist resolved.
>
> Re-run `/impeccable audit` on the attendance page after the rework
> and target a health score of 17+/20.

---

## Appendix — Screenshot index

Phase A: `A01–A06` — landing on desktop. **A01-A06 are pre-hydration
captures (6s cold-start wait was insufficient); use B-phase shots for
landing analysis.**
Phase B: `B01–B04` — year pill interactions (default 2026 / completed 2024 / in-progress / earliest)
Phase C: `C01–C03` — sidebar default / member-search filtered / notable chip redirect
Phase D: `D01–D03` — card click → member-overview redirect (D01-D02 failed selector; D03 legacy URL redirect succeeded)
Phase E: `E01–E02` — missing-members expander open / no-record-on-file group
Phase F: `F01–F02` — provenance expander open / PDF link list
Phase G: `G01–G02` — bogus member redirect / legacy param fallback
Phase H: `H01–H10` — tablet + mobile responsive. **Mobile H05-H10 are identical (Streamlit iframe scroll didn't take effect; the above-fold finding is established regardless).**
Phase I: `I01–I02` — keyboard focus. **Crashed — Streamlit body iframe not visible to Playwright; treat focus findings as static-audit only.**
