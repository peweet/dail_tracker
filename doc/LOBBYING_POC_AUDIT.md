# Lobbying (PoC) — Impeccable audit (2026-05-26)

Page audited: `/rankings-lobbying-poc` (`utility/pages_code/lobbying_3.py`).
Methodology: 42 Playwright screenshots across desktop / tablet / mobile,
11 phases — landing, sidebar search, org index, org detail, area
detail, area×politician (Stage 3), topic detail, RD index, DPO
individual, mobile org detail, empty/not-found states.
Capture script: `audit_screenshots/_lobbying_poc_capture.py`.
Screenshots: `audit_screenshots/_lobbying_poc/*.png`.

**Score: 16 / 20 — Strong, with one data-quality blocker.** This PoC
is the cleanest IA in the app: quiet H1 + prose dek with embedded
counts (no separate stat strip), three matched gateway tiles, ranked
cards with rank-chip overlays on avatars (the Interests P1-2 fix
cascaded here), unified `_return_card_html` across all surfaces, and a
context-aware back button on Stage 3. One blocker: DPO individual
firms / clients sections render as silent grey rectangles even when
the RD index card says "7 firms".

The PoC reads more like a journalistic publication than a dashboard —
that's its biggest strength relative to the production page, which is
busier and louder. Once the P0 + 5 P1s land, this page is plausible as
a candidate to replace `lobbying_2.py` in production.

---

## Part 1 — Findings

### P0 — Blocker (1)

**P0-1 — DPO firms / clients sections render silent grey rectangles.**

Evidence: `I02_dpo_above_fold.png`, `I03_dpo_firms_clients.png`. The
Lorraine Higgins detail view shows hero pills "194 returns / 7 firms /
297 politicians", but the body has "Firms represented" + empty grey
box and "Clients represented" + empty grey box. The conditional in
`lobbying_3.py:1336-1346` is:
```python
firms_df = fetch_dpo_firms(individual_name)
if not firms_df.empty:
    _section_head("Firms represented")
    _datasette_table(firms_df, columns={...})
```
So `firms_df.empty` is `False` but the rendered table is height-only
empty box. Same for `_render_dpo_individual`'s clients block at
`:1349-1360`. Either:
- `fetch_dpo_firms()` returns a DataFrame with no matching columns
  for the `_datasette_table` `columns={"lobbyist_name": "Firm", …}`
  mapping (`_datasette_table` filters to `keep = [c for c in columns
  if c in detail.columns]` — if no columns match, `display` is empty),
  OR
- The data really exists but the dataframe is rendered without rows
  (CSS / column-config issue).

**Fix** — in `_datasette_table` (line 591-617), after computing `keep`,
if `not keep` or `display.empty` after column-renaming, call
`empty_state(...)` instead of rendering an empty `st.dataframe`. Also
investigate why `fetch_dpo_firms("Lorraine Higgins")` returns rows
where none of the expected columns exist.

This is the only case where the page presents a silent failure to
citizens — every other empty state on the page is handled gracefully.

### P1 — High leverage (5)

**P1-1 — Date format inconsistent across surfaces.**

Evidence: landing dek "from 2015-09 to 2026-05" (truncated ISO);
org-detail Ibec hero "Active 2015-09-01 to 2025-09-01" (full ISO);
return cards "SEP 2025" (friendly Mon YYYY). Three formats for the
same data type within a single page. Citizens shouldn't have to
re-parse the date representation on every section.

Fix: standardise on **"Sep 2025"** (capitalised abbrev + space + year)
everywhere. The `_fmt_mmm` helper already produces this for return
cards (line 517-521). Apply it to:
- Landing hero `period_clause_html` (line 296-300).
- `_render_org` hero `period_clause_html` (line 753-757).
- `_render_area` if/when it adds period clauses.
- DPO individual hero (currently doesn't show period).

**P1-2 — Gateway tiles aren't clickable; only the button below is.**

Evidence: `A02_landing_above_fold.png` — each gateway tile (Follow a
politician / Follow an organisation / Browse by policy area) is a
read-only card; the affordance is the `st.button` rendered separately
below the card. Citizens used to "the whole card is the link"
(established by `clickable_card_link` everywhere else in the app)
may not realise they need to find the button.

Fix: wrap each `_tile_html` in a `clickable_card_link(href=...)` with
the URL that the button currently constructs. For the politician
gateway, the URL is computed from the first politician in the index
(`fetch_politician_index().iloc[0]` resolved to a member_profile_url).
Drop the separate buttons. Same change for the three topic tiles
below (line 376-383).

**P1-3 — `name_join_key()` deprecation reaches into the PoC.**

Evidence: `lobbying_3.py:273, 338, 404, 816, 1381` — five callsites
still use the deprecated helper. Round-3 introduced
`resolve_member_code()` as the canonical bridge
([[project_td_name_join_key]]); `name_join_key()` is kept only as a
back-compat fallback. All five sites here should resolve via
`resolve_member_code(name)` first, falling back to `name_join_key()`
only when the registry returns `None`. The same pattern is what the
Interests / Attendance / Payments cards use after round-3.

**P1-4 — Pluralisation gaps in card pills.**

Evidence: `H02_rd_index_above_fold.png` — Philip Carroll card shows
"1 firms" (should be "1 firm"). Similar risk on "1 politicians" /
"1 returns" wherever pill strings are built via
`f"{n:,} <word>s"`. Fix: introduce a small pluralisation helper or
use explicit `"firm" if n == 1 else "firms"` at each call site. Most
prominent in `_render_landing` (RD card list at line 463-467) and
`_render_rd_index` (line 1273-1276).

**P1-5 — "Page not found" modal surfaced on chamber-filter rerun.**

Evidence: `H04_rd_filter_applied.png` — clicking option `.nth(1)` in
the "Filter by former chamber" selectbox triggered Streamlit's
"Page not found" overlay AND simultaneously rendered the
member-overview "This TD is not in the dataset" error for the route
`?member=aacntv`. The selectbox options at line 1240-1244 mix bucket
labels (`"Dáil (12)"`) with individual chamber labels
(`"  Dáil Éireann"` — note the 2-space prefix). At least one option
appears to be parsed by something downstream as a member jump.

Either:
- A Playwright capture-tooling artifact (multi-tab navigation race),
  OR
- A real bug where a chamber label like `"aacntv"` ends up in the
  options list because of bad data in `chamber_display` and the
  page jumps to it as a member route.

Investigate by manually clicking each chamber filter option and
confirming the page stays on the PoC route. If it's data, file a
pipeline ticket for `chamber_display` cleanliness; if it's a code
path, find the navigation that uses a chamber-filter value.

### P2 — Polish (4)

**P2-1 — "2213 politicians" missing thousand-separator.**

Evidence: `E02_area_detail_above_fold.png` — Health area hero reads
"2213 politicians". Other counts on the same line use commas
("11,376 returns filed by 677 organisations"). The pol_cnt format at
`lobbying_3.py:996-998` uses `<strong>{pol_cnt}</strong>` without
`:,` — change to `<strong>{pol_cnt:,}</strong>`. Same risk for
`area_cnt` and other variables in dek_html composition.

**P2-2 — Inline `<style>` blocks for switcher selectbox background.**

Evidence: `lobbying_3.py:778-783, 1006-1011` — both `_render_org` and
`_render_area` inject inline `<style>` blocks to force the switcher
selectbox background to white (presumably to override the
warm-beige `var(--surface)` trap flagged in
[[feedback_css_surface_trap]]). CLAUDE.md forbids inline `style=""`.
Move to a named class in `shared_css.py`, e.g.
`.lp3-switcher-light { background:#ffffff !important; }` scoped via
`.st-key-lp3_org_switcher .stSelectbox > div > div { ... }` directly
in the stylesheet.

**P2-3 — Provenance dataset coverage uses raw `first_period` /
`last_period` strings.** Evidence: `_provenance_footer` at line
215-228 emits `Dataset covers: {fp} → {lp}`. If those are ISO
strings, they render the same ugly `2015-09-01 → 2026-05-01` issue
as P1-1. Pass them through `_fmt_mmm` (or a `_fmt_period` helper).

**P2-4 — Per-page selector (25 / 50 / 100) positioning.**

Evidence: `C02_org_index_above_fold.png`, `F02_stage3_above_fold.png`.
The "PER PAGE" toggle sits top-right above the paginator. Small,
easy to miss; uses the same chip styling as the page-number buttons
below but is functionally distinct. Consider either:
- Moving it to the right end of the pagination row, OR
- Increasing its visual weight (subtle "Show 25 per page" caption
  + chip group on the same row as the paginator).

### P3 — Low-priority (3)

**P3-1 — Landing date coverage uses truncated `YYYY-MM` (no day).**
`A02_landing_above_fold.png` shows "from 2015-09 to 2026-05". Org
detail uses `YYYY-MM-DD`. Inconsistent truncation within the same
page. P1-1's standardisation closes this naturally.

**P3-2 — Stage 3 heading reads as fragment.** Evidence:
`F02_stage3_above_fold.png` — "Stephen Donnelly on Health" works as
a card title but reads odd as an `<h1>`. Possible alternative:
"Stephen Donnelly · Health" (dot-separated like card meta), or
"Lobbying returns: Stephen Donnelly on Health".

**P3-3 — Sidebar "Jump to" selectbox has empty default option.**
Evidence: `B01_sidebar_search_ibec.png` — typing "Ibec" filters the
combined `combined = [""] + pol_filtered + [f"[Org] {n}" ...]` list.
The leading empty string at index 0 renders as a single-char dropdown
item. Use a placeholder like `"— select a name —"` instead of `""`.

---

## Part 2 — Uplift prompt (self-contained)

> You are uplifting the Lobbying-PoC page
> (`utility/pages_code/lobbying_3.py`) after a Playwright audit. The
> full audit is in `doc/LOBBYING_POC_AUDIT.md`; do not regress
> anything in "Positive findings".
>
> **Goal** — close 1 P0 + 5 P1 + 4 P2 in priority order.
>
> **Workflow**:
> 1. Open `lobbying_3.py`, `data_access/lobbying_data.py` (for the
>    DPO firms fetch behaviour), `ui/components.py`, `shared_css.py`.
> 2. For each finding below, write the before/after (file, line,
>    exact replacement) before editing.
> 3. After all edits, re-run the capture:
>    ```
>    $env:PYTHONIOENCODING = "utf-8"
>    python audit_screenshots/_lobbying_poc_capture.py
>    ```
>    Review diff in `audit_screenshots/_lobbying_poc/`.
> 4. Update `project_lobbying_poc_audit_2026_05_26.md` in memory.
>
> **Findings to close** (priority order):
>
> 1. **P0-1 — Silent grey rectangle on DPO firms / clients.**
>    - First reproduce: call `fetch_dpo_firms("Lorraine Higgins")` in
>      a REPL and inspect the column names. If they don't match the
>      `columns={"lobbyist_name": "Firm", ...}` mapping, that's the
>      root cause.
>    - In `lobbying_3.py:_datasette_table` (line 591-617), after
>      computing `keep = [c for c in columns if c in detail.columns]`,
>      add:
>      ```python
>      if not keep or detail.empty:
>          empty_state("No data", "Records exist but the columns are not in the expected shape.")
>          return
>      ```
>    - Then fix the column mapping in `_render_dpo_individual`'s
>      firms (line 1338-1346) and clients (line 1349-1360) blocks to
>      match whatever the underlying view actually returns.
>
> 2. **P1-1 — Standardise date format.**
>    Replace the three callsites in P1-1's evidence to use `_fmt_mmm`
>    (or a new `_fmt_period(first, last)` helper that handles None
>    gracefully). Drop all `YYYY-MM` and `YYYY-MM-DD` strings from
>    user-facing prose.
>
> 3. **P1-2 — Make gateway and topic tiles clickable.**
>    Wrap each tile in `clickable_card_link(href=..., inner_html=...)`
>    using the URL that the corresponding button currently
>    constructs. Drop the buttons. Politician gateway needs the
>    member_profile_url URL — use `resolve_member_code()` on the
>    top politician's name. Org and Area gateways use
>    `?lp3_orgindex=1` and `?lp3_area=<first area>` respectively.
>    Topic tiles use `?lp3_topic=<topic name>`.
>
> 4. **P1-3 — Migrate from `name_join_key` to `resolve_member_code`.**
>    All five sites: prefer
>    `resolve_member_code(name) or name_join_key(name)` so the
>    canonical helper drives, the deprecated one only fires as
>    fallback.
>
> 5. **P1-4 — Pluralisation in card pills.**
>    Introduce a tiny helper:
>    ```python
>    def _p(n: int, singular: str, plural: str | None = None) -> str:
>        return f"{n:,} {singular if n == 1 else (plural or singular + 's')}"
>    ```
>    Use it everywhere a `f"{n:,} {word}s"` string is built. Affects
>    landing RD card list (line 463-467), `_render_rd_index`
>    (line 1273-1276), `_render_org` politicians (line 812-815),
>    `_render_area` politicians (line 1038-1041), and a few more.
>
> 6. **P1-5 — Page-not-found on chamber filter.**
>    Reproduce manually. If a chamber_display value like `"aacntv"`
>    exists in `revolving_door_dpos.parquet`, that's a pipeline data
>    bug — file it separately. If the issue is the 2-space-prefixed
>    individual chamber options at line 1244, simplify the options
>    list to just the bucket labels and drop the individual-chamber
>    leaves.
>
> 7. **P2-1 — `:,` on `pol_cnt` / `area_cnt` in area-detail hero.**
>    `lobbying_3.py:996-998` — add `:,`.
>
> 8. **P2-2 — Extract inline switcher styles.**
>    Add `.lp3-switcher-light` (or similar) in `shared_css.py`,
>    delete the inline blocks at lines 778-783, 1006-1011.
>
> 9. **P2-3 — Apply `_fmt_mmm` in `_provenance_footer`.**
>    Line 215-228 — wrap `fp` / `lp` with `_fmt_mmm`.
>
> 10. **P2-4 — Per-page selector visual weight.**
>    Either move closer to paginator OR add a "Show N per page"
>    caption. Whatever feels least disruptive on the live page.
>
> **Out of scope** (do NOT regress):
> - Quiet H1 + prose dek with embedded counts — the page's defining
>   IA choice and its biggest strength.
> - Rank chip overlay on avatars (Interests P1-2 cascade).
> - `clickable_card_link` on ranked card lists.
> - `_return_card_html` unified pattern across all return-list
>   surfaces.
> - Empty-state callouts for not-found URLs.
> - Custom back button on Stage 3 ("← Back to Health").
> - Breadcrumbs on RD + RD detail + Org index.
> - Mobile gateway tile stacking.

---

## Part 3 — Positive findings (DO NOT REGRESS)

1. **Quiet H1 + prose dek with embedded counts** —
   `A02_landing_above_fold.png` shows "82,319 returns filed by 2,514
   organisations targeting 4,286 politicians across 32 registered
   policy areas." This is the cleanest hero in the app — no separate
   stat strip needed.
2. **Rank chip overlay on avatars** — `D03_org_detail_politicians.png`
   shows Paschal Donohoe with both his photo avatar AND a "#2" rank
   chip overlaid. The Interests P1-2 fix cascaded here cleanly.
3. **Empty-state for not-found URLs** —
   `K01_empty_org_not_found.png` shows
   "Organisation not found / No register entry on record for
   'zzz-nonexistent-org'. The URL may be a typo." Citizen-friendly
   even on a bogus URL. Same quality for area (K02) and topic (K03).
4. **`_return_card_html` unified pattern** — every list of returns
   (org, area, stage 3, topic, DPO) uses the same card shape: period
   chip + optional area pill + title in serif + optional subtitle +
   "View on lobbying.ie ↗" link. Citizens learn the pattern once.
5. **Custom back button on Stage 3** —
   `F02_stage3_above_fold.png` shows "← Back to Health" (not "← Back
   to Lobbying register"). Context-aware nav.
6. **Year-pill segmented control** on every surface that has returns
   — uses `_year_pills` helper (line 524-545) so push-down to SQL is
   uniform.
7. **Pagination with PER PAGE selector** (25/50/100) on every list
   page.
8. **Sidebar `_render_sidebar`** is calm — just search + jump-to.
   No "Notable targets" chip strip or "Browse by area" expander like
   `lobbying_2.py`. Aligned with the PoC ethos.
9. **Three gateway tiles + three topic tiles** — symmetric IA. Once
   P1-2 lands (tiles clickable), this becomes the page's strongest
   above-the-fold pattern.
10. **Mobile layout** — `A08_landing_mobile.png` and
    `J01_mobile_org_detail.png` both stack gateways / cards cleanly.
    Rank chip overlay works on narrow viewports.
11. **Breadcrumbs on multi-step flows** — `lp3_org_idx`, `lp3_rd_idx`,
    `lp3_dpo` all show "Lobbying › Section › Item" trails.
12. **Provenance expander on every Stage** via `_provenance_footer`.

---

Re-run the Playwright capture after any change:
```
$env:PYTHONIOENCODING = "utf-8"
python audit_screenshots/_lobbying_poc_capture.py
```
Writes to `audit_screenshots/_lobbying_poc/`; assumes Streamlit
running on `localhost:8501`.
