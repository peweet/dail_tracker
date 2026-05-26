# Payments page — impeccable audit (2026-05-26)

Captured via Playwright over `/rankings-payments` on the running Streamlit
app. 25 screenshots in [audit_screenshots/payments/](../audit_screenshots/payments/)
covering desktop / tablet / mobile, every segmented-control option
(Rankings + 2020–2026 year views), sidebar interaction (name search +
notable chips), card-click redirect, legacy `?member=` deep-link,
bogus-member empty state, and provenance expander.

This document has two parts:
1. **Audit findings** — what's wrong, why it matters, ranked by severity.
2. **The uplift prompt** — a single prompt ready to hand to a coding
   session to drive the rework.

Capture script: [audit_screenshots/_payments_capture.py](../audit_screenshots/_payments_capture.py)
(fixes the cold-start hydration and iframe-scroll issues that hampered
the attendance audit — first goto uses a 10 s wait, subsequent gotos
3.5 s, scrolling uses `window.scrollTo` instead of `mouse.wheel`).

---

## Part 1 — Audit findings

### Health score

| # | Dimension          | Score | Headline finding                                                                              |
|---|--------------------|-------|------------------------------------------------------------------------------------------------|
| 1 | Accessibility      | 3/4   | st.metric stack reads OK; no `<p>`-as-heading issue here; `—` cards confuse screen readers     |
| 2 | Performance        | 3/4   | Reasonable; provenance expander still always-renders; ranking parquet read via Polars at request |
| 3 | Theming            | 3/4   | Pay-* CSS uses tokens better than att-hall-*; chart still uses hardcoded `#1e40af`               |
| 4 | Responsive         | 3/4   | Mobile + tablet show real data above the fold (best in the app so far)                          |
| 5 | Anti-patterns      | 1/4   | Rankings view broken (names `—`, no TAA bands); st.metric fintech triplet; logic-firewall breach |
| **Total** |            | **13/20** | **Acceptable — significant work needed; one user-visible P0 + one architectural P0.**       |

### Anti-patterns verdict

**Pass on the AI-slop test** for the year views — the avatar + name +
position + band pill + payment-count pill + total badge card is one of
the strongest editorial designs in the app.

**Fail for the Rankings view** — three stacked `st.metric` blocks
(€23M / 171 / €136K) is the fintech-hero-metric pattern PRODUCT.md's
anti-references explicitly call out. Combined with cards that show
`—` for every name and no TAA band, the Rankings view looks like a
half-broken dashboard, not a civic accountability tool.

---

### Executive summary

- **2 P0 blocking issues** — (a) Rankings view is visibly broken
  (every name renders `—`) because the parquet schema diverged from
  what the page expects; (b) the same Rankings flow violates the
  data-access logic firewall (reads parquet + computes `.sum()` /
  `.n_unique()` in Streamlit).
- **6 P1 major issues** — default landing year is YTD; `st.metric`
  fintech-triplet; cross-page `Per-td` lowercase bug; three
  `use_container_width=True` deprecations; sidebar has no page header;
  "Top earners" in 2024+ all show `Band 12 (unmapped)` pills the user
  has no way to interpret.
- **7 P2 minor issues** — `st.markdown(unsafe_allow_html=True)` for
  card rendering; inline `style=""` attributes; hardcoded `#1e40af`
  in chart; sidebar `st.divider`; dead `as_dataframe=True` branch;
  contract drift (`name_filter` / `name_search_mode` not implemented);
  2024 year mixes 33rd + 34th Dáil.
- **3 P3 nice-to-haves** — "(YTD)" suffix on current year; "top
  earner" framing on #1–#3 cards (mild contract violation of
  `forbidden_patterns.editorial_framing`); `st.error` for missing
  data should be `empty_state`.

The single highest-leverage move is **collapsing the Rankings view
into a proper SQL view** — fix both the visible bug and the
architectural one at the same time. The year views are good. The
Rankings view is the page's only weak spot, but it's the spot users
land on as soon as they click anything other than the default year.

---

### P0 — Blocking (fix before next deploy)

**[P0-1] Rankings view shows `—` for every name; no TAA band pills**

- **Location**: [`payments.py:166-221`](../utility/pages_code/payments.py#L166-L221) (`_render_rankings`) → [`payments_data.py:142-151`](../utility/data_access/payments_data.py#L142-L151) (`fetch_alltime_ranking`).
- **Evidence**: [`B01_view_rankings_above_fold.png`](../audit_screenshots/payments/B01_view_rankings_above_fold.png),
  [`B02_view_rankings_cards.png`](../audit_screenshots/payments/B02_view_rankings_cards.png) —
  every card #1 through #20 has `#N` rank, an em-dash where the name
  should be, and a total. **No TAA band pill.** **No party.** **No
  constituency.** Three pieces of context the page promises are
  missing.
- **Root cause**: the page comment at
  [`payments.py:191`](../utility/pages_code/payments.py#L191)
  asserts the parquet has `['rank','member_name','join_key','total_amount_paid_since_2020']`.
  The actual parquet
  (`data/gold/parquet/current_td_payment_rankings.parquet`, 7,961 B,
  171 rows) has:
  ```
  ['rank', 'join_key', 'identifier', 'party', 'constituency', 'total_amount_paid_since_2020']
  ```
  `member_name` is gone — the column was renamed to `identifier` and
  the values became Oireachtas-style slugs:
  ```
  Michael-Collins.D.2016-10-03
  Danny-Healy-Rae.D.2016-10-03
  Pearse-Doherty.S.2007-07-23
  ```
  Page falls through to `name_col = None` at
  [`payments.py:187`](../utility/pages_code/payments.py#L187),
  so `name = "—"` for every row.
- **Impact**: the page's "Rankings" tab — the only way to see all-time
  totals — is unusable. A citizen who clicks "Rankings" sees a
  beautiful card grid showing €309,520, €306,990, €299,221 with no
  names attached. There is no fallback path to discovery — they have
  to click each year individually to see who is who.
- **Contract violation**: `ui_philosophy.required_patterns.taa_band_always_visible`
  ([`payments.yaml:163-164`](../dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/payments.yaml#L163-L164)).
  The Rankings view ships zero band context.
- **Fix**:
  - **Preferred**: build a registered view `v_payments_alltime_ranking`
    that joins payments + member registry to produce
    `(rank, member_name, party, constituency, taa_band_label_current,
    total_paid_since_2020)`. Drop `fetch_alltime_ranking`'s parquet
    read and query the view. See P0-2 for the rest of the architectural
    fix.
  - **Stopgap (if the registered view is a multi-PR job)**: parse the
    `identifier` slug — `name = identifier.split(".")[0].replace("-", " ")`
    yields "Michael Collins". Wrap with the existing
    `data_access.identity_resolver.resolve_member_code` so the URL
    deep-link still works. Surface party + constituency from the
    parquet columns the page is already ignoring. Mark this as a
    short-term hack in a comment because the parquet read is itself
    a P0-2 violation.

**[P0-2] `fetch_alltime_ranking` and `fetch_since_2020_summary` violate the data-access logic firewall**

- **Location**: [`payments_data.py:142-164`](../utility/data_access/payments_data.py#L142-L164).
  ```python
  df = pl.read_parquet(path).to_pandas()        # line 148
  ...
  total = float(df["total_amount_paid_since_2020"].sum())   # line 161
  members = int(df["join_key"].n_unique())                  # line 162
  ```
- **Contract**: [`payments.yaml:10-30`](../dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/payments.yaml#L10-L30):
  - `streamlit_may_read_parquet: false`
  - `forbidden_functions: [read_parquet, parquet_scan]`
  - `forbidden_clauses: [GROUP_BY_MULTI_DIM, ...]` and
    `allowed_aggregate_functions: ["COUNT(*)", "COUNT(DISTINCT col)",
    "MAX(col)", "MIN(col)"]` — `SUM(col)` is **not** allowed; this is
    a business-metric computation.
- The `data_access/payments_data.py` file lives outside the page file
  but still belongs to the Streamlit layer. The contract is unambiguous:
  no parquet reads, no SUM in retrieval. The three `st.metric` totals
  on the Rankings tab are entirely computed in Streamlit.
- **Impact**: even if the schema problem in P0-1 is fixed at the data
  layer (e.g. someone re-adds `member_name` to the parquet), the next
  schema drift will silently break the page again. The architectural
  fix is to lift the totals into the pipeline.
- **Fix**:
  - Pipeline: produce a one-row summary view
    `v_payments_alltime_summary` exposing
    `total_paid_since_2020`, `member_count`, `avg_per_td_since_2020`
    as columns. Page does
    `SELECT total_paid_since_2020, member_count, avg_per_td_since_2020
     FROM v_payments_alltime_summary LIMIT 1`.
  - Page-side change is trivial after the view exists.
  - Same applies to `v_payments_alltime_ranking` from P0-1 — produce
    it as a registered view; page does `SELECT … FROM
    v_payments_alltime_ranking WHERE rank <= 20`.

---

### P1 — Major (fix this milestone)

**[P1-1] Default landing year is the YTD year, not the most-recent completed year**

- [`payments.py:243-252`](../utility/pages_code/payments.py#L243-L252)
  — `default=year_options[0]` where `year_options[0]` is sorted DESC
  → 2026. Same pattern as attendance P1-1.
- **Evidence**: [`A02_landing_above_fold_desktop.png`](../audit_screenshots/payments/A02_landing_above_fold_desktop.png)
  defaults to 2026 with `€1,075,819 TOTAL · 2026` — vs 2025's
  `€4,908,282`. Citizen lands on a tiny number that misrepresents
  the page's scope.
- Contract: [`payments.yaml:36`](../dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/payments.yaml#L36)
  `default_selection: most_recent` — ambiguous between "most recent
  year present" and "most recent completed year". Resolve to the
  latter and document.
- **Fix**: in `_render_primary`, compute
  `default_year = year_options[1] if year_options[0] == str(today.year)
  else year_options[0]` (or use `year_selector(skip_current=True)` if
  switching to year-pills — see P1 contract drift discussion).

**[P1-2] Rankings view uses `st.metric` fintech-triplet**

- [`payments.py:171-174`](../utility/pages_code/payments.py#L171-L174):
  ```python
  c1, c2, c3 = st.columns(3)
  c1.metric("Total since 2020", f"€{total:,.0f}")
  c2.metric("TDs with payments", members)
  c3.metric("Avg per TD since 2020", f"€{avg:,.0f}")
  ```
- **Evidence**: [`B01_view_rankings_above_fold.png`](../audit_screenshots/payments/B01_view_rankings_above_fold.png) on desktop,
  [`G11_mobile_rankings_above_fold.png`](../audit_screenshots/payments/G11_mobile_rankings_above_fold.png) on mobile.
  Mobile stacks the three metrics vertically, eating ~210 px before
  any card data.
- **Contract**: [`payments.yaml:148-161`](../dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/payments.yaml#L148-L161)
  `ui_philosophy.forbidden_patterns` doesn't list st.metric explicitly,
  but PRODUCT.md "Anti-references" explicitly says "Do NOT look like a
  fintech dashboard (gradient accents, **hero metrics**, glassmorphism)".
  And the year-view uses a custom `pay-totals-strip` HTML block —
  the Rankings view is inconsistent with its own page.
- **Fix**: replace with the same `pay-totals-strip` pattern used in
  `_render_primary` for year views ([`payments.py:275-287`](../utility/pages_code/payments.py#L275-L287)). Or extract it as a `totals_strip()`
  helper in `components.py` and reuse on both branches. Same editorial
  voice top-to-bottom.

**[P1-3] "Per-td" lowercase bug in cross-page redirect callout**

- Same root cause as attendance P1-4: `member_moved_callout` at
  [`components.py:372`](../utility/ui/components.py#L372) calls
  `section_label.capitalize()` which renders the configured
  `"Per-TD payments"` as `"Per-td payments"`.
- **Evidence**: [`D03_legacy_member_param_redirect.png`](../audit_screenshots/payments/D03_legacy_member_param_redirect.png),
  [`E01_bogus_member_redirect.png`](../audit_screenshots/payments/E01_bogus_member_redirect.png),
  [`C03_after_notable_chip_redirect.png`](../audit_screenshots/payments/C03_after_notable_chip_redirect.png).
- Cross-page bug — single fix in `components.py` lifts every page
  using the helper. See the attendance audit doc P1-4 for the
  recommended fix.

**[P1-4] Three `use_container_width=True` deprecations**

- [`payments.py:415`](../utility/pages_code/payments.py#L415) (Altair chart in `render_member_payments`)
- [`payments.py:434`](../utility/pages_code/payments.py#L434) (`st.dataframe` years summary)
- [`payments.py:494`](../utility/pages_code/payments.py#L494) (`st.dataframe` payment records audit trail)
- Same as attendance P1-5. Replace with `width="stretch"`. Note:
  both `st.dataframe` branches at 434 and 494 are inside
  `if show_member_header:` — **unreachable** after Phase 6 removed
  the in-page profile flow (see P2-4).

**[P1-5] No `sidebar_page_header` call — sidebar is orphaned**

- [`payments.py:570-585`](../utility/pages_code/payments.py#L570-L585): sidebar starts directly with `sidebar_member_filter` — no kicker, no
  page title. Compare to attendance.py:709 which calls
  `sidebar_page_header("Plenary<br>Attendance")`.
- **Evidence**: [`C01_sidebar_default.png`](../audit_screenshots/payments/C01_sidebar_default.png) — the sidebar starts with "BROWSE ALL MEMBERS" + search box,
  no "Payments" identity. A user with the sidebar collapsed and then
  reopened has lost the cue.
- This was flagged in the 2026-04-30 app-wide audit
  (`project_audit_findings_2026_04_30` → COMPONENT REUSE GAPS →
  `sidebar_page_header() not called`). Lobbying_2 has the same gap.
- **Fix**: add `sidebar_page_header("TD<br>Payments")` (or "Public<br>Spending")
  as the first call inside `with st.sidebar:`.

**[P1-6] Top earners in 2024+ all show `Band N (unmapped)` pills**

- **Evidence**: [`A02_landing_above_fold_desktop.png`](../audit_screenshots/payments/A02_landing_above_fold_desktop.png) — Michael Collins (Band 12), Holly Cairns (Band 11), Danny
  Healy-Rae (Band 11), Michael Cahill (Band 10), Aindrias Moynihan
  (Band 10). All "unmapped".
- **Verified in DuckDB**: the top 5 of 2024 by total_paid are all
  bands 9–12, none of which is in the official Oireachtas TAA range
  (Dublin, 1–8).
- Contract: [`payments.yaml:77-88`](../dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/payments.yaml#L77-L88) `known_data_quality_issues.taa_band_extended_codes` acknowledges
  this — but the UI shows a `pay-taa-pill` with the raw label
  `Band 12 (unmapped)` and zero user-facing context. The page's
  whole editorial argument ("amount × band = context") collapses
  precisely for the highest-paid members.
- **Fix**: two paths, not mutually exclusive:
  1. Cosmetic: render unmapped pills with a different visual treatment
     (dotted border, "?" icon) and add tooltip text: "Band 9-12 are
     not in the official Oireachtas TAA range. The pipeline cannot
     confirm distance interpretation; the raw band number is shown."
  2. Substantive: file a pipeline ticket to resolve the extended-band
     mapping (this is the `pipeline_todo` already in the contract).
     Until resolved, the cosmetic fix is the honest minimum.

---

### P2 — Minor

**[P2-1] `st.markdown(..., unsafe_allow_html=True)` for card rendering**

- [`payments.py:219`](../utility/pages_code/payments.py#L219) (Rankings card HTML) and [`payments.py:373-381`](../utility/pages_code/payments.py#L373-L381) (identity card).
- Should be `st.html(...)` per `feedback_streamlit_api_patterns`. Same family as the SI audit P2-6 finding.

**[P2-2] Inline `style="..."` attributes**

- [`payments.py:477-479`](../utility/pages_code/payments.py#L477-L479):
  ```python
  st.html(
      f"<p style='margin:0.75rem 0 0.4rem;'><strong>Payment records — {selected_year}</strong> "
      f"<span style='font-size:0.8rem;color:var(--text-meta);font-weight:400;'>"
  ```
- Move to a `.pay-record-section-header` class in `shared_css.py`. Same as the 2026-04-30 audit's CSS DECOUPLING item.

**[P2-3] `mark_bar(color="#1e40af")` hardcoded hex**

- [`payments.py:404`](../utility/pages_code/payments.py#L404) — should be `var(--signal-good-deep)` via a CSS variable bridge, or use the `_attendance_capture.py`-equivalent strip-chart pattern.

**[P2-4] Dead `as_dataframe=True` / `show_member_header=True` branches**

- `render_member_payments(show_member_header=True)` branches at
  [`payments.py:419-442`](../utility/pages_code/payments.py#L419-L442) and [`payments.py:482-501`](../utility/pages_code/payments.py#L482-L501) only fire when the page renders its own profile — but Phase 6 removed that path. `render_member_payments` is now only called from `member_overview.py` with `show_member_header=False`.
- Either delete the dead branches and the `show_member_header` flag, or document why they remain. Same finding as attendance P2-2.

**[P2-5] Sidebar `st.divider()`**

- [`payments.py:582`](../utility/pages_code/payments.py#L582). Remove per the design skill ("Dividers look heavy. Just remove them.").

**[P2-6] Contract drift — `name_filter` / `name_search_mode` not implemented**

- [`payments.yaml:176-184`](../dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/payments.yaml#L176-L184) declares `name_filter` (single text input) and `name_search_mode` in the primary view. Page uses `st.segmented_control` with `["Rankings", "2026", "2025", ...]` options — fundamentally different. Update the contract to reflect the post-Phase-6 reality (sidebar holds the member search; primary view is segmented Rankings-or-year).

**[P2-7] 2024 year mixes 33rd + 34th Dáil members without distinction**

- DuckDB shows 245 distinct members in 2024 (vs 160 in 2021-2023 and 174 in 2025). 2024 contains both the outgoing 33rd Dáil and the incoming 34th — a TD who served two months of each appears on the 2024 leaderboard with two months of pay-roll.
- A citizen reading the page can't tell the difference. P2 rather than P1 because the totals are technically correct (the year really did pay these people that money), but the comparative ranking is misleading.
- **Fix**: add a small caption when the year crosses a dissolution: "2024 spans the dissolution of the 33rd Dáil (Nov) and election of the 34th (Dec). Some TDs served only part of the year."

---

### P3 — Polish

**[P3-1] Current YTD year needs a `(YTD)` suffix**

- 2026 shows `€1,075,819 TOTAL · 2026` next to 2025's `€4,908,282`. A user could read the small number as "spending dropped 80%". Add "(YTD)" as a suffix on the year-pill and on the totals strip when `year == current_year`. Same pattern as attendance P1-1's recommended fix.

**[P3-2] `dt-name-card-rank-top` class on #1-3 — mild contract violation**

- [`payments.py:204`](../utility/pages_code/payments.py#L204) styles ranks 1–3 differently from 4–10 via the `dt-name-card-rank-top` class. Contract `ui_philosophy.forbidden_patterns` lists `medal_emojis` and bans `editorial_framing` — visually emphasising "top earners" is in that spirit. Soften: same rank style across all ranks, OR drop the class entirely.

**[P3-3] `st.error` for missing data should be `empty_state`**

- [`payments.py:563-566`](../utility/pages_code/payments.py#L563-L566) — `st.error("No payment data available. Ensure sql_views/payments_*.sql are present and the DuckDB connection is loaded.")` exposes internal scaffolding to citizens. Use `empty_state("No payments data found", ...)` per `feedback_streamlit_api_patterns`.

---

### Patterns and systemic issues

1. **The Rankings view is the page's only weak spot — but it's load-bearing.** The segmented control labels it first; users will click it. Two architectural problems converge there (broken parquet schema + logic-firewall breach). Fix as a pair.

2. **The year-view pattern is the strongest editorial card design in the app.** Avatar + name + position + TAA band pill + payment count pill + total badge. Tablet and mobile show real data above the fold (`G02`, `G05`). The Rankings view should adopt this card pattern, dropping the bare rank/total layout.

3. **Cross-page redirect callout `Per-td` lowercase bug** affects every page that uses `member_moved_callout`. The fix in `components.py` is the single highest-leverage cross-page win.

4. **Top earners cluster at unmapped bands** — every leaderboard table the page produces for years 2024+ has its #1 spot taken by someone whose TAA band the system can't decode. The page's whole framing ("totals are determined by TAA band") fails precisely for the cases the user cares about most.

5. **Same "Phase 6 dead-code residue" pattern as attendance** — `render_member_payments(show_member_header=True)` is the in-page profile flow that no longer fires. Clean it up across all the Phase-6-affected pages in one pass.

---

### Positive findings (keep these)

- **Year-view card design** — best card layout in the app. Avatar + name + position + band pill + count pill + total badge reads as evidence, not decoration. [`A02_landing_above_fold_desktop.png`](../audit_screenshots/payments/A02_landing_above_fold_desktop.png).
- **Mobile above-fold shows real cards** — [`G05_mobile_landing_full.png`](../audit_screenshots/payments/G05_mobile_landing_full.png) puts Michael Collins's card visible at 390×844. Sets the bar the attendance page should match.
- **Glossary strip** — defines TD / TAA / PRA / PSA inline. Onboards a first-time visitor without burying the page.
- **`ui_philosophy` section in the contract** — explicitly forbids hall-of-fame, medal emojis, red-green card colors. The page respects this (subject to P3-2 nit).
- **CAVEAT copy** at [`payments.py:65-73`](../utility/pages_code/payments.py#L65-L73) explicitly tells the user "a higher total does not imply wrongdoing". Civic-voice accountability framing.
- **Custom `pay-totals-strip` HTML** ([`payments.py:275-287`](../utility/pages_code/payments.py#L275-L287)) — proves a token-aware totals widget can replace `st.metric`. Lift this pattern to the Rankings view.
- **`_flip_name` and `_clean_taa_label` presentation helpers** — small, single-purpose, normalize display without modifying upstream data. Good pattern.

---

## Part 2 — The uplift prompt

The prompt below is ready to hand to a coding session. It assumes the
person taking it has access to the repo and to the impeccable skill
context (`PRODUCT.md`, project memory). Drop it into a new conversation
or paste it after `/impeccable craft payments page` to seed
shape-then-build.

> ### Payments page — comprehensive uplift
>
> Rework `utility/pages_code/payments.py`, `utility/data_access/payments_data.py`,
> and the upstream SQL views so the Rankings tab is no longer broken
> and the page no longer reads parquet from Streamlit. Hold to
> `PRODUCT.md` (Direct · Civic · Accountable; editorial accountability
> journalism; **anti-references: no fintech hero metrics**) and the
> page contract's `ui_philosophy` section (no hall-of-fame, no medal
> emojis, no red-green colors, no editorial framing). Stay inside
> Streamlit constraints; honour the project's logic-firewall split.
>
> Audit evidence — see [doc/PAYMENTS_AUDIT.md](PAYMENTS_AUDIT.md)
> and the 25 supporting screenshots in
> [audit_screenshots/payments/](../audit_screenshots/payments/).
>
> #### Goals (in priority order)
>
> 1. **Fix the broken Rankings view AND its architectural violation
>    in one pass.** The current code reads
>    `data/gold/parquet/current_td_payment_rankings.parquet` directly
>    via Polars and computes `.sum()` / `.n_unique()` in Streamlit
>    ([`payments_data.py:142-164`](../utility/data_access/payments_data.py#L142-L164)). That parquet's schema changed at some point —
>    `member_name` was dropped, replaced by `identifier` (an
>    Oireachtas-style slug like `Michael-Collins.D.2016-10-03`). The
>    page falls through to `name = "—"` for every card.
>
>    - Create `sql_views/payments_alltime_ranking.sql` exposing
>      `(rank, member_name, party, constituency, taa_band_label, total_paid_since_2020)`.
>      Build it by aggregating `v_payments_yearly_evolution` and
>      joining to `v_member_registry` (for the canonical name +
>      identity). Pre-rank in SQL with `RANK() OVER ... ORDER BY total_paid DESC`.
>      Filter to 34th Dáil members (use the registry's roster join
>      so this is automatic).
>    - Create `sql_views/payments_alltime_summary.sql` exposing a
>      single row: `total_paid_since_2020`, `member_count`,
>      `avg_per_td_since_2020`.
>    - Drop `fetch_alltime_ranking` and `fetch_since_2020_summary`
>      parquet reads. Replace with `SELECT … FROM v_payments_alltime_ranking
>      ORDER BY rank LIMIT 20` and `SELECT … FROM v_payments_alltime_summary LIMIT 1`.
>    - Validate: Rankings cards show name + party · constituency + TAA
>      band pill + total badge (same shape as year-view cards). Top
>      totals match the prior parquet's first row (Michael Collins
>      €309,520 ± rounding).
>
> 2. **Replace the `st.metric` triplet with the `pay-totals-strip`
>    pattern.** The Rankings view uses three `st.metric` blocks
>    ([`payments.py:171-174`](../utility/pages_code/payments.py#L171-L174)) — the year view uses a custom HTML strip
>    ([`payments.py:275-287`](../utility/pages_code/payments.py#L275-L287)) that fits the editorial register much better. Lift the
>    HTML strip to a `pay_totals_strip(items)` helper in
>    `components.py`; call it from both views. Same voice everywhere.
>
> 3. **Adopt the year-view card design for Rankings.** Year cards show
>    avatar + name + position + TAA band pill + payment count pill +
>    total badge. Rankings cards show only `#N · — · €N` — a husk.
>    Once the SQL view supplies the band label and identity, render
>    Rankings cards through `_pay_card_html` exactly like year cards.
>    The page reads as one editorial system, not two.
>
> 4. **Default the year pill to the most recent COMPLETED year.**
>    Currently defaults to 2026 (YTD). At
>    [`payments.py:243-252`](../utility/pages_code/payments.py#L243-L252), select the most recent year strictly less than
>    today's year (or use `year_selector(skip_current=True)` if
>    switching to year-pills). Add a "(YTD)" suffix to the current
>    year's label so users know what they're picking when they do
>    click it.
>
> 5. **Fix the cross-page redirect callout typography.** Same finding
>    as attendance audit P1-4. `member_moved_callout` at
>    [`components.py:372`](../utility/ui/components.py#L372) calls
>    `section_label.capitalize()` which renders "Per-TD payments" as
>    "Per-td payments". One fix lifts every page using the helper
>    (attendance, payments, interests, committees, lobbying).
>
> 6. **Add `sidebar_page_header("TD<br>Payments")`** as the first call
>    inside `with st.sidebar:` (or "Public<br>Spending" — match the
>    hero kicker). Page-identity belongs at the top of the sidebar;
>    the existing flow goes straight to the member search.
>
> 7. **Handle unmapped TAA bands with civic-voice context.** Top
>    earners in 2024+ all carry `Band 12 (unmapped)` pills. The
>    contract acknowledges the data gap but the UI doesn't surface
>    it. Add a `--unmapped` modifier on the `.pay-taa-pill` (dotted
>    border, secondary color) and a tooltip: "Bands 9-12 are not in
>    the official Oireachtas TAA range. The raw band number is shown;
>    distance is unverified." Same treatment in the provenance
>    expander.
>
> #### Polish (P2 — fold into the same PR if time permits)
>
> - Replace `st.markdown(..., unsafe_allow_html=True)` with `st.html(...)`
>   at [`payments.py:219, 373-381`](../utility/pages_code/payments.py#L219).
> - Replace `use_container_width=True` with `width="stretch"` at
>   [`payments.py:415, 434, 494`](../utility/pages_code/payments.py#L415).
> - Move inline `style="..."` attributes from
>   [`payments.py:477-479`](../utility/pages_code/payments.py#L477-L479) into a `.pay-record-section-header` class in `shared_css.py`.
> - `mark_bar(color="#1e40af")` at [`payments.py:404`](../utility/pages_code/payments.py#L404) — replace with `var(--signal-good-deep)` (or a
>   CSS-variable read; Altair doesn't read CSS vars directly so use
>   the OKLCH hex equivalent and add a code comment cross-referencing
>   the token).
> - Delete the dead `as_dataframe=True` / `show_member_header=True`
>   branches in `render_member_payments`
>   ([`payments.py:419-442`](../utility/pages_code/payments.py#L419-L442), [`payments.py:482-501`](../utility/pages_code/payments.py#L482-L501)) — same Phase-6
>   residue as attendance P2-2.
> - Remove the sidebar `st.divider()` at
>   [`payments.py:582`](../utility/pages_code/payments.py#L582).
> - Replace `st.error("No payment data available...")` at
>   [`payments.py:563-566`](../utility/pages_code/payments.py#L563-L566) with `empty_state(...)`.
> - Add a "2024 spans the dissolution of the 33rd Dáil…" caption on
>   the 2024 year view (or any year that crosses a dissolution).
>   Compute by checking if member count for the year is materially
>   higher than the surrounding years.
>
> #### Contract update
>
> - [`payments.yaml:176-184`](../dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/payments.yaml#L176-L184) lists `name_filter` (single text input) and `name_search_mode` in `primary_view_flow`. Neither exists in the post-Phase-6 page. Update to match the
>   segmented Rankings-or-year control.
>
> #### Non-goals (don't do these in this PR)
>
> - No new Python/Polars enrichment in production paths; new logic
>   goes to `pipeline_sandbox/` per `project_pipeline_sandbox_rule.md`.
>   The new registered views (P0-1/P0-2) are SQL-only — they should
>   join `v_payments_yearly_evolution` to `v_member_registry`, not
>   read a new parquet.
> - Don't touch `pipeline.py` / `enrich.py` /
>   `normalise_join_key.py` (sandbox rule).
> - No CSS-architecture split or typography-scale collapse —
>   deferred design debt.
> - No `member_overview` Payments-expander rework — Phase 6 is settled.
>
> #### Acceptance
>
> Re-run [audit_screenshots/_payments_capture.py](../audit_screenshots/_payments_capture.py) after the rework. New screenshots should show:
> - Rankings view cards include name + party · constituency + TAA
>   band pill + total badge (P0-1 resolved).
> - No `pl.read_parquet` calls anywhere in `utility/data_access/payments_data.py` (P0-2 resolved).
> - No `st.metric` calls on `payments_page` (P1-2 resolved); both
>   views use `pay-totals-strip`.
> - Default landing year is the most recent COMPLETED year (P1-1).
> - Every `Per-TD payments` callout renders correctly (P1-3).
> - Sidebar has a "TD Payments" / "Public Spending" header (P1-5).
> - Unmapped band pills have a distinct visual treatment (P1-6).
> - Zero deprecation warnings in the Streamlit console.
> - All P2 polish issues from the checklist resolved.
>
> Re-run `/impeccable audit` on the payments page after the rework
> and target a health score of 17+/20.

---

## Appendix — Screenshot index

Phase A: `A01–A05` — landing on default year view (2026), full + scrolled
Phase B: `B01–B05` — segmented control: Rankings + 2020/2023/2024 year views
Phase C: `C01–C03` — sidebar default / name-search filtered / notable chip redirect
Phase D: `D01–D03` — card click → member-overview redirect (D01-D02 may be missing if no clickable_card_link was rendered; D03 legacy URL redirect confirmed)
Phase E: `E01` — bogus member redirect (empty-state fallback)
Phase F: `F01–F03` — provenance expander open / TAA bands table / PDF links
Phase G: `G01–G12` — tablet + mobile responsive. Mobile shows the first card above the fold — best in the app so far.
