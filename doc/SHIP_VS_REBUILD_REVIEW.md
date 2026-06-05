# Ship vs Rebuild Strategy — Review

**Reviewer pass:** 2026-06-05 · ground-truthing `doc/dail_tracker_ship_vs_rebuild_strategy.md` (readiness table §2, hardening list §5, coupling claims §7, framework options §8) against real code. Cross-read with `doc/fastapi_query_core_uncoupling_plan.md`.

**Headline:** most of the doc's "minimum hardening" is already built or partly built. The QueryResult/core-seam pattern it proposes as future work **already exists and is wired**. The real undone items are narrower than the doc implies — and the genuine blockers it under-weights (PII leak, headless-pipeline, no feedback path) are what actually gate a public launch.

---

## Claims Ledger

| claim | doc says | repo reality (path:line) | verdict |
|---|---|---|---|
| Empty-state helper exists | §5.3 "fix empty-state ambiguity" (implies undone) | `empty_state()` shipped + used app-wide; `todo_callout()`, `page_error_boundary()` too — `utility/ui/components.py:451,394,76` | **stale** (largely done) |
| Provenance footer needed | §5.5 add provenance footer (implies undone) | Per-page footers already render: `judiciary.py:362`, `attendance.py:240`, `interests.py:151`, `committees.py:700`, `lobbying_3.py:263`, `payments.py:161`; expander helper `ui/source_pdfs.py:301` | **stale** (done, but not via one shared component) |
| sidebar_shell helper missing | §7 "create `utility/ui/page_shell.py`…" | `sidebar_shell()` + `sidebar_subtitle/provenance/divider` already exist `components.py:1298,1261,1274,1287`; sidebar is hidden app-wide via `hide_sidebar()` `components.py:591` | **stale** (reinventing) |
| Result-aware data access is future work | §6.1 "[ ] Add result-aware data access, not only empty DataFrames"; §7 propose `QueryResult` | `QueryResult` is **already built and frozen** `dail_tracker_core/results.py:33`; 3-state model (ok/no-rows/unavailable) | **stale** (already done) |
| Data wrappers return empty DataFrames on failure | §7 "data wrappers sometimes return empty DataFrames instead of preserving failure semantics" | TRUE at the **page boundary**: wrappers unwrap `.data` and discard ok/unavailable — `procurement_data.py:39`, `judiciary_data.py:31`. Core preserves it; UI throws it away | **confirmed** (but core already has the fix) |
| Streamlit-free core is a stated direction only | §7 "stated architecture direction" | `dail_tracker_core/` exists with 12 query modules returning QueryResult (`queries/{procurement,judiciary,payments,votes,...}.py`); procurement/judiciary data-access are already thin wrappers | **stale** (built, not just stated) |
| Freshness not surfaced | §5.2 show data freshness (implies undone) | `freshness.json` IS generated at pipeline-end `pipeline.py:89` + `tools/check_freshness.py`; BUT **no page reads it** — grep finds zero `freshness.json` consumers in `utility/` | **partly confirmed** (generated, not displayed) |
| Judiciary is routed in main | §2/§6.2 "page appears routed" | Confirmed routed `app.py:125-130` (`url_path="rankings-judiciary"`); anonymises to initials, drops in-camera `judiciary.py:15-21,318` | **confirmed** |
| Procurement has no top-nav page | §2 "no stable top-nav page yet" | Confirmed — no procurement page in `app.py` nav (13 pages, none procurement); data-access + core + views exist but unsurfaced | **confirmed** |
| Local Authority / Housing not a page | §2/§6.3 | Confirmed — no LA/housing page in nav; no `procurement.py`/`local_authority*.py` page file | **confirmed** |
| legal_diary in a canonical scheduled run | §6.2 "[ ] Confirm legal diary extraction is part of a canonical run" | **NOT in pipeline.py** — grep `legal_diary\|judiciary` in `pipeline.py` = 0 matches; poller/extractor exist but no chain | **confirmed gap** |
| Beta language on every page | §5.1 | No global beta banner; grep `beta` in `utility/` = 0 page-facing hits | **confirmed gap** |
| Feedback/report-issue path | §5.6 | None in UI; only a "paste into GitHub issue" hint inside the crash expander `components.py:81` | **confirmed gap** |
| value_counts in UI (firewall) | §7 page-level grouping risk | Widespread; many marked `# logic_firewall: display_only` but several **unmarked**: `corporate.py:1580`, `statutory_instruments.py:473,570`, `judiciary.py:232` | **confirmed** |
| st.dataframe still on pages | implied "monolithic pages" | 10 occurrences across `committees.py`(5), `lobbying_3.py`(3), `member_overview.py`(2) — last is a firewall-flagged memory violation | **confirmed** (small residue) |

---

## Architectural Assessment

**The doc is arguing for a build that has substantially already happened.** Its §7 "recommended refactor before adding more pages" (create `QueryResult`, a Streamlit-free core, shared shell/empty-state/provenance components) describes the *current* `dail_tracker_core` + `utility/ui/components.py` state, not a future one. Whoever wrote it inspected GitHub without cloning (per REVIEW_CONTEXT §4) and missed:

- `dail_tracker_core/results.py` — `QueryResult` is frozen, cache-safe, three-state, documented. This is exactly the §7 "fetch_x_result() -> QueryResult" proposal, already merged.
- `dail_tracker_core/queries/*.py` — 12 domains return QueryResult; `procurement_data.py` / `judiciary_data.py` are already the "thin Streamlit cache wrapper" the uncoupling plan's §"Streamlit Wrapper Pattern" calls for.
- `components.py` already carries `sidebar_shell`, `empty_state`, `todo_callout`, `hero_banner`, `glossary_strip`, `page_error_boundary`, pagination, card primitives — i.e. ~8 of the §7 "create these components" list under different (existing) names.

So the new-components list in §7 and the `QueryResult` pattern are **reinventing**, not additive. The one genuinely additive idea is a **single shared `provenance_footer`/`freshness_banner` component**: today provenance is 6+ bespoke `_render_provenance` functions and freshness.json is generated but **read by nobody**. That consolidation is worth doing and is small.

**Coupling reality:** pages are *less* coupled than the doc fears on the data axis (SQL lives in `sql_views/`, business logic increasingly in core, all CSS in `shared_css.py`), but the **failure-semantics seam is severed at the last inch** — every wrapper does `_q.foo(conn).data`, dropping `ok`/`unavailable_reason`. So the §5.3 "blank table = no-records vs source-failed" ambiguity the doc wants fixed is *already solved in core and re-broken in the adapter*. Fixing it is ~1 line per wrapper plus a page-side render branch, not a refactor.

**Framework options (§8) vs committed direction:** the committed direction (`fastapi_query_core_uncoupling_plan.md`: DuckDB core seam → registry → optional FastAPI extra → Streamlit thin UI, Option A now / Option D medium-term) is sound and already in motion. The doc's §8 reaches the same Option-A-now / Option-D-later conclusion, so it's aligned — but it presents Dash (Option B) and a from-scratch FastAPI+Next.js (Option C) as live menu choices when the repo has already picked and partially built D. Treat §8 as confirmation, not a decision still to make. **Reject** re-opening the framework question.

---

## Devil's Advocate

**For shipping (steelman):** the hard asset really is the data + linkage + caveats, the core seam exists, helpers exist, and journalists/researchers get value from a caveated beta now. Most hardening is done. This is real.

**Against shipping now (the under-weighted risks the doc glosses):**

1. **Live PII leak makes a public judiciary launch a legal exposure, today.** The judiciary page *claims* "people shown by initials only" (`judiciary.py:318-321`) and the design anonymises — but a parallel review found the **gold legal-diary cases parquet leaks 8 of 602 real natural-person names**. The page renders `case_anonymised` verbatim (`judiciary.py:276`) and links each row to the official diary. Publishing a court listing that names a private litigant, under an explicit "we anonymise" promise, is the worst-case combination: the promise *increases* liability because it's a representation users rely on. This is a hard P0 **hold** on the judiciary tile until the gold is clean and there's a test asserting zero natural-person names (the doc's §6.2 "[ ] tests that natural-person names are anonymised" is exactly this, and it's unchecked). "Harden privacy before expanding" is too soft — it's "do not expose at all until proven."

2. **"Harden Streamlit first" is mostly NOT sunk cost — with one caveat.** The doc's hardening (provenance, empty-states, freshness, beta language, feedback) lives in the *thin UI layer* you keep regardless of a future React swap, OR in the *core* (QueryResult) which is framework-agnostic by design. So the sunk-cost worry is largely unfounded. The exception: bespoke per-page CSS (`jd-*`, `corp-*`, `si-*` families inline in pages, e.g. `judiciary.py:98-169`) is throwaway against a React rebuild. Recommendation: do hardening in core + shared components (durable), avoid investing more in per-page inline CSS.

3. **Pipeline not proven headless on Ubuntu** (per `project_freshness_architecture` memory: Unit B deferred precisely because `pipeline.py` isn't proven headless). A public beta implies a refresh cadence; if the pipeline can't run unattended on CI/cloud, "freshness" decays and the freshness banner (once wired) will broadcast staleness. legal_diary/judiciary isn't even in `pipeline.py` (`pipeline.py` grep = 0) — so the one privacy-sensitive page also has no canonical refresh and no freshness entry. Shipping a daily-court-diary page with no scheduled refresh is a credibility risk.

4. **Open firewall violations are a trust risk on a public product specifically.** Unmarked `value_counts` (`corporate.py:1580`, `statutory_instruments.py:473,570`, `judiciary.py:232`) and theme/classification-in-UI mean a page can silently reclassify on a data shape change. For internal QA that's tolerable; for a public accountability tool where the whole pitch is "we don't model, we surface verifiable data," an unaudited classification in the render path undercuts the core promise. These are P1, not blockers, but should be marked/closed on shipped pages.

5. **Moderation/feedback load is unaddressed.** There is **no feedback path in the UI at all** (§5.6 fully undone). A public civic-accountability tool naming payments/lobbying/corporate/judiciary parties *will* draw correction requests and complaints. Shipping with zero "report an issue" affordance means complaints route to nowhere (or to the founder's inbox unstructured) and there's no audit trail of disputed records — a real reputational and arguably legal-process gap.

**Net:** the doc's "ship carefully" is right in spirit but its risk weighting is inverted. It frames hardening (mostly done) as the gate and treats PII/headless/feedback (the real gates) as checklist line-items. The binding constraints are #1, #3, #5.

---

## Data Quality & Enrichments

- **Judiciary cases gold**: 8/602 real names leak (parallel-review finding). Must be fixed upstream + test-asserted before the tile is public. The page-level anonymisation design is good; the gold is the hole.
- **Personal insolvency precedent** (`corporate.py:15`, `feedback_personal_insolvency_privacy`): corporate page already excludes individual bankruptcy. Good — judiciary should inherit the same "named natural person ⇒ drop" discipline at the gold layer, not just the render layer.
- **Procurement**: never-union-money-grains discipline is real and the caveat text exists in `fastapi_query_core_uncoupling_plan.md` provenance constants — but the procurement page doesn't exist to carry it yet. Ship procurement only with the value-kind legend + `value_safe_to_sum` enforced (doc §6.1 checklist is correct here and undone).
- **Freshness**: generated, accurate, future-date-guarded (`check_freshness.py:27-30`), but invisible to users. Highest-value-per-effort enrichment: wire `freshness.json` into a shared banner.

---

## Build / Defer / Reject

| item | verdict | value / effort | reason |
|---|---|---|---|
| Wire `freshness.json` → shared freshness banner on every page | **Build** | High / Low | Generated already; just unread. Closes §5.2 for the whole app at once |
| Single shared `provenance_footer` component (consolidate 6 bespoke ones) | **Build** | Med / Low | Genuinely additive; the only real new component in §7 |
| Surface `QueryResult.ok/unavailable_reason` at the page boundary (stop discarding `.data`) | **Build** | High / Low | Core already returns it; ~1 line/wrapper + a render branch. Closes §5.3 ambiguity properly |
| Global "public beta" banner + a "report an issue" link (structured, not inbox) | **Build** | High / Low | §5.1+§5.6 fully undone; #5 is a real launch gate |
| Mark/close unmarked `value_counts` + theme classification on shipped pages | **Build** | Med / Med | Firewall integrity is the product's core promise; P1 |
| Judiciary tile public exposure | **HOLD** | — | Live PII leak (8/602 names). Gold fix + zero-names test required first |
| legal_diary/judiciary into `pipeline.py` as a canonical chain + freshness entry | **Defer→Build-before-judiciary-launch** | Med / Med | No scheduled refresh today; pairs with the HOLD |
| Procurement top-nav page | **Build (after Tile-1 caveats)** | High / Med | Backend ready; ship with value-kind legend + `value_safe_to_sum` test (§6.1) |
| Local Authority / Housing page | **Defer** | — / High | Sources not built (SSHA crosswalk pending); keep as "coming soon" tile only |
| New `QueryResult` pattern (as net-new work) | **Reject** | — | Already built (`results.py`). Doc is reinventing |
| §7 new-components list (`page_shell/kpi_card/empty_state/...` as net-new) | **Reject** | — | ~8 already exist under current names; would duplicate |
| Re-open framework choice (Dash / from-scratch FastAPI+Next) | **Reject** | — | Direction already committed (Option A now / D later) and partially built |
| Further per-page inline CSS investment | **Reject** | — | Throwaway against the eventual React swap (#2 caveat) |
| Prove `pipeline.py` headless on Ubuntu before public refresh cadence | **Build (launch-blocking for any daily-refresh tile)** | High / Med | Per `project_freshness_architecture`; freshness banner is only honest if refresh runs |

---

## Bottom Line

**Verdict: PARTIAL SHIP.** The strategy doc's core recommendation — ship Streamlit as a caveated beta, don't rebuild the UI — is correct, but its readiness picture is out of date in the project's favour and its risk weighting is backwards. The QueryResult core seam and most shared UI helpers (empty-state, sidebar shell, provenance footers, error boundary) **already exist**, so "minimum hardening" is far smaller than §5 implies: the genuinely-undone work is a global beta banner, a structured report-an-issue path, wiring the already-generated `freshness.json` into a shared banner, surfacing `QueryResult`'s failure state at the page boundary (it's discarded one line too early), and closing the unmarked `value_counts`/classification firewall residue. Ship the mature parliamentary/money pages (Members, Attendance, Votes, Payments, Interests, Lobbying, Legislation, SIs, Appointments, Corporate, Committees) behind those five low-effort fixes. **Hold the Judiciary tile** outright until the gold legal-diary parquet stops leaking 8 of 602 real natural-person names and a test asserts zero — publishing named litigants under an explicit "we anonymise" promise is the sharpest legal exposure in the app, and it is live today. Build the Procurement page next with value-kind caveats; keep Local Authority/Housing a "coming soon" tile. Reject the doc's proposed new QueryResult/component layer and any re-opening of the framework question — those decisions are made and partly built; don't pay for them twice.
