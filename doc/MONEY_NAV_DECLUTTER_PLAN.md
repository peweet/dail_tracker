# "The Money" — Navigation Declutter Plan

**Status:** proposal for owner review · **Author:** Claude · **Date:** 2026-07-08
**Scope:** IA / navigation only. No pipeline changes, no new business logic in pages.
**Governing rule:** the composable navigation graph — *every named entity is a clickable
door that carries the entity to its canonical page, both directions where related*
(doc/NAVIGATION_GRAPH.md, `feedback_entity_links_seamless_navigation`). Nothing here may
create a contextual cul-de-sac.

---

## 0. Executive summary

"The Money" holds **7 visible pages** — nearly double every other nav section — and two of
them are the owner's stated pain points ("Payments" vs "Public Payments" read as
duplicates; "Companies is somewhat duplicated"). This plan cuts it to **5**, register by
register, using the proven *Your-Council* consolidation pattern (a hub + hidden-but-routable
satellites), and separately fixes the "Companies duplicate" *perception* with role-clarity
copy + a CRO-gated cross-link.

Every claim in the impact section (§8) is traced to current code (2026-07-08) and was
**independently re-verified** by a second read-only sweep. The headline finding:
Follow-the-Money and Accommodation Spend have **zero inbound entity links** (nav-only),
while Companies has **~6** — so the two low-connectivity pages are exactly the right ones
to fold, removing **zero** graph edges. The change is **safe by construction**: an existing
CI lint (`test_internal_link_slugs.py`) is visibility-blind, so hiding these pages can't
break routing and the new hub cards pass it automatically.

**Recommended target (7 → 5 visible):**
`TD Payments · Election Finance · Procurement · Public Payments (hub) · Companies`.

### Status of work
| Item | State |
|------|-------|
| Phase 0 — rename nav label "Payments" → **"TD Payments"** (app.py L240) | ✅ **SHIPPED** (title string only; url_path unchanged; app.py parses; no test asserts the label) |
| Phase 1 — nav collapse (hide FtM + Accommodation, add Public Payments entry cards) | ✅ **SHIPPED 2026-07-09** (built by a Fable agent, independently re-verified: firewall clean · slug/link contracts pass · 17 new smoke tests + 29 existing PP tests green · url_paths untouched). **Live-verified on a fresh server (desktop + mobile):** dropdown = exactly 5 items; card click → `/follow-the-money` works; hidden routes + old deep links resolve. **Placement hoisted after live measurement:** below the glossary the cards sat under the 900px fold (y≈975; ~670px scroll on mobile, where they are the ONLY path) → moved directly under the headline stats strip → **y≈751 desktop (above fold), ~255px scroll mobile**; hero → caveat → stats still lead. |
| Phase 2 — Companies de-duplication (role-clarity ×3 + gated cross-links both ways) | ✅ **SHIPPED 2026-07-12** (started by a Fable agent, finished inline after it hit a session limit; live-verified both gate directions on real entities: JOHN SISK SON = awarded → CTA + dossier + ledger loop; NBI INFRASTRUCTURE = payments-only → no link, dead-end gone). A4 note: the review's `<\strong>` bug at procurement.py L1896 **did not verify** — markup is correct, no fix needed. |
| Phase 2.5 — Procurement §Paid bridge (Decision 6 Option B) | ✅ **SHIPPED 2026-07-12** (`_render_payments_bridge()`: caveat kept verbatim + top-5 teaser w/ company-class quarantine + two `.mf-featured` doors → PP hub + FtM; only the `section == "paid"` call site swapped; `_render_payments` untouched for FtM; `?paid_*` drills live-verified still routing). Gates: firewall clean · ruff clean · **87 tests pass** incl. 8 new in test_money_declutter_phase2.py. |
| Option C — view-family consolidation | ❌ **CLOSED AS NO-GO 2026-07-12** after reading both families: deliberately divergent (privacy gate + column semantics), documented in the view headers; only 3 shared guard lines — below the base-extraction bar. See §15.4. |
| Phase 3 — deep-embed: in-page sections on the PP hub | ✅ **SHIPPED 2026-07-12.** Council-pattern switcher [Browse the register · Trace a payment · Accommodation spend] with consumable `?pp=` param; the Phase-1 entry cards became **soft-nav section openers** (`?pp=trace`/`?pp=accom` — live-verified zero reload via window sentinel). Trace = FtM search (`href_base="/follow-the-money"`) + featured tiles, all links **absolute onto `/follow-the-money`** so exactly ONE stateful trail surface exists (no `mf_trail` on PP — the §8.5 state-collision risk designed out, not managed). Accommodation renders **fully inline** (`render_accommodation_body(embedded=True)` — compact heading, no double hero); both satellite routes stay alive (live-verified). Bogus `?pp=` falls back to browse. Gates: firewall clean · ruff clean · **91 tests pass** (4 new). |

### Independent review (Fable, 2026-07-08) — verdict: YES-WITH-FIXES
An adversarial read-only review verified the plan's code claims (6 Companies inbound edges,
zero-inbound FtM/Accommodation, the visibility-blind slug lint, shared `supplier_normalised`,
no CI break — all confirmed) and surfaced five corrections, now folded in below:
1. **`/company` is built on the awards register and shows "Company not found" for
   payments-only suppliers** (~86% of paid suppliers) — an *existing* contextual cul-de-sac
   the Phase-1 hub would amplify (see §9; new gating in §7 and §12-D5).
2. **Procurement already embeds a full paid-register browse** ("Who actually gets paid?",
   procurement.py L337/L779/L4278) — the real, unowned duplication (new §12 Decision 6; §1/§5
   corrected).
3. The mobile / overflow benefit was overstated (§8.7 corrected).
4. "Reposition Companies last" was a **no-op** — it is already last (removed from §6/§8.1/§10).
5. The register bridge is a live **name-norm string** join, not CRO-only (§2/§7 corrected),
   which enables a cheaper Phase 2.

---

## 1. Problem

The top-nav section **"The Money"** holds **7 visible pages** vs 1–4 elsewhere (What They
Own 1 · Your Area 3 · Members & Parliament 4 · Law & Records 4 · Influence 3). It reads as
an undifferentiated pile.

Current contents (`utility/app.py` nav block, L239–286):

| # | Nav label | url_path | What it actually is | Register / keyspace |
|---|-----------|----------|---------------------|---------------------|
| 1 | Payments | `rankings-payments` | **TD** payments — money politicians *receive* (expenses/allowances) | member pay |
| 2 | Election Finance | `rankings-election-spending` | GE2024 hub (donations + party + candidate spend) | political money in |
| 3 | Procurement | `rankings-procurement` | eTenders/TED **awards** + suppliers/authorities/categories/lobbying tabs — **also embeds a "Who actually gets paid?" paid-register browse** (L337/L779/L4278) | **awards** (+ paid browse) |
| 4 | Follow the Money | `follow-the-money` | Trail nav over the **payment graph** (`?paid_supplier=`/`?paid_publisher=`) | **payments** |
| 5 | Accommodation Spend | `accommodation-spend` | Neutral IPAS/Ukraine accommodation deep-dive | payments (subset) |
| 6 | Public Payments | `rankings-public-payments` | Public-body PO/payment disclosures >€20k (publishers/suppliers) | **payments** |
| 7 | Companies | `company` | Org-360 dossier — one firm across *all* registers | seller entity node |

## 2. Diagnosis — three distinct problems

**(A) Label collision.** "Payments" is **TD** payments (money *in* to politicians);
"Public Payments" is **State** spend (money *out* to suppliers). Side by side the two
labels are indistinguishable and describe opposite money flows. *(Fixed in Phase 0.)*

**(B) Register mixing.** The seven blend two unrelated stories —
*money to the political class* (TD Payments, Election Finance) and *money the State pays
out* (Procurement, Follow the Money, Accommodation, Public Payments, Companies) — and
within the second story they mix two **never-summable registers**: **awards** (Procurement:
eTenders/TED ceilings) vs **payments** (Public Payments + Follow the Money: disclosed
PO/payment lines). Keeping these adjacent without signalling the boundary is both clutter
*and* an honesty risk (`caveats.PROCUREMENT_AWARDS`, 3-grain never-sum rule).

**(C) "Companies is a duplicate view."** Real *as a perception*, but a merge was already
investigated and **rejected on data grounds** (doc/archive/FOLLOW_THE_MONEY_IA_EXPLORATION.md).
There are **three supplier surfaces**:

| Surface | URL / key | What it uniquely gives |
|---------|-----------|------------------------|
| **Companies** | `/company?supplier=` · awards-side `supplier_norm` | Full org-360 footprint: awards + TED + payments *summary* + lobbying + corporate + charity + EPA, one firm one URL |
| **Procurement → Suppliers tab** | `/rankings-procurement` · `supplier_norm` | Ranked awards leaderboard (browse/discover) |
| **Follow-the-Money supplier node** | `?paid_supplier=` · payments-side `supplier_normalised` | The **only navigable payment graph**: per-body ranked list → drill to ledger, SPENT/COMMITTED toggle |

Validated (do **not** recompute — numbers from the IA exploration):
- The registers are bridged **in production by a shared name-norm string** — the company
  dossier already joins awards-side `supplier_norm` to payments-side `supplier_normalised` by
  string equality (company.py L259 → `v_procurement_payments WHERE supplier_normalised = ?`).
  **CRO is the *verified-identity* key, not the only join.** But `/company` itself is built on
  the *awards* summary, so it only resolves for firms that are *also* awarded: of **17,633**
  paid company-suppliers, **6,455** have a CRO and only **2,405** are also awarded → a
  Companies↔payments hand-off resolves **~14%** one way, **~37%** the other (clean 1:1 where it
  exists). The **~86% that don't resolve hit "Company not found"** (company.py L235–241, §9).
- Companies is **not** a superset of the FtM node and vice-versa — each holds what the other
  lacks. **FtM must not be deleted; Companies must not be merged.**
- `procurement.py::_supplier_href` (L211–214) **already** routes awards-side supplier cards
  to `company_profile_url()` → `/company`. Companies is therefore the *canonical seller node*
  and a live entity-link target; deleting it would break that edge.

**Conclusion:** (C) is a **role-clarity + cross-link** problem, not a delete. The
"duplication" is that three surfaces show "a supplier" without telling the user which
question each answers.

## 3. Approaches considered

Three were put to the owner at the outset; the recommendation is **A**.

| Approach | What it does | Verdict |
|----------|--------------|---------|
| **A — Consolidate into hubs** *(recommended)* | Fold Follow-the-Money + Accommodation into a Public Payments hub (Your-Council pattern); hide old pages but keep routes; rename Payments→TD Payments. **7→5.** | Matches proven precedent, register-honest, addresses both pain points. |
| **B — Relabel only** | Rename Payments→TD Payments + tighten labels; no merges. **7→7.** | Fast/low-risk but doesn't reduce count; = Phase 0 alone. |
| **C — Split into two sections** | Keep "The Money" for State spend; move TD Payments + Election Finance to a new "Political Money" section. | Reduces per-section crowding but *adds* a section and doesn't consolidate the State-spend pile. |

## 4. Design constraints (non-negotiable)

1. **Circular graph.** Every entity keeps travelling. Any page removed from the menu stays
   **hidden-but-routable** (the `_interests_redirect_page` pattern, app.py L83–96; the
   Your-Area hidden pages, app.py L157–188). Verify on the entity-bearing **detail** state.
2. **Route stability.** `entity_links.PAGES` (entity_links.py L67–92) maps logical keys →
   `url_path` slugs. **Keep every `url_path` unchanged** — only `visibility` and *where a page
   is reached from* may change. (Exactly how `rankings-council-spending` kept resolving after
   the council collapse.)
3. **Never-sum.** Awards and payments stay **separate** with their existing caveats; no UI
   element may present a blended awards+payments total.
4. **Logic firewall.** Hub landings render from existing renderers/registered contracts.
   Navigation cards are pure `<a href>`. The one data-layer touch (CRO cross-link lookup)
   goes in a view, not a page.
5. **SPA soft-nav.** Same-page `?param` cards soft-navigate (`ui/spa_links.py`); cross-page
   `/slug?…` links do real navigation (intended).

## 5. The keyspace map (drives the grouping)

```
MONEY TO POLITICIANS            MONEY THE STATE PAYS OUT
─────────────────────           ─────────────────────────────────────────────
TD Payments  (member pay)       AWARDS register        PAYMENTS register
Election Finance (donations,    └─ Procurement         ├─ Public Payments
   party + candidate spend)        (eTenders/TED)       └─ Follow the Money (graph nav)
                                                         └─ Accommodation Spend (subset)
                                SELLER ENTITY NODE
                                └─ Companies (org-360, spans ALL of the above)
```

The honest boundary is **awards vs payments**. Procurement (awards) and Public Payments
(payments) stay distinct. Follow-the-Money and Accommodation both live in the **payments**
keyspace → they fold *into Public Payments*, not Procurement. That is what makes the
consolidation semantically correct rather than arbitrary — and what keeps it never-sum-safe.

## 6. Recommended target IA — 7 → 5 visible

| # | Nav label | Change | Notes |
|---|-----------|--------|-------|
| 1 | **TD Payments** | **renamed** from "Payments" ✅ | url_path `rankings-payments` unchanged |
| 2 | **Election Finance** | unchanged | — |
| 3 | **Procurement** | unchanged (stays top-level flagship) | awards register; most cross-linked node |
| 4 | **Public Payments** | becomes a small **hub** | absorbs Follow the Money ("Trace a payment") + Accommodation Spend ("Featured"); both routes stay hidden-but-routable |
| 5 | **Companies** | keep (**already last** — no reorder) + **role-clarity** + fix the payments-only dead-end (§9) | canonical seller node; target of supplier cards on both registers |

Result: **TD Payments · Election Finance · Procurement · Public Payments · Companies.**
Register boundary preserved; both pain points addressed; matches the Your-Council precedent.

### Two owner toggles beyond the default
- **7 → 4 "Public Spending" super-hub** (fold Procurement *and* Public Payments into one).
  **Rejected as default** — Procurement is the most cross-linked node (buyer `?authority=`,
  supplier cards); nesting it adds a click and *blurs the never-sum boundary*.
- **Companies 5 → 4** by demoting Companies to hidden-but-routable. **Not the default** —
  browse-every-firm is a genuine discovery path; the higher-confidence fix for "duplicate"
  is role-clarity, not removal. (Its 6 inbound edges survive either way — see §8.0.)

## 7. Companies de-duplication workstream (answers pain point C)

Independent of the nav count. This is the *already-designed, not-yet-built* half of
doc/archive/FOLLOW_THE_MONEY_IA_EXPLORATION.md:

1. **Role-clarity copy** on all three supplier surfaces so each states its question:
   - Companies → *"Everything one firm touches — awards, payments, lobbying, corporate."*
   - Procurement Suppliers tab → *"League table of awarded suppliers."*
   - Follow-the-Money node → *"Trace exactly who paid this firm, line by line."*
2. **CRO-gated bidirectional cross-link** Companies ⇄ payments/FtM: where a CRO match exists
   (the clean ~14%/~37% subset), render a "See payments to this firm" / "See this firm's full
   profile" link; where it does **not**, render **no link** (never a dead end, never a false
   hand-off). This *closes a currently one-way/absent loop* — a graph improvement.
3. **Plumbing (revised after review — cheaper).** No new view is required for the default. The
   dossier already joins the registers by name-norm string, so **gate each link on a live
   lookup**: render Companies→payments only when `fetch_payments_for_supplier_result(supplier_norm)`
   is non-empty, and render the *existing* PP→`/company` CTAs (public_payments.py L444, L720)
   only when the supplier resolves on the awards summary — match-or-no-link, never a false
   hand-off. Also fix the stale "degrades gracefully" comment at public_payments.py L437–440
   (it doesn't — it dead-ends, §9). A CRO lookup view stays available as an *optional*
   enhancement to recover name-mismatch cases, not a prerequisite.

## 8. Impact analysis (deep, code-traced + independently verified)

All claims traced to current code (2026-07-08) and re-verified by a second read-only sweep.

### 8.0 The inbound-edge asymmetry (why these two pages are the right ones to fold)

Every in-app navigation edge *into* each Money page:

| Page | Inbound entity-link edges (in-app) | Reached today by | Verdict |
|------|-----------------------------------|------------------|---------|
| **Companies** (`/company`) | **~6** — `company_profile_url()` from procurement.py L214, L1784, L2452, L3758 + public_payments.py L444, L720 | supplier cards on **both** registers + nav + URL | convergence node — keep; robust even if demoted |
| **Public Payments** (`rankings-public-payments`) | its own `?publisher=`/`?supplier=` drills | nav + URL | becomes the hub spine |
| **TD Payments** (`rankings-payments`) | `PAGES["payments"]` (defined but **unused**) | nav + `?member=` legacy redirect + URL | rename only |
| **Follow the Money** (`follow-the-money`) | **0** — the only refs are its own file, the nav entry, a *comment* at procurement.py:1778, and audit scripts. **No page builds a `/follow-the-money` link.** | **nav item ONLY**, or a typed URL | zero edges → nav-dependent |
| **Accommodation Spend** (`accommodation-spend`) | **0** — no `entity_links` helper, reads no query params | **nav item ONLY**, or URL | pure leaf |

**Implication:** Follow the Money and Accommodation are the *only* two Money pages with zero
inbound entity edges — wholly dependent on the nav slot for discovery. That is exactly why
(a) they are the correct pages to remove from the menu (removing Companies would sever 6 live
edges; removing these severs none), and (b) **their Public Payments entry cards are
load-bearing, not decorative**. Folding Follow the Money into the payments-register browse
*raises* its discoverability above today's single nav slot.

*Architectural note:* the whole app navigates via `<a href>` only — **zero** `st.page_link` /
`st.switch_page` calls exist (the only hit is the "why we avoid switch_page" docstring at
entity_links.py L7). So the entry cards are idiomatic and there is no hidden nav mechanism.

### 8.1 Per-change impact

| Change | Files (traced) | Route/link effect | Risk |
|--------|----------------|-------------------|------|
| Rename Payments→TD Payments **(done)** | app.py L240 title string | url_path + `PAGES["payments"]` unchanged (latter unused) | ~zero |
| Hide Follow-the-Money | app.py L259–264 `visibility="hidden"` | route stays; **must** pair with a PP entry card (0 other edges) | low |
| Hide Accommodation | app.py L265–270 `visibility="hidden"` | route stays; pair with a PP "Featured" card | low |
| PP becomes hub | public_payments.py — insert 2 cards between the glossary strip (L793) and `st.tabs` (L805); pure `<a href>` | no url_path change; adds 2 inbound edges | low |
| Companies role-clarity + CRO cross-link | copy in company.py, procurement.py, follow_the_money.py + 1 lookup view + 1 `data_access` wrapper | new CRO-gated edges both ways | med |
| ~~Reposition Companies last~~ | — | **no-op — Companies is already last** (app.py L280, after Public Payments L271) | — |

### 8.2 Route & deep-link integrity

- **url_paths are frozen.** Hiding is `visibility="hidden"`, never a slug change. So all 6
  Companies edges, the API route `/v1/housing/accommodation-spend` (api/routers/housing.py L34),
  external bookmarks, and the URL-based audit sweeps keep resolving.
- **An existing CI lint enforces the whole contract.** `test/utility/test_internal_link_slugs.py`
  builds the set of registered slugs by regex-matching `url_path=` in app.py **regardless of
  `visibility`**, then asserts every hand-rolled `href="/slug"` and every `PAGES` value resolves
  to it. So (a) `visibility="hidden"` keeps the slug registered → hiding is **safe by
  construction**; (b) the new cards `href="/follow-the-money"` / `href="/accommodation-spend"`
  **pass this lint** and it would **catch** any card pointing at a deleted/unknown slug.
- **No `entity_links.PAGES` edit needed.** `follow`/`accommodation` keys don't exist (only
  `"company": "company"`, L83). `PAGES["payments"]` (L73) is defined but **unused**.
- **Entry-card targets** (param schemes differ — must be exact):
  - Generic "Trace a payment" → `/follow-the-money` (clean landing; `follow_the_money_page`
    pops `mf_trail` on a param-less landing, L544). Canonical pre-seeded shape (from FtM's own
    featured card, L489): `/follow-the-money?paid_publisher=<name>&paid_tier=SPENT`
    (`paid_tier` ∈ {`SPENT`,`COMMITTED`}).
  - Per-supplier "trace this firm's payments" from a PP supplier profile →
    `/follow-the-money?flow_supplier_lines=<supplier_normalised>`. **PP and FtM share the
    `supplier_normalised` key** (PP L720; FtM L356/L556), so the hand-off resolves 1:1.
    (FtM's real forward keys are `flow_group` and `flow_supplier_lines` — there is no
    `?flow_supplier=`.)
  - Accommodation → bare `/accommodation-spend` (reads no params).

### 8.3 Data-honesty impact (never-sum) — the grouping *protects* it

Folding both satellites **into Public Payments (payments register)** keeps the hub
single-register: PP, Follow the Money and Accommodation are all payments-side. So the hub
introduces **no awards↔payments adjacency** and no new summing temptation. Folding into
Procurement (awards) would have mixed the two never-summable grains. The existing PP caveat —
*"a different register from eTenders / TED … never added to them"* (public_payments.py
L774–781) — already governs the whole hub. **Net honesty risk: none added; boundary reinforced.**

### 8.4 Code & firewall impact

- **Phase 1 is nav + two anchor tags.** The cards are `<a href>` with no groupby/merge/parquet,
  so `tools/check_streamlit_logic_firewall.py` stays green on the modified `public_payments.py`.
  No `data_access` change in Phase 1.
- **Phase 2** adds static role-clarity strings (firewall-safe) plus the CRO cross-link, whose
  join lives in a **view + a `data_access` wrapper**, never in a page. Keys to co-expose:
  `supplier_norm` ↔ `supplier_normalised` ↔ `company_num`.

### 8.5 Session-state & behavioural impact

- Follow the Money owns `st.session_state["mf_trail"]` (L190–207, reset on landing L544). In
  **Phase 1** the hub card is a real cross-page navigation to `/follow-the-money`, a separate
  `st.Page` render subtree — so `mf_trail` neither leaks into nor collides with Public Payments
  state.
- **SPA soft-nav caveat:** same-page `?param` clicks are soft-navigated; the cross-page
  `/follow-the-money?…` card link is a real navigation (intended, required to swap subtrees).
- **Phase 3 only:** embedding the FtM trail *inside* the PP page would put `mf_trail` + PP
  state on one page → needs key-namespacing + reset-on-section-switch. Main reason Phase 3 is
  rated higher-risk and kept optional.

### 8.6 Test & verification impact

- **No CI test breaks; one CI test actively protects the change.**
  - `test/utility/test_internal_link_slugs.py` — the slug lint (see §8.2). Visibility-blind →
    permits the hidden routes + new cards; catches a broken href.
  - `test/utility/test_link_reachability.py` — data-contract tests (`company_profile_url` L81–85,
    `paid_supplier_drill`, `paid_publisher_drill`). Nav-insensitive → pass.
  - `test/api/test_api_new_domains.py:74` **and** `test/sql_views/test_sql_views.py:3029`
    (`test_accommodation_spend_views_build`) — the "accommodation" tests; both nav-insensitive
    (API caveat + SQL-view build) → unaffected.
- **Coverage gap (not a regression):** there is **no** page-smoke test for either removed page.
  Add a minimal `test_follow_the_money_page.py` / `test_accommodation_spend_page.py` during
  Phase 1; the fresh-server Playwright pass is otherwise the only net.
- **Stale artefacts (non-CI, optional to refresh):** `_topnav_diagnose.py:78` label list (already
  lists "Interests"); `_dead_link_sweep.py` / `_dead_link_audit.py` hard-code the removed slugs
  as visible (still resolve, but no longer mirror the menu).
- **Per-phase gates:** firewall checker · page-smoke subset (`-m "not integration and not sql
  and not sources and not bronze"`) · the two CI link tests · fresh-server Playwright on the
  changed routes (watch process pileup / ~60–90s cold start) · nav-graph harness
  `audit_screenshots/_nav_graph_*.py` (asserts the `/company?supplier=` edge the change preserves).

### 8.7 Reach & platform impact

- **Mobile (corrected after review):** below 768px Streamlit **drops the top nav entirely**
  and the sidebar drawer is the only navigation (shared_css.py L205–211); on desktop the row
  budget is counted in **sections, not pages** ("all 11 sections fit on one row", L266–267).
  So hiding two pages *inside* "The Money" does **not** reduce horizontal overflow. The real
  benefit is **scannability** of the section's dropdown/drawer (7→5 shorter). **Cost to weigh:**
  hidden pages also disappear from the mobile drawer, so on phones the deep-placed PP entry
  cards become the *only* path to FtM/Accommodation → Phase 1 needs a mobile-viewport check that
  the cards are reachable without excessive scroll.
- **API / core untouched:** nav `visibility` is a pure Streamlit concern; `api/`,
  `dail_tracker_core/queries/housing.py`, `dossiers.housing_accommodation_spend` and all MCP
  tools are unaffected.

### 8.8 Explicitly NOT impacted (blast-radius bound)

Election Finance · Procurement's awards internals · every non-Money section · the data
pipeline / gold parquet / views (except the one *additive* Phase-2 lookup view) · MCP tools ·
the payments-graph params on procurement / council_spending / your_council (they stay in-page;
they never routed to `/follow-the-money`).

## 9. Circular-graph audit (does anything stop travelling?)

- **Follow the Money hidden:** top-nav is its *only* discovery path today (0 inbound entity
  edges, §8.0; procurement's apparent "cross-links" are same-page `?paid_*` soft-nav that stay
  on `/rankings-procurement`). After: reached from the PP "Trace a payment" card + the
  per-supplier `?flow_supplier_lines=` hand-off + direct URL. Route alive → **entity still
  travels, and discoverability net-improves.** ✓
- **Accommodation hidden:** terminal topic page, no entity drill-out to lose; reached from the
  PP "Featured" card + URL. ✓
- **Renames / reorder:** url_paths untouched → **all `entity_links` resolve.** ✓
- **Companies (kept) — CORRECTION (Fable):** the 6 inbound edges keep *routing*, but the
  **entity does not always resolve**. `/company` matches only the awards summary and renders
  "Company not found" (company.py L235–241) for a payments-only supplier — so the two
  PP→`/company` CTAs (L444, L720) already **dead-end for ~86% of paid suppliers**. This is a
  pre-existing *contextual* cul-de-sac (route resolves, entity doesn't) the earlier draft
  wrongly certified healthy, and Phase 1 — making PP the payments front door — **amplifies** it.
  **Required fix:** gate those existing CTAs match-or-no-link (§7, §12-D5) and add the
  Companies→payments outbound edge → the supplier↔ledger loop closes *without* new false
  hand-offs. ✓ only after gating.
- **If Companies demoted (toggle):** it keeps all 6 inbound edges (not orphaned), but its
  *browse/search* entry point is lost → add a "browse all companies" affordance to the Suppliers
  tabs first. Gated on that. ⚠️

**Net:** the plan **removes zero edges**, **adds** the Companies⇄payments loop, and **upgrades**
Follow the Money from one nav-only entry to a discoverable hub mode. Graph-positive on every axis.

## 10. Build phases & file manifest

- **Phase 0 — Labels ✅ SHIPPED.** Rename Payments→TD Payments. *Files:* `utility/app.py` L240.
- **Phase 1 — Nav collapse via entry cards (low risk).** `visibility="hidden"` on Follow-the-Money
  + Accommodation; add two entry cards to the Public Payments landing — **place them before the
  zero-publishers early return at L795–797** (inserting "between L793 and L805" would straddle it),
  so the cards render even in the degraded-data state; Companies is already last (no reorder).
  Achieves **7→5 visible** with *no deep code merge*.
  *Files:* `utility/app.py` (visibility + order); `utility/pages_code/public_payments.py` (2 cards);
  optional CSS in `utility/shared_css.py`; new `test/utility/test_follow_the_money_page.py` +
  `test_accommodation_spend_page.py` smoke tests.
- **Phase 2 — Companies de-duplication (medium).** Role-clarity copy on all three surfaces +
  the live-lookup-gated cross-links (§7). *Files:* `company.py`, `procurement.py`,
  `follow_the_money.py` (copy + gated links, incl. gating the *existing* PP CTAs); fix the stale
  "degrades gracefully" comment (public_payments.py L437–440) and the live `<\strong>` HTML bug
  in `_render_paid_supplier_panel` (procurement.py L1896) while there; optional CRO lookup view
  + `procurement_data.py` wrapper + a contract test.
- **Phase 3 (optional) — Deep embed.** Turn the PP entry cards into an in-page `segmented_control`
  [Browse · Trace a payment · Accommodation] (Your-Council Phase-4 style), pulling the FtM trail
  rail into the payments profiles (needs the §8.5 state-namespacing). Higher lift; only if zero
  extra clicks is wanted.

## 11. Verification plan (run per phase)

1. `python tools/check_streamlit_logic_firewall.py` — pages stay logic-free.
2. `.venv/Scripts/python -m pytest -q test/utility/test_internal_link_slugs.py test/utility/test_link_reachability.py`
   — slug + reachability contracts.
3. Page-smoke subset: `.venv/Scripts/python -m pytest -q -m "not integration and not sql and not sources and not bronze"`.
4. Fresh-server Playwright on `/rankings-payments`, `/rankings-public-payments`, `/follow-the-money`,
   `/accommodation-spend`, `/company` (restart Streamlit — cached conn won't hot-reload; ~60–90s
   cold start; watch process pileup).
5. Nav-graph harness `audit_screenshots/_nav_graph_*.py` — confirm no edge regressed, `/company?supplier=` intact.

## 12. Owner decisions (defaults chosen — approve or override)

| # | Decision | Default (recommended) | Alternatives | Your call |
|---|----------|-----------------------|--------------|-----------|
| 1 | Target depth | **7→5** (register-honest) | 7→4 super-hub · relabel-only (Phase 0 alone) | ☐ |
| 2 | Companies | **keep visible, reposition last, role-clarity** | demote to hidden-but-routable (needs browse affordance first) | ☐ |
| 3 | FtM + Accommodation depth | **Phase-1 entry cards first** | Phase-3 deep embed now | ☐ |
| 4 | "Public Payments" label | **keep** (established + `PAGES` key; "TD Payments" already disambiguates) | rename ("State Payments" / "Money Paid Out") | ☐ |
| 5 | Cross-link plumbing (**revised**) | **gate on the existing `fetch_payments_for_supplier_result` non-empty result** — self-proving against the exact FtM key, zero new views; also apply match-or-no-link to the *existing* PP→company CTAs (L444/L720) | add a CRO lookup view only as optional name-mismatch recovery | ☐ |
| 6 | **Procurement's "Who actually gets paid?" browse vs the PP hub** (same paid corpus in 3 places — full deep-dive §15) | **Option B — slim Procurement §Paid to a lifecycle *bridge* (compact summary + CTAs into PP + FtM); PP becomes the one full payments browse** | A: cross-link only · C: full view-family converge (defer) · D: remove §Paid (rejected) | ☐ |

A one-liner such as *"go with the defaults"* or *"5, keep Companies, entry cards"* is enough to start Phase 1.

## 13. Rejected alternatives

- **Merge/delete Companies into the supplier drills** — data-blocked (no shared key beyond CRO;
  ~14% resolution) and would break the awards-side supplier→`/company` edge (6 inbound edges).
- **Delete Follow the Money** — it is the only navigable payment graph; not a subset of anything.
- **One "Public Spending" total across Procurement + Public Payments** — violates the never-sum
  boundary; keeping them siblings is the point (awards ≠ payments).

## 14. Appendix — verified code anchors

- Nav block: `utility/app.py` L239–286 · rename shipped at L240.
- Companies inbound: `utility/pages_code/procurement.py` L214 (`_supplier_href`), L1784, L2452,
  L3758 · `utility/pages_code/public_payments.py` L444, L720.
- Companies node: `utility/ui/entity_links.py` L149 (`company_profile_url`), L83 (`PAGES["company"]`).
- Public Payments router/landing: `public_payments.py` L736 (`public_payments_page`), L739–748
  (params), L774–781 (never-sum caveat), L793 (glossary), L805 (`st.tabs`).
- Follow the Money: `follow_the_money.py` L539 (`follow_the_money_page`), L541–563 (router),
  L100–151 (`_current_node`), L190/193/207/544 (`mf_trail`), L489 (featured deep-link shape).
- Accommodation: `accommodation_spend.py` L195 (`accommodation_spend_page`), reads no params.
- CI guards: `test/utility/test_internal_link_slugs.py`, `test/utility/test_link_reachability.py`.
- Precedent: `utility/pages_code/your_council.py` (hub + segmented_control + `?yc=` + hidden-but-routable).

---

## 15. Decision 6 deep-dive — resolving the payments-browse duplication

*Planned 2026-07-08 after tracing all three surfaces. This is the resolution of §12 Decision 6 —
the item Fable surfaced and the one that most directly answers the owner's "duplicated" complaint.*

### 15.1 The duplication, precisely

One gold file — `data/gold/parquet/procurement_payments_fact.parquet` — is surfaced by **two
parallel SQL view families** and browsable in **three UI places**:

| Surface | Entry | Renderer | View family |
|---|---|---|---|
| **Procurement → "Who actually gets paid?"** | `/rankings-procurement?tab=paid` | `_render_payments()` (procurement.py L779): tier toggle + Top suppliers / Top bodies | `v_procurement_payments*` (procurement_payments.sql L46) |
| **Public Payments page** | `/rankings-public-payments` | tabs: Public bodies / Suppliers / What the money buys | `v_public_payments*` (procurement_public_payments.sql L39) |
| **Follow the Money landing** | `/follow-the-money` | **reuses** `_render_payments()` + trail rail + featured/search (follow_the_money.py L512) | (same as Procurement §Paid) |

Both view families read the **same parquet** (verified L39/L46). So Procurement §Paid and the whole
Public Payments page are **two near-identical browses of the same data**, and FtM's landing *is*
Procurement's §Paid browse plus a trail. That is the "same corpus in three places."

**NOT duplication — shared infra, keep:** the profile drills `_render_payments_publisher_profile` /
`_render_payments_supplier_profile` (procurement.py L1353/L1559) are reused by **four** pages
(procurement, follow_the_money, council_spending, your_council). Correctly shared; untouched.

**Why Public Payments is the right canonical home:** it has the "What the money buys" category lens
(`v_payments_by_category*`), the national-finance context strip, and the **privacy gate**
(`public_display` withholds likely-personal / sole-trader suppliers at the view boundary).
*Build check: confirm Procurement §Paid applies the same gate; if it's less gated, converging on PP
is also a privacy fix.*

### 15.2 Options

| Option | Move | Trade-off |
|---|---|---|
| **A — Cross-link only** | Add CTAs in Procurement §Paid → PP hub + FtM | Lowest risk; two full browses still remain |
| **B — Bridge (recommended)** | Slim Procurement §Paid to a *lifecycle bridge*: compact "what was actually paid" summary anchored to the awards story → deep-links into the PP hub (full browse) + FtM (trace). **One canonical payments browse (PP).** | Medium; preserves the awarded≠paid pivot; removes the duplicate browse |
| **C — Full converge (long-term)** | B + converge the two view families onto one shared browse component | Highest; a data-layer refactor; defer |
| **D — Remove Procurement §Paid** | Delete the section | Rejected — breaks the awards→paid lifecycle pivot inside the flagship |

### 15.3 Recommended: B, sequenced after Phase 1

The awarded-ceiling→actually-paid juxtaposition is core to the project's thesis and belongs *in*
Procurement, next to "Who wins contracts?". But it needn't be a *full* browse there — the full
suppliers/bodies browse should live in one place, and the plan already makes **Public Payments that
place**. So Procurement keeps the *story* (a compact bridge) and defers the *browse* to PP.

**Precise mechanic (safe — doesn't touch shared infra):**
- Procurement §Paid stops calling the full `_render_payments()` and calls a **new compact
  `_render_payments_bridge()`**: the existing corpus caveat (L789–799, kept — it carries the
  never-sum boundary) + a short "top few" teaser + two CTAs → `/rankings-public-payments`
  ("Browse the full payments register") and `/follow-the-money` ("Trace a payment chain").
- **FtM keeps calling the full `_render_payments()`** — its landing is unchanged; nothing breaks.
- The **shared profile drills stay** — council_spending, your_council, FtM and Procurement drill-downs
  all keep working.
- Result: exactly one full payments *browse* (PP), one lifecycle *bridge* (Procurement §Paid), one
  *trace* tool (FtM) — three distinct jobs, zero duplicate browse.

### 15.4 Impact

- **Never-sum:** preserved — the "paid ≠ awarded, never summed" caveat stays on the bridge (L790–798).
- **Circular graph:** *improved* — Procurement §Paid → PP hub → FtM becomes a signposted flow; today
  §Paid is a parallel branch that never points at PP.
- **Blast radius:** only the `_render_payments()` call site at procurement.py L4278 changes; view
  families, drills, and every other consumer are untouched; FtM unchanged.
- **Firewall:** the bridge is caveat + teaser + links — no new page logic.
- **Data-layer (Option C) — INVESTIGATED 2026-07-12, CLOSED AS NO-GO.** Reading both families
  killed the "duplication" premise: they are **deliberately divergent and documented as such**.
  `v_public_payments` is the hard-gated public surface (`WHERE public_display = TRUE`,
  procurement_public_payments.sql L8–12: *"this is what distinguishes it from
  v_procurement_payments, the analyst feed that omits the gate"*); `v_procurement_payments`
  deliberately omits the gate and exposes a different column set (`realisation_tier`,
  `vat_status`, canonicalised `paid_status`, `po_number`, `cro_*` vs `amount_semantics`,
  `quarter`, `extraction_confidence`). Only 3 trivial guard lines are shared — far below the
  house bar for base extraction (project_sql_view_consolidation: correctness-critical shared
  logic, genuinely identical, never tidiness; same verdict as the rejected awards mega-base).
  The family already reuses internally (both summaries read `FROM v_procurement_payments`).
  A merged base would either break the privacy gate or silently equalize two surfaces designed
  to differ. **§15.1's "build check" answered:** Procurement §Paid is *deliberately* less
  view-gated, compensated at the UI layer (company-class-only clickability, preserved by the
  bridge teaser) — a documented posture divergence for the owner to be aware of, not a defect.

### 15.5 Sequencing & verification

1. **Phase 1 first** (establish PP as the hub) — the bridge's CTAs need the hub to exist.
2. **Then Phase 2.5 — the bridge.** Independently sequenceable from the Companies de-dup (Phase 2).
3. **Verify:** firewall checker · fresh-server Playwright that Procurement `?tab=paid` shows the bridge
   + both CTAs resolve · PP still renders the full browse · FtM landing unchanged · council_spending /
   your_council spending sections still render (shared-drill regression check).

---

*Cross-refs: doc/NAVIGATION_GRAPH.md · doc/archive/FOLLOW_THE_MONEY_IA_EXPLORATION.md ·
doc/ENTITY_CROSSWALK_ORG_DOSSIER_DESIGN.md · memory `project_money_nav_declutter`,
`project_council_pages_consolidation`, `feedback_entity_links_seamless_navigation`,
`project_follow_the_money_feature`, `project_navigation_graph_2026_06_20`.*
