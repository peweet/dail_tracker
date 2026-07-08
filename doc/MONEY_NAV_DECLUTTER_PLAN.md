# "The Money" — Navigation Declutter Plan

**Status:** proposal for owner review · **Author:** Claude · **Date:** 2026-07-08
**Scope:** IA / navigation only. No pipeline changes, no new business logic in pages.
**Governing rule:** the composable navigation graph — *every named entity is a
clickable door that carries the entity to its canonical page, both directions where
related* (doc/NAVIGATION_GRAPH.md). Nothing here may create a contextual cul-de-sac.

---

## 1. Problem

The top-nav section **"The Money"** holds **7 visible pages** — nearly double every
other section (What They Own 1 · Your Area 3 · Members & Parliament 4 · Law & Records
4 · Influence 3). It reads as an undifferentiated pile, and two of the seven are the
owner's stated pain points.

Current contents (`utility/app.py` nav block, ~L239–286):

| # | Nav label | url_path | What it actually is | Register / keyspace |
|---|-----------|----------|---------------------|---------------------|
| 1 | Payments | `rankings-payments` | **TD** payments — money politicians *receive* (expenses/allowances) | member pay |
| 2 | Election Finance | `rankings-election-spending` | GE2024 hub (donations + party + candidate spend) | political money in |
| 3 | Procurement | `rankings-procurement` | eTenders/TED **awards** + suppliers/authorities/categories/lobbying tabs | **awards** |
| 4 | Follow the Money | `follow-the-money` | Trail nav over the **payment graph** (`?paid_supplier=`/`?paid_publisher=`) | **payments** |
| 5 | Accommodation Spend | `accommodation-spend` | Neutral IPAS/Ukraine accommodation deep-dive | payments (subset) |
| 6 | Public Payments | `rankings-public-payments` | Public-body PO/payment disclosures >€20k (publishers/suppliers) | **payments** |
| 7 | Companies | `company` | Org-360 dossier — one firm across *all* registers | seller entity node |

## 2. Diagnosis — three distinct problems

**(A) Label collision.** "Payments" is **TD** payments (money *in* to politicians);
"Public Payments" is **State** spend (money *out* to suppliers). Side by side the two
labels are indistinguishable and describe opposite money flows. This alone makes the
section feel muddled.

**(B) Register mixing.** The seven blend two unrelated stories —
*money to the political class* (TD Payments, Election Finance) and *money the State
pays out* (Procurement, Follow the Money, Accommodation, Public Payments, Companies) —
and within the second story they mix two **never-summable registers**: **awards**
(Procurement: eTenders/TED ceilings) vs **payments** (Public Payments + Follow the
Money: disclosed PO/payment lines). Keeping these visually adjacent without signalling
the boundary is both clutter *and* an honesty risk (see `caveats.PROCUREMENT_AWARDS`,
3-grain never-sum rule).

**(C) "Companies is a duplicate view."** The owner's remark is real *as a perception*,
but a merge was already investigated and **rejected on data grounds**
(doc/archive/FOLLOW_THE_MONEY_IA_EXPLORATION.md). There are **three supplier surfaces**:

| Surface | URL / key | What it uniquely gives |
|---------|-----------|------------------------|
| **Companies** | `/company?supplier=` · awards-side `supplier_norm` | Full org-360 footprint: awards + TED + payments *summary* + lobbying + corporate + charity + EPA, one firm one URL |
| **Procurement → Suppliers tab** | `/rankings-procurement` · `supplier_norm` | Ranked awards leaderboard (browse/discover) |
| **Follow-the-Money supplier node** | `?paid_supplier=` · payments-side `supplier_normalised` | The **only navigable payment graph**: per-body ranked list → drill to ledger, SPENT/COMMITTED toggle |

Validated (do **not** recompute — numbers from the IA exploration):
- The two registers are **linked by no key except CRO**. Of **17,633** paid
  company-suppliers, **6,455** have a CRO number and only **2,405** are *also*
  eTenders-awarded. So a Companies↔payments hand-off resolves **~14%** one way, **~37%**
  the other. Key resolution is a clean 1:1 (0 ambiguous) where it exists.
- Companies is **not** a superset of the FtM node and vice-versa — each holds what the
  other lacks. **FtM must not be deleted; Companies must not be merged.**
- `procurement.py::_supplier_href` (L211–214) **already** routes awards-side supplier
  cards to `company_profile_url()` → `/company`. Companies is therefore the *canonical
  seller node* and a live entity-link target; deleting it would break that edge.

**Conclusion:** (C) is a **role-clarity + cross-link** problem, not a delete. The
"duplication" the owner feels is that three surfaces show "a supplier" without telling
the user which question each answers.

## 3. Design constraints (non-negotiable)

1. **Circular graph.** Every entity keeps travelling. Any page removed from the menu
   stays **hidden-but-routable** (the `_interests_redirect_page` / Your-Council Phase-4
   pattern) so `entity_links`, deep links and cross-page CTAs keep resolving. Verify on
   the entity-bearing **detail** state, not the landing.
2. **Route stability.** `entity_links.PAGES` (entity_links.py L67–92) maps logical keys
   → `url_path` slugs. **Keep every `url_path` unchanged** — only `visibility` and
   *where a page is reached from* may change. (This is exactly how
   `rankings-council-spending` kept resolving after the council collapse.)
3. **Never-sum.** Awards and payments stay as **separate** siblings/sections with their
   existing caveats; no UI element may present a blended awards+payments total.
4. **Logic firewall.** Hub landings render from existing renderers/registered contracts.
   Navigation cards are pure `<a href>` — no groupby/merge/parquet in pages. The one
   data-layer touch (CRO cross-link lookup) goes in a view, not a page.
5. **SPA soft-nav.** Same-page `?param` cards soft-navigate automatically
   (`ui/spa_links.py`); cross-page `/slug?…` links do real navigation (intended).

## 4. The keyspace map (drives the grouping)

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
(payments) must remain distinct. Follow-the-Money and Accommodation both live in the
**payments** keyspace → they fold *into Public Payments*, not into Procurement. That is
what makes the consolidation semantically correct rather than arbitrary.

## 5. Recommended target IA — 7 → 5 visible

| # | Nav label | Change | Absorbs / notes |
|---|-----------|--------|-----------------|
| 1 | **TD Payments** | **rename** from "Payments" | disambiguates from Public Payments; url_path `rankings-payments` unchanged |
| 2 | **Election Finance** | unchanged | — |
| 3 | **Procurement** | unchanged (stays top-level flagship) | awards register; most cross-linked node |
| 4 | **Public Payments** | becomes a small **hub** | absorbs **Follow the Money** ("Trace a payment") + **Accommodation Spend** ("Featured") as reached-from-here destinations; both routes stay alive |
| 5 | **Companies** | keep, reposition last + **role-clarity** | canonical seller node; target of Procurement supplier cards |

Result: **TD Payments · Election Finance · Procurement · Public Payments · Companies**
— down from 7, register boundary preserved, both pain points addressed. Matches the
Your-Council precedent (one register-spine hub, satellites reached from it, old pages
hidden-but-routable).

### Two owner toggles beyond the default
- **7 → 4 "Public Spending" super-hub** (fold Procurement *and* Public Payments into one
  hub with Awards/Payments sections). **Rejected as default** — Procurement is the most
  cross-linked node (buyer `?authority=`, supplier cards), nesting it adds a click and
  *blurs the never-sum boundary*. Offered only if the owner prioritises count over the
  awards/payments separation.
- **Companies → 5 → 4** by demoting Companies to hidden-but-routable (still reached from
  every supplier card + a "browse all companies" affordance added to the Suppliers
  tabs). **Not the default** — browse-every-firm is a genuine discovery path; the safer,
  higher-confidence fix for "duplicate" is role-clarity, not removal.

## 6. Companies de-duplication workstream (answers pain point C)

Independent of the nav count. This is the *already-designed, not-yet-built* half of
doc/archive/FOLLOW_THE_MONEY_IA_EXPLORATION.md:

1. **Role-clarity copy** on all three supplier surfaces so each states its question:
   - Companies → *"Everything one firm touches — awards, payments, lobbying, corporate."*
   - Procurement Suppliers tab → *"League table of awarded suppliers."*
   - Follow-the-Money node → *"Trace exactly who paid this firm, line by line."*
2. **CRO-gated bidirectional cross-link** Companies ⇄ payments/FtM: where a CRO match
   exists (the clean ~14%/~37% subset), render a "See payments to this firm" / "See this
   firm's full profile" link; where it does **not**, render **no link** (never a dead
   end, never a false hand-off). This *closes a currently one-way/absent loop* — a graph
   improvement, not just cleanup.
3. **Plumbing:** the cross-link needs the two URL keys (`supplier_norm`,
   `supplier_normalised`) exposed together. `v_procurement_entity_chain` is the bridge
   backbone but exposes only `company_num` + `display_name`. **Default:** add a tiny
   additive lookup view (display-plumbing, no re-baseline) rather than widening the
   chain view. This is the *only* data-layer change in the whole plan.

## 7. Impact analysis (deep, code-traced)

All claims below are traced to current code (2026-07-08), not assumed. The single most
important finding reframes the whole plan:

### 7.0 The inbound-edge asymmetry (why these two pages are the right ones to fold)

I mapped every in-app navigation edge *into* each Money page. The result is lopsided —
and it is the justification for the whole design:

| Page | Inbound entity-link edges (in-app) | Reached today by | Verdict |
|------|-----------------------------------|------------------|---------|
| **Companies** (`/company`) | **~6** — `company_profile_url()` called from procurement.py L214, L1784, L2452, L3758 + public_payments.py L444, L720 | supplier cards on **both** registers + nav + URL | convergence node — safe to keep, robust even if demoted |
| **Public Payments** (`rankings-public-payments`) | its own supplier drills; parent of the hub | nav + URL | becomes the hub spine |
| **TD Payments** (`rankings-payments`) | `entity_links.PAGES["payments"]` | nav + member cross-links + URL | rename only |
| **Follow the Money** (`follow-the-money`) | **0** — grep for the route across all `.py` finds only its own file, the nav entry, a *comment* at procurement.py:1778, and audit scripts. **No page builds a `/follow-the-money` link.** | **nav item ONLY**, or a hand-typed URL | zero entity edges → nav-dependent |
| **Accommodation Spend** (`accommodation-spend`) | **0** — no `entity_links` helper, page reads no query params | **nav item ONLY**, or URL | pure leaf |

**Implication:** Follow the Money and Accommodation are the *only* two Money pages with
zero inbound entity edges — they are wholly dependent on the nav slot for discovery.
That is exactly why (a) they are the correct pages to remove from the menu (removing a
well-connected node like Companies would sever 6 live edges; removing these severs none),
and (b) **their Public Payments entry cards are load-bearing, not decorative** — without
a card, each becomes URL-only. Folding Follow the Money into the payments-register browse
actually *raises* its discoverability above today's single nav slot.

### 7.1 Per-change impact

| Change | Files (traced) | Route/link effect | Risk |
|--------|----------------|-------------------|------|
| Rename Payments→TD Payments **(done)** | app.py L240 title string | url_path `rankings-payments` + `PAGES["payments"]` unchanged | ~zero |
| Hide Follow-the-Money | app.py L259–264 `visibility="hidden"` | route stays; **must** pair with a PP entry card (0 other edges) | low |
| Hide Accommodation | app.py L265–270 `visibility="hidden"` | route stays; pair with a PP "Featured" card | low |
| PP becomes hub | public_payments.py — insert 2 entry cards between the glossary strip (L793) and `st.tabs` (L805); pure `<a href>` | no url_path change; adds 2 inbound edges | low (Phase 1) |
| Companies role-clarity + CRO cross-link | copy in company.py, procurement.py, follow_the_money.py + 1 lookup view + 1 `data_access` wrapper | new CRO-gated edges both ways | med |
| Reposition Companies last | app.py order within "The Money" | unchanged | ~zero |

### 7.2 Route & deep-link integrity

- **url_paths are frozen.** Hiding is `visibility="hidden"`, never a slug change. So all 6
  Companies edges, the API route `/v1/housing/accommodation-spend` (api/routers/housing.py
  L34), external bookmarks, and `audit_screenshots/_dead_link_sweep.py` (which lists
  `follow-the-money`, `accommodation-spend`, `company` and visits them by URL) keep
  resolving.
- **An existing CI lint already enforces the whole contract.**
  `test/utility/test_internal_link_slugs.py` builds the set of "registered" slugs by
  regex-matching `url_path=` in app.py **regardless of `visibility`** (`_URLPATH_RE`), then
  asserts every hand-rolled `href="/slug"` in `pages_code/*.py` and every `entity_links.PAGES`
  value resolves to that set. Consequences, both load-bearing: (a) `visibility="hidden"`
  keeps the `url_path` line, so the slug stays registered and hiding is **safe by
  construction**; (b) the new "Trace a payment" / "Featured" cards — `href="/follow-the-money"`
  and `href="/accommodation-spend"` — **pass this lint precisely because both routes stay
  registered**, and the lint would **catch** any future card that points at a deleted/unknown
  slug. This is a firmer guarantee than the audit scripts; it is a real pytest test.
- **No `entity_links.PAGES` edit needed.** `PAGES` keys `follow`/`accommodation` do not
  exist (only `"company": "company"`, entity_links.py L83). Note also `PAGES["payments"]`
  (L73) is defined but **unused** — no `*_url()` helper builds it — so the TD-Payments
  rename touches nothing downstream.
- **Whole-app nav is `<a href>` only.** There are **zero** `st.page_link` / `st.switch_page`
  calls in the codebase (confirmed by the independent sweep; the only hit is the
  "why we avoid switch_page" docstring at entity_links.py L7). So the entry cards are
  idiomatic and there is no hidden nav mechanism to audit beyond hrefs + `st.Page` entries.
- **Entry-card targets** (param schemes differ, so this must be exact):
  - Generic "Trace a payment" → `/follow-the-money` (clean landing; `follow_the_money_page`
    pops `mf_trail` on a param-less landing, L544). The landing's *own* featured card shows
    the canonical deep-link shape `/follow-the-money?paid_publisher=<name>&paid_tier=SPENT`
    (L489; `paid_tier` ∈ {`SPENT`,`COMMITTED`}) if a pre-seeded body is wanted.
  - Per-supplier "trace this firm's payments" from a PP supplier profile →
    `/follow-the-money?flow_supplier_lines=<supplier_normalised>`. **PP and FtM share the
    `supplier_normalised` key** (PP L720 links supplier cards by it; FtM routes on
    `flow_supplier_lines`/`paid_supplier` = the same payments-side norm, L356/L556), so the
    hand-off resolves 1:1. (FtM's real forward keys are `flow_group` and
    `flow_supplier_lines` — there is no `?flow_supplier=`; don't invent one.)
  - Accommodation → bare `/accommodation-spend` (reads no params at all).

### 7.3 Data-honesty impact (never-sum) — the grouping *protects* it

Folding both satellites **into Public Payments (payments register)** keeps the hub
single-register: Public Payments, Follow the Money and Accommodation are all payments-side.
So the hub introduces **no awards↔payments adjacency** and therefore no new summing
temptation. Had the satellites been folded into Procurement (awards), the hub would have
mixed the two never-summable grains. The existing PP caveat — *"a different register from
eTenders / TED … never added to them"* (public_payments.py L774–781) — already governs
the whole hub. **Net honesty risk: none added; boundary reinforced.**

### 7.4 Code & firewall impact

- **Phase 1 is nav + two anchor tags.** The entry cards are `<a href>` navigation with no
  groupby / merge / parquet read, so `tools/check_streamlit_logic_firewall.py` stays green
  on the modified `public_payments.py`. No `data_access` change in Phase 1.
- **Phase 2** adds static role-clarity strings (firewall-safe) plus the CRO cross-link,
  whose join lives in a **view + a `data_access` wrapper**, never in a page. The keys to
  co-expose are `supplier_norm` (awards) ↔ `supplier_normalised` (payments) ↔ `company_num`;
  `v_procurement_entity_chain` is the backbone but surfaces only `company_num`+`display_name`
  → default is a small additive lookup view (no re-baseline of any gold output).

### 7.5 Session-state & behavioural impact

- Follow the Money owns `st.session_state["mf_trail"]` (L190–207, reset on landing L544).
  In **Phase 1** the hub card does a real cross-page navigation to `/follow-the-money`, a
  separate `st.Page` render subtree — so `mf_trail` neither leaks into nor collides with
  Public Payments state. Clean.
- **SPA soft-nav caveat:** same-page `?param` clicks are soft-navigated
  (`ui/spa_links.py`); the *cross-page* `/follow-the-money?…` card link is a real
  navigation (intended, and required to swap page subtrees). No behavioural surprise.
- **Phase 3 only:** embedding the FtM trail *inside* the PP page (segmented_control) would
  put `mf_trail` and PP's own state on one page — that needs deliberate key-namespacing and
  a reset-on-section-switch. This is the main reason Phase 3 is rated higher-risk and kept
  optional.

### 7.6 Test & verification impact

- **No CI test breaks, and one CI test actively protects the change.**
  - `test/utility/test_internal_link_slugs.py` — the slug lint (see §7.2). Visibility-blind,
    so it *permits* the hidden routes + the new hub cards and would *catch* a broken href.
  - `test/utility/test_link_reachability.py` — data-contract tests (`company_profile_url`
    L81–85, `paid_supplier_drill`, `paid_publisher_drill`). Nav-visibility-insensitive → pass.
  - `test/api/test_api_new_domains.py:74` — the only "accommodation" test; an API caveat
    test independent of the Streamlit nav.
- **Coverage gap to note (not a regression):** there is **no** page-smoke test for either
  removed page (no `test_follow_the_money_page.py` / `test_accommodation_spend_page.py`;
  smoke tests exist for votes/payments/procurement/public_payments/company/etc.). So the
  fresh-server Playwright pass on `/follow-the-money` and `/accommodation-spend` after
  Phase 1 is the real safety net — worth adding a minimal smoke test for each while here.
- **Stale artefacts (not blocking, non-CI):** `_topnav_diagnose.py:78` hard-codes a label
  list already stale (still lists "Interests"); `_dead_link_sweep.py` / `_dead_link_audit.py`
  hard-code the removed slugs as *visible* — they still resolve (routable) but no longer
  mirror the menu. Refresh optional.
- **Per-phase gates:** `tools/check_streamlit_logic_firewall.py` · page-smoke subset
  (`-m "not integration and not sql and not sources and not bronze"`) · the two CI link
  tests above · fresh-server Playwright on the changed routes (watch process pileup / ~60–90s
  cold start) · nav-graph harness `audit_screenshots/_nav_graph_*.py` (asserts the
  `/company?supplier=` edge the change preserves).

### 7.7 Reach & platform impact

- **Mobile:** the top-nav has a known <768px overflow failure mode (memory
  `topnav_mobile_breakpoint`). Cutting "The Money" 7→5 removes two of the widest labels and
  strictly *reduces* horizontal overflow — a real mobile win, not just desktop tidiness.
- **API / core untouched:** nav `visibility` is a pure Streamlit concern; `api/`,
  `dail_tracker_core/queries/housing.py`, `dossiers.housing_accommodation_spend` and all
  MCP tools are unaffected. Blast radius is the UI nav layer only.

### 7.8 Explicitly NOT impacted (blast-radius bound)

Election Finance · Procurement's awards internals · every non-Money section · the data
pipeline / gold parquet / views (except the one *additive* Phase-2 lookup view) · MCP
tools · the payments-graph params on procurement / council_spending / your_council (they
stay in-page; they never routed to `/follow-the-money`).

## 8. Circular-graph audit (does anything stop travelling?)

- **Follow the Money hidden:** *correction to the earlier draft* — it is **not** reached
  from procurement cross-links today; the top-nav is its *only* discovery path (0 inbound
  entity edges, §7.0). After: reached from the PP "Trace a payment" card + the per-supplier
  `?flow_supplier_lines=` hand-off + direct URL. Route alive → **entity still travels, and
  discoverability net-improves.** ✓
- **Accommodation hidden:** terminal topic page, no entity drill-out to lose; reached from
  the PP "Featured" card + URL. ✓
- **Renames / reorder:** url_paths untouched → **all `entity_links` resolve.** ✓
- **Companies (kept):** its 6 inbound edges are unchanged; it **gains** a CRO-gated outbound
  edge to the payment ledger → a currently absent supplier↔ledger loop **closes**. ✓
- **If Companies demoted (toggle):** unlike FtM/Accommodation it keeps all 6 inbound edges,
  so it is *not* orphaned — but its *browse/search* entry point is lost, so add a "browse
  all companies" affordance to the Suppliers tabs first. Gated on that. ⚠️

**Net:** the plan **removes zero edges**, **adds** the Companies⇄payments loop, and
**upgrades** Follow the Money from one nav-only entry to a discoverable hub mode. It is
graph-positive on every axis.

## 9. Build phases (each independently shippable & verifiable)

- **Phase 0 — Labels (1 file, minutes, ~zero risk).** Rename Payments→TD Payments. Ship
  alone; immediate clarity win.
- **Phase 1 — Nav collapse via entry cards (low risk).** `visibility="hidden"` on
  Follow-the-Money + Accommodation; add two entry cards to the Public Payments landing;
  reorder Companies last. Achieves **7→5 visible** with *no deep code merge* — the pages
  still render standalone, just reached from the hub (mirrors Your-Council Phase 1).
- **Phase 2 — Companies de-duplication (medium).** Role-clarity copy on all three
  surfaces + the CRO-gated cross-link + the lookup view. Answers pain point C.
- **Phase 3 (optional) — Deep embed.** Turn the Public Payments entry cards into an
  in-page `segmented_control` [Browse · Trace a payment · Accommodation] (Your-Council
  Phase-4 style), pulling the FtM trail rail into the payments profiles. Higher lift;
  only if the owner wants zero extra clicks.

**Verification each phase:** `python tools/check_streamlit_logic_firewall.py` · page-smoke
tests (`-m "not integration and not sql and not sources and not bronze"`) · fresh-server
Playwright on the changed routes (watch for process pileup) · the nav-graph harness
(`audit_screenshots/_nav_graph_*.py`) to confirm no edge regressed. Restart Streamlit to
pick up by-name imports / view DDL (cached conn won't hot-reload).

## 10. Owner decisions (defaults chosen — approve or override)

1. **Target depth** — *Default: 7→5* (register-honest). Alt: 7→4 super-hub (rejected),
   or relabel-only (Phase 0 alone).
2. **Companies** — *Default: keep visible, reposition last, add role-clarity.* Alt:
   demote to hidden-but-routable (needs the browse affordance first).
3. **FtM/Accommodation depth** — *Default: Phase 1 entry-cards first*, Phase 3 embed
   optional/later.
4. **"Public Payments" label** — *Default: keep* (established + `entity_links` key);
   "TD Payments" already disambiguates. Alt: rename to "State Payments" / "Money Paid Out".
5. **CRO cross-link plumbing** — *Default: tiny additive lookup view* (no re-baseline).
   Alt: +2 cols on `v_procurement_entity_chain`.

## 11. Rejected alternatives

- **Merge/delete Companies into the supplier drills** — data-blocked (no shared key
  beyond CRO; ~14% resolution) and would break the awards-side supplier→`/company` edge.
- **Delete Follow the Money** — it is the only navigable payment graph; not a subset of
  anything.
- **One "Public Spending" total across Procurement + Public Payments** — violates the
  never-sum boundary; the whole point of keeping them siblings is that awards ≠ payments.

---

*Cross-refs: doc/NAVIGATION_GRAPH.md · doc/archive/FOLLOW_THE_MONEY_IA_EXPLORATION.md ·
doc/ENTITY_CROSSWALK_ORG_DOSSIER_DESIGN.md · your_council.py (precedent) · memory
`project_money_nav_declutter`, `project_council_pages_consolidation`,
`feedback_entity_links_seamless_navigation`.*
