# Public-Payments "Category Lens" — Design Spec (preserve precisely)

**Status:** design / not built (the supporting **gold views are already built & tested**; the UI is not).
**Created:** 2026-06-13.
**Owner intent:** "I love this idea — preserve it very precisely." This doc is the canonical spec.
**Companion:** `doc/archive/MONEY_FLOW_DATA_AUDIT.md` (the data backbone + the §0a build log that produced the views).

> One line: a **"What the money buys"** lens on the **Public Payments** page — browse public-body spend by
> the bodies' OWN published purpose (category) → drill to the named vendors that received it → out to each
> vendor's existing Company dossier. Modelled on **Tussell** (market-intelligence profiles over aggregate,
> imperfect data), **not** ProZorro (whose visuals depend on centralized, complete, high-quality data we
> do not have). Honesty about coverage/quality is a first-class UI element, not a footnote.

---

## 0. Why Tussell, explicitly NOT ProZorro

ProZorro / spending.gov.ua look great because their *substrate* is one centralized, mandatory,
machine-readable system with a single supplier ID and complete coverage. Their treemaps / "follow every
hryvnia" / live dashboards are honest **because the data underneath is complete and clean.**

Ours is the opposite and must wear it openly:
- ~60 publishers, voluntary formats, **PDF-parsed at varying quality**;
- **~85% of lines carry a published purpose** (the rest are "Uncategorised");
- truncated descriptions; **fragmented vendor identity** (Mosney ≠ Mosney Holidays — different/again missing CRO);
- conservative **over-quarantine** of suffix-less companies (De La Rue, An Post, BearingPoint hidden);
- **over-€20k only**; and **paid ≠ awarded ≠ budget** (different OCDS tiers, never summed).

→ Borrow **Tussell's pattern** (profiles with spend-over-time, top-N, category cut — tolerant of aggregate
imperfection). **Do NOT** borrow ProZorro's completeness-implying visuals (treemap, "total public spend"
headline, "follow every euro"). A slick dashboard over messy data would overclaim — the one thing this
project must never do.

---

## 1. Where it goes (IA placement — mapped to EXISTING surfaces, no new page)

The app already has four money pages, split by lifecycle tier (the "never sum across registers" rule):

| Nav page | url_path | Register | Tier |
|---|---|---|---|
| Payments | `rankings-payments` | TD/Senator salaries & expenses | money *to politicians* |
| Election 2024 | `rankings-election-spending` | SIPO donations / party finance | political finance |
| **Procurement** | `rankings-procurement` | eTenders / TED **contract awards** | **AWARDED** |
| **Public Payments** | `rankings-public-payments` | public-body **over-€20k paid/ordered** | **SPENT / COMMITTED** |
| _Company_ (hidden) | `company` | cross-register **per-firm dossier** (awards + payments + lobbying + CRO) | — |

**`spend_category` is a column on the PAYMENT fact**, so the lens lives on **Public Payments**, never on
Procurement (that's the *awards* register — different tier). The **Company** page already IS the Tussell
"supplier's whole book of business," so we reuse it rather than build a new supplier profile.

```
 TOP NAV:  … Payments | Election 2024 | Procurement | Public Payments | …
                                          (awarded)     (paid/ordered)
                                                            │
                                  [ Public bodies ] [ Suppliers ] [ What the money buys ]●  ← NEW tab
                                                            │
                                          ?category=…    ?supplier=…   ?publisher=…
                                                            │
                              vendor link in a category ───┘→ Company page (existing dossier
                                                                 + new "what they were paid for" block)
```

| New piece | Home surface |
|---|---|
| Browse by category (lens + coverage panel) | Public Payments → **new 3rd tab** "What the money buys" |
| Category profile (tier split, trend, top vendors, source) | Public Payments → **`?category=` route** |
| "What they were paid for" (a firm's category cut) | **Company** dossier (existing) — add one block |
| "Top categories" for a buyer | Public Payments **publisher profile** — add one block |

Nothing new on the Procurement page.

---

## 2. Wireframes (preserve precisely)

### 2.1 New tab — "What the money buys" (coverage honesty FIRST, not a footer)

```
 Public-Body Payments     [ Public bodies ] [ Suppliers ] [ What the money buys ]●
 ─────────────────────────────────────────────────────────────────────────────────
 ⓘ What this shows — and doesn't.  Over-€20k purchase orders & payments that 63
   public bodies have PUBLISHED. Not "all public spending": excludes sub-€20k,
   bodies that don't publish, and withheld personal-name rows. 85% of lines carry
   a published purpose; the rest show as "Uncategorised". Categories are the
   bodies' OWN words, de-noised — not a standard taxonomy.        [ Download CSV ]

 Where the published money goes        Tier: (Paid ▾)        Sort: (Value ▾)
   School Building Projects        ██████████████████████  €2.13bn  (1 body)
   IP / Asylum Accommodation       ████████                €578m    (1 body)
   IRCG Helicopter Service (SAR)   ███                     €228m    (1 body)
   Pandemic Vaccine                ███                     €213m    (1 body)
   Uncategorised (purpose not pub.)██████████              €2.89bn  (14 bodies)  ⚠
   …                                                       [ show all 17,975 ▸ ]

 ┌── ranked category cards (click → profile) ──────────────────────────────┐
 │ 1  School Building Projects   1,575 lines·1 body·240 vendors [€2.13bn paid]│
 │ 2  IP / Asylum Accommodation  2,927 lines·1 body·310 vendors [€578m ordered]│
 └──────────────────────────────────────────────────────────────────────────┘
```

Ranked **horizontal bars** for the overview (native Streamlit/Altair — NO treemap, NO new dependency).

### 2.2 Category profile (`?category=`)

```
 ← All categories
 IP / Asylum Accommodation                    Dept of Justice · 2018–2026
 ┌─ Ordered (committed) ─┐   ┌─ Paid (actual) ─┐      ← tiers NEVER blended
 │  €578m / 2,927 lines  │   │  €— (none)       │
 └───────────────────────┘   └──────────────────┘

 Over time            ordered █                              [ tier-split bars by year ]
   2022 ▏███   2023 ▏█████   2024 ▏████████   2025 ▏██████   2026 ▏██

 Top vendors (paid/ordered to)                          [ ranked by value ▾ ]
   Cape Wrath Hotel        €34m ordered · 24 lines · CRO 0xxxx   [source ▸]
   Mosney                  €24m ordered ·  7 lines · CRO 11917   [source ▸]
   Bridgestock Care        €19m ordered · 45 lines · CRO 587776  [source ▸]
   …  → each vendor links to its Company dossier
 ⚠ Vendors shown as published — not merged. "Mosney" & "Mosney Holidays" appear
   separately; we can't verify they're one company.        [ Download CSV ]
 ⓘ This is ordered/paid history — not a contract price or a "going rate".
   For what's tendered/awarded → see eTenders contracts in this category ▸
```

### 2.3 Company dossier — add one block ("book of business" already exists here)

```
 BearingPoint                                  (existing dossier: awards + payments + lobbying + CRO)
 …existing sections…
 ── What they were paid for ──────────────  ── Paid/ordered over time ──────────
   IM&T Support        €40m ordered          2022 ▏██  2023 ▏███  2024 ▏████
   Consultancy         €11m ordered          (tier-split bars)
```

### 2.4 Public Payments publisher profile — add "Top categories" + over-time (same components).

---

## 3. Data views (firewall-clean; pipeline owns all aggregation)

**Already built & tested** (`sql_views/procurement/procurement_payments_by_category.sql`):
- `v_payments_by_category` — category × tier (the overview + cards)
- `v_payments_by_category_publisher` — publisher × category × tier (buyer block)
- `v_payments_category_suppliers` — category × supplier × tier, **cro_company_num surfaced, NOT merged** (the drill)

**To add** (same file, same rails — `WHERE value_safe_to_sum AND public_display`, `GROUP BY … realisation_tier`):
- `v_payments_category_by_year` — category × year × tier (category-profile trend)
- `v_payments_supplier_categories` — supplier × category × tier (Company "what they were paid for")
- `v_payments_supplier_by_year`, `v_payments_publisher_by_year` — profile trends

`spend_category` itself is a **pipeline-owned column** on the gold payment fact, derived by
`canon_spend_category()` in `extractors/procurement_payments_consolidate.py` — the publisher's own
`description`, canonicalised ONLY for truncation + casing ("department's exact words"). UI must NOT re-derive it.

---

## 4. Code changes (file by file) — SMALL footprint

**1. `sql_views/procurement/procurement_payments_by_category.sql`** — append the 4 new views (§3).

**2. `utility/data_access/public_payments_data.py`**
- `get_public_payments_conn()` → register `["procurement_public_payments.sql", "procurement_payments_by_category.sql"]`
- add thin `@st.cache_data` wrappers (no modelling — mirror `fetch_supplier_summary_result`):
  `fetch_categories_result`, `fetch_category_profile_result`, `fetch_category_suppliers_result`,
  `fetch_category_by_year_result`, `fetch_supplier_categories_result`, `fetch_entity_by_year_result`,
  `fetch_category_coverage`.

**3. `utility/pages_code/public_payments.py`**
- `public_payments_page()`: `if params.get("category"): _render_category_profile(...); return` + 3rd tab.
- `_render_categories()` — coverage panel + ranked-bar overview (`st.bar_chart`/Altair) + card grid
  (reuse `_card`, `clickable_card_link`; new `_category_href`).
- `_render_category_profile()` — `back_button`, tier metric cards, `_render_trend()`, top-vendor cards
  (reuse `_supplier_href` → Company dossier), `card_sources_html`, `st.download_button`, caveat + awards cross-link.
- `_render_trend(df)` — tier-split bars by year; reused by all profiles.
- `_render_coverage_panel()` — from `fetch_category_coverage` + `fetch_coverage`.
- enrich `_render_publisher_profile()` with a "Top categories" block.

**4. `utility/pages_code/company.py`** — add a "What they were paid for" block (uses `v_payments_supplier_categories`).

**5. Charts** — native only (`st.bar_chart` / small Altair), per the `display-data` rules. No `st.dataframe`
on primary views. No new dependency.

**6. Tests** (`test/extractors/test_procurement_payments_fact.py`)
- new views: tier-only + **reconciliation** (category_by_year sums to category total; supplier_categories sums to supplier total).
- the existing logic-firewall checker already scans `public_payments.py` — keep it modelling-free.
- `civic-ui-review` pass at the end.

---

## 5. Honesty rails (non-negotiable, carried from the page's existing model)
- **One tier per section** — "paid €X" / "ordered €X", never a blended figure.
- **Sum-safe only**; **"Uncategorised" shown plainly** as its own bucket (≈15%), never hidden.
- **Vendor names NOT operator-merged** — caveat inline; CRO surfaced for optional downstream roll-up; true
  merge = deferred `dim_supplier` work.
- **Coverage/provenance panel is phase 1**, not an afterthought — scope copy ("what public bodies have
  *published* about over-€20k spend"), never "all public spending".
- **"Paid ≠ winnable price"** note; **awards = cross-link, never unioned** with payments.

## 6. Explicitly OUT of scope
- ProZorro-style treemap / "total public spend" headline / "follow every euro" framing.
- SME-share metric (no clean SME flag on the payment fact — would be inference).
- Vendor operator-merge in the UI (needs `dim_supplier`).

## 7. Suggested build order
1. The 4 new views + their reconciliation/tier tests (backbone, no UI risk).
2. Data-access wrappers + register the view file.
3. Categories tab + coverage panel + category profile (the core lens).
4. `_render_trend()` + Company "what they were paid for" + publisher "top categories".
5. CSV export + awards cross-link + `civic-ui-review`.

---

## 8. Bigger picture — the money-page IA is getting unwieldy (PARKED, do not block this)

Four money pages — **Payments** (PSA), **Election 2024**, **Procurement** (awarded), **Public Payments**
(paid) — plus the hidden **Company** dossier, is a lot for a citizen who just thinks *"where does my money
go."* They must already know AWARDED-vs-PAID-vs-PSA to land on the right page. The `archive/MONEY_FLOW_DATA_AUDIT.md`
§6 floated a single **"Follow the money" hub**: one entry that explains the tiers and routes to the right
register (by recipient / by body / by category / by place), with the three honest lanes (Contracts awarded /
Money paid out / Money to politics) that never share a total.

**Decision:** ship the Category Lens onto **Public Payments now** (it fits cleanly, small footprint), and
treat the **money-page redesign / "Follow the money" hub as a separate, larger effort** to scope later —
the Category Lens is designed to slot into that hub unchanged (it's just "the category entry point").
This spec is the precise artifact to carry into that redesign.
