# Procurement page redesign — design brief (2026-06-11)

Companion to `doc/PROCUREMENT_NUGGETS.md` (validated signals + 8 new views, all
registered and contract-checked). This brief is the IA checkpoint: agree the
structure here, then build. No code in this pass.

## 1. Diagnosis

The current page (`utility/pages_code/procurement.py`) is organised by
**register**: Tab "Who wins contracts?" → picker {National eTenders / EU TED /
Overlaps} → picker {supplier / authority / category} → card grid → drill-down.
That is the *publisher's* mental model. Consequences:

- 3 levels of nesting before any content.
- The same legal entity appears in up to **four disconnected places** (eTenders
  supplier card, TED winner card, lobbying-overlap card, payments supplier
  card) with no link between them — even though `v_procurement_entity_chain`
  now proves 1,313 companies bridge awards↔payments and 668 bridge all three.
- Users must know what "eTenders", "TED" and "purchase orders" are before they
  can ask "how much State money does John Sisk get?".
- Line items are thin: an award row shows a value with no plain-language
  statement of what *kind* of value it is; a payment row hides its description,
  quarter and VAT status.

Every successful real-world analogue organises by **entity and question**, not
register: USAspending (recipient/agency/award profiles), OpenTender.eu (buyer/
supplier profiles carrying integrity indicators), Tussell/Contracts Finder
(search-first, pipeline-first), ProZorro/Dozorro (factual red-flag feed that
links to underlying notices). Registers become provenance badges.

## 2. Target information architecture

```
Procurement
├─ HERO: search box (suppliers + authorities + CPV typeahead)
│        + 3–4 prose lede findings (from v_procurement_coverage_stats,
│          v_procurement_supplier_concentration, v_procurement_competition_by_cpv,
│          v_procurement_new_entrants)
├─ Lens 1  WHO GETS THE MONEY      (entity rankings — default lens)
├─ Lens 2  OPEN RIGHT NOW          (TED open tenders, deadline-sorted)
├─ Lens 3  PATTERNS                (factual signal feed → filtered lists)
├─ Profile A  SUPPLIER (?supplier= / ?company=)   ← THE PRODUCT
├─ Profile B  PUBLIC BODY (?authority= / ?paid_publisher= merged)
└─ Footer: glossary, data completeness, provenance (kept, consolidated)
```

Registers (eTenders / TED / payments / AFS) appear ONLY as per-row provenance
badges and one glossary entry.

### Lens 1 — Who gets the money
Three sub-lenses (suppliers / public bodies / categories) — KEEP the existing
summary views and card grids, but de-duplicate the entity across registers:
one supplier card shows "N awards (national + EU) · paid €X (where published)"
sourced from `v_procurement_supplier_summary` + `v_procurement_entity_chain`.
Grain numbers stay separate on the card (count vs paid floor); never one total.

### Lens 2 — Open right now
Promote `v_procurement_ted_tenders` out of the EU-register sub-toggle:
deadline-sorted cards, "still open" pill, buyer link to Profile B. This is the
single most *actionable* view for SMEs/journalists (Tussell's lesson). Copy
notes estimates are never summable.

### Lens 3 — Patterns (the new-views payoff; Dozorro lesson, no-inference rules)
A feed of factual signal cards, each linking to a filtered list:
- "Single-bid rates range 14% (construction) to 42% (recreation/culture)"
  → `v_procurement_competition_by_cpv`, drill to `v_procurement_competition`
    per buyer. (Lot-level rates, national baseline 22.8%.)
- "17% of 2024 awards went to first-time suppliers, down from 51% in 2016"
  → `v_procurement_new_entrants` (left-censored years de-emphasised).
- "207 supplier–buyer pairs have won in 6+ different years"
  → `v_procurement_incumbency` (central-purchasing badge ON).
- "224 suppliers won ≥80% of their awards from one buyer"
  → `v_procurement_supplier_dependency` (same badge).
- "Q4 carries 41% more ordering value than an average quarter"
  → `v_procurement_quarter_profile`.
- "PFH Technology is paid by bodies in 10 of 15 public sectors"
  → `v_procurement_supplier_sector_breadth` (collision guard: hide rows where
    a short generic name has high n_raw_variants).
Every card: one observable fact + its caveat line + a "see the list" link.
No composite scores, no rankings labelled "worst", no causal phrasing.

### Profile A — Supplier (the flagship)
One page per entity, merged across registers via CRO/`supplier_norm` keys.
Sections, each with its own provenance footer and register badge:
1. **Header**: name, CRO pill (status, company number), lobbying-register pill,
   charity pill — co-occurrence wording only.
2. **Contracts awarded (ceilings, not payments)** — eTenders history from
   `v_procurement_awards` + TED history from `v_procurement_ted_awards`, one
   chronological list, register badge per row. Each row: date, buyer, CPV in
   plain English, value + value_kind IN WORDS ("framework ceiling — not a
   payment" / "shared across lots"), call-off linkage where resolvable
   (`v_procurement_call_off_links`: "call-off under framework X").
3. **Payments received (where published)** — `v_procurement_payments` lines:
   description, quarter, tier toggle (SPENT/COMMITTED), VAT pill. Coverage
   sentence: "payments registers cover only a slice of public bodies — absence
   here is not absence of payment" (the 68% no-trace fact).
4. **Competition context** — this supplier's single-bid share vs the 17.5%
   national baseline (OpenTender-style indicator bar), TED subset only.
5. **Relationships** — top buyers (`v_procurement_incumbency` rows for this
   supplier), dependency share if ≥5 awards.
The grain separation note between sections 2 and 3 is one calm sentence
(USAspending's obligated-vs-outlaid framing), not a warning wall.

### Profile B — Public body
Mirror of A: what they buy (CPV mix), who wins (top suppliers + top-supplier
share), competition health (`v_procurement_competition` row + procedure mix),
quarterly ordering shape (`v_procurement_quarter_profile`), payments published
(+ AFS coverage context for councils — keep the existing AFS section), open
tenders by this buyer. Merges the current separate `?authority=` (awards) and
`?paid_publisher=` (payments) drill-downs into one entity page with the same
grain-section pattern.

## 3. View dependencies

Existing, reused: v_procurement_supplier_summary, v_procurement_authority_summary,
v_procurement_cpv_summary, v_procurement_awards, v_procurement_ted_awards,
v_procurement_ted_supplier_summary, v_procurement_ted_tenders,
v_procurement_competition, v_procurement_payments,
v_procurement_payments_supplier_summary, v_public_payments_publisher_summary,
v_procurement_afs_* (3), v_procurement_coverage_stats,
v_procurement_supplier_concentration, v_procurement_lobbying_overlap,
v_procurement_charity_overlap, year-summary views.

New (shipped 2026-06-11, registered via the `procurement_*.sql` glob):
v_procurement_entity_chain, v_procurement_new_entrants,
v_procurement_competition_by_cpv, v_procurement_incumbency,
v_procurement_supplier_dependency, v_procurement_quarter_profile,
v_procurement_supplier_sector_breadth, v_procurement_call_off_links.

### TODO_PIPELINE_VIEW_REQUIRED (do not compute in UI)
1. ~~**v_procurement_entity_search**~~ — SHIPPED 2026-06-11
   (`procurement_zz_entity_search.sql`; zz_ prefix for dependency order).
   Columns: entity_kind, display_name, url_key, n_records, n_counterparties,
   awarded_value_safe_eur, paid_safe_eur (CRO-joined, suppliers only),
   cro_company_num, on_lobbying_register.
2. ~~**v_procurement_supplier_single_bid**~~ — SHIPPED 2026-06-11. Lot-level,
   sole-winner notices only (multi-winner notices excluded and counted, since
   notice-level lot counts can't be attributed to one winner).
3. **v_procurement_supplier_velocity** — YoY paid growth restricted to
   publishers present in BOTH years (see NUGGETS Tier 2; naive version is
   forbidden — corpus expansion masquerades as growth).
4. **Lobbying timeline per supplier** — needs `period` retained in
   procurement_lobbying_overlap gold (currently counts only). Gold rebuild.
5. **eTenders notice URL** — awards rows have no source-notice link
   (TED rows have notice_url). TODO_INGESTION: carry a notice URL or
   reconstruct from Tender ID if eTenders exposes a stable pattern; never
   fabricate links.

### Known data bug — FIXED 2026-06-11
~~539 TED-silver rows have n_tenders_received = 0~~ — extractor fixed
(cancelled/no-bid lots no longer pollute the notice min), silver rebuilt from
bronze, contract test green. Bonus find fixed in the same pass:
`v_procurement_competition` exposed an old notice-level schema while the core
query layer (MCP + API) selects lot-level columns — the view now ships the
lot-level schema with notice dedup, restoring the MCP `procurement_competition`
tool and `/procurement/competition` API route.

## 4. Deep-link compatibility

Preserve and redirect: `?supplier=<norm>` → Profile A; `?authority=<name>` and
`?paid_publisher=<name>&paid_tier=` → Profile B (tier becomes a section toggle);
`?cpv=<code>` → category list filtered. No inbound link may 404 or blank.

## 5. What the old page buried — fixed

1. Same entity split across 4 surfaces → one Profile A (chain coverage:
   €4.35bn / 22.6% of safe payments value is chained).
2. Register-first nav → question-first lenses; registers = badges.
3. value_kind invisible on award rows → stated in words per row.
4. Payment rows hid description/quarter/VAT → shown; empty descriptions shown
   AS "no description published" (16.6% of lines — honesty, not padding).
5. TED open tenders buried two pickers deep → top-level lens.
6. Competition signals confined to a strip → Patterns lens + profile indicator.
7. Framework nesting unexplained → call-off rows name their parent framework
   where resolvable (455 rows), say "parent not in published corpus" otherwise.
8. OGP central-buyer mechanics conflated with bilateral relationships → badge.

## 6. Build order

1. Pipeline fixes: TED bidcount bug; entity-search + single-bid views (TODOs
   1–2). 2. Profile A on top of entity_chain (biggest payoff, no IA breakage —
   it's a richer drill-down behind existing supplier links). 3. Lens
   restructure (hero search + 3 lenses, redirects). 4. Patterns lens. 5.
   Profile B merge. Each step ships independently; `/civic-ui-review` after
   steps 2–5. House rules apply throughout: cards primary, st.dataframe only in
   drill-downs, shared_css.py/components.py helpers, #ffffff card surfaces,
   quiet H1 + prose dek hero, provenance footers, year pills, 390px check.
