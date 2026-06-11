# Procurement Nuggets — derived-signal audit (2026-06-11)

Every candidate signal below was validated with live DuckDB queries against the
committed parquets (queries preserved in `c:/tmp/proc_nuggets*.py`). Numbers are
real, from the corpus as of 2026-06-11. Ranking = story value × data coverage ×
implementation cost.

**Invariants respected throughout** (see `doc/PROCUREMENT_BUILD_PLAN.md` §4b and
`doc/DATA_MAP.md`): the three money grains — payments (realised), eTenders/TED
awards (ceilings), TED tender estimates — are never summed or compared as
totals; only `value_safe_to_sum` rows enter aggregates; overlap signals are
co-occurrence facts, never causal claims; every signal is a prompt to look,
never a verdict.

Corpus anchors: eTenders awards 59,435 rows (2013-01 → 2025-12, 0% null dates);
TED awards 13,230 (2024+ eForms); TED tenders 11,250; payments fact 167,190
lines (120,751 public-display).

---

## Tier 1 — shipped as views in this pass

### 1. Cross-register entity chain (the unified-profile backbone)
**View: `v_procurement_entity_chain`** — grain: one row per CRO `company_num`.

- 4,169 CRO-matched eTenders suppliers; 4,164 TED; 4,118 payments.
- **1,313 companies appear in both eTenders awards and payments; 668 chain
  across all three registers.**
- Chained entities hold **€4.35bn of the €19.22bn safe payments total (22.6%)**
  — the "follow the money" profile is viable for the heart of the corpus, not a
  fringe.
- Example (all public-register facts): John Sisk and Son (Holdings) Ltd — 28
  eTenders award rows, 6 TED awards, **€513m** traced payments. Roadstone Ltd —
  121 / 23 / €155.5m.
- Caveat: presence in one register and absence in another is **register
  coverage**, not money missing — 2,856 of 4,169 awarded CRO entities (68%)
  have no payment trace, mostly because their buyers don't publish payment
  lists (only ~7% of State spend is visible in the payments corpus).

### 2. Market entry is collapsing (new-entrant rate)
**View: `v_procurement_new_entrants`** — grain: one row per year.

- Share of eTenders awards going to first-time winners: 51.3% (2016) → 32.7%
  (2018) → 23.8% (2020) → **17.3% (2024)**. Monotonic decline.
- Caveat: left-censoring — the corpus starts 2013, so early years are inflated
  by definition (everyone is "new" in year one). The view flags years before
  2016 `is_left_censored`. The 2018→2024 halving is robust to this.
- Factual framing only: "a shrinking share of contract awards goes to
  first-time suppliers" — consistent with consolidation, framework
  centralisation, OR maturing data; never asserted as market failure.

### 3. Single-bid rate by sector (OpenTender's flagship KPI, by market)
**View: `v_procurement_competition_by_cpv`** — grain: one row per CPV division
(TED 2024+, the eForms bid-count window). THE RATE IS LOT-LEVEL: single-bid
lots / lots-with-a-bid-count, deduped to one row per notice — the notice-level
min-across-lots metric over-states single-bid on multi-lot notices (IT
services: 46.7% notice-level vs 26.6% lot-level) and is not used.
National lot-level baseline **22.8%** (1,773 / 7,773 bid-counted lots).

- Highest (≥100 lots): Recreation/Culture **41.9%**, Hotel/Catering 40.2%,
  Repair/Maintenance 37.1%, R&D 36.6%.
- Lowest: Construction **13.9%**, Architecture/Engineering 16.6%,
  Medical equipment 19.4%.
- The spread (13.9%→41.9%) is the story: competition health is a property of
  the *market*, not just the buyer. Complements the per-buyer
  `v_procurement_competition` (top buyers ≥40 lots: University of Galway
  73.9%, University of Limerick 51.1%, Donegal County Council 40.0%).
- Denominator honesty: lots without a reported bid count are excluded from the
  rate; lot totals shown alongside.

> 2026-06-11 follow-up, post-publication of the first draft of this doc:
> (a) the 539 `n_tenders_received = 0` rows were a real extractor bug
> (cancelled/no-bid lots polluting the notice min) — FIXED in
> `extractors/ted_ireland_extract.py` and the silver rebuilt from bronze;
> contract test green. (b) `v_procurement_competition` was found exposing an
> OLD notice-level schema while `dail_tracker_core/queries/procurement.py`
> (MCP + API) selects lot-level columns (`n_lots_with_bidcount`,
> `single_bid_lot_pct`) — a live runtime break; the view now ships the
> lot-level schema with notice dedup. All single-bid numbers in this doc are
> the corrected lot-level figures.

### 4. Incumbency streaks (repeat winners, year after year)
**View: `v_procurement_incumbency`** — grain: one row per supplier×authority
pair (company-class).

- 666 pairs have won in ≥4 distinct years; **207 in ≥6 years; max 12 of the 13
  corpus years** (Starrus Eco Holdings ↔ OGP, 70 awards across 12 years).
- Deloitte Ireland LLP ↔ OGP: 202 awards over 11 distinct years.
- Caveat: The Office of Government Procurement is a **central purchasing body**
  — an OGP "streak" means repeated central-framework success, not a bilateral
  buyer relationship. The view carries `authority_is_central_purchasing` so the
  UI can badge it.

### 5. Supplier dependency (one-buyer suppliers)
**View: `v_procurement_supplier_dependency`** — grain: one row per supplier
(≥5 awards, company-class).

- Of 1,405 suppliers with ≥5 awards, **224 take ≥80% of their awards from one
  authority; 109 take ≥95%**.
- Non-OGP examples: Global Rail Services Ltd — 53/58 awards (91%) from Irish
  Rail; S. Duffy Plant Hire — 49/54 (91%) Irish Rail; D.M Morris Ltd — 40/44
  (91%) Wicklow County Council.
- Same OGP caveat as #4 (Dell 110/111 via OGP is framework mechanics, not
  dependency in the risk sense) — same `authority_is_central_purchasing` flag.
- Factual framing: "X won N of its M awards from Y" — a structure fact;
  dependency is often a perfectly healthy specialist relationship.

### 6. Q4 ordering spike (year-end seasonality)
**View: `v_procurement_quarter_profile`** — grain: publisher × quarter
(COMMITTED tier).

- Q4 carries 21,008 COMMITTED lines / €2.54bn safe vs a €1.79bn average across
  Q1–Q3 — **a +41% year-end value spike**.
- Most Q4-skewed publishers (≥200 lines): Galway County 52.3% of lines in Q4,
  Clare 51.5%, Pobal 44.7%, Longford 42.9%, Marine Institute 41.8%.
- Framing: seasonality fact; "use-it-or-lose-it" is one known public-finance
  explanation but invoicing cycles and grant schedules are others — describe
  the shape, never the motive.

### 7. Sector breadth (who supplies the whole State)
**View: `v_procurement_supplier_sector_breadth`** — grain: one row per
payments-fact supplier (company-class).

- PFH Technology — paid by publishers in **10 sectors** (26 publishers),
  €149.9m safe; Deloitte LLP — 11 sectors / €97.3m; Vodafone — 11 sectors /
  €40.8m.
- 15 sectors exist in the corpus (local_government 57k lines → media_culture
  138). Breadth is the signal n_publishers alone hides: 20 councils is one
  sector; HSE + councils + justice + regulators is structural reach.

### 8. Framework → call-off linkage (the nesting, made visible)
**View: `v_procurement_call_off_links`** — grain: one row per call-off award row.

- 2,277 call-off rows, **all** carrying a `Parent Agreement ID`; 455 (20%)
  resolve to a parent framework notice inside the corpus.
- This is the only place the corpus lets us connect a framework ceiling to its
  downstream awards — exactly the "everything is nested and unclear" pain.
  Where the parent resolves, the UI can render "call-off under framework X
  (ceiling €Y)"; where it doesn't, "parent agreement not in published corpus"
  is itself an honest transparency fact.

---

## Tier 2 — validated, real signal, not shipped yet

- **Supplier velocity (fastest-growing paid suppliers).** Raw 2024→2025 SPENT
  growth surfaces real names (Kilsaran Roadsurfacing €0.8m→€12.1m; Kenny
  Civils €0.9m→€10.6m — road-programme construction). BUT the publisher set
  grew in 2025, so naive YoY mixes corpus expansion with real growth. A fair
  view must restrict both years to publishers present in both — doable, just
  needs the same-publisher-set denominator. Defer until that's built; never
  ship the naive version.
- **Estimated vs awarded delta (TED tender → TED award pairing).** 3,283
  buyer×CPV pairs join across the two notice types — enough for a v2 "awards
  that came in far above the estimate" feature, but pairing needs care
  (publication-number cross-references where present, else
  buyer+CPV+time-window heuristics with a confidence tier). TODO_V2.
- **Award-but-no-payment asymmetry.** 68% of CRO-matched awarded suppliers
  have no payments trace. Useful as profile copy ("payments registers cover
  only a slice of buyers"), already folded into `v_procurement_entity_chain`
  semantics; not a standalone view.

## Tier 3 — weak signal or copy-only (checked, numbers recorded)

- **Deadline pressure** (OCP red flag): median dispatch→deadline gap is 32
  days; only 6.1% of competitive tenders give <15 days. Ireland looks healthy
  here — worth one line of copy ("median 32 days to bid"), not a feature.
- **Threshold bunching**: the payments histogram cliff at €20k (5,200 lines in
  the €20–21k band vs 36 just below) is the *disclosure threshold*, i.e. a
  coverage fact for the completeness expander, not a splitting signal. A real
  splitting test would need sub-threshold data nobody publishes.
- **Round-number award values**: 50.3% of eTenders values are €10k-round,
  37.0% €100k-round — confirms award values are estimates/ceilings, not
  invoices. Feeds glossary/caveat copy ("award values are round-number
  ceilings"), not a view.
- **Market capture by authority×CPV**: only 1 of 282 markets with ≥10 awards
  has a single supplier winning ≥80% — too rare to carry a lens. The CPV
  duopoly cut (top-2 share: Cleaning 58.2%, Playground equipment 51.6%) is the
  better version — could fold a `top2_share_pct` column into `v_procurement_cpv_summary`
  later.
- **Description opacity, quantified**: 16.6% of public payment lines have an
  empty description, 14.8% are ≤12 characters, and the top "descriptions"
  include literal `y` (2,093 lines) and `no` (889). This *is* the user-felt
  line-item problem — it's a source-data limitation to disclose honestly per
  row, not something any view can conjure away.

## TODO_INGESTION (flagged, not faked)

- **CRO incorporation dates** → "company age at first award". Match table has
  `company_num` only; needs a CRO bulk-register join (the bulk register is
  already automated per source-health work — likely cheap).
- **Contract amendments/modifications** — not in the eTenders CSV at all; the
  classic cost-overrun red flag is impossible until a new source (e.g.
  eForms modification notices on TED) is ingested.
- **Lobbying-return periods next to award dates** — `procurement_lobbying_overlap.parquet`
  carries counts only, no dates; a timeline would need a re-join to the
  lobbying silver with `period` retained. TODO_PIPELINE (gold rebuild), not UI.
