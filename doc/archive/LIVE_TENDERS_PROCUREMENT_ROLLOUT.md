# Rolling Live National Tenders into the Procurement Page — Plan

**Created:** 2026-06-14. **Status:** plan (no code). **Scope:** surface the national (sub-EU-threshold)
live tender pipeline we captured into the existing Procurement page, beside the TED data already there.
**Companion:** `doc/MONEY_FLOW_DATA_AUDIT.md` (tiers/honesty), `doc/MONEY_PAGES_SHAPE_BRIEF.md` (IA).

> **Verdict: no new tab, no new page.** The Procurement page already has the exact home — tab **"Open
> right now"** (`tabs[2]`), with two segments that are **TED-only today**:
> `"Open tenders"` → `_render_ted_tenders()` and `"Contract terms ending"` → `_render_expiring_contracts()`.
> The rollout = add the national eTenders data we built (`v_procurement_live_tenders`,
> `v_procurement_expiring_contracts_etenders`) **alongside** TED in those two segments. The PLANNED-tier
> framing and "never summed" caveat copy already exist — we extend, not invent.

---

## 1. Why it belongs on Procurement (not Public Payments)

The lifecycle is **PLANNED (open now) → AWARDED (won) → SPENT (paid)**. The Procurement page IS the
contracting register (awards = AWARDED; the "Open right now" tab = the PLANNED front of the same pipeline).
Public Payments is the SPENT register (a different page). Live tenders are pre-award, so they sit on
Procurement — completing the forward end of the pipeline it already shows. This is the Stotles/Tussell
"pipeline" view, on a page that already half-exists.

**The win:** TED covers **EU-threshold only** (big tenders). The national eTenders feed adds the
**sub-threshold** mass — **844 open tenders across ~791 buyers** (schools, councils, water schemes). So the
tab goes from "the big EU tenders" to "the full live pipeline."

## 2. What changes, segment by segment (in the existing `tabs[2]`)

### 2a. "Open tenders" — unify TED + national
`_render_ted_tenders()` → extend to `_render_open_tenders()`:
- Source A (exists): TED EU-journal competition notices (`fetch_ted_tenders_result`).
- Source B (new): national eTenders open tenders (`v_procurement_live_tenders` — closing-soonest, buyer,
  deadline, days-to-close, estimated value, detail link).
- **Present as ONE closing-soonest list** with a per-row **register badge** — `EU-journal (TED)` vs
  `National (eTenders)` — and a count line: *"X open now: Y national + Z EU-journal."*
- Keep the existing **"only still open" toggle** and the **PLANNED/estimate caveat** (already written).
- Add a **freshness line**: *"National opportunities as of <retrieved_utc>."*

### 2b. "Contract terms ending" — add the national expiring view
`_render_expiring_contracts()` (TED `v_procurement_expiring_contracts`, ~36%) → add the national
`v_procurement_expiring_contracts_etenders` (~2,188 expiring in 24mo: JobPath €142.5m×2, BAM €182m,
Sisk €154m). Same register-badge treatment; same "advertised term, not a verified event" caveat.

## 3. Honesty rails (mostly already present — two additions)

Already on the page (reuse verbatim):
- **PLANNED tier** — estimated value is a *buyer estimate before any award*; **never** summed with awards
  or payments. The caveat copy at `_render_ted_tenders` already says this.
- **Only-open toggle**, **lead with counts**, **source link per row** (→ TED or the eTenders detail page).

New, required because the national feed is a live snapshot:
- **Freshness indicator + staleness guard** — show `retrieved_utc`; if the snapshot is older than N days,
  show a "may be out of date" note (a stale open-tenders list with passed deadlines is misleading — this is
  the one real risk).
- **Register provenance** — label each row's source (two registers, never merged value).

## 4. Prerequisites — the gate

1. ✅ **Promoted sandbox → silver (2026-06-14).** `extractors/etenders_live_tenders_extract.py` (logging via
   setup_standalone_logging, SOURCE/provenance + license, coverage JSON, `save_parquet`) writes
   `data/silver/parquet/etenders_live_tenders.parquet`. Sandbox extractor removed; the diagnostic probe
   `pipeline_sandbox/etenders_live_probe.py` kept. First silver run: 2,175 rows → **825 open / 398 buyers**.
2. ✅ **Views repointed** to the silver path; **5 contract tests green** off silver.
3. ⏳ **Activate polling** — `tools/poll_live_tenders.ps1 -Register` (wrapper built + repointed to the silver
   extractor; **user opt-in** — the harness blocks auto-creating a standing scraper task). The page is only
   honest if the snapshot is current, so register polling BEFORE wiring the page to real users.
4. ⏳ **(optional, for stability)** run the silver extractor a few times over a few days to confirm the grid
   layout is stable before the UI depends on it.

The data + view + tests are now silver and green. The remaining gate item before the **page** ships to users
is **active polling** (freshness) — do not surface a stale, un-polled snapshot.

## 5. Plumbing (firewall-clean — aggregation stays in views)

- Views are built + tested: `v_procurement_live_tenders(_summary)`, `v_procurement_expiring_contracts_etenders`.
  They already auto-register via the `procurement_*.sql` glob.
- **Add data-access wrappers** in `utility/data_access/procurement_data.py` (mirror `fetch_ted_tenders_result`):
  `fetch_live_tenders_result(only_open, limit)`, `fetch_live_tenders_summary_result()`.
- **UI renders only** — cards reuse the `pr-*` family (`_card`, `_value_pill`, `_authority_href`); no modelling
  in the page (the logic-firewall checker scans `procurement.py`).

## 6. Cross-links (nice-to-have, after the core)

- From an **authority/buyer** (awards or payments profile) → *"N open tenders from this buyer now."*
- From a **CPV/category** → *"open tenders in this category"* (national feed has buyer + title; CPV would
  need the tender detail page — a later enrichment, not blocking).

## 7. Sequencing

0. **Gate:** promote sandbox→silver + activate polling + repoint views.
1. Data-access wrappers (`fetch_live_tenders_*`).
2. Extend `_render_ted_tenders` → `_render_open_tenders` (TED + national, register badges, freshness line);
   extend `_render_expiring_contracts` with the national view.
3. Cross-links + `civic-ui-review` pass (PRODUCT.md loaded first).

## 8. Out of scope (explicit)
- Never sum estimated values; never merge the two registers' values; PLANNED stays separate from
  AWARDED/SPENT. No new tab/page. No CPV scrape of tender detail pages (later enrichment).

---

### One-paragraph summary
The Procurement page already has an **"Open right now"** tab with **"Open tenders"** and **"Contract terms
ending"** segments — both TED-only. Roll the national eTenders data we built into both (unified lists with a
register badge + a freshness line), which expands the live pipeline from EU-threshold-only to the full
national picture (844 open / 791 buyers). The PLANNED-tier honesty rails already exist. The only hard
prerequisite is **promoting the sandbox snapshot to silver and turning on polling first** — the page must not
read a stale, un-polled scrape. Then it's ~2 data-access wrappers + extending 2 existing render functions.
