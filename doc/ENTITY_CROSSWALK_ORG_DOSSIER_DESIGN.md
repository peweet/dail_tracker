# Company Entity Crosswalk + Organisation 360° Dossier — Design

**Status:** design-only (no code written yet). Owner sign-off required before any pipeline change or gold promotion.
**Date:** 2026-06-29.
**Siblings:** `doc/BUYER_DOSSIER_DESIGN.md` (the *buyer*/public-body side — separate name-space, do not conflate), `doc/BI_SPINOUT_ARCHITECTURE.md`, `doc/PROCUREMENT_INTELLIGENCE_ROADMAP.md`.

---

## 1. Goal

One composed record — `build_organisation_dossier(entity)` — that fuses everything the State's public record knows about a single **private organisation**, across registers that today live in silos:

- **Procurement** — eTenders/TED awards it won (as supplier) + the buyers it sells to + competition signal
- **Lobbying** — its lobbying footprint (as registrant *and* client) + which politicians/bodies it targeted
- **Ministerial diaries** — meetings ministers logged with it
- **Corporate notices** — receivership / wind-up / examinership events (Iris Oifigiúil) + CBI-authorisation status
- **Identity** — CRO company number + name aliases
- **Charity** — filed finances, if it is a registered charity
- **Compliance** — EPA licence flag

This is the project's actual thesis (money + influence + corporate fused on one entity), and the commercial wedge for the BI/buyer-dossier product. **No comparable service serves a pre-composed organisation dossier.**

### Why this and not "more dossiers"
The composition plumbing is cheap (proven: ~a day per dossier). The **valuable, hard, defensible** part is the **entity-resolution spine** underneath — matching the same org across registers where the names differ. That spine, once built, sharpens three things at once: the org dossier, the existing supplier dossier, and the proc×lobbying overlap. The spine is the asset; the dossier is a thin layer on it.

### Explicitly out of scope (do NOT rebuild)
- **Buyer identity** (public bodies: eTenders↔TED↔payments-publisher↔council) — that is `buyer_xref`, already designed in `doc/BUYER_DOSSIER_DESIGN.md` with a draft built at `c:/tmp/buyer_xref_draft/`. Different name-space, different problem. This doc is the **supplier/company** side only.
- The supplier dossier — **extend**, don't replace (`dossiers.build_supplier_dossier`).
- The EPA flag (`has_epa_licence`) and CRO match — already on `v_procurement_supplier_summary`.

---

## 2. The core problem: divergent normalisers

Entity resolution hinges on a single normalised name key. A canonical normaliser **already exists** and several extractors already use it correctly — but four sources diverge, which is the documented "distress join = 0 across 38,335 suppliers" bug.

**Canonical:** `shared/name_norm.py::name_norm_expr()` → NFD accent-fold, **UPPERCASE**, strip a 13-item LEGAL_SUFFIX set (LTD/LIMITED/DAC/PLC/CLG/UC/…), punctuation→space, collapse whitespace.

| Source | Normaliser | Key case | Verdict |
|---|---|---|---|
| Procurement awards (`supplier_norm`) | `shared/name_norm` | UPPER | ✅ canonical |
| Procurement payments (`supplier_normalised`) | `shared/name_norm` | UPPER | ✅ canonical |
| `procurement_supplier_cro_match` | `shared/name_norm` | UPPER | ✅ canonical (→ `company_num`) |
| Proc×lobbying xref (`procurement_lobbying_xref.py`) | `shared/name_norm` | UPPER | ✅ canonical (lobbying link **works today**) |
| Corporate notices → CRO (`cro_corporate_xref_enrichment.py`) | `shared/name_norm` | UPPER | ✅ canonical |
| **CBI distress** (`cbi_registers_extract._norm_firm`) | local, NFKD, **lowercase**, different suffix set | lower | ❌ **breaks join** |
| **Receiver enrich** (`corporate_receiver_enrich._norm_entity`) | local, NFKD, **lowercase** | lower | ❌ breaks join |
| **Charities** (`charity/charity_normalise.name_norm_expr`) | UPPER but **omits NFD accent-fold** | UPPER (no fold) | ⚠️ accented-name asymmetry ("Tirlán"≠"TIRLAN") |
| Ministerial diary (`diary_org_match.norm` / `diary_company_influence.fold`) | NFKD, lowercase, gazetteer-tiered | lower | ⚠️ context-specific; align base fold |

**The strong key is CRO `company_num`** — carried (or joinable) by procurement (via cro_match), payments (`cro_company_num`), corporate notices (via cro_xref), charities (`cro_number`), and EPA (`company_num`). Name-normalisation is the *bridge* for rows without a resolved CRO number.

---

## 3. Architecture

Spine key = **`company_num` (strong) ∪ `entity_norm` (UPPERCASE `shared/name_norm`, bridge)**. Every cross-register link carries a **`match_tier`**: `cro_exact` › `norm_exact` › `unresolved`. **Fail-closed: below `norm_exact`, cross-register fusion is suppressed, not guessed** (provenance discipline — `feedback_provenance_is_users_domain`).

### Phase 0 — Normaliser unification (foundational, independently valuable)
Migrate the four divergent normalisers onto `shared/name_norm.py` so all keys are comparable:
- `cbi_registers_extract._norm_firm` → `shared/name_norm` (fixes the distress-join-zero bug)
- `corporate_receiver_enrich._norm_entity` → `shared/name_norm`
- `charity/charity_normalise.name_norm_expr` → add the NFD accent-fold (or import shared)
- `diary_company_influence.fold` / `diary_org_match.norm` → align the *base* fold to `shared/name_norm`; keep the gazetteer confidence-tiering on top

**This phase stands alone** — it fixes a real, quantified bug (corporate distress never joins to suppliers) regardless of whether the dossier is ever built. Do it first, ship it on its own.

**Guards (mandatory, ETL conventions):** Polars only; before/after **match-count parity** per source (the normaliser change *moves* the match set — quantify it, don't surprise-ship); regenerate `output_baseline.json` deliberately after a reviewed shift; row-floor guard; atomic zstd write. Treat the corporate/charity gold re-cut as a data-anchored promotion (`feedback_pipeline_changes_data_anchored_promotion`).

### Phase 1 — The entity spine
- `extractors/entity_xref_build.py` (Polars) → `data/gold/parquet/entity_xref.parquet`
- `sql_views/entity/entity_xref.sql` → `v_entity_xref`

One row per resolved entity:
`entity_id` (= `company_num` when known, else a stable hash of `entity_norm`), `entity_norm`, `display_name`, `cro_company_num`, presence flags (`in_procurement`, `on_lobbying_register`, `has_diary_meeting`, `has_corporate_notice`, `is_charity`, `has_epa_licence`), and per-source raw aliases. Built by unioning the per-source normalised keys, grouping by `company_num ∪ entity_norm`, carrying `match_tier`.

**Hard exclusions baked into the spine:** sole traders / natural persons are dropped (`supplier_class != 'sole_trader_or_individual'`, the existing guard) — no natural-person profiling, no personal data (`feedback_personal_insolvency_privacy`).

### Phase 2 — The composition (core, has a real consumer: MCP)
- `dail_tracker_core/queries/entity.py` — retrieval over `v_entity_xref` + reuse existing supplier/lobbying/corporate/charity/diary queries
- `dossiers.build_organisation_dossier(entity)` — resolve `entity` (name or CRO num) → `entity_id` via the spine, then fan across sources. Each fused panel carries its `match_tier`; the whole record carries the co-occurrence caveat. Returns `None` if unresolved; suppresses (not zeroes) any panel whose link is below `norm_exact`.

### Phase 3 — Exposition (gated; respect the "freeze HTTP surface" rule)
- **MCP tool `organisation_dossier(name)` first** — it has a live consumer (you, in Claude Code).
- `/v1/organisations/{entity}/dossier` **only when a named consumer needs it** (a React screen / a buyer). It's a day's mechanical work then; building it ahead of demand is the over-the-top pattern we agreed to avoid.

---

## 4. Honesty / caveat rails (load-bearing — this is the highest false-causation surface in the app)

The org dossier deliberately co-locates money, influence, and distress for one entity — exactly where a reader will infer causation. Rails:

1. **Co-occurrence by ENTITY, never causation.** Procurement + lobbying + diary + corporate are *the same organisation on each register* — there is no key linking a specific lobby/meeting to a specific contract. (`caveats.ENTITY_COOCCURRENCE`.)
2. **Three money grains never summed** — AWARD ceiling ≠ ORDERED/PAID ≠ BUDGET. Verb-led on every €. (`caveats.MONEY_GRAINS`, `caveats.PROCUREMENT_AWARDS`, `caveats.PUBPAY`.)
3. **`match_tier` on every fused panel; below exact → suppressed, not guessed.**
4. **Undercount is stated** — exact normalised-name / CRO matching misses subsidiary and trading-name variants; the dossier under-reports rather than over-claims.
5. **No individuals** — sole traders / natural persons excluded; corporate notices name no person; a legal-status notice is a fact, not a verdict.

---

## 5. Build sequence (small, parity-validated PRs — the proven pattern)

- **PR0 — Normaliser unification.** Migrate the 4 divergent normalisers to `shared/name_norm`; before/after match-count parity per source; re-baseline corporate/charity gold under review. *Ships value on its own (fixes distress join).*
- **PR1 — `v_entity_xref` spine.** Extractor + view + tests (uniqueness on `entity_id`; `match_tier` vocab; sole-trader exclusion; an invariant in `test/contracts/`).
- **PR2 — Composition.** `queries/entity.py` + `dossiers.build_organisation_dossier` + caveats + tests (resolve-by-name and by-CRO; unresolved→None; a fused panel below exact is suppressed).
- **PR3 — MCP tool** (gated on owner sign-off).
- **PR4 — HTTP route** (only on real demand).

---

## 6. Risks & open questions (owner decisions — do not decide autonomously)

- **Normaliser migration shifts existing match counts.** The corporate/charity match SET changes when the key changes. Must be quantified and reviewed before re-baselining (could move headline distress/charity-overlap numbers). **Decision:** approve the re-baseline per source.
- **`entity_id` for non-CRO entities.** Hash of `entity_norm` is stable but collides on the undercount cases. **Decision:** accept hash-of-norm, or require CRO for an `entity_id` (suppress non-CRO orgs from the spine).
- **`match_tier` thresholds + labels** (same governance as `buyer_xref`).
- **Diary normaliser** — fully migrate to `shared/name_norm`, or keep its gazetteer tiering and only align the base fold? (Recommend: align base fold, keep tiering.)
- **Charity NFD fix** re-keys charity matches — re-baseline.

---

## 7. What already exists (reuse, don't rebuild)

- `shared/name_norm.py::name_norm_expr` — the canonical normaliser (the spine key).
- `procurement_supplier_cro_match.parquet` — supplier_norm → `company_num`.
- `procurement_lobbying_overlap.parquet` — supplier↔lobby link (works today, supplier_norm-keyed).
- `cro_xref_corporate_notices.parquet` — corporate notice → `company_num` (already UPPERCASE/canonical).
- `charities_enriched.parquet` (`cro_number`), `epa_supplier_compliance.parquet` (`company_num`), `diary_company_influence.parquet`.
- `dossiers.build_supplier_dossier` — the procurement-only precursor to extend.
