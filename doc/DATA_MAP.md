# DATA MAP — the single status board for every dataset (read this FIRST)

> **Purpose:** one place to see *where every dataset is* across all domains — parliamentary,
> public-money, legal/regulatory, political-finance, reference — and what must never be
> conflated. When the picture feels complicated, this is the reset. Last updated 2026-06-04.
>
> **Two rules this doc enforces:**
> 1. **The maturity gate** (below) — every dataset sits at exactly one tier ⓪–⑤. "Done"
>    means *the tier you intended*, not "a script exists".
> 2. **The 3 money-grains never sum across each other** (§ Money grains).
>
> Companion docs: `PROCUREMENT_MASTER.md` (procurement + value taxonomy §4b),
> `PER_LA_AFS_BUILD_PLAN.md`, `PUBLIC_PAYMENTS_FACT_SCHEMA.md`,
> `new_public_money_legal_sources_claude_backlog.md` (tier-⓪ new sources),
> `new_sources_value_and_features_claude_plan.md` (feature ideas).

---

## The maturity gate — every dataset is at ONE of these tiers

| Tier | Meaning | Where it lives |
|---|---|---|
| **⓪ scoped** | data pulled & profiled, **no ETL** (throwaway probes) | `c:\tmp\*_scoping\`, a plan doc |
| **③ sandbox** | extractor built, writes parquet, **gitignored / not Cloud-readable**, not in pipeline | `data/sandbox/` or gitignored `data/silver/` |
| **silver/gold built** | clean parquet, conventions met, **not yet wired** | `data/silver/` or `data/gold/` |
| **② production, NO page** | wired as a `pipeline.py` chain → committed gold; **users can't see it** | gold + chain |
| **① LIVE** | production chain **and** a user-facing Streamlit page | gold + chain + page |
| **④ validated, not built** | source proven viable, extractor not written to gold | plan doc |
| **⑤ blocked** | needs OCR / Playwright / access unblock | — |

**The current bottleneck is surfacing, not ingesting** — a large backlog sits at ②/③ (built
but invisible). Prefer moving things rightward (③→②→①) over starting new ⓪ ingests.

---

## MASTER STATUS BOARD (all domains)

### Parliamentary (the theyworkforyou core) — mostly ① LIVE
| Dataset | Tier | Chain / page |
|---|---|---|
| Members + profiles | ① | `members` → `member-overview` |
| Attendance | ① | `attendance` → `rankings-attendance` |
| Votes | ① | → `rankings-votes` |
| Questions | ① | feeds `member-overview` |
| Interests (register) | ① | `interests` → `rankings-interests` |
| TD payments (PSA) | ① | `payments` → `rankings-payments` |
| Committees | ① | → `rankings-committees` |
| Legislation / Bills | ① | `legislation` → `rankings-legislation` |
| Debates floor-speech | ⑤ | blocked on AKN-XML (DAIL-290) |
| Seanad | ② / parity ③ | `seanad` chain; parity ETL sandbox (`seanad_*_experimental.py`) |

### Public money — the big ②/③ backlog (see deep-dive §A–D)
| Dataset | Tier | Note |
|---|---|---|
| Lobbying | ① | `lobbying` → `rankings-lobbying` |
| Corporate notices + CRO xref | ① | → `rankings-corporate` |
| **eTenders procurement** (+lobbying overlap) | **① LIVE** | `procurement` chain → gold + 5 views + `procurement_data.py` + page `rankings-procurement` (Suppliers/Authorities/Categories/Lobbying-overlap tabs); firewall-clean, tested |
| **TED EU awards** | ② | `ted` chain, no page |
| **Amalgamated AFS** (national LA finance) | **② gold, NO page** | `afs` chain; no finance page exists |
| CBI authorised firms | ② | `cbi` chain, no page |
| **Per-LA AFS — revenue** (21 councils) | ③ | gitignored silver; validated |
| **Per-LA AFS — capital** (21 councils) | ③ | gitignored silver; validated + cross-checked |
| **LA supplier payments** (20 councils) | ③ | gitignored silver |
| public_payments_fact (central/semi-state) | ③ | schema-drifted (`amount_semantics`); **churning publisher set run-to-run (~25 publishers / ~17k rows, 2026-06-04) — unstable, don't conform yet** |
| **NPHDB payments** (National Paediatric Hospital Dev Board) ⭐new | ③ | 260 rows / €193.8m / COMMITTED; ⚠ **born on legacy `amount_semantics`** — drift is *spreading*, not converging |

### Legal / regulatory
| Dataset | Tier | Note |
|---|---|---|
| Statutory Instruments | ① | → `rankings-statutory-instruments` |
| Iris appointments | ① | `iris` → `rankings-appointments` |
| Iris corporate/insolvency notices | ① | feeds `rankings-corporate` |
| **SI legal-state** (revoked/amended) | ④ built | `si_current_state.parquet`, needs UI wiring |
| **SI amendment graph** | ④ built | `v_si_amendments` (1,484 edges), needs UI |
| **SI LRC subject enrichment** | ④ spike | PR1 ready (90% match), gold-untouched |
| **Judiciary** (appointments/revolving-door/courts) | ④ validated | green core proven, no extractor-to-gold |

### Political finance
| Dataset | Tier | Note |
|---|---|---|
| **SIPO donations / expenses** | ⑤ blocked | OCR engine is the bottleneck (own context) |

### Reference / context (denominators, not facts)
| Dataset | Tier | Note |
|---|---|---|
| Constituency population / boundaries | built | per-capita denominator usage still pending |
| SSHA social-housing waiting list | ⓪ flagged | needs LA→constituency crosswalk |
| Member external links / related_docs | ① | linkage helpers |

### ⓪ Scoped new sources (data pulled, NO ETL — `project_new_sources_scoping_2026_06_04`)
| Source | Verdict | value_kind / use |
|---|---|---|
| LA budget tables (SDCC/Fingal/Roscommon) | ✅ best fit | `budget_allocated` — planned-vs-actual vs AFS (1:1 division match) |
| PAC report metadata | ✅ best reuse | plugs into `oireachtas_pdf_poller.py` |
| Housing Adaptation Grants | ✅ easiest money-fact | LA-aggregated, zero PII |
| LGAS statutory audit reports | ✅ bounded | `audit_finding` — ~400 born-digital PDFs |
| C&AG reports metadata | ✅ broad | 252 docs |
| REV / Voted Expenditure | ✅ (parked) | `voted_expenditure` — dept-level context only; user lukewarm |
| CPO cases | 🟡 privacy guard | `cpo_land_acquisition_signal` — NEVER spend; address-leak quarantine |
| NTA board minutes | 🟡 signal only | `board_approved` — ~0% carry € |
| FOI/AIE disclosure logs | 🟡 lead layer | `foi_lead` — per-body adapters |
| Project Ireland 2040 | ⚠️ cost weak | project/lifecycle map; cost is bands, not € |
| Sports Capital | ❌ unreachable | defunct host |

---

## Money grains: 3 families, NEVER sum across them

Every public-money "€" belongs to exactly one grain; a euro in one is **not** comparable to a
euro in another. Consolidation (when it comes) only unions *within* the payment grain.

```
BUDGET / BY-SERVICE-DIVISION     →  what a body spends per service area (AFS accounts)
AWARD / CONTRACT-CEILING         →  contracts advertised/awarded — NOT money paid
PAYMENT / SPENT (supplier-level) →  actual € to a NAMED supplier
```

The 2-axis taxonomy (`realisation_tier` + `value_kind`) in `PROCUREMENT_MASTER.md` §8 is
the controlled vocab; lock it before any cross-source merge.

**⚠️ The figures are EXTRACTION-DERIVED — there is no single authoritative total.** Almost every
euro is *parsed out of a published document* (PDF PO lists, AFS statements, sometimes scans), not
read from a ledger. So a number carries **two** independent qualifiers: its **tier** (what kind
of money — ceiling/ordered/paid) **and** its **extraction confidence** (how well we even know the
number — OCR/column/VAT/grain risk). Coverage is partial (20/31 councils, ~19 publishers, HSE/
Tusla pending), so every aggregate is a **floor**, never *the* total. Carry `extraction_status`/
`extraction_confidence`/`source_file_url` on every row; present totals as "at least €Y, from N
documents — indicative, not audited," with each € linked to its source. (Full principle:
`PUBLIC_PAYMENTS_FACT_SCHEMA.md` §A.8; UI rules in §C.4.)

---

## DEEP DIVE — the money facts (file / layer / grain / status)

### §A. Local-authority money — 4 facts, 3 grains (NEVER reconcile across them)
| Fact | File | Layer | Grain | Scope | Rows | Status |
|---|---|---|---|---|---|---|
| Amalgamated AFS | `data/silver/parquet/afs_amalgamated_divisions.parquet` | silver | revenue I&E by division | national, all-31 summed, 2016–23 | 64 | ② chain `afs` |
| Per-LA AFS — revenue | `data/silver/parquet/la_afs_divisions.parquet` | silver | revenue **net**-exp by division | per-council (21) | 168 | ③ gitignored |
| Per-LA AFS — capital | `data/silver/parquet/la_afs_capital_divisions.parquet` | silver | **capital** exp by division | per-council (21) | 159 | ③ gitignored |
| LA payments | `data/silver/parquet/la_payments_fact.parquet` | silver | **per-supplier** PO/payment | per-council | 11,091 | ③ gitignored |

Producers: `afs_amalgamated_extract.py`, `la_afs_extract.py`, `la_afs_capital_extract.py`,
`procurement_la_payments_extract.py`. Coverage JSONs in `data/_meta/`.

> **The housing insight (both AFS facts agree):** Housing nets ≈ €0 on the *revenue* account
> (HAP/RAS recoupment + rents pass through) yet is the dominant *capital* line (€2.5bn pooled,
> 34–68% of each council's capital) — but ~98% DHLGH-grant-funded. The LA is largely a
> **conduit for centrally-financed housing**. This is the per-constituency feature's hook.

### §B. Procurement AWARDS — 2 facts (AWARD grain — ceilings, NOT spend)
| Fact | File | Layer | Rows | Status |
|---|---|---|---|---|
| eTenders awards | `data/gold/parquet/procurement_awards.parquet` | gold | 59,439 | ① LIVE — page `rankings-procurement` |
| ↳ supplier↔CRO match | `data/gold/parquet/procurement_supplier_cro_match.parquet` | gold | — | ② |
| ↳ lobbying overlap | `data/gold/parquet/procurement_lobbying_overlap.parquet` | gold | — | ② |
| TED EU awards | `data/silver/parquet/ted_ie_awards.parquet` | silver | 13,126 | ② not wired to a page |

⚠️ Award values are framework/DPS **CEILINGS, not money paid** (the "€570bn that isn't", 24×
overcount). Only `value_safe_to_sum` rows may be summed. Never concat into payments.

### §C. Central / semi-state PAYMENTS — 1 fact (PAYMENT grain)
| Fact | File | Layer | Rows | Status |
|---|---|---|---|---|
| public_payments_fact | `data/sandbox/parquet/public_payments_fact.parquet` | sandbox | ~8,021 | ③ NOT promoted |

⚠️ Still on the drifted `amount_semantics` column — converge to `value_kind`+`realisation_tier`
before promotion. HSE/Tusla emits a DQ **JSON not parquet** + a third vocab (coordination item).

### §D. Supporting / cross-reference (gold)
| Fact | File | Note |
|---|---|---|
| CRO ↔ corporate notices | `data/gold/parquet/cro_xref_corporate_notices.parquet` | company-number xref |
| CBI authorised firms (+xrefs) | `data/sandbox/parquet/cbi_*.parquet` | Central Bank register |
| related docs | `data/silver/parquet/related_docs.parquet` | document linkage |

---

## ⚠️ Complexity traps

1. **Stale sandbox duplicates.** `data/sandbox/` holds old copies of `procurement_awards`,
   `procurement_supplier_cro_match`, `afs_amalgamated_divisions` — leftover probe artifacts.
   The **gold/silver** versions are authoritative; ignore the sandbox copies of these three.
2. **The ②/③ backlog is invisible to users.** All four LA/AFS facts + public_payments are
   gitignored-silver/sandbox (local only); eTenders/TED/amalgamated-AFS/CBI are gold-or-chain
   but **have no page**. Lots of finished data, nothing a user can open.
3. **Gitignore.** `*.parquet` is globally ignored; AFS/LA/TED silver parquets need an explicit
   negation rule before they're Cloud-readable (deferred to avoid a `.gitignore` edit race).

---

## SURFACE BACKLOG — move things rightward (the priority list)

Effort is better spent surfacing the ②/③ backlog than starting new ⓪ ingests.

1. ~~**Procurement page** (②→①)~~ ✅ **DONE 2026-06-06** — page `rankings-procurement` is LIVE
   (Suppliers/Authorities/Categories/Lobbying-overlap tabs), firewall-clean, view+core tests
   green. The "Phase 3 deferred" note was stale — it had already shipped.
2. **Council Finance / "Your Area"** (③→①) — *highest novelty*, now the top open item. Per-LA AFS revenue+capital
   (21 councils, validated). Needs: gitignore-negation (Cloud), the **LA→constituency
   crosswalk**, ideally OCR the last 4 scanned councils. Hook = the housing insight above.
3. **public_payments_fact convergence** (③→silver-ready) — prerequisite for any payment-grain
   union; conform to the 2-axis taxonomy first.
4. **Wire the ④ SI enhancements** (legal-state / amendment-graph / LRC) onto the existing SI
   page — built, just need UI.

Feature concepts that combine these (per `new_sources_value_and_features_claude_plan.md`):
LA Profile · Public Body Profile · Supplier Dossier · Infrastructure Project Profile · FOI
Lead Pack.

## Where consolidation stands (the union, gated)

Still the **ingestion phase**; **wholesale consolidation NOT begun** (premature merges corrupt
the data). When it starts: only **payment-grain** sources union (public_payments + LA payments
+ HSE/Tusla, after taxonomy conformance); **award-grain** (eTenders/TED) and **budget-grain**
(AFS family) stay as separate sibling facts. See `PROCUREMENT_MASTER.md`.

**The target model is now specified (`PUBLIC_PAYMENTS_FACT_SCHEMA.md` Part A, 2026-06-04),
following established practice rather than a house scheme:**
- **OCDS** (Open Contracting Data Standard) lifecycle stages → `realisation_tier`
  (PLANNED→AWARDED→COMMITTED→SPENT); never sum across stages.
- **Kimball**: one fact per *business process* → **two grain-separated facts sharing one column
  contract** — `fct_award` (eTenders+TED) and `fct_payment` (LA + public-body + HSE/Tusla) —
  *physically separate so a cross-grain `SUM()` is impossible*. AFS budget = a third grain,
  sibling, never unioned.
- **Conformed dimensions built once**: `dim_supplier` (the single place name→CRO matching
  happens), `dim_buyer`; lobbying/SIPO are **bridges off `dim_supplier`**, not facts.
- **Additivity enforced in the view layer** (`value_safe_to_sum` + tier-scoped metrics), with a
  test that no view sums across `realisation_tier`. Scaled to Polars+DuckDB (no SCD2/warehouse).

> **On the `amount_semantics` "drift" (assessed 2026-06-04 — low stakes):** new payment sources
> (NPHDB, public_payments) use the column name `amount_semantics`, but the **values are the SAME
> clean vocabulary** as `value_kind` (`payment_actual`, `po_committed`) — it's a column rename +
> a deterministic `realisation_tier` derivation (`po_committed`→COMMITTED, `payment_actual`→
> SPENT), absorbed at the staging layer. `value_safe_to_sum` (the dangerous axis) is already set
> on all of them. So this is cosmetic, gated (no merge yet), and cheap-per-source — **not a
> threat.** The thing to ACTUALLY watch: a genuinely **new `value_kind` token** (esp. HSE/Tusla's
> rumoured `payment_incl_vat`/`invoice_payment`, which fold in a VAT-basis decision) — *that*
> would be a real fork needing a crosswalk, not a rename. Optional tidy-up: have new producers
> emit `value_kind`+`realisation_tier` directly; not required.
