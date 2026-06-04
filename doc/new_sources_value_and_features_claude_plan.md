# Claude Plan: New-Source Value & Potential Features

**Status:** SCOPING / EXPLORATION ONLY — data probed and profiled (2026-06-04), **no ingestion, no ETL committed**. This doc records *what each candidate source could add* (data + user-facing features) and, more importantly, *how they combine* into profile pages that don't exist elsewhere in Irish civic tech.

**Companions:**
- `doc/new_public_money_legal_sources_claude_backlog.md` — the source backlog (links, probe recipe, value-type discipline).
- `doc/current_source_health_coverage_gaps_claude_plan.md` — source-HEALTH monitoring for *existing* sources (separate concern).
- Scoping verdicts + concrete endpoints/schemas/gotchas: memory `project_new_sources_scoping_2026_06_04.md`. Raw probe data cached transiently in `c:\tmp\tier1_scoping\` and `c:\tmp\tier2_scoping\`.

**Discipline (carried from existing rules):** never union different money-meanings (voted budget vs grant-allocated vs grant-paid vs award vs payment vs project-cost-band vs non-money signal); preserve value_kind + realisation_tier (PROCUREMENT_BUILD_PLAN §4b); every public fact links to source and preserves caveats; inference welcome in planning, forbidden in app UI copy.

---

## The core insight

Most of these sources do **not** add new transaction grains you must model from scratch. They add **context, accountability, and lifecycle stages** *around* the spend data already extracted (procurement awards, LA payments, public-body payments, AFS, HSE/Tusla). That is their leverage: they make the existing hard-won money data legible as a story — plan → decide → acquire → award → pay → audit.

The two honest exceptions are REV (commodity, stale at 2022) and Project Ireland 2040 cost (band-only, sparse) — framing aids, not facts.

---

## Per-source: data added → features unlocked

### Accountability / oversight layer

**PAC report metadata** (✅ best reuse — rides existing `oireachtas_pdf_poller.py`)
- *Data:* catalogue of Committee of Public Accounts reports (date, title, examined body, Dáil term, PDF). ~38 across recent terms; metadata 100% URL-derivable, no PDF parse.
- *Features:* "examined by PAC" strip on a **Public Body Profile**; enrichment of existing **committees** pages; accountability timeline. Phase 2 (parse findings) → "what PAC flagged about body X."

**C&AG report metadata** (✅ broad)
- *Data:* 252 Comptroller & Auditor General reports — VFM, overruns, procurement weaknesses — keyed to departments/bodies/projects, back to 1922 (120 Special Reports + Appropriation Accounts + ROAPS).
- *Features:* audit layer on **Public Body Profile** ("3 VFM reports, 2 overrun findings"); overrun context attaching to specific **infrastructure projects**; interlinks with PAC (C&AG reports are what PAC examines). Needs 2-stage crawl for PDF URL/year.

**LA statutory audit reports** (✅ bounded, clean join)
- *Data:* ~400 LGAS reports (31 councils × 2012–2024) on rates collection, procurement, governance, controls. Born-digital, no OCR, standardized template; clean council+year join.
- *Features:* audit/accountability tab on a **Local Authority profile**, sitting directly on the LA payments + AFS spend already shown — "here's the spend, *and* what the auditor said about its control." Cross-council comparison of recurring control failures.

### Money-context layer

**REV / Voted Expenditure** (✅ clean but ⚠️ stale 2022; user lukewarm)
- *Data:* national voted budgets by department→programme→subhead, current/capital split, 1994–2022. 2022 gross reconciles to published REV (€82.97bn).
- *Features:* budget **denominator** on a Public Body Profile → a **voted → awarded → paid funnel** ("voted €X; traced €Y in awards/payments = Z% coverage"); long department spending trends. *Caveat:* dept-level only (no supplier join), frozen 2022 — context, not a current-accountability flagship.

**LA budget tables (SDCC / Fingal / Roscommon)** (✅ best *fit* to existing assets)
- *Data:* adopted budget by the **same A–H service divisions as `la_afs_divisions`** (confirmed 1:1), plus income-by-source.
- *Features:* **planned-vs-actual-by-division variance** per council ("budgeted €46m Roads, AFS actual €X"); income composition (rates / LPT / grants / parking). Only ~3 of 31 councils publish structured CSV → illustrative pilot, not national.

**Housing Adaptation Grants** (✅ easiest standalone money-fact, zero PII)
- *Data:* grant payments + allocations by LA, by year, by type (Older People / Disability / Mobility Aids). LA-aggregated.
- *Features:* plugs into the **constituency/geographic** dimension — "what housing-grant money flows to my area," and a **per-capita comparison** across councils using the constituency-population denominator already built; allocated-vs-drawn-down gap (which councils underspend). A clean strand of a Public Money Explorer.

### Infrastructure / decision-signal layer

**Project Ireland 2040** (⚠️ cost weak, but the spine)
- *Data:* 1,936 capital projects — name, delivery body, county, sector, lifecycle stage, year (cost only as sparse bands, ~18% joinable).
- *Features:* **spine of an Infrastructure Project Profile / map** — "capital projects in my county," lifecycle tracking (Strategic Assessment → Implementation → Complete). The connective backbone the next three sources hang off by scheme/project name. Cost = soft band annotation only.

**CPO cases (An Coimisiún Pleanála)** (🟡 GO with privacy guard)
- *Data:* land-acquisition authorisations by scheme + public body, decision + dates. Applicants ~always public bodies.
- *Features:* the **earliest signal** in a project's life (land acquired before contracts/payments) — a "land acquisition" stage on the Infrastructure Project Profile; scheme-level "what's being compulsorily acquired in my area." `value_kind = cpo_land_acquisition_signal` (never spend). **Privacy:** Housing-Act/Derelict-Site/Protected-Structure cases leak private home addresses → case-type quarantine + leak-string guard (mirror personal-insolvency pattern); applicant-only parties; never ingest inspector-report/order/submission PDFs.

**NTA board minutes** (🟡 approval-signal only; TII/HSE later)
- *Data:* board **decisions** — contract awards (e.g. Luas O&M → Keolis Amey; cyber → EY), procurement commencements, CPO activations, capital updates. ~180 born-digital PDFs 2009–2026.
- *Features:* "what did the board decide and award to whom" decision log on a transport-body profile; the **approval stage** between CPO and payment. NTA is already in public-body payments, so an award noted in minutes can later match the actual payment. *No euro values* (~0%) — decision/signal feed; needs segmenter + cheap LLM finisher; false-positive risk ("noted the contract" ≠ approval).

**FOI / AIE disclosure logs** (🟡 federated lead-layer)
- *Data:* per-body request logs — topic, requester category, decision, dates. Clean CSV/XLSX for DLR / Dept of Justice / DCC.
- *Features:* an **FOI Lead Pack** — "what journalists and the public are asking the State about," topic trends per body, investigative lead-generation. `value_kind = foi_lead`, never presented as fact. No requester names (category only); name-leak guard on free-text description. Per-body adapters (no universal parser); DAFM stale.

---

## Where it gets powerful: combination features

The individual sources are useful; the payoff is that several assemble into **profile pages that don't exist elsewhere in Irish civic tech** — and the project already owns the expensive middle (procurement/payments).

### 1. Infrastructure Project Profile (most novel)
For a scheme like MetroLink / BusConnects, a full lifecycle:

```
Project Ireland 2040 (the project + stage)
  → CPO cases (land acquired)
    → NTA/TII board minutes (procurement approved, contract awarded)
      → procurement awards [EXISTING] (the contract)
        → public-body / LA payments [EXISTING] (money out)
          → C&AG (overrun / VFM findings)
```

*Plan → land → approve → award → pay → audit*, for one public project. The project already has the award and payment grains.

### 2. Public Body Profile
```
REV (voted budget)
  + procurement/payments [EXISTING] (spend)
  + PAC + C&AG (audited)
  + FOI logs (what's being asked)
  + board minutes (decisions)
```
A single "follow this body's money and accountability" view, anchored on the voted→awarded→paid funnel.

### 3. Local Authority Profile
```
AFS actuals [EXISTING]
  + LA budget tables (planned, SAME A–H divisions)
  + LA payments [EXISTING]
  + Housing grants (per-capita via constituency population)
  + LA statutory audit reports (auditor findings)
```
Plan → spend → audit, per council, with a per-capita lens.

### 4. Supplier Dossier
```
procurement awards [EXISTING]
  + payments [EXISTING]
  + board-minute contract awards
```
Every public euro and decision attached to one company.

The geographic/constituency dimension (already built, incl. population denominator) threads grants, CPO, and capital projects into "what's happening with public money in my area."

---

## Readiness & suggested sequencing (when/if ingestion begins)

Ranked by effort-to-value, drawn from the real-data probe:

1. **LA budget → AFS planned-vs-actual** — best fit; 1:1 division match; pilot on SDCC/Fingal/Roscommon.
2. **PAC report metadata** — lowest effort; reuses existing Oireachtas poller; metadata-only index first.
3. **Housing Adaptation Grants** — cleanest standalone money-fact; zero PII; per-capita angle.
4. **LA statutory audit reports** — bounded (~400 PDFs, no OCR); metadata index now, finding-extraction later.
5. **C&AG metadata** — broad accountability; pairs with PAC.
6. **Project Ireland 2040** — ingest project metadata as the infrastructure spine; treat cost as band annotation only.
7. **CPO cases** — scheme-level signal with the address privacy guard.
8. **NTA board minutes** — decision/signal feed; segmenter + LLM; no euro values.
9. **FOI/AIE logs** — federated lead layer; start DLR + Justice + DCC.

Excluded: Sports Capital (unreachable hosting); TED/eTenders (already in progress).

---

## Public UI rule (carried from backlog)

Do not add a new page per source on ingestion. First use each as enrichment/context inside the combination profiles above (Public Money Explorer, Public Body Profile, LA Profile, Infrastructure Project Profile, Supplier Dossier, FOI Lead Pack). Every public fact links to source and preserves caveats; non-money signals (CPO, board approvals, FOI leads, project cost-bands) must never be presented or summed as spend.
