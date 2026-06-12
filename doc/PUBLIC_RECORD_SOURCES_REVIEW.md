# Public-Record Intelligence Sources — Review

**Reviewer pass:** 2026-06-05 · **Focus doc:** `doc/dail_tracker_public_record_intelligence_sources_for_claude.md`
**Mandate:** de-duplicate every cluster against what's already ingested/scoped; score the genuinely-new ones; shortlist ≤5 that complete an existing half-built surface. Action-mode = analysis-first (review doc + read-only probe only).

> **Headline:** this brief is ~85% a re-issue of sources already ingested, scoped, or explicitly verdicted elsewhere — most decisively `doc/new_public_money_legal_sources_claude_backlog.md` (2026-06-04) and `extractors/procurement_publishers_seed.py`. Its *new* contribution is a wall of **board-minutes / FOI-logs / protected-disclosure** sources, which are the **lowest-tractability, lowest-join, highest-PII** material in the whole backlog and largely **scope sprawl into "general due diligence" the project can't maintain.** The few things worth keeping are structured registries that complete a surface the project has already half-built.

---

## Claims Ledger

| claim | doc says | repo reality (path:line) | verdict |
|---|---|---|---|
| Semi-state PO/payment publishers (Irish Rail, ESB, EirGrid, NTA, TII, HSE, Tusla, Ent. Ireland, IDA, SEAI, Uisce, NTMA/NDFA, Courts, Pobal, Sport Ireland) are untapped | brief §1–§3 lists them as fresh "due-diligence" finds | every one already a seed row with landing URL + grain/format hints | **stale** — `extractors/procurement_publishers_seed.py:83-322` |
| eTenders is a "core public-procurement source" worth researching | brief §3 | already gold, 5 views, page deferred | **stale** — `DATA_MAP.md:57,153-156`; chain `procurement` |
| LA AFS / LA budgets / LGAS audit reports are new | brief §3,§5 | AFS ② chain + per-LA ③; LA budgets + LGAS audit both ⓪-scoped | **stale** — `DATA_MAP.md:60-64,90-99`; `IDEAS.md:51-55,114` |
| Project Ireland 2040 tracker is a fresh infrastructure feed | brief §2,§4,§5 | scoped 2026-06-04, verdict "cost is bands not € / cost weak" | **stale** — `IDEAS.md:102,120`; `new_public_money_legal_sources_claude_backlog.md:83-141` |
| C&AG + PAC reports are a new accountability layer | brief §5 | both 🔬 scoped (C&AG 252 docs, PAC rides `oireachtas_pdf_poller`) | **stale** — `IDEAS.md:112-113`; backlog §6-§7 |
| NTA/TII board minutes are a strong decision layer | brief §1,§10 | NTA scoped 🟡 "signal only, ~0% carry €"; backlog §3-§4 already wrote the parse plan | **stale** — `DATA_MAP.md:101`; backlog `:222-335` |
| CPO cases are a new land-acquisition signal | brief §4 | scoped 🟡 with privacy guard, `cpo_land_acquisition_signal` | **stale** — `DATA_MAP.md:99`; `IDEAS.md:117` |
| Housing Adaptation Grants worth ingesting | brief §4 | scoped 🔬 "easiest money-fact, zero PII" | **stale** — `DATA_MAP.md:93`; `IDEAS.md:115` |
| FOI/AIE disclosure logs are high-signal | brief §1,§6 (large) | scoped 🟡 `foi_lead` "lead layer; proves existence, no extractable content" | **stale** — `DATA_MAP.md:101`; backlog §12 |
| REV / Voted Expenditure is useful macro context | brief §3 | scoped ✅(parked) "frozen 2022, user lukewarm" | **stale** — `DATA_MAP.md:98`; `IDEAS.md:116` |
| Iris public appointments don't exist yet (board appointments) | implied by §1 board pages | LIVE page + enrichment, 1,248 rows; Iris is the canonical appointment spine | **wrong** — `utility/pages_code/public_appointments.py`; `public_appointments_enrichment.py`; `IDEAS.md:88,97` |
| ERDF/EU-funds beneficiaries (NWRA/Southern Assembly) | brief §7 | only `💡 idea` in `ENRICHMENTS.md:900` (F.4); not seeded, not probed | **confirmed (genuinely new)** |
| State Boards register (membership.stateboards.ie / publicjobs) | brief §1 board pages (+ second-pass brief §7.1) | no extractor; not in DATA_MAP public-body universe; only in sibling brief | **confirmed (genuinely new)** |
| Protected-disclosure *counts* per body are a dataset | brief §6 (≈15 sources) | nowhere — and it's a count, not a record | **confirmed new, but near-zero value** |
| Board minutes published as regular archives (TII/NTA/HSE/LDA/SBCI/MARA) | brief §1 | none ingested; NTA scoped as signal-only | **confirmed new (mostly out of scope, see DA)** |

ERDF XLSX schema confirmed live via `pipeline_sandbox/probe_review_src1_stateboards_erdf.py` (read-only): NWRA `2021-2027-beneficiaries-Oct-2025.xlsx` = HTTP 200, real `.sheet` content-type, sheet "List of operations", 48 rows, EU-standard header incl. `Beneficiary's name`, `Beneficiary ID`, `Total cost of the operation`, `Fund concerned`, `Contractor name`. State Boards membership page = 200 but thin (8 KB shell — likely JS portal; deeper probe needed before scoring tractability higher). **Deeper probe 2026-06-11: NOT a JS portal — plain static HTML, three-level hierarchy** (`/` → 20 dept links `/en/department/<name>/` → per-body pages `/en/board/<name>/`). Per-member HTML table fields: name, first appointed, reappointed, expiry date, position type, basis of appointment (PAS Process vs ministerial appointment vs nominating body, e.g. "Nominating body ICMSA"); page also carries gender balance per board. **No occupation/employer field** — person→company linkage (e.g. an executive's day job) is not in the register and would need external enrichment. No CSV/JSON export; straightforward scrape.

---

## Architectural Assessment

The brief is a **source-discovery** document (its own §preamble), and on that axis it is competent. But measured against the project's actual state it mostly re-discovers settled ground:

1. **The publisher universe is already seeded and graded.** `procurement_publishers_seed.py` is a 50-row, enum-validated, source-of-truth registry that already names ~all of the brief's semi-state "promising clusters" with landing URLs, grain, format, privacy-risk, and direct file URLs harvested from live HTTP (`:231-322` is a 2026-06-04 discovery sweep). The brief adds *more bodies* (LDA, SBCI, MARA, QQI, HEA, EirGrid, ESB) but the **mechanism** — seed → `probe_procurement_publishers.py` → `procurement_public_body_extract.py` → `public_payments_fact` — already exists and is the right home for any net-new PO/payment publisher. New bodies are a one-line seed edit, not a research project.

2. **The accountability/lifecycle layer is already a verdicted backlog.** `new_public_money_legal_sources_claude_backlog.md` (2026-06-04) is, almost item-for-item, the brief's §2/§4/§5/§6: PI2040, CPO, NTA/TII/HSE board minutes, C&AG, PAC, REV, LA audit, grants, LA budgets, FOI/AIE — each with a probe spec and a `value_kind`. The brief surfaces **no new analysis** on these; it just re-lists them with more semi-state instances.

3. **The genuinely-new material clusters at the wrong end of every axis.** Board minutes and FOI/protected-disclosure logs are: PDF-or-portal (format-hard), per-body bespoke (31-LA-style maintenance drift × ~15 semi-states), carry **~0% extractable € values** (NTA already graded that way at `DATA_MAP.md:101`), and FOI logs by construction **prove records exist but contain no extractable content** — a `foi_lead` layer, never a fact table. Protected-disclosure "datasets" are a single annual integer per body.

4. **Value-grain discipline holds — if anything is built.** The brief correctly never invites summation, and the project's controlled vocab (`backlog:62-79`, `DATA_MAP.md:107-119`) already has the right buckets (`board_approved_award`, `audit_finding`, `foi_lead`, `grant_or_subvention`, `future_project_cost_range`). The risk isn't grain-mixing; it's **ingesting low-join context the UI can't surface** while the real bottleneck (a procurement page, a council-finance page) stays unbuilt.

---

## Devil's Advocate

**Is board-minute / risk-register / disclosure-count ingestion mission-serving?** Mostly **no.** The mission (per `COMPETITIVE_LANDSCAPE` via `IDEAS.md:16`) is **cross-source joins on entities a user searches** — a TD, a supplier, a constituency, a public body. Test each new cluster against "does it join to something searchable, and is it a *dataset* not a *records-exist signal*":

- **Board minutes (TII/NTA/HSE/LDA/SBCI/MARA):** join key = a project name buried in free-text prose; cadence quarterly/monthly × ~15 bodies; format = redacted PDF; €-content ≈ 0. This is the **scope-sprawl trap**: it reads like due-diligence but yields an unmaintainable per-body PDF-prose pipeline whose output is "the board discussed procurement" — not searchable, not summable, not joinable. **Reject as a cluster.** (The one narrow exception — a *contract-award noted in minutes* with a named supplier + € — is already the `board_approved_award` value_kind in the backlog and should ride the NTA scoped probe, not a new effort.)

- **FOI / AIE / protected-disclosure logs (~20 sources in §1+§6):** this is the clearest **"records-exist, no content"** case. A disclosure log proves a request was made; it carries no spend, no contract, no verifiable fact — and the requester field is **PII to strip**. As an investigative *lead* layer (`foi_lead`) it's already scoped and correctly deferred. As ingestion it's **vanity** — it never joins to anything a user searches. **Reject.**

- **Protected-disclosure annual counts (Revenue/HSA/IDA/SBCI/…):** a single integer per body per year. Not a dataset. **Reject.**

- **PII / defamation exposure:** the brief repeatedly steers toward "who did what" board/disclosure material. Board minutes name natural persons and redact selectively; FOI logs name requesters; CPO/Housing-Act material leaks private addresses (already guarded, `IDEAS.md:117`); protected-disclosure context near a named individual is defamation-adjacent. The project's privacy precedent (personal insolvency suppressed; judiciary anonymised) means most of this material would have to be **stripped to the point of carrying no signal**. The juice isn't worth the squeeze.

**Verdict on the brief as a whole:** *most of this is out of scope.* The right reading is not "ingest the board-minute / FOI / disclosure web," it's "the few **structured registries** here that complete a surface already half-built." Everything else is either done, already scoped, or sprawl.

---

## Data Quality & Enrichments

The handful worth keeping, with discipline:

- **ERDF/EU-funds beneficiaries (NWRA + Southern + DPER 2014-20):** **probe-confirmed clean.** EU "List of operations" is a *mandated, standardised* schema → low maintenance, stable structure across the 3 Irish publishers. Named beneficiary + Beneficiary ID + named contractor → joins to the **supplier/CRO dimension** the project already owns. `value_kind = grant_or_subvention` with the allocated-vs-paid split kept separate; **never sum with procurement** (different grain; `Total cost of the operation` is total project cost incl. private co-finance, not state spend). PII: beneficiaries are mostly bodies/companies; a sole-trader/individual quarantine (same rule as procurement) covers the tail. Feeds **Supplier Dossier** + a constituency funding angle.

- **State Boards register (membership.stateboards.ie + publicjobs annual reports):** **complements, doesn't duplicate, Iris.** Iris captures appointment *events*; the register is the *current roster + the public-body universe* (publicjobs cites 240+ agencies). That universe is exactly the missing spine behind every "Public Body Profile" join (procurement publisher ↔ C&AG audited-body ↔ AFS). Tractability **confirmed by the 2026-06-11 deeper probe** (see Audit table note above): static HTML, dept → board → member-table hierarchy, six clean fields per member incl. basis-of-appointment; no occupation/employer field, no export — plain scrape. Join key = body name (alias table needed). PII: board members are public office-holders — low risk, but keep to **role + body + term**, no contact data.

- **Housing Adaptation Grants** (already scoped 🔬, zero-PII, LA-aggregated) and **LA budget tables** (already scoped, 1:1 A–H division match to AFS) are the cheapest items that complete the **Council Finance / "Your Area"** surface — but they're *already in the backlog*, so they belong to that effort, not this brief.

Everything else (board minutes, FOI logs, disclosure counts, annual-report PDFs, governance-code/ToR pages) is context-prose with no clean join key — enrichment only in the sense of "background reading," not data.

---

## Build / Defer / Reject

| item | verdict | value / effort | reason |
|---|---|---|---|
| Semi-state PO/payment publishers (Irish Rail, ESB, EirGrid, NTA, TII, HSE, Tusla, NTMA, Courts, Pobal, SEAI, etc.) | **Already scoped/seeded** | — | `procurement_publishers_seed.py:83-322`; net-new bodies = 1-line seed edit, not research |
| eTenders / OGP / CWMF / TED | **Already done** | — | gold + chain (`DATA_MAP.md:57,153-158`) |
| LA AFS / LA budgets / LGAS audit reports | **Already scoped/built** | — | AFS ②; LA-budget+LGAS-audit ⓪ (`IDEAS.md:51-55,114`) |
| Project Ireland 2040 tracker | **Defer (already scoped, weak)** | low / med | cost is bands not € (`IDEAS.md:102,120`); revisit only as project-spine for Infra Project Profile |
| C&AG + PAC report metadata | **Defer (already scoped)** | med / low | metadata-only rides existing poller; accountability layer, not a fact |
| Housing Adaptation Grants | **Build — via existing backlog** | med / low | zero-PII LA money-fact; belongs to Council-Finance surface (`IDEAS.md:115`) |
| LA budget tables (planned-vs-actual) | **Build — via existing backlog** | med / low | 1:1 AFS division match; completes Council-Finance variance story |
| **ERDF / EU-funds beneficiaries (NWRA + Southern + DPER)** | **Build (shortlist #1)** | high / low | probe-confirmed standardised XLSX; named beneficiary+contractor → supplier dim; new grant grain |
| **State Boards register (current roster + body universe)** | **✅ BUILT 2026-06-12** | high / med | `extractors/stateboards_roster_extract.py` + `wikidata/stateboards_wikidata_enrich.py` → `v_stateboards_roster` / `v_stateboards_boards`; `stateboards` pipeline chain |
| NTA/TII/HSE/LDA/SBCI/MARA board minutes | **Reject** | low / high | free-text PDF, ~0% €, per-body drift, PII; award-in-minutes already = `board_approved_award` on NTA probe |
| FOI / AIE disclosure logs (~20 bodies) | **Reject (keep as `foi_lead` only)** | low / high | records-exist, no extractable content; requester PII; never joins |
| Protected-disclosure annual counts (Revenue/HSA/IDA/SBCI/…) | **Reject** | ~0 / med | one integer per body/year — not a dataset |
| Annual reports / governance codes / ARC ToRs / "Where Your Money Goes" | **Reject** | low / med | narrative context, no clean join key; `whereyourmoneygoes` is a dashboard, not a feed |
| Planning apps / MyPlan / derelict-site registers | **Defer** | med / med | spatial; needs LA→constituency crosswalk (the standing blocker, `IDEAS.md:139`); pairs with housing effort |

**Shortlist (≤5) that complete an EXISTING half-built surface:**
1. **ERDF/EU-funds beneficiaries** → feeds *Supplier Dossier* + constituency funding; `value_kind=grant_or_subvention`, never sum with award/payment. (probe-confirmed)
2. **State Boards register** → feeds *Public Body Profile* (the public-body universe spine joining publishers ↔ C&AG-audited ↔ AFS); role+body+term only. (probe-first)
3. **Housing Adaptation Grants** *(already scoped)* → *Council Finance / "Your Area"*; LA-aggregated, zero PII.
4. **LA budget tables** *(already scoped)* → *Council Finance*; planned-vs-actual variance off the 1:1 AFS division match.
5. **C&AG/PAC report metadata** *(already scoped)* → *Public Body Profile* accountability tab; metadata-only on the existing poller, `value_kind=audit_finding`.

Items 3–5 are **already in `new_public_money_legal_sources_claude_backlog.md`** — listed here only to show what *this* brief's clusters map onto. The brief's net-new contribution is items **1 and 2** plus a large reject pile.

---

## Bottom Line

This brief is a thorough but largely redundant re-issue: roughly 85% of its clusters are already ingested (eTenders/TED/AFS), already seeded (the entire semi-state PO/payment universe lives in `procurement_publishers_seed.py`), or already verdicted in `new_public_money_legal_sources_claude_backlog.md` (PI2040, CPO, C&AG, PAC, REV, LA audit/budget, FOI, board minutes). Its genuinely-new material — the board-minutes, FOI/AIE, and protected-disclosure web of §1 and §6 — sits at the wrong end of every axis the project cares about: PDF/portal formats, per-body 31-LA-style maintenance drift, ~0% extractable euros, PII/defamation exposure, and "records-exist" signals that never join to a searchable entity. That is scope sprawl into general due-diligence the project explicitly can't maintain while its real bottleneck is *surfacing* the procurement/AFS data it already holds. Keep exactly two new things: the **ERDF/EU-funds beneficiary lists** (probe-confirmed clean, standardised XLSX, joins to the supplier/CRO dimension, new grant grain) and a **probe-first State Boards register** (the public-body-universe spine that complements — never duplicates — the Iris appointment events and unlocks Public Body Profile). Reject the rest, and spend the effort moving the existing ②/③ backlog rightward to a page.
