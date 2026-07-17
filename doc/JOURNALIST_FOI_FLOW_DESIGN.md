---
tier: SPEC
status: LIVE
domain: sources
updated: 2026-06-22
supersedes: []
read_when: designing or explaining the journalist investigation-to-FOI-request flow that joins Dáil Tracker spend data with publicinformation.ie's FOI log
key: SPEC|LIVE|sources
---

# Design doc — Journalist investigation → FOI request, on a combined spend + FOI-log dataset

**Audience:** gingertechie (publicinformation.ie) — viability assessment for a partnership.
**Author side:** Dáil Tracker (Irish public-spending / accountability corpus).
**Status:** Evidence-backed design (2026-06-22). All numbers below are from real queries against
both datasets, not estimates. Caveats are called out inline.

---

## 0. TL;DR — is this viable?

**Yes, and the two datasets are unusually complementary.** You hold the *demand* side of Irish
transparency (what citizens/journalists have *asked* public bodies, and what was released or
refused). We hold the *supply* side (what those same bodies actually *spent*, *bought*, and *who
lobbied them*). Neither dataset is very interesting alone to a mass audience; **joined, they turn a
static FOI directory into an investigation engine that ends in a new, well-targeted FOI request.**

The empirical join works (72% of our spending publishers match your body register on a naive
normalised name join; the misses are short-name variants fixable with a one-time crosswalk). The
journalist flow is not hypothetical — we reproduced it end-to-end on your real records (§5).

The honest limit: **your disclosure-log data is lead-grade, not fact-grade** (~46% of records are
fully analysis-usable). That's fine — in this flow the FOI log is used to *find leads and check what's
already been asked*, not as a clean fact table. Design around that and it's a strength.

---

## 1. The two assets, audited

### 1a. Your FOI corpus (`publicinformation-data/public/`, audited 2026-06-22)
| Measure | Value |
|---|---|
| FOI disclosure records (public copy) | **56,894** |
| Bodies with ≥1 record | 74 |
| Bodies with ≥50 analysis-grade (dated + described) records | **45** |
| Processed bodies (status file) | 229 · **52** with a parsed FOI log · **83** with an FOI email |
| Decision status split | Part-Granted 16,323 · Refused 13,338 · Granted 12,318 · (canonical 80.8%, null 13.2%) |
| **Largest requester type** | **Journalist — 17,801 records** (+ Media 1,307) |
| Decision-date span | 2019–2026, steady ~3,000–4,300/yr |
| Per-body identity (CSO) | `public_body_id`, `parent_id`, `government_department_id`, `sector`, `nace_code`, **`cro`** |
| Plus | topic tags (`topics.json`; Housing = 3,088), live search API, **outreach engine** (tokenised email + Mistral reply-classification) |

**Quality (from your own `baseline.json`/`eval`):** PDF extraction 94.9%, but clean-extraction only
55.2%; decision_status canonicalised 73.8%; "valid" records (real date + non-empty summary) ≈ 29.7k
(~46%); FOI-email accuracy 26.7%. Column-mapping itself is strong (98.7%). **Verdict: a genuinely
useful ~30k-record core, wrapped in a noisy tail — treat as leads + corroboration.**

### 1b. Our spend / accountability corpus (`data_coverage`)
| Domain | Scale |
|---|---|
| Public-body payments (>€20k PO/payment lines) | **227,698 lines**, €39.07bn (safe), 72 publishers, 20,833 suppliers, 2012–2026 |
| Procurement awards (eTenders/OGP) | 44,120 rows, €11.76bn (safe), 1,889 authorities, 10,004 suppliers, 2013–2026 |
| TED (EU OJ) award notices | 36,603 notices, €16.99bn, winners 2024+ |
| Lobbying × procurement overlap | 235 firms on both registers (Dell €124m, BAM €116m, Sisk €114m …) |
| Competition signal | TED 2024+ single-bid rate per buyer (Univ Galway 73.9% — highest) |
| Council accountability | NOAC collection rates, derelict-sites levy, planning overturn; 31 CE roster |
| Live gap canary | `source_fetch_failures` — which bodies' spend feeds are breaking *right now* |

**Money-grain rule (must survive any join):** awards (ceiling at award), payments (cash out), and
allowances are three different grains — never summed. Our facts carry `amount_semantics` /
`realisation_tier` / `value_safe_to_sum` flags; the flow preserves them.

---

## 2. The empirical join — does body↔body actually match?

Tested by normalising names (lowercase, strip punctuation + corporate/stopword tokens) and matching
our spend publishers/authorities against your `public_body_name` register:

| Join | Result |
|---|---|
| Our **72 payment publishers** → matched to your body register | **52 (72%)** |
| Our payment publishers that **also have a parsed FOI log** in your data | **19 (26%)** |
| Our **2,249 procurement authorities** → matched (naive) | 133 (6%) — but the *high-value* ones (depts, councils, big agencies) match; the long tail is tiny/EU/one-off buyers |

**The ~20 unmatched payment publishers are all short-name/variant misses, not real gaps** — e.g.
`National Paediatric Hospital Development Board`, `Children's Health Ireland (CHI)`, `Tusla – Child
and Family Agency`, `National Transport Authority`, `Teagasc`, `Munster Technological University`.
All exist in your 229-body register under a different surface form.

**Conclusion:** a **one-time `public_body_id ↔ our publisher_id` crosswalk** — built with our
existing name-normalisation/NFKD tooling and hand-checked for the top ~100 bodies by spend — closes
the gap to near-100% for the bodies that matter. After that, `cro` gives a clean secondary join for
incorporated bodies, and `government_department_id`/`parent_id` give department-family rollups. This
is the single dependency for everything downstream (Phase 0).

---

## 3. The journalist investigation → FOI flow (the core design)

A repeatable 6-stage pipeline. Each stage is backed by one dataset; the hand-offs are the product.

```
 ┌── 1. ENTRY ─────────────────────────────────────────────────────────────┐
 │ Journalist starts from a body, a topic, or a supplier.                   │
 │   • "What did the Dept of Children spend on accommodation?"  (our spend)  │
 │   • "Show me single-bid contracts at University X."          (our TED)    │
 └───────────────────────────────┬─────────────────────────────────────────┘
                                 ▼
 ┌── 2. ANOMALY (our data) ──────────────────────────────────────────────────┐
 │ Surface the signal: a large/odd payment, a settlement, a single-bid       │
 │ cluster, a lobbying×award overlap, a BROKEN spend feed (live canary).     │
 │ Carry the grain flag (committed vs paid) so it's honest.                   │
 └───────────────────────────────┬───────────────────────────────────────────┘
                                 ▼
 ┌── 3. WHAT'S PUBLISHED? (our data) ────────────────────────────────────────┐
 │ Is the detail already in the structured record? If yes → no FOI needed.   │
 │ If the record STOPS here (no sub-tier, no per-location, no reason text) →  │
 │ that boundary is the FOI target. (We literally know where our data ends.)  │
 └───────────────────────────────┬───────────────────────────────────────────┘
                                 ▼
 ┌── 4. ALREADY ASKED? (your FOI log + search API) ──────────────────────────┐
 │ Search your disclosure log for the body + topic:                          │
 │   • GRANTED before → fetch the released doc; you're done, no FOI.          │
 │   • REFUSED before → you learn the body resists + the refusal grounds →    │
 │     craft a narrower request and cite the prior refusal for internal review.│
 │   • NEVER asked → clean new FOI.                                           │
 └───────────────────────────────┬───────────────────────────────────────────┘
                                 ▼
 ┌── 5. DRAFT THE ASK (templated) ───────────────────────────────────────────┐
 │ Auto-compose a request scoped to the exact gap, pre-filled with the body's │
 │ FOI email (your register) + grain-correct figures (our data) for context.  │
 └───────────────────────────────┬───────────────────────────────────────────┘
                                 ▼
 ┌── 6. SEND + TRACK (your outreach engine) ─────────────────────────────────┐
 │ Tokenised reply address → Scaleway send → inbound webhook → Mistral        │
 │ classifies the reply (provides_files / refusal / no_log …). Released docs  │
 │ + figures flow back into the corpus with FOI-sourced provenance.          │
 └────────────────────────────────────────────────────────────────────────────┘
```

**Why each side is load-bearing:** stages 2–3 (where's the money, where does the record stop) are
*ours*; stage 4 (what's already been asked/refused) is *yours*; stage 6 (send + classify) is *yours*.
Neither side can run the whole loop alone. That's the partnership in one diagram.

---

## 4. The decisive UX move: "already asked?" before "ask"

The thing that makes this more than another FOI portal: **stage 4 stops journalists re-filing what's
already public, and turns refusals into strategy.** Your decision_status data makes this real — on the
asylum-accommodation topic alone, requests run **194 Refused : 59 Granted**, so a journalist learns
*before drafting* that this body refuses the broad version and must narrow the ask. No existing Irish
tool does this. It's directly buildable on your `search.ts` + `decision_status` field.

---

## 5. Two worked examples (end-to-end on REAL records)

### Example A — National Children's Hospital: what the €130.7m settlements covered
1. **Anomaly (our payments_fact):** National Paediatric Hospital Development Board → BAM Building =
   **€130.7m**, of which **€126.7m is two line items** whose own descriptions read *"Conciliator's
   Recommendation No. 25 — Notice of Dissatisfaction issued"* (€107.6m, 2024) and *"…No. 29…"*
   (€19.1m, 2025). Grain = `po_committed`.
2. **Where our record stops:** the *heads of claim* (what the settlements paid for) and the
   sub-contractor tier beneath BAM are not in any structured feed.
3. **Already asked? (your log):** NPHDB has **no disclosure log** in your data; Dept of Health holds
   **70** children's-hospital FOI records and DPER **33** — context, but not the settlement detail.
   So the settlement question is **genuinely un-asked at source.**
4. **The FOI (to NPHDB, email from your register):** *"Please provide Conciliator's Recommendations
   No. 25 and No. 29 on the main construction contract with BAM Building Ltd; the Notices of
   Dissatisfaction referenced; and a schedule of amounts paid under each, broken down by head of
   claim, 1 Jan 2024–present."*

### Example B — Department of Children: asylum/accommodation spend per location
1. **Anomaly (our data + live canary):** DCEDIY is our biggest money gap — and `source_fetch_failures`
   shows DCEDIY's own PO PDFs **failing ingestion right now** (circuit-breaker tripped, 9 files
   skipped). IPAS/Ukraine accommodation spend per provider/location is invisible.
2. **Already asked? (your log — DCEDIY, 1,172 records):** an individual asked for *"a complete list of
   buildings, properties, or accommodation facilities that have received I[PAS funding]"* →
   **Refused (2025-04-09)**. Journalists asked for complaints logs → **Part-Granted**. Topic-wide:
   **194 Refused : 59 Granted.**
3. **Strategy from the refusal:** the broad "list of all buildings" is refused. Narrow it to an
   aggregate that's harder to refuse, and cite the prior refusal for internal review.
4. **The FOI (to DCEDIY):** *"A schedule of total payments of €20,000+ to accommodation providers
   under IPAS and the Ukraine (BOTP) programme for 2023–2025, by provider and county (not individual
   addresses), with contract value and start/end date."*

Both examples were assembled purely from data already in the two systems. That is the viability proof.

---

## 6. Which of OUR tables are most valuable to YOU (publicinformation.ie)

Ranked by how much each turns your FOI body-directory into an investigation tool. All are per-public-
body, so they attach directly to your `public_body_id` via the Phase-0 crosswalk.

| # | Our table / view | What it gives your users | Why it's high-value to you |
|---|---|---|---|
| 1 | **`procurement_payments_fact`** (227k lines, per-body outbound spend) | "What did *this body* actually pay, to whom, for what" | The single best companion to an FOI page — every body page gains a real spend profile + the lines where the description stops (FOI targets). Joins on body. |
| 2 | **`source_fetch_failures`** (live canary) | "Which bodies' spend disclosures are broken/missing *now*" | A *live FOI-targeting feed* — broken/zero-harvest publishers are exactly where an FOI for the underlying records is justified. Unique to us. |
| 3 | **procurement awards + TED winner history** | "What contracts this body awarded, to which firms, at what value" | Contracts are the other half of FOI accountability; pairs with disclosure logs on tenders. |
| 4 | **`procurement_competition`** (single-bid, TED 2024+) | "How competitive is this buyer" (single-bid rate) | An integrity signal per body that *motivates* an FOI for single-tender justifications (factual signal, never a verdict). |
| 5 | **`procurement_lobbying_overlap`** | "Which firms paid by this body also lobbied government" | Turns a body page into a influence-context page; co-occurrence only, caveat travels with it. |
| 6 | **ministerial diaries / `who_ministers_meet`** | "Who the relevant minister met, on what topic" | Context for department-level FOIs; corroborates lobbying. |
| 7 | **NOAC council accountability + `la_chief_executives`** | Collection rates, derelict-sites levy, who runs each council | Directly enriches your 31 local-authority body pages. |
| 8 | **`supplier_groups`** (e.g. BAM rollup) | Corporate-group consolidation across legal entities/SPVs | Lets a body's payments roll up to the real corporate parent — the children's-hospital case needs this. |

**Lower value to you:** attendance, votes, member interests, SIPO election finance, judiciary — these
are person/politics-centric, not body-spend-centric, so they don't enrich an FOI body directory.

**Delivery options for you:** (a) consume our published `/v1/data` exports / parquet for the per-body
tables; (b) we expose a small per-body JSON endpoint keyed by the crosswalk; (c) batch hand-off of the
crosswalk + payments-by-body summary. (a) or (b) keep it data-layer only (no codebase coupling; AGPL
stays clean).

---

## 7. What we'd need from you / open questions

1. **`public_body_id` stability** — can we pin a crosswalk to it (you persist an id-map, so we think yes)?
2. **Search API limits** — is `search.publicinformation.ie` OK for our build-time/poll use, or should we ingest `foi-disclosures.json` in batch?
3. **Outreach `/create` exposure** — do we deep-link users into your outreach UI, or can we call `/create` to launch a request from our gap pages?
4. **Quality filter** — expose the `valid`-grade + `missing_columns` flags in the API so we only surface analysis-grade records in the flow?
5. **Direction** — do you want our per-body spend tables flowing *into* publicinformation.ie pages, the FOI flow living on *our* side, or both (mutual embed)?

---

## 8. Viability verdict & phased plan

**Verdict: viable and genuinely novel.** The join is real, the flow reproduces on live records, and
the two corpora are complementary rather than overlapping. The main work is a crosswalk and a UI; no
new heavy ingestion is required to prototype.

| Phase | Work | Depends on |
|---|---|---|
| **0** | Build `public_body_id ↔ publisher_id` crosswalk (name-norm + hand-check top ~100 by spend); secondary `cro` join | — |
| **1** | Ingest `foi-disclosures.json` + wire search API as a read source; show "FOI log" + "spend profile" together on a body page (filter to valid-grade) | Phase 0 |
| **2** | Build stages 2–4 of the flow: anomaly → "where our record stops" → "already asked?" on a body/topic | Phase 1 |
| **3** | Wire stage 5–6: templated draft → your outreach engine; classified replies feed back with FOI provenance | Phase 2 + outreach access |
| **4** | Close the loop: released figures patch the relevant facts (grain-caveated) | Phase 3 |

Start with the two worked examples in §5 as the demo — they already have all the data behind them.
