# NDFA / PPP contracts — new-source scoping (2026-06-15)

**Status:** SCOPING ONLY — no data pulled, no ETL. Verdict + source map + effort below.
**Why:** the payments↔tender linkage work (see `memory/project_payments_award_linkage_2026_06_15`)
left a structural residual: ~€9bn of public payments that link to NO eTenders/TED award, dominated
by **PPP/concession Special Purpose Vehicles** — `NBI Infrastructure DAC` (€1.34bn), `BAM Schools
Bundle Three Ltd`, `BAM School Bundle 4 Ltd`, `MPFI Schools Ltd`, `Inspired Spaces Bundle 5`,
`Direct Route Tuam`, `M50 Concession`, `Transdev`, courts/housing bundles. These were never
procured through the eTenders/TED *award-notice* corpus, so no amount of name-matching can link
them (proven: CRO-anchoring recovers €0 of this — no winning CRO entity exists). They need a
**new source**: the PPP project register.

---

## 1. The problem this source solves

A PPP availability deal awards a long-term contract to a **ring-fenced SPV** (`BAM Schools Bundle
Three Ltd`), legally distinct from the parent/consortium that builds it (`BAM Contractors Ltd`,
which *does* win eTenders and already links). The State pays the SPV a **unitary payment** (capital
+ finance + lifecycle FM, typically over 25 years). Our payment data captures those unitary
payments (Dept of Education, Courts Service, TII, etc.) but has no award record to tie them to.

A PPP project register would let us:
- **Label** an unlinked PPP payee as *"PPP unitary payment — [project], financial close [year],
  consortium [X]"* instead of "unlinked".
- **Reconcile** the SPV to the parent consortium that wins competitive tenders (BAM's PPP footprint
  + its eTenders footprint become one entity story).
- Supply the **award context** (procurer = NDFA, sponsoring dept, capital value, contract term)
  that eTenders/TED structurally lack for PPPs.

---

## 2. Source map — there is NO single clean dataset; it's a 4-source assembly

| Source | What it has | Format | Role |
|---|---|---|---|
| **DPER Central PPP Policy Unit** — [gov.ie PPP projects](https://www.gov.ie/en/public-private-partnership/publications/projects/) | Official project inventory: name, sector, sponsoring authority, status | HTML project pages (no Excel/CSV) | **Spine** — the canonical project list |
| **C&AG PPP overview chapters** — e.g. [2017 Ch.4](https://www.audit.gov.ie/en/find-report/publications/2017/chapter4-overview-of-public-private-partnerships.pdf) | Project-level financial tables: capital cost + unitary-payment commitments + totals | PDF (table extraction needed) | **Financial reconciliation** — the value figures |
| **NDFA project pages** — [ndfa.ie/projects](https://www.ndfa.ie/) (e.g. Schools Bundle, Higher Ed Bundle 1/2, Social Housing Bundle 3) | Per project: **SPV / PPP Co name**, equity consortium, build contractor(s), FM operator, financial-close + operational dates | HTML, one page per project | **SPV→consortium map** — the join key to our payees |
| **TII PPP** — [tii.ie roads PPP](https://www.tii.ie/en/roads-tolling/projects-and-improvements/ppp/) | The ~15 **road** PPPs (M50, Tuam, etc.): concession company, contractor, contract dates | HTML | Roads sector (different procurer than NDFA) |
| (corroboration) **NTMA/NDFA Annual Report** — [2024 NDFA report](https://www.ntma.ie/annualreport2024/documents/National-Development-Finance-Agency.pdf) | Aggregate unitary-payment liabilities; portfolio capital value (~€8.4bn advisory book) | PDF | Totals sanity-check |

**Special case — NBI (€1.34bn, the single biggest unlinked payee):** NOT an NDFA PPP. It is the
**National Broadband Plan** subsidy/concession, contracted by DECC to National Broadband Ireland
(Granahan McCourt). Needs its own one-row treatment, not the NDFA register.

---

## 3. Proposed shape — a CURATED registry, not a scraper

Scale is **small and finite**: roughly 30–40 PPP projects total (Schools Bundles 1–5, Higher-Ed
Bundles 1–2, Social Housing Bundles 1–3, Courts Bundle, Primary Care Centres, Grangegorman, the
~15 TII road PPPs, a handful of health/justice). This is **hand-curation territory** — the same
pattern as the other `data/_meta/*.csv` source-of-truth files (NACE map, publisher seed,
CRO-overrides) — NOT an automated ingest.

Deliverable: `data/_meta/ppp_project_registry.csv`, one row per SPV/payee, columns:

```
spv_payee_norm      name_norm_expr of the SPV as it appears in payments (the JOIN KEY)
project_name        "Schools Bundle 3", "M50 Upgrade", …
sector              education | housing | justice | health | roads | broadband
sponsoring_body     paying department/agency (Dept of Education, Courts Service, TII, …)
procurer            NDFA | TII | DECC
consortium          equity/parent (e.g. BAM PPP, Macquarie, DIF, John Sisk)
parent_cro_num?     CRO of the parent that wins competitive tenders, where identifiable
capital_value_eur   project capital cost (C&AG / NDFA)
unitary_term_years  contract length (e.g. 25)
financial_close_yr  award/close year
source_url          NDFA/C&AG/TII page the row was read from
```

The link/payments layer then left-joins payee_norm → registry to (a) tag the payment as a PPP
unitary payment and (b) attach project + consortium + parent_cro.

---

## 4. Value-taxonomy caution (must not break the never-sum rule)

A **unitary payment is its own value kind** — a long-term availability commitment bundling
construction + financing + 25-year facilities management. It is NOT comparable to, and must NEVER
be summed with, either an eTenders/TED **award ceiling** or a competitive-tender **realised
payment**. It is a distinct `realisation_tier` (a multi-decade PPP commitment) per the existing
`doc/PUBLIC_PAYMENTS_FACT_SCHEMA.md` taxonomy. Present it on its own track: *"paid €X this year
under a €Y, 25-year PPP unitary commitment."*

---

## 5. Verdict + effort

- **Value: HIGH.** Directly addresses the largest unlinked bucket and converts a confusing
  "unlinked" gap into an explained PPP track. Also surfaces the parent↔SPV relationship that
  matters for any "who really got the money" view.
- **Effort: LOW–MEDIUM, but CURATION not engineering.** ~1–2 days to read ~30–40 NDFA/TII/C&AG
  project pages into the CSV, cross-checked against the C&AG financial tables. No scraper, no API,
  no schema churn. The only code is a left-join + a `value_kind = ppp_unitary` label.
- **Privacy: clean** — all corporate / published. No PII.
- **Risk:** the C&AG figures are point-in-time and the bundles renamed/refinanced over the years;
  curate the SPV name as it appears in OUR payments (verify against `public_payments_fact`), not
  the prospectus name. Refinancing means one project can have >1 SPV name across years.

**Recommendation:** worth doing as the next enrichment after the linkage work plateaus. Build the
registry CSV by hand (high precision, small N), join in a labelling pass, keep PPP unitary payments
on a separate tier. Add a pointer in `doc/IDEAS.md` (the master idea map).

---

## 6. WHAT WAS PULLED & WHAT IS USABLE (2026-06-15 first pass)

Built **`data/_meta/ppp_project_registry.csv`** — 25 rows, keyed on `spv_payee_norm`
(= name_norm of the payee, so it joins straight to the payments fact). Sources verified live: the
[TII PPP page](https://www.tii.ie/en/roads-tolling/projects-and-improvements/ppp/) (8 toll +
5 DBFOM + 2 MSA road schemes) and NDFA / World-Bank PPP pages (Bundle 1 = MPFI/Macquarie,
Bundle 2 = Pymble, Bundle 5 = Inspiredspaces).

**Coverage — €2.83bn of the ~€9.0bn unlinked (≈31%) is now labellable:**

| group | rows | € observed | confidence |
|---|---|---|---|
| group | rows | PAID (payment_actual) | COMMITTED (po_committed) | confidence |
|---|---|---|---|---|
| NBI / National Broadband (DECC, **not** NDFA) | 1 SPV | €1.17bn | €0.17bn | high |
| Schools bundles (NDFA, Dept of Education) | 9 | ≈€0.66bn | €0 | high (B1/2/3/4/5) · med (Glasgiven/OHLA/CSM) |
| Roads / Luas (TII) | 11 | ≈€0.50bn | €0 | high (named schemes) · med (Egis/Globalvia O&M) |
| Courts bundle (NDFA, Courts Service) | 4 | €0 | ≈€0.16bn | high (BAM Courts) · low (CCC GP1 stubs) |
| **TOTAL** | 25 | **€2.50bn** | **€0.33bn** | — |

### ⚠️ The sums are NOT a clean €2.83bn — three corrections (do not over-state)

1. **PAID ≠ COMMITTED — never add them.** €2.50bn is `payment_actual` (money out); €0.33bn is
   `po_committed` (orders raised, not disbursed). They are different `realisation_tier`s — the
   project's never-sum rule. The **entire Courts bundle is commitment, not spend**, and €0.17bn of
   NBI is too. The registry now carries `paid_eur` / `committed_eur` as **separate columns** so they
   can't be re-merged.
2. **It's a ~14-year cumulative, not a figure.** The PAID total spans **2012–2026**; the annual
   run-rate climbs from €7m (2012) → ~€500–590m (2024–25). State it as *"at least €2.5bn paid to
   these SPVs across 2012–2026, as extracted, ~€500m/yr recently"* — never a single bare number.
3. **NBI is €1.17bn paid + €0.17bn committed, across 2 publishers.** Broadband policy moved from
   DCEEnv to the new **Dept of Culture, Communications** in 2025; the €0.17bn committed sits under
   the new dept (2025–26). Watch the 2025 boundary for continuity/overlap before any annual figure.

**Data-quality caveat:** **€0.26bn of the PAID total has NO year** (`year` null) — ~10%; it can be
totalled but not placed in time. (No single-row outliers though — max single row is 1–15% of an
SPV total, so no bogus total-row inflation.)

**Usable now:** the **label** (project · sector · procurer · consortium · paid-vs-committed ·
year-range) per SPV — converting the biggest "unlinked" gap into an explained *PPP track* tied to
the consortium/parent that wins competitive tenders. Left-join as `value_kind = ppp_unitary`, but
**keep paid and committed on separate lines and show the year span** — do not headline a blended €.

**NOT pulled (the gap):** per-project **capital values / unitary-payment schedules** — C&AG PPP
tables are image/compressed PDFs WebFetch can't parse (off-box OCR follow-up, the SIPO route).

**Low-confidence rows to firm up by hand:** `CSM PPP SERVICES` (which bundle?), `CCC GP1` /
`IPP CCC GP1` (Courts holding-co structure), the exact `Globalvia`/`Egis` road each O&M attaches to.
