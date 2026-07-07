# DATA_GRAINS.md — money grains and trust boundaries

The single most important rule in Dáil Tracker: **money fields are not
interchangeable.** Procurement award values, framework ceilings, call-offs,
purchase-order commitments, actual payments, audited expenditure, election
expenses, political donations, and parliamentary allowances are different
*grains*. They answer different questions, come from different regimes, and are
measured at different points in a contract's life. **Only sum values where a view
explicitly marks them as safe to sum.**

This document defines each grain, states the never-sum rules, and sets out the
trust boundaries that apply to every figure the project shows. It is the
companion to [`DATA_LIMITATIONS.md`](DATA_LIMITATIONS.md) (per-dataset caveats)
and [`SOURCES.md`](SOURCES.md) (what is implemented).

---

## 1. The grains

| Grain | What it is | What it is **not** | Where it lives |
|---|---|---|---|
| **Award value** | Estimated contract/award value at the point of award (eTenders/TED notice). | Cash paid. | `procurement_awards` (`value_kind`, `value_safe_to_sum`) |
| **Framework / DPS ceiling** | Notional multi-year *maximum* / route-to-market value. Repeated across every supplier row on a multi-supplier framework. | Drawdown or spend. | `procurement_awards` (`is_framework_or_dps`) |
| **Call-off** | A purchase made under a framework. | The full contract universe; not additive with the framework ceiling. | `procurement_awards` (`is_call_off`) |
| **PO committed** | A purchase-order commitment from a published >€20k disclosure. | Final cash paid. | `procurement_payments_fact` (`amount_semantics`, `realisation_tier`) |
| **Payment actual** | A realised payment, where the source's semantics support it. | An authoritative ledger; it is extraction-derived. | `procurement_payments_fact` (`paid_flag`, `realisation_tier`) |
| **Audited expenditure** | Accounting expenditure from Annual Financial Statements / accounts. | A cash-flow or a PO total. | LA AFS facts, `cso_*` government-finance |
| **Donation** | A declared political donation (above threshold). | Campaign spend. | `sipo_donations` |
| **Candidate expense** | A candidate's own campaign-spend return. | A donation; not the national-agent figure. | `sipo_candidate_expenses_fact` |
| **Party / national-agent expense** | A party's campaign-spend return filed by the national agent. | The same as candidate spend (overlapping, non-additive). | `sipo_*_expense_*`, `sipo_ge2020_*` |
| **Allowance / payment (T&A)** | A parliamentary Travel & Accommodation Allowance or payment. | Procurement, salary, or expenses. | `payments_full_psa` |

### Schema columns that carry the grain

The procurement facts are self-describing — the safety rule travels with the
row, it is never inferred at read time:

- **`value_kind`** — e.g. `awarded`, `estimate_advertised`, `framework_ceiling`.
- **`value_safe_to_sum`** — boolean. **The only column you may `SUM()` on.**
- **`value_shared_across_suppliers`** — a multi-supplier framework repeats one ceiling per supplier row; summing double-counts.
- **`amount_semantics`** / **`realisation_tier`** — `ordered` (PO commitment) vs `paid` (actual payment) on the payments fact.
- **`vat_status`** — never total across VAT bases; only ~32k of ~424k payment rows carry a known basis.
- **`disclosure_basis`** / **`disclosure_threshold_eur`** — the regime a row was published under (see §3).
- **`supplier_class`** — exclude `public_body` to avoid counting central→council grant transfers *and* council→contractor payments as one (double-count trap).

---

## 2. The never-sum rules

> Do **not** add public-body payments to procurement awards.
> Do **not** treat framework / DPS ceilings as spend.
> Do **not** add SIPO donations to election expenses.
> Do **not** sum GE2020 and GE2024 figures into one ledger.
> Do **not** union or sum eTenders and TED award registers.
> Do **not** reconcile local-authority AFS against PO/payment disclosures without a stated methodology.
> Do **not** sum candidate expenses and national-agent expenses (overlapping views of the same spend).

### Concrete magnitudes (why this matters)

| Trap | Naive total | Sum-safe / honest figure | Multiplier |
|---|---|---|---|
| eTenders awards (sum all rows) | ~€649bn | ~€15.6bn (`value_safe_to_sum`) | ~40× overstated |
| TED awards (sum all rows) | ~€624bn | meaningless — 375 pan-EU outliers ≈ €586bn | trust **count + median**, never sum |
| eTenders ∪ TED | — | **never union** — ~66% of TED winners also appear in eTenders by name | siblings, cross-referenced per firm |
| NPHDB raw sum | dominated by one row | one €107.6m BAM adjudicator award ≈ 49% of corpus | never headline the raw sum |

### Award ≠ payment ≠ audited spend

These three meet only at the **supplier spine**, never blended into one number:

- **Awards** carry CPV (what the money is *for*) but are an estimate at award time.
- **Payments** carry no CPV (so "what was it for?" is answerable only on the award side) and are extraction-derived from PDFs.
- **Audited expenditure** is accounting data at a different grain again.

Any aggregate built from extracted PDFs is a **floor** — "at least €Y, from the
documents we could read" — never the definitive amount.

---

## 3. Disclosure regimes are not uniform

The >€20k payments fact is **not** a single "€20,000 / Circular 07/2012" regime.
The basis and threshold vary per publisher and are carried per row:

| Publisher type | Threshold / basis |
|---|---|
| Most departments / agencies | over €20k, FOI Act 2014 model publication scheme |
| HSE | model threshold €100k |
| CHI | over €25k incl-VAT |
| Utilities (ESB, EirGrid, Uisce Éireann) | outside the scheme entirely |

The same is true of **AFS grains** (per-LA revenue net-expenditure, per-LA
capital expenditure, national amalgamated layer, and the cash-PO
`la_payments_fact` are distinct — sum only within a (council, year)) and of
**political finance thresholds** (donations only above the party threshold;
candidate expenses bounded by per-constituency statutory limits of
€38,900 / €48,600 / €58,350).

---

## 4. Real-terms (deflation) caveat

Procurement values can be shown in real terms using a chain-linked CSO **CPA07**
CPI deflator (2012→2025 ≈ +24.7%). Deflation **re-expresses, it does not
correct** — it scales a nominal € into base-year purchasing power and neither
creates nor fixes magnitude errors. A missing year returns null (treated as
"leave nominal / exclude"), never 1.0. General CPI **understates** the 2021–22
construction-cost surge, so a construction-materials WPI (WPM39) is the secondary
deflator for construction-heavy spend. ~41% of summable awards are multi-year but
booked to a single year, so single-year deflation is approximate and labelled.

---

## 5. Trust boundaries

These apply to **every** figure the project shows.

### 5.1 What the project is

- **Not an official record.** The upstream publisher is authoritative in every case.
- **A discovery / research / accountability layer.** Strongest for finding leads and cross-referencing public records, weakest as a final authority on a single number.
- **Provenance-preserving.** Every displayed claim should trace to a source query or public document. High-stakes claims should be checked against the original PDF/source URL.

### 5.2 What it does **not** do

- It does **not** infer wrongdoing, influence, or causation.
- A declared interest, lobbying contact, payment, or attendance figure is **not** evidence of wrongdoing.
- **Lobbying overlap is co-occurrence, not causation.** An organisation appearing in both procurement and lobbying data is a lead, not proof of influence.
- **A ministerial diary meeting is access, not proof of influence** — diaries are self-curated, non-exhaustive, and quarterly-in-arrears.
- **A single-bid procurement signal is a market signal, not a verdict.**
- **More disclosure can mean more transparency**, not more risk — absence of declared interests is not proof none exist.

### 5.3 Extraction and matching are imperfect

- PDF/OCR extraction can fail silently (a parser can succeed technically while shipping incomplete rows). Government websites and PDF layouts change.
- Entity matching can miss or collide. The normalised-name join key is "an engineering compromise, not a legal identity key." CRO match rates run ~46–61%.
- CRO / lobbying / charity / supplier joins are **leads, not proof**.
- OCR figures are **not authoritative** without source verification; SIPO and ministerial-diary OCR rows carry explicit "verify against the official PDF (page N)" caveats and confidence flags.

### 5.4 Privacy boundaries

- Some rows are **privacy-quarantined** (likely-personal suppliers set `public_display=False`; the parser refuses to write a leak).
- Some personal data must **never** be exposed through the API/catalog — the API catalogue is deliberately curated, not an auto-dump (SIPO donor addresses, personal insolvency, etc. are excluded). See [`SOURCES.md`](SOURCES.md#api-resources).
- Cross-reference panels that name private individuals (CRO directors, charity trustees, CBI-register firms) sit in the **sandbox layer, not gold**, pending a documented lawful basis. Personal-insolvency notices are excluded by policy.

### 5.5 Minimum verification before public use

1. Check the official upstream source.
2. Confirm the date range.
3. Check the person's status at the time (TD / Senator / Minister / office-holder / former member).
4. Check whether the row came from API data, PDF extraction, or manual CSV/OCR.
5. For lobbying, confirm whether the count is distinct returns or exploded activity rows.
6. For interests, check whether a blank means `nil`, `not published`, `not extracted`, or `not applicable`.
7. For generated URLs, open the link and confirm it points to the expected record.
