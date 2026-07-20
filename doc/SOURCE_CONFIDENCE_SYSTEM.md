---
tier: SPEC
status: LIVE
domain: procurement
updated: 2026-06-28
supersedes: []
read_when: designing confidence badges, trust grades, or caveat wording across mixed-provenance procurement/BI data
key: SPEC|LIVE|procurement
---

# Source-Confidence & Caveat System

Design for the procurement / business-intelligence product. Goal: when one report mixes
official-API rows, PDF-extracted values, OCR rows, fuzzy and exact matches, manual drops,
sandbox sources and derived signals, **the reader can tell at a glance how much to trust each
item — and the system makes it structurally impossible to overclaim.**

This design **builds on fields that already exist** (see §10) rather than inventing a parallel
vocabulary. It standardises them into one cross-fact *confidence envelope*, derives a single
headline trust grade, and renders deterministic badges. No figure is invented; provenance and
licensing remain the owner's domain — this system only *surfaces* what the data already records.

---

## 1. Principles

1. **Trust is bounded by the weakest link.** A record's headline grade is the *minimum* of its
   component grades — an official-API value reached by a fuzzy name match is only as trustworthy
   as the match.
2. **The verb is the disambiguation.** Never a bare €. Every figure renders with its grain verb
   — *paid / ordered / awarded / up-to / estimated / donated / claimed*.
3. **Every total is a floor.** Coverage is partial and most € are parsed from documents. Headlines
   say "at least €X from N records", never "the total".
4. **Separation beats annotation.** Incompatible money-grains live in physically separate facts
   and separate UI sections; a caveat is the last line of defence, not the first.
5. **Link back or say why you can't.** Every figure carries a source URL/document, or an explicit
   "no public source" sentinel.
6. **The record, not a verdict.** We report what is published. We never imply causation
   (lobbying→contract) or wrongdoing (single-bid).

---

## 2. Data model — the confidence envelope

Eleven canonical fields travel with **every record that can reach a report**. Most already exist
on the gold facts; the design unifies their names and vocabularies and adds the few missing ones.

| # | Canonical field | Type | Allowed values | Derives from existing |
|---|---|---|---|---|
| 1 | `source_type` | enum | dataset family — see §5.A | `publisher_id` / `group`, `source_registry.group` |
| 2 | `extraction_method` | enum | `official_api`, `official_csv_xlsx`, `pdf_extracted`, `ocr_extracted`, `manual_drop`, `derived` | `parser_name`, `source_registry.check_type`, `minister_briefs.extraction_method` (scattered today) |
| 3 | `match_method` | enum | `exact`, `strong`, `fuzzy`, `weak`, `none` | existing `match_method` {exact_unique, exact_ambiguous, no_match} |
| 4 | `match_confidence` | float 0–1 / null | 1.0 / 0.7 / 0.5 / 0.2 / null | existing `match_confidence` {0.9, 0.5, 0.0} (rescaled) |
| 5 | `value_kind` | enum | 9 grains — see §4 | existing `value_kind` (subset) + `amount_semantics` |
| 6 | `safe_to_sum` | bool | true / false | existing `value_safe_to_sum` |
| 7 | `freshness` | struct | `{as_of, fetched_utc, stale_after_days, status}` | existing `freshness.json` |
| 8 | `source_url` | url / null | landing or listing page | existing `source_landing_url` |
| 9 | `source_document` | struct | `{url, hash, page}` | existing `source_file_url` + `source_file_hash` + `source_page_number` |
| 10 | `pipeline_status` | enum | `live`, `sandbox`, `experimental`, `quarantined` | `doc/DATA_MAP.md` maturity tiers + `data/_meta/quarantine/` |
| 11 | `caveat` | struct | `{auto_flags: [..], text: str\|null}` | existing `caveat_text_detected` + `source_caveat` |

Two supporting fields that already exist and gate everything money-related:

- `vat_status` ∈ {`incl_vat`, `excl_vat`, `unknown`} — never sum across different bases.
- `privacy_status` ∈ {`ok`, `review_personal_data`, `public`, `quarantined`} and `supplier_class`
  / `public_display` — the PII gate (§9 rule 4).

### Closed enums (extend `services/data_contracts.py`)

Today `data_contracts.py` validates `value_kind`, `realisation_tier`, `extraction_confidence`,
`vat_status`, `supplier_class`, `privacy_status` as **hard** `ColumnRule`s. This design adds four
more closed enums and widens `value_kind` to the product-wide union (§4):

```python
SOURCE_TYPE        = frozenset({...})            # §5.A, one per dataset family
EXTRACTION_METHOD  = frozenset({"official_api", "official_csv_xlsx",
                                "pdf_extracted", "ocr_extracted",
                                "manual_drop", "derived"})
MATCH_METHOD       = frozenset({"exact", "strong", "fuzzy", "weak", "none"})
PIPELINE_STATUS    = frozenset({"live", "sandbox", "experimental", "quarantined"})
VALUE_KIND = frozenset({                          # report-level union; each fact validates a subset
    "contract_award_value", "framework_or_dps_ceiling",   # awards fact (verified in data)
    "estimate_advertised", "pre_award_estimate",          # PLANNED grains (eTenders-live / TED)
    "po_committed", "payment_actual",                     # payments fact (verified in data)
    "net_expenditure_actual", "capital_expenditure_actual",  # AFS accrual
    "donation", "election_expense", "allowance"})         # Phase-1 ADD — no value_kind col yet
```

> **Reuse the existing stored strings — do not rename columns.** Real data (§14) carries
> `value_kind` as `contract_award_value` / `framework_or_dps_ceiling` on the awards fact and
> `po_committed` / `payment_actual` on the payments fact — *not* the tidier label names. The
> canonical enum therefore **adopts those exact strings**; the tidy names live only in the UI
> label layer (§4 "Grain label"). The widening is additive — no existing row changes meaning.
> The payments fact's contract stays `{payment_actual, po_committed}`; each fact validates its
> own subset.

---

## 3. The composite trust grade (one badge to read)

Readers should not parse 11 fields per row. Derive **one** headline grade with a pure,
testable function. Each component maps to a tier *ceiling*; the record's grade is the **minimum**
(weakest-link). This is the single most important guard against overclaiming.

```
trust_grade(record) = min(
    ceiling_extraction(extraction_method),
    ceiling_match(match_method),          # NONE when record is single-source (no join)
    ceiling_pipeline(pipeline_status),
    ceiling_freshness(freshness.status),
    ceiling_caveat(caveat.auto_flags),
)
```

| Grade | Label | Color token | Granted when every component is at least… |
|---|---|---|---|
| A | **Verified** | `signal-good` | official_api/csv · match exact or n/a · live · fresh · no blocking caveat |
| B | **Reported** | `accent` | pdf_extracted clean · match strong · live · fresh |
| C | **Extracted** | `surface-deep` | ocr_extracted or pdf+caveat · match fuzzy · live |
| D | **Indicative** | `signal-bad-subtle` | match weak · derived · sandbox/experimental · stale · estimate/ceiling |

Component ceilings (defaults — labels/thresholds are an owner sign-off, §13):

| Component | → A | → B | → C | → D |
|---|---|---|---|---|
| `extraction_method` | official_api, official_csv_xlsx | pdf_extracted (no caveat flag) | ocr_extracted, pdf+caveat | manual_drop, derived |
| `match_method` | exact, none¹ | strong | fuzzy | weak |
| `pipeline_status` | live | — | — | sandbox, experimental, quarantined² |
| `freshness.status` | ok | — | — | stale |
| `caveat.auto_flags` | none | — | vat/estimate note | blocking (e.g. parsed-amount-only) |

¹ `none` = no cross-reference was attempted (single-source record); it does **not** cap the grade.
A *failed* attempt is `weak`/`none`-with-confidence-0, which **does** cap it.
² `quarantined` never reaches a report at all (§9 rule 4) — listed for completeness.

---

## 4. Money-grain taxonomy (the never-sum core)

Nine grains, grouped into **five families**. `safe_to_sum` is only meaningful *within one family
and one partition*; **cross-family arithmetic is always forbidden** and has no shared key.

| `value_kind` (stored string) | Grain label | Family | `realisation_tier` | Verb | `safe_to_sum` default |
|---|---|---|---|---|---|
| `contract_award_value` | Award value | Procurement-award | AWARDED | "awarded" | with caution³ |
| `framework_or_dps_ceiling` | Framework ceiling | Procurement-award | AWARDED | "up to" | **NEVER** |
| `estimate_advertised` / `pre_award_estimate` | Estimated value | Procurement-award | PLANNED | "estimated" | **NEVER** |
| `po_committed` | PO committed | Procurement-spend | COMMITTED | "ordered" | yes (publisher × period) |
| `payment_actual` | Payment (actual) | Procurement-spend | SPENT | "paid" | yes (publisher × period × vat) |
| `net_expenditure_actual` / `capital_expenditure_actual` | Audited expenditure | Public-accounts | SPENT (accrual)⁶ | "spent (audited)" | within (council, year) only |
| `donation` ⁵ | Donation | Political-finance | — | "donated" | within (recipient, year) |
| `election_expense` ⁵ | Election expense | Political-finance | — | "spent on campaign" | within (candidate/party, election) |
| `allowance` ⁵ | Allowance | Member-finance | — | "claimed" | within (member, year, scheme) |

³ `contract_award_value` sums only for **single-supplier, non-framework** awards below the large-award
review floor (`is_large_award_review`, ≥€50M excluded). Framework ceilings repeat per supplier
and are notional maxima — never spent, never summed.

⁵ `donation` / `election_expense` / `allowance` **do not yet exist as a `value_kind` column** on
their facts (SIPO donations/expenses, member allowances). Adding the column to those grains is
Phase-1 work, not an existing field — until then they are grain-tagged only by their source_type.

⁶ AFS facts are tagged `realisation_tier=SPENT` (accrual), **not** BUDGET, and carry **no
`value_safe_to_sum` column** — summing is governed by the "(council, year)" caveat in code, not a
flag (verified §14). The envelope's `safe_to_sum` field is therefore absent on this grain.

**The never-sum matrix** (✅ may sum within partition · ⛔ never):

|  | award | ceiling | estimate | PO | payment | audited | donation | expense | allowance |
|---|---|---|---|---|---|---|---|---|---|
| **award** | ✅³ | ⛔ | ⛔ | ⛔ | ⛔ | ⛔ | ⛔ | ⛔ | ⛔ |
| **PO** | ⛔ | ⛔ | ⛔ | ✅ | ⛔⁴ | ⛔ | ⛔ | ⛔ | ⛔ |
| **payment** | ⛔ | ⛔ | ⛔ | ⛔⁴ | ✅ | ⛔ | ⛔ | ⛔ | ⛔ |

⁴ PO-committed and payment-actual are *both* "realised spend" but are different lifecycle points
with no shared key — `awarded − paid` and `ordered + paid` are fictions. Show one tier per
section. Also never sum `public_payments_fact` + `la_payments_fact` (the TII road-grant
triple-count trap) — `supplier_class=public_body` flags the intergovernmental transfers to strip.

---

## 5. Label definitions

### 5.A `source_type` — dataset family
`etenders_award`, `ted_award`, `public_body_payment`, `hse_tusla_payment`, `la_payment`,
`disclosed_bq_extract`, `cro_company`, `lobbying_return`, `charity_financials`,
`corporate_notice`, `ministerial_diary`, `sipo_donation`, `sipo_expense`,
`afs_audited_accounts`, `member_allowance`, `derived_signal`.

### 5.B `extraction_method` — how the value was lifted
| Label (UI) | Enum | Meaning |
|---|---|---|
| **Official API** | `official_api` | Structured pull from an official endpoint (CSO PxStat, Oireachtas API). Highest fidelity. |
| **Official CSV/XLSX** | `official_csv_xlsx` | Published machine-readable export (eTenders OGP CSV, data.gov.ie). |
| **PDF extracted** | `pdf_extracted` | Parsed from a born-digital PDF text layer (camelot/fitz, reading-order parser). |
| **OCR extracted** | `ocr_extracted` | Read from a scanned image (Tesseract / Vision / Paddle). Verify against source. |
| **Manual drop** | `manual_drop` | Hand-curated CSV / one-off owner-supplied drop. Trust = the curator. |
| **Derived** | `derived` | Computed/inferred signal (ratios, overlaps, links). Not a primary observation. |

### 5.C `match_method` — how an entity was cross-referenced
| Label (UI) | Enum | conf | Meaning | maps from |
|---|---|---|---|---|
| **Exact match** | `exact` | 1.0 | Company-number key, or unique normalised-name → single CRO entity. | `exact_unique` |
| **Strong match** | `strong` | 0.7 | High-similarity name match passing a verified threshold (future fuzzy lane). | (reserved) |
| **Fuzzy match** | `fuzzy` | 0.5 | Token/similarity match; plausible but unverified. | (reserved) |
| **Weak match** | `weak` | 0.2 | Ambiguous — name resolves to *several* companies; entity not established. | `exact_ambiguous` |
| **(none)** | `none` | null | Single-source record, or no match found. No entity claim made. | `no_match` |

> **This directly fixes the roadmap's flagged bug:** today 400 `exact_ambiguous` rows render
> identically to firm matches because the binary `_cro_pill` reads only `match_method` and an
> arbitrary `.first()` company number. Mapping `exact_ambiguous → weak` makes the UI tell the
> truth: *"possible match — several companies share this name"*, with no company-number assertion.

### 5.D `pipeline_status` — maturity
`live` (in production chain + surfaced) · `sandbox` (gitignored experiment) ·
`experimental` (derived/unvalidated signal) · `quarantined` (rejected — never served).

---

## 6. Badge taxonomy

Reuse the existing CSS foundation — `.dt-badge` family (role/status pills) and `.con-grain`
(grain-colour tags). Add three badge **rails** plus state badges and one explain-popover.

```
┌────────────────────────────────────────────────────────────┐
│  John Sisk & Son                                            │
│  €221,400,000  [ PAID ]  [ ✓ Verified ]  [ ⚷ Exact match ] │   ← three rails
│                 grain      trust            match           │
│  as of 2026-03-31 · OPW supplier payments · ↗ source PDF   │   ← provenance line
└────────────────────────────────────────────────────────────┘
```

**Rail 1 — Grain badge** (always, next to every €). Colour by family; text = verb-label.
`PAID` (green) · `ORDERED` (brown) · `AWARDED` (blue) · `UP TO — ceiling, not spent` (amber) ·
`ESTIMATE` (grey) · `AUDITED` (teal) · `DONATION` / `EXPENSE` / `ALLOWANCE` (political-finance).

**Rail 2 — Trust badge** (the §3 composite). `✓ Verified` · `Reported` · `Extracted` ·
`⚠ Indicative`. One per record. Clicking opens the explain-popover.

**Rail 3 — Match badge** (only on joined/cross-referenced records). `⚷ Exact match` ·
`Strong match` · `~ Fuzzy match` · `? Possible match` (weak). Hidden for single-source rows.

**State badges** (additive, shown only when true): `OCR — verify` · `SANDBOX` · `EXPERIMENTAL` ·
`STALE` · `excl. VAT` / `incl. VAT` · `PII suppressed`.

**Explain-this-figure popover** (`st.popover` — currently unused anywhere, so genuinely new).
Body = the full envelope: grain + verb, trust grade and *why* (the binding component), match
detail, extraction method, freshness, and the source link. Template in §7.

---

## 7. Wording library

Final caveat/legal copy is the owner's to ratify (§13); these are drop-in defaults, matched to
the existing tone in `attendance.py` / `election_2024.py` / `payments.py`.

### 7.1 Grain captions (the verb, never a bare €)
- payment_actual — *"€{amt} **paid** by {publisher} in {period}."*
- po_committed — *"€{amt} **ordered** (purchase orders raised) — not necessarily paid."*
- award_value — *"Contract **awarded** at €{amt}."*
- framework_ceiling — *"Framework valued **up to** €{amt} — a ceiling across all suppliers, not money spent."*
- estimated_value — *"**Estimated** at €{amt} at notice stage — the buyer's pre-tender figure."*
- audited_expenditure — *"€{amt} **audited expenditure** ({council}, {year}, accrual)."*
- donation / election_expense / allowance — *"**donated** / campaign **spend** / **claimed**…"*

### 7.2 Trust-badge tooltips
- Verified — *"From an official source, exactly matched. Highest confidence."*
- Reported — *"Parsed from an official published document. Reliable; not a structured feed."*
- Extracted — *"Read from a scan or a caveated document. Check against the source where it matters."*
- Indicative — *"A weak match, an estimate, or an experimental signal. Treat as a pointer, not a fact."*

### 7.3 Match-badge wording (the no-overclaim line)
- exact — *"Matched to {company} (CRO {num}) by company number."*
- strong/fuzzy — *"Likely {company} — matched by name, not company number."*
- **weak** — *"Possible match only: several companies share this name. We have **not** established this is the same company."*
- none — *(render nothing; make no entity claim.)*

### 7.4 Explain-popover body
```
{grain_label} · {verb} €{amt}
Trust: {grade} — {binding_reason, e.g. "limited by an OCR-read source"}
Match: {match_sentence from 7.3}
Source: {extraction_label} · as of {as_of} {· STALE if stale}
↗ {source_url}   📄 {source_document.url} (p.{page})
This figure is one record; totals on this page are a floor, never audited.
```

### 7.5 Standing disclaimers (render once per section)
- **Floor** — *"Coverage is partial. Every total here is **at least** this much — a floor, not the full picture."*
- **No-sum** — *"These figures measure different stages of public money and are **never added together**."*
- **No-causation** — *"Co-occurrence is not causation. A meeting or lobbying return does **not** mean it caused any contract or decision."*
- **Single-bid** — *"A single bid is a competition signal, **never** a verdict — many single-bid contracts are entirely proper."*

### 7.6 Email / report wording (no email system exists yet — these are the templates)
```
Subject: {Entity} — your {weekly|monthly} procurement brief

{Entity} appears in {N} records this period.

  • Paid:     at least €{paid}  (from {n_pay} supplier-payment records)
  • Awarded:  €{awarded} across {n_aw} contracts  — shown separately; never added to "paid"
  • Frameworks: up to €{ceiling}  — a ceiling, not money spent

How to read this  ·  Figures are parsed from official published sources and are a floor, not an
audited total. Money at different stages (awarded / ordered / paid) is never summed. Where a
figure is read from a scan it is marked "verify". A lobbying return or meeting in this brief is
context only — it does not imply it caused any contract.

Every figure links to its source: {permalink}
Data as of {as_of}. Methodology: {methodology_url}
```

---

## 8. Worked examples

1. **Go-Ahead Transport — €1,486,000,000.** `value_kind=framework_ceiling`,
   `is_large_award_review=true`, `safe_to_sum=false`. Renders `UP TO — ceiling, not spent`
   (amber), Trust **Reported**, excluded from every headline total. *Never* the top of a "paid"
   ranking — exactly the bug the large-award guard fixed.
2. **HSE supplier payment, incl-VAT.** `payment_actual`, `vat_status=incl_vat`. Grain `PAID`
   (green) + state badge `incl. VAT`. Sum-safe **only** with other incl-VAT HSE rows; the test
   suite blocks any sum that mixes it with excl-VAT publishers.
3. **SIPO donation, OCR-read scanned return, individual donor.** `extraction_method=ocr_extracted`,
   `value_kind=donation`, `privacy_status=review_personal_data`. Trust **Extracted** + `OCR —
   verify`. The **individual donor name and address are suppressed** (PII gate); only the
   aggregate/recipient side renders.
4. **Supplier ↔ CRO `exact_ambiguous`.** `match_method=weak`, `match_confidence=0.2`. Match badge
   `? Possible match`, wording from §7.3-weak, **no company number printed**, not counted as a
   confirmed cross-register entity.
5. **Firm appears in both lobbying returns and an award.** Rendered on the same profile but in
   separate panels, each grain-labelled, with the **no-causation** disclaimer. No arrow, no
   "after lobbying", no shared total. (See `project_mcp_procurement_lobbying_overlap`: never sum
   awarded_value across the lobbying overlap.)

---

## 9. Rules (formalised — each is also a test in §12)

| # | Rule | Mechanism |
|---|---|---|
| R1 | **Never sum across incompatible value-kinds.** | Physically separate facts (`fct_award`/`fct_payment`/accounts); view-layer sums filter `safe_to_sum=true` AND single `value_kind` family AND single `vat_status`. Tested. |
| R2 | **Never imply lobbying caused a contract.** | Separate UI panels; no shared total; no causal verbs; no-causation disclaimer; blocklist lint (§12.6). |
| R3 | **Never imply single-bid = wrongdoing.** | Label is "% of contract *lots* with a single bidder"; single-bid disclaimer; blocklist lint forbids "rigged/corrupt/because" near competition copy. |
| R4 | **Never expose quarantined / personal data.** | `privacy_status ∈ {review_personal_data, quarantined}` and `public_display=false` suppressed; quarantine parquet never joins a served view. Tested. |
| R5 | **Always link back to source.** | Every €-bearing served row has `source_url` or `source_document`, or an explicit `no_public_source` sentinel. Tested. |

---

## 10. Existing fields that already support this (Task 6)

| Envelope need | Already in the project | Where |
|---|---|---|
| safe-to-sum flag | `value_safe_to_sum` (bool) | `procurement_public_body_extract.py:104`; awards: `procurement_etenders_extract.py` |
| value kind / grain | `value_kind`, `realisation_tier`, `amount_semantics` | `data_contracts.py:63-64,132-133`; consolidator |
| closed-enum enforcement | `ColumnRule(..., "hard")` over 6 enums | `services/data_contracts.py:63-138` |
| extraction confidence | `extraction_confidence` {high,medium,low}, `extraction_status`, `caveat_text_detected`, `source_caveat` | `procurement_public_body_extract.py:112-118` |
| match method + confidence | `match_method` {exact_unique,exact_ambiguous,no_match}, `match_confidence` {0.9,0.5,0.0} | `procurement_etenders_extract.py:539-549`; `procurement_supplier_cro_match.parquet` |
| name-normalised join key | `name_norm_expr` (NFKD + legal-suffix strip), TD anagram key | `shared/name_norm.py`; `shared/normalise_join_key.py` |
| source URL / document / hash | `source_landing_url`, `source_file_url`, `source_file_hash`, `source_page_number` | `procurement_public_body_extract.py:94-109` |
| VAT base | `vat_status` {incl_vat,excl_vat,unknown} | `data_contracts.py:69`; consolidator |
| privacy / PII gate | `privacy_status`, `supplier_class`, `public_display` | `data_contracts.py:70-83`; schema cols |
| pipeline status / maturity | DATA_MAP tiers ⓪–⑤; quarantine rule/reason/run_id/timestamp | `doc/DATA_MAP.md`; `shared/quarantine.py`; `data/_meta/quarantine/*` |
| freshness | `freshness.json` {measure, as_of, stale_after_days, status}; `freshness_line()` | `data/_meta/freshness.json`; `utility/data_access/freshness_data.py` |
| source registry | 18-field per-source metadata (check_type, grain, caveat, privacy_risk, parser_wired…) | `tools/build_source_registry.py`; `data/_meta/source_registry.generated.json` |
| disclosure regime | `disclosure_basis`, `disclosure_threshold_eur`, `threshold_vat`, `body_procurement_class`, `regime_note` (verified on gold fact) | `extractors/_publisher_regime.py`; consolidator |
| UI caveat boxes | `.pr-caveat`, `.mo-cc-caveat`, `.dt-provenance-box`; page `_CAVEAT` prose | `utility/shared_css.py:5597,2261,3360`; pages_code/* |
| badges / pills | `.dt-badge*`, `.con-grain*`, `.pr-paid-tag`, `.don-vmark` "to verify" | `utility/shared_css.py:1191,6078,5845,5432` |
| provenance UI | `provenance_expander()`, `render_source_links()`, approved source-url cols | `utility/ui/source_pdfs.py:330`; `utility/ui/source_links.py` |
| export | `export_button()` (deferred CSV) on ~13 pages; bulk `/v1/data` API | `utility/ui/export_controls.py`; `api/main.py` |
| page contracts | `known_data_quality_issues`, `per_year_source` blocks (schema v5) | `dail_tracker_bold_ui_contract_pack_v5/.../page_contracts/*.yaml` |

**Gaps to add:** unified `source_type` / `extraction_method` / `pipeline_status` columns on the
served facts (today scattered/implicit); the `MATCH_METHOD`/`PIPELINE_STATUS`/`SOURCE_TYPE`/
`EXTRACTION_METHOD` closed enums; the `derive_trust_tier()` function; the badge render helper;
the `data_confidence` contract block; an email/report wording module.

---

## 11. Implementation plan

**Phase 0 — Vocabulary lock (no UI). LANDED 2026-07-20** (owner sign-off given). In
`services/data_contracts.py`: the closed enums (`SOURCE_TYPE`, `EXTRACTION_METHOD`,
`MATCH_METHOD`, `PIPELINE_STATUS`, `FRESHNESS_STATUS`, `CAVEAT_SEVERITY`) + `MATCH_METHOD_ALIASES`
for the stored CRO dialect; the §3 component ceilings as swappable module dicts (§13 says
parameterise, not hard-code); pure `assess_trust()` / `derive_trust_tier(record) -> 'A'|'B'|'C'|'D'`;
and `ENVELOPE_RULES`, folded into BOTH `PAYMENT_FACT_RULES` and `AWARD_FACT_RULES` so the shipped
gates enforce it — inert while the columns are absent, automatic once Phase 1 lands. T3 ships as
`test/contracts/test_trust_tier.py`. **This module is now the single definition of the 4 bands** —
`.claude/rules/evidence.md` applies the same scale to claims/reviews.

**§3 vocabulary correction — `match_method` gains `failed`.** A falsification spike (2026-07-20)
found that §3-as-written grades a FAILED join as **Verified**: the doc expresses "attempted and
found nothing" as `none`-with-`match_confidence`-0, so whenever that nullable number is NULL the
record reads as "no join attempted" and is not capped. The stored dialect's `no_match` already
carries the distinction, so `failed` is now a first-class `MATCH_METHOD` value (ceiling D) and
`no_match` aliases to it; `none` keeps its footnote-1 meaning of *not attempted* (does not cap).
**Rule: the categorical value decides the ceiling — a nullable confidence number may only refine
the ambiguous `none` case, never be the sole evidence that a join succeeded.**

Two Phase-0 items deliberately NOT done, both needing an owner call:
- *Widening `VALUE_KIND` to the union* — the shipped code keeps `VALUE_KIND` (payment grain) and
  `AWARD_VALUE_KIND` (award grain) as separate closed enums precisely so an award kind cannot pass
  the payment-fact contract. Widening to a union would weaken that never-sum guard, so it is left
  alone pending a decision.
- *`grain_label()`* — money-grain badge wording is reserved to the owner under §13.

**Phase 1 — Backfill the envelope on gold facts (additive).** Map `amount_semantics → value_kind`;
add `source_type`, `extraction_method`, `pipeline_status` columns (derive from `parser_name` +
`source_registry`); rescale `match_confidence`; **surface the existing `match_confidence` into
`procurement_supplier_summary.sql`** (the roadmap's flagship additive fix — stop dropping it at
`:53`). Row counts and figures unchanged; only metadata added.

**Phase 2 — UI components (firewall-clean).** `utility/data_access/confidence.py` resolves the
envelope → badge spec (logic lives here); `utility/ui/confidence_badges.py` renders the three
rails + state badges + the `st.popover` explainer from a contract, no business logic. Extend
`shared_css.py` with the trust/grain badge classes (reuse `.dt-badge`/`.con-grain` tokens).

**Phase 3 — Contract enforcement.** Add a `data_confidence` block to page contracts
(`confidence_level`, `caveat_text`, `grain`, `known_limitations`). The reviewer/firewall checker
fails any page that renders € without a declared grain + trust source.

**Phase 4 — Email/report + export.** A `utility/reports/wording.py` module holding the §7.6
templates and the standing disclaimers; exports carry the envelope columns + a methodology note;
the bulk `/v1/data` API documents the envelope.

**Phase 5 — CI.** Wire the §12 overclaim suite into the test run (`-m "not integration"` fast
subset where possible) and the firewall check.

---

## 12. Tests to prevent overclaiming

| # | Test | Asserts |
|---|---|---|
| T1 | **No cross-grain / cross-vat sum** | Static scan of registered views + `data_access` aggregations: every SUM/total filters `safe_to_sum=true`, a single `value_kind` family, and a single `vat_status`. Extends the existing SQL-view test. |
| T2 | **Closed-enum contract** | Every envelope field validates against its `frozenset`; out-of-vocab → quarantine, never served. (Extends `data_contracts.py` hard rules.) |
| T3 | **Trust-tier weakest-link** | Property test: degrading *any* component never *raises* `derive_trust_tier`; an OCR/weak/sandbox/stale record can never grade **A**. |
| T4 | **Match-claim guard** | For `match_method ∈ {fuzzy, weak, none}` no rendered string contains a CRO company number or "is the same company"; `weak` must contain "possible"/"not established". Unit test on the wording resolver + grep over `pages_code`. |
| T5 | **No-causation lint** | Blocklist regex over page + email templates forbids causal joins of lobbying/diary → contract/decision ("because they lobbied", "led to the contract", "won after meeting"). |
| T6 | **No single-bid verdict lint** | Forbids "rigged/corrupt/wrongdoing/because" within N tokens of single-bid/competition copy; requires the "% of *lots*" phrasing. |
| T7 | **PII suppression** | No served view exposes a row with `privacy_status ∈ {review_personal_data, quarantined}` or an individual `supplier_class` with `public_display=false`; quarantine parquet never appears in any served view. |
| T8 | **Source-link presence** | Every €-bearing row in a served contract has non-null `source_url` or `source_document`, or the explicit `no_public_source` sentinel. |
| T9 | **Freshness honesty** | `freshness.status=stale` ⇒ the badge resolver emits `STALE`; a fresh record never does. |
| T10 | **Estimate/ceiling never headlined as spend** | Any "paid/spent" headline excludes `value_kind ∈ {framework_ceiling, estimated_value}` and large-award-review awards. |

---

## 13. Open decisions reserved for the owner

These are product/judgement calls the system should *parameterise*, not hard-code (consistent
with the procurement roadmap's deferred sign-offs):

- Trust-grade **labels and thresholds** (Verified/Reported/Extracted/Indicative wording; where
  each component ceiling sits).
- Exact **money-grain badge wording** and whether to show `award − paid` divergence (recommend
  **no** ratio by default).
- Whether to **suppress** ambiguous (`weak`) company-number matches entirely vs. show with the
  "possible match" caveat (recommend show-with-caveat).
- Final **legal/caveat copy** and the methodology page text.
- Which datasets are `live` vs `experimental` at launch (DATA_MAP tiers are the input).

Provenance, licensing and republication remain entirely the owner's domain; this system surfaces
trust signals from the data — it does not gate what may be published.

---

## 14. Validation against real data (2026-06-28)

Profiled the live gold parquet (schema via lazy `collect_schema`; distributions via single-column
`value_counts` — no full-frame reads). The plan holds; three corrections were folded back in above.

**Confirmed exactly as designed**

| Claim | Real data |
|---|---|
| CRO `match_method` / `match_confidence` | `exact_unique` 6,047 (0.9) · `exact_ambiguous` 400 (0.5) · `no_match` 3,532 (0.0) — matches §5.C; remap `exact_ambiguous→weak` is right |
| Gold payments fact carries the envelope | 41 cols: `value_kind`, `realisation_tier`, `value_safe_to_sum`, `vat_status`, `supplier_class`, `privacy_status`, `public_display`, `extraction_confidence`, `extraction_status`, `caveat_text_detected`, `source_landing_url`, `source_file_url`, `disclosure_basis` all present |
| The four "gaps to add" are genuinely absent | `source_type`, `extraction_method`, `pipeline_status`, `source_url` **not** present — Phase-1 backfill is real work |
| PII gate guards real rows | `privacy_status=review_personal_data` 19,295 · `public_display=false` 20,314 · `supplier_class` includes `sole_trader_or_individual` 18,783 / `sole_trader` 21,365 |
| `extraction_confidence` feeds the trust ceiling | high 316,854 · medium 103,329 · low 5,361 (all 3 tiers populated) |
| Large-award guard is live | awards `is_large_award_review=true` 2,452; `value_safe_to_sum=false` 46,359 vs true 16,404 (ceilings correctly excluded) |
| Lobbying-overlap pre-filters to safe awarded | `procurement_lobbying_overlap` exposes only `awarded_value_safe_eur`, no payment column — R2's data side already disciplined |
| Freshness / quarantine structures | `freshness.json` = `{measure, as_of_utc_date, stale_after_days, status, stale_datasets[]}`; quarantine JSON = `{n_rows_quarantined, frac, breaches{severity,…}}` — §2/§7 match |

**Corrections folded back into the design**

1. **`value_kind` stored strings ≠ tidy label names.** Awards store `contract_award_value` /
   `framework_or_dps_ceiling`; payments store `po_committed` / `payment_actual`. §2 enum and §4
   table now use the **actual strings**, with tidy names demoted to the UI label layer. *Adopt
   existing strings; do not rename columns.*
2. **`donation` / `election_expense` / `allowance` have no `value_kind` column yet.** Those facts
   (SIPO, allowances) aren't in the procurement value contract — the 3 grains are a Phase-1
   addition, now flagged ⁵ in §4, not presented as existing.
3. **`vat_status` has no `excl_vat` rows in current data** — only `unknown` 331,684 and `incl_vat`
   93,860. The cross-VAT sum hazard is real (don't blend the 93,860 incl-VAT rows with the
   unknown-basis majority); the badge should read **"incl. VAT" / "VAT basis unknown"**, not
   "excl. VAT", until excl-VAT data actually lands.

**Note on the awards-by-value spot check:** a top-5-by-`value_eur` probe surfaced null-value rows
(Polars sorts nulls first) rather than the Go-Ahead ceiling — a script artifact, not a data
problem. The guard itself is validated by the 2,452 `is_large_award_review` count, so the specific
€1.486bn row was **not** re-confirmed this pass (low priority; the mechanism is proven).

### Pass 2 — testing the *guards*, not just column existence (crosstabs)

Re-ran with cross-tabs to confirm the never-sum and PII rules actually **hold in the data**:

**Guards hold 100% — confirmed**
- **Largest awards are all correctly excluded.** Top-8 by value (non-null) are *all*
  `framework_or_dps_ceiling`, `value_safe_to_sum=false`, `is_large_award_review=true` — six NTA
  frameworks at €2.5bn, two LDA at €2.0bn. The Go-Ahead €1.486bn sits *below* these, all flagged.
- `framework_or_dps_ceiling` → `safe_to_sum=false` for **17,964 / 17,964** (100%).
- `is_large_award_review=true` → false for **2,452 / 2,452** (100%).
- `value_shared_across_suppliers=true` → false for **23,018 / 23,018** (100%). `safe_to_sum=true`
  is *only* single-supplier, non-framework, non-large `contract_award_value` (16,404) — exactly §4³.
- **Payments**: `supplier_class=public_body` → false for **8,158 / 8,158** (the TII road-grant
  intergovernmental-transfer exclusion holds); `unknown` → false for 4,359 / 4,359.

**PII suppression is airtight — confirmed (and a stale memory corrected)**
- `privacy_status=review_personal_data` → `public_display=false` for **19,295 / 19,295** (100%).
- `supplier_class=sole_trader_or_individual` → false for **18,783 / 18,783**; `id_code` → false for
  **1,531 / 1,531**. (Note: `sole_trader` — a registered business name — stays *displayable*; only
  the ambiguous `sole_trader_or_individual` is suppressed. Deliberate, useful distinction.)
- **HSE/Tusla PII blocker is now FIXED.** A 24-day-old memory (`reference_data_map`) warned
  7,409 HSE/Tusla individual rows were `public_display=True`. Current data: all **4,698**
  `sole_trader_or_individual` rows are `public_display=False`. The blocker has since been closed.

**Asserted grains now verified in data**
- AFS: `la_afs_divisions` = `net_expenditure_actual`/SPENT (776); `la_afs_capital` =
  `capital_expenditure_actual`/SPENT (782); `afs_amalgamated` = `net_expenditure_actual`/SPENT (64).
- `estimate_advertised` confirmed real: `etenders_live_tenders` = PLANNED, 2,363 rows, with
  `value_safe_to_sum`. (`pre_award_estimate` **not** found in the files queried — downgrade to
  unverified; it may be extractor-only or in a tenders-not-awards file.)

**Two new material findings (fed back into the rules above)**
1. **`value_safe_to_sum=true` does NOT partition by VAT.** Both `incl_vat` (92,786) and `unknown`
   (319,521) payment rows are flagged sum-safe. So the flag *alone* permits a cross-VAT blend — the
   view layer **must additionally partition by `vat_status`**. This makes test **T1 necessary, not
   redundant**: `safe_to_sum` is a within-VAT guard, not a cross-VAT one.
2. **Privacy-vocabulary dialects across facts.** Gold `procurement_payments_fact` uses
   `privacy_status ∈ {ok, review_personal_data}`; silver `la_payments_fact` uses
   `{public, quarantined}` (+ a `privacy_reason` col); `hse_tusla` is still on the *old*
   `amount_semantics` vocab with no `value_kind`/`vat_status`. The `data_contracts.py` enum is the
   union, so all validate — but the **trust/PII resolver must accept both dialects**, or the facts
   should converge before the envelope is computed. This is the same "vocab drift to converge"
   risk flagged in `project_procurement_phase_taxonomy`.
