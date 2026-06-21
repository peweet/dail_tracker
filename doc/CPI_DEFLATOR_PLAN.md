# CPI Deflator Enrichment — Impact & Data-Safety Plan

**Status:** scoped, not started. **Date:** 2026-06-21.
**Source:** CSO PxStat `CPA07` (CPI by commodity group, annual, All Items, 1975–2025), CC-BY.
Optional secondary: `WPM39` (construction materials WPI, 2021+). See
`memory/project_cso_esri_deflator_scoping_2026_06_21.md` for the scoping evidence.

---

## 0. What this is — and what it is NOT

A deflator **re-expresses** a euro figure from one year into the purchasing power of a
base year (here **2025 €**). It does **not** correct or replace the recorded figure.

> The nominal numbers in our facts are **correct as recorded** — they are what the source
> documents say. Real-terms is a *labelled lens* ("€X in 2025 prices, CPI-deflated"), not a
> claim that the nominal figure was wrong. This distinction is load-bearing for an
> accountability platform: we must never imply the official/published € was inaccurate.

So the honest framing of the user's question — "it would drastically affect the money sums
but make it more accurate" — is refined to:

- It does **not** change any sum **at the default view** (see §2: nominal stays canonical,
  real-terms is opt-in).
- When toggled on, it **re-expresses** sums in constant euros: **+12.5%** on awards across
  2012–25 (up to **+24%** on the oldest years), collapsing to **0%** at 2025.
- "More accurate" = **more honest cross-year comparison & trend**, not error-correction.

---

## 1. Deflatability profile (measured against live facts)

### `procurement_awards` (62,763 rows)
| Slice | Rows | Deflation treatment |
|---|---|---|
| `value_safe_to_sum = true` (the only rows in any sum) | **16,404** | deflate by award year |
| — of which parseable award year | **16,404 (100%)** | ✅ full join coverage |
| — of which duration > 12 months (multi-year) | **6,032 of 14,875 w/ duration (~41%)** | ⚠️ single-year deflation is approximate |
| `framework_or_dps_ceiling` / `is_large_award_review` | 17,964 / 2,452 | already `safe_to_sum = false`; deflate only if shown individually, never summed |

Nominal canonical total (safe): **€15.637bn** — must be byte-identical after the change at default.

### `procurement_payments_fact` (220,099 rows)
| Slice | Rows | Treatment |
|---|---|---|
| `value_safe_to_sum = true` | **206,424** | deflate per row by its period year |
| — year in 2012–2025 | **193,202** | ✅ exact (each row is period-stamped) |
| — year null | **7,152 (3.5%)** | ⚠️ cannot deflate → keep nominal, real = null |
| — year 2026 | ~6,070 | use latest factor (≈1.0) or hold nominal |
| `po_committed` vs `payment_actual` | 145,852 / 74,247 | deflate both; never mix (existing `realisation_tier` already gates) |

Nominal canonical total (safe): **€34.391bn** — must be byte-identical after the change at default.

**Why payments deflate more cleanly than awards:** each payment row carries its own
period, so deflation is exact per row. A multi-year *award* booked to its award year is an
approximation (true spend spans years) — still far more honest than mixing nominal years,
but it carries a caveat.

---

## 2. Design — additive, non-destructive (the "won't destroy our data" guarantee)

1. **Nominal is canonical and untouched.** `value_eur` (awards) and `amount_eur` (payments)
   are the source of truth and are **never overwritten**. We add a *derived* real-terms value,
   never mutate the existing column.
2. **Deflator is a standalone reference table/view**, not embedded math:
   - new gold `cso_cpi_deflator` (year → index, factor-to-2025) from `CPA07`, built the same
     way as every other table in `extractors/cso_pxstat_extract.py` (same API + fidelity gate
     + `save_parquet` + row-floor exemption — it's tiny reference data, not a floored fact).
   - a registered SQL view `v_cpi_deflator` exposing `(year, cpi_index, deflator_to_2025)`.
3. **Real value is computed in a registered view**, not in Streamlit (logic-firewall compliant):
   `value_eur_real_2025 = value_eur * deflator_to_2025` joined on award/payment year.
   None of the classification columns (`value_safe_to_sum`, `value_kind`, `is_framework_or_dps`,
   `is_large_award_review`, regime fields) are read or rewritten by this join — they keep gating
   exactly as today.
4. **UI = opt-in toggle, default NOMINAL.** A "Show in 2025 prices" toggle on procurement /
   payments / bid-signal. **Default off** → every currently-displayed figure and every cited
   number is identical. Real-terms is always labelled `(2025 €, CPI-deflated)`.
5. **Bid-signal integration:** add `award_*_real_2025` band columns alongside the existing
   nominal bands so the CPV comparison can be inflation-honest, *without* removing the nominal
   bands. (This is the highest-value consumer — it currently mixes award years.)

This is the same sandbox→vet→promote, data-anchored discipline used for every other gold change.

---

## 3. Acceptance gates — proof it doesn't destroy, and does improve

Before promotion, a guard test (`tests/`) must assert ALL of:

**Non-destruction (identity at default):**
- [ ] `value_eur` / `amount_eur` columns byte-identical pre/post (hash compare).
- [ ] Nominal safe totals unchanged: awards **€15.637bn**, payments **€34.391bn**.
- [ ] Row counts unchanged on both facts; no new nulls in any existing column.
- [ ] Every existing registered view still builds (dependency-order tripwire).
- [ ] Row-floor guard still passes on the 3 procurement facts.

**Deflator correctness:**
- [ ] `deflator_to_2025` for 2025 == 1.0 exactly (identity year).
- [ ] Series monotonic-ish & bounded (every factor ≥ 1.0 for years ≤ 2024; no factor > ~1.4).
- [ ] Rebuilt from CSO chain-linked %-change matches an independent CPI cross-check
      (e.g. CSO's own 2012→2025 cumulative ≈ +26.8%) within tolerance.

**Improvement (real-terms is meaningful):**
- [ ] Real ≥ nominal for every pre-2025 year on summable slices (sign check).
- [ ] Coverage logged: % of summable value that is cleanly deflatable vs caveated
      (multi-year awards, null-year payments) — **no silent truncation**; the caveat is shown.

If any non-destruction gate fails → do not promote. The change is purely additive, so the
expected result is all green.

---

## 4. Accuracy caveats to surface (honesty, not hidden)

1. **Multi-year awards** (~41% of summable awards) booked to award year → approximate. Label
   the awards real-terms view as "approximate for multi-year contracts".
2. **Null-year payments** (3.5%) cannot be deflated → remain nominal, excluded from any
   real-terms subtotal with a footnote count.
3. **Base year** = 2025 (latest full annual). State it explicitly everywhere.
4. **CPI is economy-wide.** For construction-heavy spend, general CPI under-states 2021–22 cost
   surges — that's the `WPM39` secondary deflator's niche (Works contracts only, 2015+ floor).

---

## 5. Implementation steps (when approved)

1. Add `CPA07` to `extractors/cso_pxstat_extract.py`; build `cso_cpi_deflator` gold (chain-link
   from the %-change Statistic — the index level is split across base-month rebasings).
2. Register `v_cpi_deflator`; add `value_eur_real_2025` / `amount_eur_real_2025` to the
   award & payment registered views (join on year).
3. Extend the bid-signal view with `award_*_real_2025` bands.
4. Add the guard test (§3). Sandbox → vet against acceptance gates → promote as its own checkpoint.
5. Wire the default-off "2025 prices" toggle into procurement / payments / bid-signal UI.
6. (Optional, later) `WPM39` Works-only secondary deflator with the 2015 back-history flag.

---

## 6. Recommendation

**Proceed.** The change is additive and reversible, defaults are unchanged (zero risk to the
carefully-assembled facts or any cited figure), and it materially improves the honesty of every
cross-year comparison — most usefully the bid-signal CPV bands. The "drastic" effect is bounded
(+12.5% aggregate, re-expression not correction) and only ever visible behind an explicit,
labelled toggle. ESRI is discarded as a data source (confirmed three independent ways).
