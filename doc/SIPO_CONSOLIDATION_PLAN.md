# SIPO extraction — consolidation plan (refreshed 2026-06-05, post-reorg/promotion)

> **Status:** PLAN ONLY. Much of the *structural* cleanup this plan originally called for
> has already happened via the reorg/promotion (see "What's already done"). What remains is
> the **in-code de-duplication** + the **cap-repair correctness fix**. Sandbox rule no longer
> applies the same way — SIPO has graduated to `extractors/` + medallion `data/silver|gold`.
> Still execute the dedup at a stable plateau (no SIPO OCR workers running, no pending live
> edits), per `feedback_refactor_timing`.

## What's already done (the reorg/promotion did this)

- **Code graduated** `pipeline_sandbox/` → **`extractors/`** (no SIPO files left in sandbox).
- **Data promoted** to medallion layers:
  - silver: `data/silver/sipo/{sipo_expenses_fact, sipo_donations_fact, sipo_expense_items_fact, sipo_expense_categories_fact}.parquet`
  - gold: `data/gold/parquet/{sipo_expenses_fact, sipo_donations}.parquet` (via `extractors/sipo_promote_to_gold.py`).
- **Throwaway probes + helpers deleted** — the ~7 `probe_sipo_ocr_*.py`, `build_part4_no_ocr.py`,
  `_explore_sipo_quality.py`, `sipo_textlayer_party.py`, `sipo_expenses_roster_fix.py` are gone.
  (Original plan step "delete probes" = ✅ done.)
- **Test moved** to `test/test_sipo_data_quality.py`, repointed to `data/silver/sipo/`,
  **18/18 pass** (incl. the `assigned_over_limit_verify` guard).
- **Both data tracks complete:** expenses (9 parties, 399 rows) + donations (74 rows) in
  silver AND gold.

## Current `extractors/` SIPO inventory (~1,590 lines, 8 files)

| File | Lines | Role |
|---|---|---|
| `sipo_expenses_paddle_etl.py` | 499 | Part 3 candidate-summary. **Reference design** — two-stage (OCR→`c*.json` cells, `--parse-only` parse), born-digital text path `{sf, aontu}`. |
| `sipo_expense_items_paddle_etl.py` | 363 | Part 4 items + category totals. **Still single-stage** (no born-digital / no `--parse-only`). |
| `sipo_donations_paddle_etl.py` | 303 | Donations (money received). |
| `sipo_promote_to_gold.py` | 101 | silver → gold promotion. |
| `_sipo_watchdog.py` | 94 | candidate hang-bounded driver |
| `_sipo_items_watchdog.py` | 88 | items driver (near-identical) |
| `_sipo_donations_watchdog.py` | 68 | donations driver (near-identical) |
| `_sipo_queue.py` | 74 | orchestrator (wait-for-free → run watchdogs) |

## Duplication still present (the remaining work)

| Duplicated piece | Copies (in the 3 ETLs) | Consolidate to |
|---|---|---|
| `parse_money` | 3 | `sipo_common.parse_money` |
| `ocr_page` (render→PaddleOCR→cells) | 3 | `sipo_common.ocr_page` |
| PaddleOCR init (`enable_mkldnn=False`, `text_det_limit_*`) | 3 | `sipo_common.make_ocr()` |
| checkpoint + DPI retry-ladder OCR loop | 3 | `sipo_common.cache_cells()` |
| `text_layer_cells`, `match_constituency`, `STATUTORY_LIMIT`, `cluster_rows`, `parse_money` | 2–3 | `sipo_common` |
| `PARTY_JOBS`, `BORN_DIGITAL` | 2–3 | `sipo_common` (single source) |
| zstd parquet-write boilerplate | many | `sipo_common.write_parquet()` |
| **watchdog drivers** | **3 near-identical** | one generic `extractors/_sipo_watchdog.py` |

## Target shape (~6 files in `extractors/` + 1 test)

```
extractors/
  sipo_common.py              # NEW — shared core (below)
  sipo_expenses_paddle_etl.py # Part 3: parse_candidate_row + main (thin)
  sipo_expense_items_paddle_etl.py # Part 4: parsers + main (thin; GAINS born-digital + --parse-only)
  sipo_donations_paddle_etl.py# donations: parser + main (thin)
  sipo_promote_to_gold.py     # unchanged (re-run after the cap-repair, see below)
  _sipo_watchdog.py           # ONE generic, parameterised driver (collapses 3 → 1)
  _sipo_queue.py              # orchestrator (update its watchdog calls)
test/test_sipo_data_quality.py# safety net (already on silver; grows with donations)
```

**`extractors/sipo_common.py`:** paths (silver/gold + `_ckpt` roots), `PARTY_JOBS`,
`BORN_DIGITAL`, `STATUTORY_LIMIT`, `load_constituencies()`, `norm`, `match_constituency`,
`parse_money`, `cluster_rows`, `rightmost_money`/x-band, `text_layer_cells`, `make_ocr()`
(Windows gotchas ONCE), `ocr_page()`, **`cache_cells()`** (the single checkpoint+retry+
born-digital loop), `write_parquet()`, and **`flag_amount()`**.

**`flag_amount()` is where the two correctness fixes land** (applies to expenses + donations):
- `over_limit_verify` (spend > limit) and **`assigned_over_limit_verify`** (assigned > limit)
  — already emitted by the live ETL; centralise them.
- **Cap-repair (the still-open fix):** a value > limit with a dropped decimal (≥4 digits) →
  `÷100`, emitted **flagged** `reconstructed_verify`, never as a bare figure. This is what
  brings FF's gold total down from the inflated **€3.44M** to the real ≈€375k (the 4
  expenditure + 3 assigned ×100 outliers: Fitzpatrick €709,513→€7,095.13, O'Callaghan
  €1,944,000→€19,440, etc.).

## Migration order (each step gated by `test/test_sipo_data_quality.py`)

1. **Create `extractors/sipo_common.py`** (primitives copied verbatim from the expenses ETL).
   No callers yet → tests still pass.
2. **Re-point `sipo_expenses_paddle_etl.py`** to core; delete local copies. `--parse-only`
   → byte-identical silver expected; 18/18.
3. **Re-point `sipo_expense_items_paddle_etl.py`** to core; **add born-digital + `--parse-only`**
   via `cache_cells`. Re-parse SF/Aontú/FF → SF still reconciles 8/8 (€205,033.66).
4. **Re-point `sipo_donations_paddle_etl.py`** to core.
5. **Land `flag_amount()` with the cap-repair**; `--parse-only` re-parse expenses (no OCR)
   → FF total drops to the repaired figure, all rows flagged honestly. **Then re-run
   `sipo_promote_to_gold.py`** so GOLD reflects the repair (gold currently carries the
   inflated FF €3.44M — this step closes that gap). Full suite green.
6. **Collapse the 3 watchdogs** into one generic `_sipo_watchdog.py <etl> <ckpt> <done> <keys>`;
   update `_sipo_queue.py`. Smoke-test on an already-cached party (no OCR).

(No "delete probes" / "move out of sandbox" steps — the reorg already did them.)

## Pre-execution gate (plateau)
- [ ] No SIPO OCR workers running (candidate/items/donations) — check via
      `_sipo_queue.ocr_busy()` or a process scan.
- [ ] No pending live edits to the `extractors/sipo_*` files.
- [ ] silver + gold baselines captured (row counts per party) so the migration can prove
      "no regression except the FF cap-repair we intend."
- [ ] Remember step 5 **re-promotes to gold** — the dedup isn't done until gold reflects the
      repaired FF figures.

## Expected outcome
- 8 `extractors/` files → ~6; duplicated symbols → single definitions; 3 watchdogs → 1.
- Items ETL gains born-digital + `--parse-only` (parity with expenses).
- **FF's inflated gold total fixed** via the shared cap-repair; one PaddleOCR config, one
  parquet-write convention, the no-inference/privacy posture documented once.
