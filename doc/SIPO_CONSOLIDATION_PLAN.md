# SIPO extraction — consolidation plan

> **Status:** PLAN ONLY. Do **not** execute while SIPO OCR/parse jobs are in flight
> (donations OCR, the `_sipo_queue.py` run, candidate/items re-parses). These files are
> being live-edited and are imported by running processes — refactoring now would cause
> edit conflicts and could break in-flight jobs. Per the project's refactor-timing rule
> (`project_reorg_plan` / `feedback_refactor_timing`): **plan + audit now, execute at the
> stable plateau.** Sandbox rule still applies — all code stays under `pipeline_sandbox/`.

## Why

The SIPO work grew organically across **22 files (~4,000 lines)** with three production
ETLs, three near-identical watchdogs, and the same primitives copy-pasted. This plan
collapses it to ~7 files with single definitions, and lands the two open correctness
fixes (FF ÷100 cap-repair, assigned-over-limit guard) in one shared place.

## Current inventory (2026-06-04)

**Production ETLs (3):**
- `sipo_expenses_paddle_etl.py` (485) — Part 3 candidate-summary. Two-stage
  (OCR→`_ckpt/<key>/c*.json` cells, then `--parse-only` parse). Born-digital text-layer
  path for `{sf, aontu}`. The most evolved; treat as the reference design.
- `sipo_expense_items_paddle_etl.py` (362) — Part 4 itemised + category totals. **Single
  stage** (always loads PaddleOCR; no born-digital / no `--parse-only`).
- `sipo_donations_paddle_etl.py` (286) — donations (money received). Newest track.

**Watchdogs (3, near-identical — differ only in ETL path / ckpt dir / done-check):**
- `_sipo_watchdog.py` (94) · `_sipo_items_watchdog.py` (88) · `_sipo_donations_watchdog.py` (68)

**Orchestrator:** `_sipo_queue.py` (74) — waits for OCR-free, runs watchdogs sequentially.

**One-off / helper / now-redundant:**
- `build_part4_no_ocr.py` — built Part 4 for FF(cache)+SF/Aontú(text) without OCR.
  **Subsumed** once the items ETL gains born-digital + `--parse-only`.
- `sipo_expenses_roster_fix.py` (201), `sipo_textlayer_party.py` (158),
  `_explore_sipo_quality.py` (94) — audit whether their logic folds into core or is dead.
- `probe_sipo_doc_census.py`, `probe_sipo_donations_assess.py` — assessment probes.

**Investigation throwaways (delete after consolidation — see `project_sipo_ocr`):**
`probe_sipo_ocr_text.py`, `probe_sipo_ocr_geometry.py`, `probe_sipo_ocr_columns.py`,
`probe_sipo_ocr_repair.py`, `probe_sipo_ocr_analysis.py`, `probe_sipo_ocr_extract.py`,
`probe_sipo_ocr_paddle.py` (~7 files, ~1,090 lines).

**Tests:** `test_sipo_data_quality.py` (337) — KEEP; it's the migration safety net.

## Duplication audit

| Duplicated piece | Copies | Consolidate to |
|---|---|---|
| `parse_money` | 4 | `sipo_common.parse_money` |
| `ocr_page` (render→PaddleOCR→cells) | 3 | `sipo_common.ocr_page` |
| PaddleOCR init (`enable_mkldnn=False`, `text_det_limit_*`) | 3 | `sipo_common.make_ocr()` |
| `text_layer_cells` (born-digital) | 2 | `sipo_common.text_layer_cells` |
| `match_constituency`, `norm`, `DIRECTIONS`, `STATUTORY_LIMIT` | 2 | `sipo_common` |
| `cluster_rows`, `rightmost_money`, x-band split | 2–3 | `sipo_common` |
| `PARTY_JOBS`, `BORN_DIGITAL` | 3 | `sipo_common` (single source of truth) |
| checkpoint + DPI retry-ladder OCR loop | 3 | `sipo_common.cache_cells()` |
| `compression="zstd", compression_level=3, statistics=True` | 6 | `sipo_common.write_parquet()` |
| watchdog drivers | 3 | one generic `_sipo_watchdog.py` |

## Target architecture (~7 files)

```
pipeline_sandbox/
  sipo_common.py              # NEW — shared core (see below)
  sipo_expenses_paddle_etl.py # Part 3: parse_candidate_row + main (thin)
  sipo_expense_items_paddle_etl.py # Part 4: parse_item_row/parse_summary_row + main (thin)
  sipo_donations_paddle_etl.py# donations: parse_donation_row + main (thin)
  _sipo_watchdog.py           # ONE generic, parameterised driver
  _sipo_queue.py              # orchestrator (unchanged)
  test_sipo_data_quality.py   # safety net (unchanged + grows with donations)
```

**`sipo_common.py` contents:**
- Paths: `OUT_DIR`, `BY_PARTY_DIR`, `CKPT_ROOT`, `CKPT_ITEMS`, `SCAN_DIR`.
- `PARTY_JOBS`, `BORN_DIGITAL`, `STATUTORY_LIMIT`, `DIRECTIONS`.
- `load_constituencies()` → `(norm_keys, norm_to_name, name_to_seats)`.
- `norm`, `match_constituency`, `parse_money`, `cluster_rows`, `rightmost_money`.
- `text_layer_cells(page)` (born-digital).
- `make_ocr()` — PaddleOCR factory; the Windows gotchas live here ONCE
  (`enable_mkldnn=False`; `text_det_limit_side_len=1280`, `text_det_limit_type="max"`).
- `ocr_page(ocr, page, tmp_png, dpi)`.
- **`cache_cells(key, pdf, ckpt_dir, *, born_digital, ocr=None)`** — the single
  checkpoint + retry-ladder (2×300→1×200→skip) + born-digital-text-layer loop. The three
  ETLs’ OCR loops collapse into calls to this.
- `write_parquet(df, path)` — the zstd/level-3/statistics convention in one place.
- **`flag_amount(spend, assigned, limit, conf, row_conf)`** → `(value, flag)`. Houses the
  shared correctness rules so they apply to expenses AND donations:
  - `over_limit_verify` when `spend > limit`;
  - **NEW `assigned_over_limit_verify`** when `assigned > limit` (fixes the open
    `test_assigned_within_statutory_limit` failure — FF Jim O'Callaghan €1,944,000,
    Michael Cahill €1,458,750);
  - **NEW cap-repair**: a value `> limit` with the decimal dropped (≥4 digits) → `÷100`,
    emitted **flagged** `reconstructed_verify`, never as a bare figure (fixes FF's 4 ×100
    expenditure outliers: Fitzpatrick €709,513→€7,095.13, etc.);
  - `spend_gt_assigned_verify`, `low_confidence_verify`, `ok`, `no_amount` as today.

**Generic watchdog** — `_sipo_watchdog.py <etl_script> <ckpt_subdir> <done_suffix> <keys…>`
(or a small `WATCHERS` registry keyed by `expenses|items|donations`). Same stall-kill +
resume logic; one implementation.

## Migration order (each step gated by `test_sipo_data_quality.py`)

Do it as a sequence of small, independently-verifiable steps — never one big-bang edit:

1. **Create `sipo_common.py`** with the primitives, copied verbatim from the reference
   (`sipo_expenses_paddle_etl.py`). No callers yet. Run tests (still green — nothing wired).
2. **Re-point `sipo_expenses_paddle_etl.py`** to import from core; delete its local copies.
   Run `--parse-only` and the full test suite → byte-identical parquet expected.
3. **Re-point `sipo_expense_items_paddle_etl.py`** to core; **add born-digital + `--parse-only`**
   via `cache_cells`. Re-parse SF/Aontú/FF → must match `build_part4_no_ocr.py` output
   (SF €205,033.66 reconciles 8/8). Then **delete `build_part4_no_ocr.py`**.
4. **Re-point `sipo_donations_paddle_etl.py`** to core.
5. **Land `flag_amount()`** with the cap-repair + assigned-guard; re-parse expenses
   (`--parse-only`, no OCR) → FF total drops to the repaired figure, both open test
   failures clear, full suite green.
6. **Collapse the 3 watchdogs** into one generic driver; update `_sipo_queue.py` calls.
   Smoke-test on an already-cached party (no OCR needed).
7. **Delete** the 7 `probe_sipo_ocr_*.py` throwaways + audit/dead helpers
   (`sipo_expenses_roster_fix.py`, `sipo_textlayer_party.py`, `_explore_sipo_quality.py`)
   — keep only what core didn't absorb.

Steps 1–6 are reversible and test-guarded; step 7 is cleanup once nothing imports them.

## Pre-execution gate (the plateau)

Execute only when ALL hold:
- [ ] Donations OCR finished (`_log_donations.txt` shows completion, parquet written).
- [ ] `_sipo_queue.py` finished (socdem + Part-4 items done) — completion watcher fired.
- [ ] No SIPO python workers running (candidate/items/donations) — `_sipo_queue.ocr_busy()` False.
- [ ] No pending live edits to the ETLs (no "modified since read").
- [ ] `test_sipo_data_quality.py` baseline captured (current pass/fail noted) so the
      migration can prove "no regression except the two we intend to fix."

## Expected outcome
- 22 files → ~7; duplicated symbols → single definitions.
- The items ETL gains born-digital + `--parse-only` (parity with the candidate ETL).
- The two open test failures resolved in shared code, for expenses *and* donations.
- One PaddleOCR config, one parquet-write convention, one watchdog — the Windows gotchas
  and the no-inference/privacy posture documented in one place.
