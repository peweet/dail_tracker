# SIPO GE2024 political-finance pipeline

Production extraction of the **2024 general election (34th Dáil)** SIPO returns:
national-agent **election expenses** (candidate summaries + itemised) and party
**donations**. OCR-derived from the official scanned PDFs; every figure carries a
`flag` + confidence and must be shown with a "verify vs the official SIPO PDF"
caveat (no-inference rule).

Graduated out of `pipeline_sandbox/` to first-class `extractors/` on 2026-06-05.

## Layout

```
extractors/
  sipo_expenses_paddle_etl.py        # Part 3 candidate-summary (assigned / spend), per party
  sipo_expense_items_paddle_etl.py   # Part 4 itemised line items + category totals
  sipo_donations_paddle_etl.py       # party donations (cache + parse)
  _sipo_watchdog.py                  # bounds PaddleOCR hangs/segfaults for the expenses ETL
  _sipo_items_watchdog.py            # …for the Part-4 items ETL
  _sipo_donations_watchdog.py        # …for the donations cache stage
  _sipo_queue.py                     # waits for OCR-free, runs the watchdogs sequentially
  build_part4_no_ocr.py              # Part-4 for FF (cache) + SF/Aontú (born-digital), no OCR
  sipo_promote_to_gold.py            # silver → gold (donations + expenses)
test/
  test_sipo_data_quality.py          # invariants on the SILVER expenses fact (+ scorecard)
  test_sipo_expenses.py              # invariants on the GOLD expenses fact (view-facing)
data/bronze/scan_pdf/                # source PDFs (8 party expense returns + donations)
data/silver/sipo/                    # intermediate facts + by_party/ + OCR _ckpt cache (gitignored)
data/gold/parquet/                   # sipo_expenses_fact.parquet, sipo_donations.parquet (committed)
sql_views/sipo_expenses_base.sql     # v_sipo_expenses_base  → reads gold
sql_views/sipo_donations.sql         # v_sipo_donations(+_by_party) → reads gold
data/_meta/sipo_ge2024_expenses_sources.md   # provenance + statutory limits
doc/SIPO_OCR_INVESTIGATION.md        # why PaddleOCR (Tesseract mangled the crisp scans)
```

## Two-stage design (why it survives OCR)

PaddleOCR is expensive (~25 min/party) and intermittently segfaults/hangs on this
Windows build. So OCR is **cached** once (`data/silver/sipo/.../c*.json` raw cells)
and the **parser re-runs freely** against the cache — no re-OCR. The watchdogs bound
hangs (kill + resume from checkpoints; a DPI retry ladder skips a deterministically
bad page). Born-digital returns (SF, Aontú) skip OCR via the embedded text layer.

## Run order

```bash
# 1. OCR (one-shot, expensive — via the watchdogs, one PaddleOCR at a time)
./.venv/Scripts/python.exe extractors/_sipo_watchdog.py ff fg lab green socdem pbp
./.venv/Scripts/python.exe extractors/_sipo_donations_watchdog.py
# 2. Re-parse from cache anytime (fast, no OCR) — writes data/silver/sipo/*.parquet
./.venv/Scripts/python.exe extractors/sipo_expenses_paddle_etl.py --parse-only
./.venv/Scripts/python.exe extractors/sipo_donations_paddle_etl.py parse
./.venv/Scripts/python.exe extractors/build_part4_no_ocr.py
# 3. Promote silver → gold (the SQL views read gold)
./.venv/Scripts/python.exe extractors/sipo_promote_to_gold.py
# 4. Validate
./.venv/Scripts/python.exe -m pytest test/test_sipo_data_quality.py test/test_sipo_expenses.py -q
```

The OCR `_ckpt` cache is committed-by-value into `data/silver/sipo/` (gitignored),
so steps 2–4 reproduce the facts on any machine **without** re-OCR.

## Validity anchors (engine-independent QA)

- Constituency must be in the closed set of **43** (fuzzy-matched, scored).
- Spend / assigned must respect the **statutory limit** (€38,900 / €48,600 / €58,350
  for 3/4/5-seat). Over-limit values are decimal-loss OCR mis-reads and are **flagged**
  (`over_limit_verify` / `assigned_over_limit_verify`), never shipped as fact.
- Σ spend reconciles to the form's printed TOTAL where present (Green = €36,729.60 exact).
- Nameless candidate rows (two-line-layout OCR phantoms) are dropped transparently —
  the name is never invented.

## Privacy

Donor **home addresses** (`donor_address_raw`) are PII: captured in silver but
**dropped** in `promote_to_gold` so they never reach git or the UI. Donor name +
amount + party are the public SIPO record. Gold carries figures + flags only.
