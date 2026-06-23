# Disclosed BigQuery export — data-quality scan & ingest-cleaning rules

Scan of `data/raw_bq/bq-results-20260619-122315-1781871808837.csv` (582,119 rows). All findings drive the silver-lane cleaning before any gold merge.

## Encoding / structure
- UTF-8, no BOM, decodes cleanly; **0** U+FFFD replacement chars (earlier mojibake was console-only). 26,158 rows carry accented chars.
- **589,822 physical lines vs 582,119 logical rows** → ~7,700 embedded newlines inside quoted STRING fields (classic BQ string-export artifact).

## Field hygiene → CLEANING RULES (apply in the silver extractor)
| Issue | Count | Rule |
|---|---|---|
| Embedded newline in a field | 5,914 | strip `\n` → space |
| Tab / carriage-return in a field | 102 / 8 | strip → space |
| Untrimmed supplier/description | 1,540 | `.strip()` |
| entity double-space ("Agency :  X") | 710 | collapse whitespace after stripping "Agency :" |
| Literal null tokens in PO/Supplier/Desc (`'null'` 32, `'n/a'` 65) | 97 | coerce to real NULL |
| Empty Supplier | 3,122 | NULL (do not drop — many are rollup/agg rows) |
| Empty Description | 22,559 | NULL |
| Empty PO | 240,100 | **signal, not error** — the payment-list vs PO-commitment regime divider |

## Total column
- No scientific notation. 1 negative, 47 zeros (flag/quarantine). 321,233 rows (55%) carry cents.
- 105 lines ≥ €50m — overwhelmingly the roll-up bodies (below).

## ⚠ Roll-up bodies — EXCLUDE from line-item gold
Three bodies publish whole-category quarterly aggregates, not €20k purchase orders (100% blank supplier, avg line value 100–1000× a real PO):

| Body | rows | avg line | max line | gross |
|---|---:|---:|---:|---:|
| Irish Water | 401 | €27.6m | €506.5m | ~€11.0bn |
| Eirgrid | 305 | €9.7m | €594.2m | ~€3.0bn |
| Gas Networks Ireland | 374 | €5.8m | €43.9m | ~€2.2bn |

**€16.2bn total.** These would (a) halt `guard_payment_fact` (no clean per-line `amount_semantics`) and (b) be meaningless as line items. Exclude from the disclosed lane's first cut (integration plan §3, independently confirmed here). High-value bodies with *real* suppliers/POs (TII €1.17m avg, NAMA, National Paediatric Hospital €107m max) are genuine capital POs — keep.

## Duplicates — do NOT dedup within-lane
- 20,274 exact-duplicate extra rows (3.5%). **18,336 (90%) are blank-PO payment lines** — legitimate repeat payments of an identical amount in the same quarter, not export artifacts. Only 1,938 are numbered-PO rows.
- Within-lane dedup would destroy real payments. Dedup is needed **only cross-lane** (the HSE-in-two-lanes double-count trap, integration plan §5). Preserve the disclosed lane row-for-row.

## Consistency (clean)
- `year_quarter` == `Year`-`QTR` for **all** 582,119 rows (0 mismatch). QTR ∈ {q1,q2,q3,q4} only. No mid-file header/footer rows. Row count is organic (not a round export cap); 2011 (9 rows) and 2026 (15,029) are genuine partial tails.
