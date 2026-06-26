# Disclosed national PO/payments dataset — deliverables index

Read in this order. All work is read-only/sandbox; nothing promoted to gold/silver; no git run.

Source: `data/raw_bq/bq-results-20260619-122315-1781871808837.csv` (582,119 rows · 216 bodies · 2011-q1→2026-q1).

## Read first
1. **`FINDINGS.md`** — is it trustworthy, what does it add, what are the semantics. (Verification workflow.)
2. **`POTENTIAL.md`** — how rich / how far back / what can be determined + ranked feature & story leads. (Deep-dive workflow.)
3. **`../../doc/DISCLOSED_PO_INTEGRATION_PLAN.md`** — how to merge it into silver→gold (design only; code-verified). (Integration workflow.)

## Supporting analysis
- `COVERAGE_FINDINGS.md`, `THEME_TRENDS_FINDINGS.md`, `cross_corpus_leverage.md` — narrative detail behind §2/§3 of POTENTIAL.md.
- `per_body_coverage.csv` — every body's first/last quarter, continuity %, gross.
- `body_regime.csv` / `body_regime_crosswalk.csv` — per-body payment-vs-PO regime + the 53-rename → existing-publisher crosswalk.
- `candidate_new.csv` — the 141 genuinely-new bodies.
- `market_structure.csv` / `body_concentration*.csv` / `top_suppliers_overall.csv` / `biggest_named_lines.csv` — supplier concentration & headline lines.
- `cross_body_suppliers.csv` — every supplier × number of bodies (the whole-of-State footprint).
- `theme_trends.csv` / `theme_year_regime_long.csv` / `theme_trend_early_late.csv` — category trends over time.
- `richness_detail.csv`, `earlier_than_ours.csv`, `top25_classification.csv`, `fact_publishers.csv`, `bq_bodies.csv` — field profiling & comparisons.

## The one rule that governs everything
The ~€117bn gross is meaningless. Payment-list bodies, PO-commitment bodies, and utility category roll-ups must **never** be summed together. Every euro stays within a single body or is flagged gross-line-value-not-spend.
