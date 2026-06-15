---
name: feedback_pipeline_changes_data_anchored_promotion
description: AXIOM — any pipeline change must be data-anchored AND follow the sandbox→validate→promote strategy; no loose edits to production
metadata:
  type: feedback
---

**AXIOM (owner, 2026-06-15, stated firmly): every change to the PIPELINE must be (1) anchored in
data and (2) follow the promotion strategy. Loose engineering decisions are unacceptable.**

"Pipeline" = code that writes the production gold/silver tables the app reads (e.g.
`procurement_payments_consolidate.py` → `data/gold/parquet/procurement_payments_fact.parquet`).
Sandbox/pre-ETL tools that write `data/sandbox/` (e.g. `procurement_award_spend_link.py`) are NOT
the production pipeline, but the same discipline applies before promoting their output.

**Why:** triggered after I (a) over-stated a "€2.83bn PPP" figure that secretly mixed value tiers
(payment_actual + po_committed) and 14 years of cumulative payments, then (b) wired a public-body
reclassification straight into the GOLD consolidation and re-ran it — a production change made too
fast, bundled into a rapid sequence, without a deliberate validated promotion step. The owner had
me revert the pipeline change.

**How to apply — before touching pipeline code:**
1. ANCHOR IN DATA FIRST. Prove the change against the actual rows in a sandbox probe — counts,
   tier split (never sum payment_actual + po_committed), year span (cumulative ≠ a figure),
   null/aggregate/outlier rows, double-count across publishers. State numbers honestly with their
   caveats; do not present a blended total as a clean fact.
2. FOLLOW THE PROMOTION STRATEGY. sandbox probe → curated/vetted artifact (e.g. a reviewed
   `data/_meta/*.csv` with a confidence threshold) → ONLY THEN wire into the pipeline as its own
   deliberate, validated step — not folded into a batch of other edits. The CRO-override promotion
   (probe → overlap≥0.85 + ≥2-token vetting → CSV → wire) is the GOOD template; copy it.
3. Production gold changes get their own checkpoint. Don't re-run the gold writer as a side effect.

Default to a sandbox artifact + a recommendation, and let the owner approve promotion. Related:
[[project_payments_award_linkage_2026_06_15]] (the incident), [[feedback_refactor_timing]].
