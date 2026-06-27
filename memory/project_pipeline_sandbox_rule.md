---
name: project_pipeline_sandbox_rule
description: pipeline_sandbox holds DELIBERATE live scrapers (not just dead probes); how to tell load-bearing from dead before deleting/moving
metadata:
  type: project
---

`pipeline_sandbox/` is half throwaway probes, half **deliberately-sandboxed live
scrapers**. "It's in sandbox" is NO signal about whether it's safe to delete/move.

**The pattern:** fragile network scrapers stay in `pipeline_sandbox/` on purpose (run
by hand, NOT wired into `pipeline.py`/GH-Action) and write `data/sandbox/…parquet`. A
transform-only `extractors/*_promote_to_gold.py` reads that vetted parquet → writes the
**committed** gold projection → SQL view → test. Committed gold is the safety net.
LIVE sandbox extractors are invoked by **path string**, not import — so grep-for-imports
misses them (ISIF / CBI / EU-TAM via `enrichment_promote_to_gold.py`; EPA via
`epa_promote_to_gold.py`; council_minutes, news_mentions, housing, committee_evidence,
disclosed_po_spend, courts_reader, `_planning_output/`, etc.). Full table in `doc/SANDBOX_MAP.md`.

2026-06-27 audit: almost every committed-gold promotion is already test-anchored. The one
exposed gap was **EPA** (committed gold + natural-person privacy mask, only view-level smoke
coverage) — closed by extracting `project_supplier_compliance()` + `gold_pii_columns()` and
adding `test/extractors/test_epa_promote_privacy.py`. User pivoted AWAY from deleting dead
probes toward promoting/anchoring confident code; nothing was deleted.

**Why:** a naive "sandbox = scratch, safe to nuke" or import-only reference check would
delete load-bearing extractors and break the gold-promote chain + tests. Nearly did.

**How to apply:** before retiring/moving anything in sandbox, run
`git grep -l -- "<basename>" -- extractors services sql_views test doc planning_rules`.
If it's promoted to committed gold, make sure its promote-transform is test-anchored
(privacy drop + no-PII-column + value/no-money invariant) before editing it. Git history
is the archive for genuine deletions — see the dead-probe candidate list in `doc/SANDBOX_MAP.md`.
Related: [[feedback_personal_insolvency_privacy]], [[project_logic_firewall_graduation_2026_06_20]],
[[feedback_pipeline_changes_data_anchored_promotion]], [[reference_data_map]].
