# pipeline_sandbox/_archive — retired probes (moved, not deleted)

Dead probes with **zero references in live code or tests** (verified 2026-07-16),
moved here instead of deleted so they can be restored trivially. Git history is
still the ultimate archive; this folder just keeps them visible and one `git mv`
away from return.

**Restore any file:** `git mv pipeline_sandbox/_archive/<file> pipeline_sandbox/<file>`
(or `git checkout 18dd551 -- pipeline_sandbox/<file>` from the pre-archive commit).

Nothing here is imported, invoked by path string, or exercised by a test — checked
against `extractors/ services/ sql_views/ test/ doc/ utility/` before moving.

| File | Why retired | Superseded by / status |
|---|---|---|
| `_seanad_output/` (2026-08-01) | Seanad vote-history / senator-payment output tree, 33 MB, 13 files | Zero references — `rg "_seanad_output"` across the repo returned no files. Data only (`.csv`/`.parquet`), no code. Snapshotted in the `restic_sandbox` repo under tag `pre-move` before the move. |
| `etenders_live_probe.py` | eTenders live-pull probe | ITT login/JS-gated dead end |
| `etenders_itt_pull_probe.py` | ITT formula pull probe | login/JS-gated dead end (ITT gated) |
| `inspect_hse_tusla.py` | throwaway inspection script | one-off |
| `cpo_planning_prospect_probe.py` | CPO compensation prospect probe | CPO feature parked |
| `procurement_unlinked_payees_probe.py` | unlinked-payee QA probe | superseded by `services/coverage_qa.py` |
| `si_department_backfill.py` | one-off SI department backfill | already applied |
| `procurement_la_registry.py` | LA route registry | routes merged into `extractors/procurement_la_payments_extract.py` (provenance note retained there) |

Archived from HEAD `18dd551`.
