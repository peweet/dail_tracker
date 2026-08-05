# Migration-readiness toolkit

One home for the tools that measure how ready this project is to move off Streamlit
and onto the cloud, and that track every data source we depend on. Each tool is a
**gauge or a ratchet** — a gauge measures and never fails; a ratchet exits non-zero
when something regresses, for CI.

Run everything from the repo root (the tools resolve paths relative to it):

    python tools/migration/<tool>.py

## The tools (this directory)

| Tool | Kind | What it answers | Writes |
|---|---|---|---|
| `scan_framework_coupling.py` | gauge | How coupled is the code to Streamlit? (import graph + `st.*` surface) | stdout / `--json` |
| `extract_url_contract.py` | gauge + `--check` ratchet | What are the deep-link routes + query params any React router must keep? | `doc/URL_CONTRACT.md` |
| `extract_class_contract.py` | gauge + `--check` ratchet | What CSS class vocabulary must a new frontend emit to preserve the styling? | `doc/CLASS_CONTRACT.md` |
| `check_api_parity.py` | ratchet | Which core query functions are unreachable from the API (Streamlit-only)? | baseline below |
| `scan_cloud_readiness.py` | gauge | How exposed are we to WAF blocks / local-path blockers before a cloud move? | `doc/CLOUD_READINESS.md` |
| `build_source_cadence.py` | generator + `--check` ratchet | Seed/sync the unified source-cadence ledger from every registry | `data/_meta/source_cadence.csv` |
| `check_source_cadence.py` | gauge + `--strict` canary | Which sources are DUE / OVERDUE / BROKEN / TAKEN_DOWN? (cadence + health-duration; folds in the former source_health_ledger) | stdout / `--json` + `source_cadence_state.json` |

## The outputs (kept in their required homes — see below)

**Docs** — in `doc/` so `build_doc_index.py` indexes them:
- [FRAMEWORK_DECOUPLING_PLAN.md](../../doc/FRAMEWORK_DECOUPLING_PLAN.md) — the decoupling plan + all five ratchets + CORS/auth readiness
- [URL_CONTRACT.md](../../doc/URL_CONTRACT.md) · [CLASS_CONTRACT.md](../../doc/CLASS_CONTRACT.md) — the two frozen contracts
- [CLOUD_READINESS.md](../../doc/CLOUD_READINESS.md) — WAF exposure + runtime blockers
- [SOURCE_CADENCE_PROCEDURE.md](../../doc/SOURCE_CADENCE_PROCEDURE.md) — the registry map + new-source procedure

**Data** — in `data/_meta/` so the `.gitignore` negation keeps it tracked:
- [source_cadence.csv](../../data/_meta/source_cadence.csv) — the public source-registry cadence ledger

**Baselines** — in `tools/baselines/` (the ratchet baseline convention):
- `api_parity_baseline.txt` · `unstyled_classes_baseline.txt` · `markup_inline_baseline.json`

## Why the outputs did NOT move into this directory

Physical co-location stops at the scripts on purpose. Three machineries depend on the
other files staying where they are:
- `build_doc_index.py` scans `doc/` — docs moved here would drop out of the doc map.
- `data/_meta/source_cadence.csv` is git-tracked only via a `!data/_meta/` negation in
  `.gitignore`; elsewhere it becomes untracked and the cadence tools lose their input.
- `tools/baselines/` is the ratchet-baseline home read by every `--check`.

This README is the single index that ties them together, so retrieval is one hop even
though the files respect their homes.

## Related code (not a gauge, lives with its layer)

- `utility/data_access/_cache.py` + `test/test_cache_shim.py` — the framework-neutral
  caching shim (the §2.1 decoupling step); lives in the data-access layer it serves.

## The recommended pre-migration sweep

    python tools/migration/scan_framework_coupling.py      # coupling snapshot
    python tools/migration/scan_cloud_readiness.py         # WAF / blocker exposure
    python tools/migration/check_source_cadence.py         # what's due to pull
    python tools/migration/extract_url_contract.py --check # deep-link contract intact
    python tools/migration/extract_class_contract.py --check
    python tools/migration/check_api_parity.py             # API parity intact
    python tools/migration/build_source_cadence.py --check # ledger covers all sources
