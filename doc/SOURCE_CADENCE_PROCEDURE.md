# Source cadence — the ledger, the registry map, and the new-source procedure

The **cadence ledger** (`data/_meta/source_cadence.csv`) is the unified, meta-level
index of every data source we target: its refresh cadence, when it is next due, the
poller that pulls it, the runner it must run on, and the test package(s) that cover it.
It exists so a source that needs pulling in the future gets flagged before it goes
stale — 165 sources is past what anyone eyeballs.

Tools: `tools/migration/build_source_cadence.py` (seed/sync) · `tools/migration/check_source_cadence.py`
(the DUE/OVERDUE checker, reusing `freshness_status.py`'s cadence×grace logic).

## The registry map — why there is more than one, and which is authoritative

The registries are a **layer, not duplicates**. Consolidation verdict: keep the layer,
make the cadence ledger the single unified INDEX over it (done — it ingests the two
live registries and lists the rest here).

| Registry | Rows | Role | In the ledger? |
|---|---:|---|---|
| `data/_meta/source_registry.generated.json` | 129 | **Canonical live pipeline sources**, generated from 5 in-code configs | **Yes** (`registry=source_registry`) |
| `planning_rules/_corpus_registry/planning_corpus_seed.csv` | 36 | **Parallel live registry** for the planning corpus; grew its own `update_cadence` column | **Yes** (`registry=planning_corpus`) |
| `extractors/procurement_publishers_seed.py` (SEEDS → `procurement_publishers/publishers_seed.csv`, 77) | — | **Input that FEEDS** the canonical registry's procurement sources | No — arrives via the generated registry |
| `extractors/procurement_la_seed.py` | — | Input seed (LA payments) | No — feeds the registry |
| `pipeline_sandbox/public_decisions/seed_registry.csv` | 69 | **Experimental target list** (HSE forums, public-decision bodies); not live | No — promote first |
| `pipeline_sandbox/council_minutes/council_seeds.csv` | 31 | Experimental council-minutes targets | No — promote first |
| `pipeline_sandbox/disclosed_po_spend/build/tranche1_registry.csv` | 11 | Sandbox tranche of disclosed-PO bodies | No — promote first |
| `data/_meta/ppp_project_registry.csv` | 25 | **Not a source registry** — a curated entity/data table (PPP projects + amounts) | No — different kind |

**No redundant duplication was found.** The seeds feed the canonical registry; the
sandbox lists are pre-promotion targets. The one genuine overlap was *cadence
semantics*: `planning_corpus_seed.csv` had independently invented an `update_cadence`
column. That is now folded into the ledger, so cadence is curated in one place.

### The canonical registry is generated from these 5 configs
`oireachtas_pdf_poller.SOURCES` · `ipas_sources.IPAS_SOURCES` ·
`procurement_public_body_extract.PUBLISHERS` · `procurement_la_payments_extract.SCHEMA_MAP`
· `afs_amalgamated_extract.URLS` + `procurement_hse_tusla_parser.SPECS` +
`MANUAL_SOURCES`. To change what sources exist, edit a config and regenerate — never
hand-edit the generated JSON.

## Ledger columns

`source_id, name, group, registry, cadence, cadence_days, next_expected,
release_window, poller, runner, test_packages, curated, notes`

- **cadence / cadence_days** — the human label and the numeric the checker compares.
  `cadence_days=0` = one-off (never due). Seeded with a guess flagged `curated=no`.
- **next_expected / release_window** — a known future date (`2026-10`) or a window
  hint (`Jan/May/Sep`, `autumn`). This is what turns a stale-check into a *due*-check.
- **poller** — the module (or poll method) that pulls it.
- **runner** — `residential` if any host is behind the gov.ie WAF (see
  `doc/CLOUD_READINESS.md`), else `cloud`. The seed guess matched the WAF gauge 27/27.
- **test_packages** — a rough "which tests to run when this source changes" hint,
  derived by scanning `test/` for the poller module name. `<none>` flags a test gap
  (all 36 planning rows are `<none>` — real thin coverage, worth noting).
- **curated** — `no` until a human sets the real cadence, then `yes`. The checker
  reports uncurated rows as `REVIEW`, so it never pretends to judge a guessed cadence.

## The procedure for a NEW source

1. **Add the source** to its config (a poller `SOURCES`/`PUBLISHERS`/seed) and
   regenerate the canonical registry: `python tools/build_source_registry.py`.
   (Planning-corpus datasets: add a row to `planning_corpus_seed.csv`.)
2. **Sync the ledger**: `python tools/migration/build_source_cadence.py`. The new source is
   appended with a guessed cadence, its poller, runner, and test_packages pre-filled,
   `curated=no`.
3. **Curate the row**: set `cadence` / `cadence_days`, add `next_expected` /
   `release_window` if the release calendar is known, and set `curated=yes`. This is
   the only manual step and only the operator can do it (the release calendar is not
   derivable from code).
4. **Verify**: `python tools/migration/check_source_cadence.py --due-only` shows nothing
   surprising; `--strict` is green.

### The ratchet that enforces it
`python tools/migration/build_source_cadence.py --check` **exits 1 on drift** — any source in a
registry that is missing from the ledger (or a ledger row whose source vanished). Wire
it into the fast test subset / the source-health canary so a new source cannot ship
without a ledger row. Baseline rule as elsewhere: the drift check fails the build; you
sync, you never suppress.

## Promoting a sandbox source

When a `pipeline_sandbox/*` target goes live, it gains a config entry and a generated
registry `source_id` — at which point step 1 above already covers it. Until then it
stays in its sandbox seed and is tracked here, in the table above, as a known target.
