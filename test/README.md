# Test layout

The test tree **mirrors the source package layout** — each subdirectory maps to
the production package it exercises, so a reader can find the tests for any
module by following the same path. (This replaces the old flat `test/test_*.py`
dump; see `git log` for the move.)

```
test/
  conftest.py            # shared markers (integration / bronze / sources / sql)
  fixtures/              # shared fixture data (api/, payments/, sql_views/, ...)
  HANDS_OFF_TEST_PLAN.md

  api/                   # tests for  api/            (FastAPI exposition layer)
  charity/               #            charity/
  corporate/             #            corporate/
  dail_tracker_core/     #            dail_tracker_core/  (db + queries/ + results)
  extractors/            #            extractors/     (PDF/CSV ETL: afs, cro, sipo, si, procurement, ...)
  iris/                  #            iris/
  lobbying/              #            lobbying/
  members/               #            members/
  payments/              #            payments/
  pdf_infra/             #            pdf_infra/
  reference/             #            reference/
  services/              #            services/       (HTTP engine, API scrapers, url builders)
  shared/                #            shared/         (name/join-key normalisation)
  sql_views/             #            sql_views/      (registered DuckDB view contracts)
  tools/                 #            tools/          (freshness, source-health, regression guards)
  utility/               #            utility/        (Streamlit pages: imports + page smoke)
  wikidata/              #            wikidata/

  # cross-cutting suites (span several packages — no single source dir):
  pipeline/              # silver/gold parquet-layer schema contracts (pipeline.py output)
  seanad/                # Seanad-parity feature: ETL reuse + view wiring
```

## Conventions

- **Placement** — a test lives under the package whose code it imports/asserts.
  When a test spans packages, file it under the cross-cutting suite that names
  the feature (`pipeline/`, `seanad/`) rather than an arbitrary owner.
- **Path anchors** — every subdir is one level below `test/`, so file-relative
  paths are: repo root = `Path(__file__).resolve().parents[2]`, shared fixtures
  = `Path(__file__).resolve().parents[1] / "fixtures"`.
- **Packages** — each subdir has an empty `__init__.py`. With `test/__init__.py`
  present, pytest's prepend import mode auto-inserts the repo root onto
  `sys.path`, so source imports (`import config`, `from services import ...`)
  resolve without per-file `sys.path` hacks.
- **Discovery / CI is unchanged** — `testpaths = ["test"]` collects recursively
  and the CI lanes still select by marker
  (`pytest -m "not integration and not sql and not sources and not bronze"`,
  and the `sql` lane separately).
