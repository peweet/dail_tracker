"""Iris Oifigiúil domain: poll the gazette PDFs → silver notices → derived gold
(statutory instruments, bill-SI links, public appointments, corporate notices).

Top-level package (same pattern as ``services/``, ``shared/``, etc.). Run via
``python -m iris.<step>`` so the repo root (cwd) is on ``sys.path`` and
``import config`` resolves. Default input/output dirs come from ``config``
constants (BRONZE_DIR/SILVER_DIR), NOT ``__file__`` — so the package move is
path-safe. Internal graph: ``incremental_shards`` ↔ ``etl_polars`` (the lazy
circular ETL pair) and ``silver_rebuild`` → ``etl_polars``. ``si_entity_enrichment``
(the SI gold step) deliberately stays at repo root — it is cross-imported by
``wikidata.ministerial_tenure_build`` too.
"""
