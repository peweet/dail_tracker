# Civic planning extractor guidance

- These are live extractors, not disposable sandbox probes.
- Use Polars plus `services.http_engine`, `services.coverage_io.save_coverage`, `services.parquet_io.save_parquet`, and `services.extract_runner.run_extractor`.
- Keep coordinate systems, geometry repair, source URLs, and coverage evidence explicit. Do not infer a missing planning fact from nearby records.
- Run the matching `test/planning/` tests and `uv run python tools/dev.py conventions`.

