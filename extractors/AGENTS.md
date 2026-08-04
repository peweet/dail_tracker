# Extractor guidance

- Use Polars and the shared helpers: `services.http_engine` for HTTP, `services.coverage_io.save_coverage`, `services.parquet_io.save_parquet`, and `services.extract_runner.run_extractor`.
- Treat source fidelity as part of the contract: retain source URL/page/date fields and quarantine ambiguous parses instead of silently guessing.
- Never inspect an entire source corpus or parquet in agent context. Use a bounded representative sample and report completeness/recall separately from parser correctness.
- Add or update the closest `test/extractors/test_<name>.py` test. Run `uv run python tools/dev.py conventions` as well as focused tests.

