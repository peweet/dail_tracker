import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: requires pipeline output files to exist (run pipeline.py first)",
    )
    config.addinivalue_line(
        "markers",
        "bronze: requires bronze ingestion to have run (no network)",
    )
    config.addinivalue_line(
        "markers",
        "sources: requires network — checks external PDF/API endpoints",
    )
    config.addinivalue_line(
        "markers",
        "sql: requires pipeline output; executes DuckDB SQL views",
    )
