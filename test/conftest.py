"""Shared pytest configuration.

The first import caps BLAS threads for the test process. pytest imports pandas/numpy
through most of the suite, and an uncapped OpenBLAS reserved ~650 MB of commit per
process on a 20-core box — with several sessions and hooks live, that is what pushed
the machine into OOM (see services/runtime_env.py).
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

import services.runtime_env  # noqa: E402,F401


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
