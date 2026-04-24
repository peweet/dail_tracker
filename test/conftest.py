import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: requires pipeline output files to exist (run pipeline.py first)",
    )
