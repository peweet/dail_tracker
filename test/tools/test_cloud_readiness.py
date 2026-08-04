"""AST-level regressions for the cloud-readiness source scanner."""

from __future__ import annotations

from pathlib import Path

from tools.migration.scan_cloud_readiness import _runtime_signals, runtime_resilience


def test_docstrings_and_comments_do_not_count_as_runtime_signals() -> None:
    source = '''"""requests.get("https://gov.ie") and Path("C:/tmp/private")"""
# requests.get("https://gov.ie")
VALUE = "portable"
'''
    signals = _runtime_signals(source, "example.py")
    assert signals["local_paths"] == []
    assert signals["bare_requests"] is False
    assert signals["gov_ie"] is False


def test_live_calls_and_path_literals_are_detected() -> None:
    source = '''from pathlib import Path
import requests
CACHE = Path("C:/tmp/cache.csv")
requests.get("https://gov.ie/file.csv")
'''
    signals = _runtime_signals(source, "example.py")
    assert signals["local_paths"] == ["C:/tmp/cache.csv"]
    assert signals["bare_requests"] is True
    assert signals["gov_ie"] is True


def test_python_encoding_cookie_is_honoured(tmp_path: Path) -> None:
    source = tmp_path / "latin1_source.py"
    source.write_bytes(b'# -*- coding: latin-1 -*-\nLABEL = "caf\xe9"\n')
    result = runtime_resilience([source])
    assert result["scan_errors"] == {}


def test_parse_errors_are_reported_instead_of_skipped(tmp_path: Path) -> None:
    source = tmp_path / "broken.py"
    source.write_text("def incomplete(:\n", encoding="utf-8")
    result = runtime_resilience([source])
    assert str(source) in result["scan_errors"]
