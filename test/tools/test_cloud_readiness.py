"""AST-level regressions for the cloud-readiness source scanner."""

from __future__ import annotations

from pathlib import Path

from tools.migration.scan_cloud_readiness import (
    PROJECT_ROOT,
    _runtime_signals,
    discover_runtime_files,
    runtime_resilience,
)


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
    source = """from pathlib import Path
import requests
CACHE = Path("C:/tmp/cache.csv")
requests.get("https://gov.ie/file.csv")
"""
    signals = _runtime_signals(source, "example.py")
    assert signals["local_paths"] == ["C:/tmp/cache.csv"]
    assert signals["bare_requests"] is True
    assert signals["direct_http_calls"] == [{"kind": "requests.get", "line": 4}]
    assert signals["gov_ie"] is True


def test_requests_aliases_from_imports_and_sessions_are_detected() -> None:
    source = """import requests as req
from requests import get as imported_get
from requests import Session as Client

def client() -> Client:
    return Client()

direct = req.post("https://example.test")
imported = imported_get("https://example.test")
session = client()
bound = session.head("https://example.test")
chained = req.Session().request("GET", "https://example.test")
"""
    signals = _runtime_signals(source, "aliases.py")
    assert [(call["kind"], call["line"]) for call in signals["direct_http_calls"]] == [
        ("requests.post", 8),
        ("requests.get (imported as imported_get)", 9),
        ("requests.Session.head", 11),
        ("requests.Session.request", 12),
    ]


def test_urllib_module_and_function_aliases_are_detected() -> None:
    source = """import urllib.request as web
from urllib import request as request_api
from urllib.request import urlopen as open_url

web.urlopen("https://example.test/a")
request_api.urlopen("https://example.test/b")
open_url("https://example.test/c")
"""
    signals = _runtime_signals(source, "urllib_aliases.py")
    assert [call["kind"] for call in signals["direct_http_calls"]] == [
        "urllib.request.urlopen",
        "urllib.request.urlopen",
        "urllib.request.urlopen (imported as open_url)",
    ]


def test_shared_engine_import_is_detected_through_alias() -> None:
    source = """from services import http_engine as transport
transport.fetch_bytes("https://example.test/file")
"""
    signals = _runtime_signals(source, "engine_alias.py")
    assert signals["uses_engine"] is True
    assert signals["bare_requests"] is False


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


def test_runtime_discovery_covers_orchestrators_packages_and_pipeline_tools() -> None:
    discovered = {path.relative_to(PROJECT_ROOT).as_posix() for path in discover_runtime_files()}
    assert "pipeline.py" in discovered
    assert "members_refresh.py" in discovered
    assert "corporate/cro_poller.py" in discovered
    assert "planning/civic/extractors/planning_appeal_outcomes.py" in discovered
    assert "services/http_engine.py" in discovered
    assert "tools/build_source_health.py" in discovered


def test_transport_exemption_is_reported_with_rationale() -> None:
    result = runtime_resilience([PROJECT_ROOT / "services" / "http_engine.py"])
    exemption = result["transport_exemptions"]["services/http_engine.py"]
    assert "canonical shared" in exemption["reason"]
    assert exemption["calls"]
    assert result["bare_requests"] == []
