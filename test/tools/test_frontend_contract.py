"""Machine-readable frontend contract regression tests."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

from tools.migration import build_frontend_contract as contract

ROOT = Path(__file__).resolve().parents[2]


def test_committed_frontend_contract_matches_live_sources() -> None:
    committed = contract.DEFAULT_OUT.read_text(encoding="utf-8")
    assert committed == contract.render_manifest()


def test_frontend_contract_is_framework_neutral_and_complete() -> None:
    manifest = contract.build_manifest()

    assert manifest["schema_version"] == 1
    assert len(manifest["routing"]["routes"]) >= 30
    assert len(manifest["routing"]["query_parameters"]) >= 58
    assert len(manifest["styling"]["styled_classes"]) >= 900
    assert manifest["styling"]["page_local_stylesheets"]

    for route in manifest["routing"]["routes"]:
        assert set(route) == {"path", "title", "module"}
        assert route["path"].startswith("/")

    stylesheet = ROOT / manifest["styling"]["shared_stylesheet"]["path"]
    payload = stylesheet.read_bytes()
    assert manifest["styling"]["shared_stylesheet"]["sha256"] == hashlib.sha256(payload).hexdigest()
    assert manifest["styling"]["shared_stylesheet"]["bytes"] == len(payload)

    serialized = json.dumps(manifest)
    assert "session_state" not in serialized
    assert "app.py line" not in serialized


def test_frontend_contract_check_fails_closed_on_drift(tmp_path: Path, monkeypatch, capsys) -> None:
    stale = tmp_path / "frontend_contract.json"
    stale.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(contract, "DEFAULT_OUT", stale)
    monkeypatch.setattr(sys, "argv", ["build_frontend_contract.py", "--check"])

    assert contract.main() == 1
    assert "Frontend contract DRIFT" in capsys.readouterr().err
