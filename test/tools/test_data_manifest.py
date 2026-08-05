"""Regression tests for the restore-verification manifest."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_module():
    path = Path("tools/data_manifest.py")
    spec = importlib.util.spec_from_file_location("data_manifest_test_subject", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_check_does_not_overwrite_the_expected_manifest(monkeypatch, tmp_path: Path) -> None:
    module = _load_module()
    manifest = tmp_path / "backup_manifest.tsv"
    original = "# expected baseline\nold-sha\t1\tbronze/source.pdf\n"
    manifest.write_text(original, encoding="utf-8")

    monkeypatch.setattr(module, "MANIFEST_PATH", manifest)
    monkeypatch.setattr(module, "_scan", lambda: {"bronze/source.pdf": ("new-sha", 2)})
    monkeypatch.setattr(module, "setup_standalone_logging", lambda _name: None)
    monkeypatch.setattr(sys, "argv", ["data_manifest.py", "--check"])

    assert module.main() == 1
    assert manifest.read_text(encoding="utf-8") == original
