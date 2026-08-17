"""Deterministic project and runtime path resolution."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from paths import PROJECT_ROOT, RESOURCE_ROOT, absolute_path, configured_path, runtime_path


def test_project_root_is_the_packaged_resource_root() -> None:
    assert PROJECT_ROOT == RESOURCE_ROOT
    assert (RESOURCE_ROOT / "sql_views").is_dir()


def test_relative_paths_are_anchored_to_the_project_not_cwd(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    assert absolute_path("data/example.parquet") == (PROJECT_ROOT / "data/example.parquet").resolve()


def test_configured_relative_path_is_deterministic(monkeypatch) -> None:
    monkeypatch.setenv("DAIL_TEST_PATH", "data/cache")
    assert configured_path("DAIL_TEST_PATH", "unused") == (PROJECT_ROOT / "data/cache").resolve()


def test_runtime_path_uses_absolute_configured_root(tmp_path: Path, monkeypatch) -> None:
    root = tmp_path / "runtime"
    monkeypatch.setenv("DAIL_RUNTIME_DIR", str(root))
    result = runtime_path("downloads", "source.csv")
    assert result == (root / "downloads/source.csv").resolve()
    assert result.is_absolute()


@pytest.mark.parametrize("parts", [("..", "escape"), ("subdir", "..", "..", "escape")])
def test_runtime_path_rejects_traversal(tmp_path: Path, monkeypatch, parts: tuple[str, ...]) -> None:
    monkeypatch.setenv("DAIL_RUNTIME_DIR", str(tmp_path / "runtime"))
    with pytest.raises(ValueError, match="escapes"):
        runtime_path(*parts)


def test_config_data_root_can_live_outside_the_installed_resources(tmp_path: Path) -> None:
    data_root = (tmp_path / "mounted-data").resolve()
    env = dict(os.environ, DAIL_DATA_DIR=str(data_root), PYTHONPATH=str(PROJECT_ROOT))
    completed = subprocess.run(
        [sys.executable, "-c", "from config import DATA_DIR; print(DATA_DIR)"],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    assert Path(completed.stdout.strip()) == data_root


def test_data_consumers_follow_the_configured_root(tmp_path: Path) -> None:
    data_root = (tmp_path / "mounted-data").resolve()
    env = dict(os.environ, DAIL_DATA_DIR=str(data_root), PYTHONPATH=str(PROJECT_ROOT))
    # planning/product/ is the gitignored private overlay, absent from a public
    # checkout. The two public consumers below are asserted everywhere; the siting
    # root only where the overlay exists.
    siting_present = (PROJECT_ROOT / "planning" / "product" / "paths.py").exists()
    command = (
        "from dail_tracker_core.buyer_xref import XREF_CSV; "
        "from services.data_contracts import QUARANTINE_DIR; "
        "print(XREF_CSV); print(QUARANTINE_DIR); "
    )
    expected = [
        str(data_root / "_meta" / "procurement_publishers" / "buyer_xref.csv"),
        str(data_root / "_meta" / "quarantine"),
    ]
    if siting_present:
        command += "from planning.product.paths import DATA as SITING_DATA; print(SITING_DATA)"
        expected.append(str(data_root))
    completed = subprocess.run(
        [sys.executable, "-c", command],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )
    assert completed.stdout.splitlines() == expected
