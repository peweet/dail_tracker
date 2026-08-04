"""Deterministic project and runtime path resolution."""

from __future__ import annotations

from pathlib import Path

import pytest

from paths import PROJECT_ROOT, absolute_path, configured_path, runtime_path


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
