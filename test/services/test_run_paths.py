"""Security and determinism checks for per-run filesystem paths."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from paths import PROJECT_ROOT
from services import run_paths


def _use_temporary_log_root(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    log_dir = tmp_path / "logs"
    monkeypatch.setattr(run_paths, "LOG_DIR", log_dir)
    monkeypatch.setattr(run_paths, "RUNS_DIR", log_dir / "runs")
    monkeypatch.setattr(run_paths, "LATEST_POINTER", log_dir / "latest_run_id.txt")
    return log_dir


def test_generated_run_id_is_accepted() -> None:
    run_id = run_paths.make_run_id()
    assert run_paths.validate_run_id(run_id) == run_id


@pytest.mark.parametrize(
    "run_id",
    ["", ".", "..", "../escape", "nested/escape", r"nested\escape", "C:escape", "white space"],
)
def test_run_dir_rejects_non_component_ids(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, run_id: str) -> None:
    log_dir = _use_temporary_log_root(monkeypatch, tmp_path)

    with pytest.raises(ValueError, match="run_id"):
        run_paths.run_dir(run_id)

    assert not (tmp_path / "escape").exists()
    assert not (log_dir / "runs" / "nested").exists()


def test_run_dir_returns_a_contained_absolute_path(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _use_temporary_log_root(monkeypatch, tmp_path)

    path = run_paths.run_dir("manual-run_01")

    assert path.is_absolute()
    assert path.parent == run_paths.RUNS_DIR.resolve()
    assert (path / "steps").is_dir()


def test_latest_pointer_validates_before_writing(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _use_temporary_log_root(monkeypatch, tmp_path)

    with pytest.raises(ValueError, match="run_id"):
        run_paths.write_latest_pointer("../outside")

    assert not run_paths.LATEST_POINTER.exists()


def test_git_sha_is_read_from_project_root(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}

    def fake_run(command: list[str], **kwargs: object) -> SimpleNamespace:
        seen["command"] = command
        seen.update(kwargs)
        return SimpleNamespace(stdout="abc123\n")

    monkeypatch.setattr(run_paths.subprocess, "run", fake_run)

    assert run_paths.get_git_sha() == "abc123"
    assert seen["command"] == ["git", "rev-parse", "--short", "HEAD"]
    assert seen["cwd"] == PROJECT_ROOT
