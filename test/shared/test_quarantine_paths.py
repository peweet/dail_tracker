"""Path-boundary tests for the shared quarantine writer."""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("polars")

from paths import PROJECT_ROOT
from shared.quarantine import QUARANTINE_DIR, _quarantine_path


def test_default_quarantine_directory_is_absolute_and_repo_anchored() -> None:
    assert QUARANTINE_DIR.is_absolute()
    assert QUARANTINE_DIR.relative_to(PROJECT_ROOT) == Path("data/silver/_quarantine")


@pytest.mark.parametrize(
    ("source", "run_id"),
    [
        ("../private", "run-1"),
        ("payments", "../run-1"),
        ("payments", "run/1"),
        ("payments", r"run\\1"),
        ("Payments", "run-1"),
    ],
)
def test_quarantine_path_rejects_unsafe_components(tmp_path: Path, source: str, run_id: str) -> None:
    with pytest.raises(ValueError):
        _quarantine_path(tmp_path, source, run_id)


def test_quarantine_path_is_absolute_and_normalises_iso_colons(tmp_path: Path) -> None:
    result = _quarantine_path(tmp_path, "payments", "2026-08-04T12:34:56+00:00-a1b2")
    assert result.is_absolute()
    assert result.parent == tmp_path.resolve()
    assert result.name == "payments_2026-08-04T12-34-56+00-00-a1b2.parquet"
