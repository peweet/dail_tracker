"""Regression tests for the data-refresh-only CI classifier."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from tools.ci_data_refresh import is_data_refresh_only, is_publish_path, paths_between
from tools.publish_data import _validate


def test_publish_paths_are_recognised() -> None:
    assert is_publish_path("data/gold/parquet/procurement_awards.parquet")
    assert is_publish_path("data/silver/parquet/etenders_live_tenders.parquet")
    assert is_publish_path("data/_meta/freshness.json")
    assert is_publish_path("data/_meta/heartbeats/money_flow.json")


def test_data_refresh_only_requires_a_nonempty_allowlisted_diff() -> None:
    assert is_data_refresh_only(
        [
            "data/gold/parquet/procurement_awards.parquet",
            "data/_meta/heartbeats/live_tenders.json",
        ]
    )
    assert not is_data_refresh_only([])
    assert not is_data_refresh_only(["data/gold/parquet/procurement_awards.parquet", "utility/app.py"])


def test_classifier_rejects_nearby_but_unpublishable_paths() -> None:
    assert not is_publish_path("data/silver/parquet/unrelated.parquet")
    assert not is_publish_path("data/_meta/source_health.json")
    assert not is_publish_path(".github/workflows/ci.yml")


@pytest.mark.parametrize(
    "deleted_path",
    [
        "data/silver/parquet/etenders_live_tenders.parquet",
        "data/_meta/freshness.json",
    ],
)
def test_publish_gate_rejects_deleted_runtime_artifacts(tmp_path: Path, deleted_path: str) -> None:
    with pytest.raises(SystemExit, match="allowed runtime artifact was deleted"):
        _validate(tmp_path, [deleted_path], tolerance=0.5)


def test_publish_gate_allows_an_existing_metadata_update(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "data" / "_meta" / "freshness.json"
    path.parent.mkdir(parents=True)
    path.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        "tools.publish_data.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="", stderr=""),
    )

    _validate(tmp_path, ["data/_meta/freshness.json"], tolerance=0.5)


def test_classifier_fails_closed_for_renamed_publishable_data(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "tools.ci_data_refresh.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout=("R100\0data/gold/parquet/old.parquet\0data/gold/parquet/new.parquet\0"),
        ),
    )

    assert paths_between("base", "head") is None
