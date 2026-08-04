"""Scanner outputs must not depend on the caller's working directory."""

from __future__ import annotations

from pathlib import Path

import pytest

from tools.migration import extract_class_contract, extract_url_contract


@pytest.mark.parametrize("module", [extract_url_contract, extract_class_contract])
def test_relative_output_is_anchored_to_absolute_project_root(tmp_path, monkeypatch, module) -> None:
    project_root = (tmp_path / "project").resolve()
    monkeypatch.setattr(module, "PROJECT_ROOT", project_root)

    assert module.output_path(Path("doc/contract.md")) == project_root / "doc" / "contract.md"


@pytest.mark.parametrize("module", [extract_url_contract, extract_class_contract])
def test_absolute_output_is_preserved(tmp_path, module) -> None:
    destination = (tmp_path / "outside" / "contract.md").resolve()

    assert module.output_path(destination) == destination
