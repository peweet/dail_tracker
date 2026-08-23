"""Contract tests for the optional PyO3 name-normalisation trial."""

from __future__ import annotations

import importlib.util

import pytest

from shared.name_norm import name_norm_many, name_norm_str

CORPUS: list[object] = [
    None,
    "",
    "  Tirl\u00e1n Ltd.  ",
    "Turner & Townsend",
    "ACME Holdings Limited",
    "The O'Connell Group PLC",
    "Designated Activity Company",
    "R & D (Ireland) DAC",
    "na\u00efve co\u00f6perative",
    2026,
]


def test_python_batch_preserves_the_scalar_contract() -> None:
    assert name_norm_many(CORPUS) == [name_norm_str(value) for value in CORPUS]


def test_adapter_rejects_invalid_backend_and_worker_count() -> None:
    with pytest.raises(ValueError, match="unknown name-normalisation backend"):
        name_norm_many(["Acme"], backend="accelerated")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="workers must be at least one"):
        name_norm_many(["Acme"], workers=0)


def test_native_backend_is_explicit_when_the_extension_is_absent() -> None:
    if importlib.util.find_spec("dail_native") is not None:
        pytest.skip("the optional extension is installed for this environment")

    with pytest.raises(RuntimeError, match="optional dail_native PyO3 trial is not built"):
        name_norm_many(["Acme"], backend="native")


def test_native_and_shadow_match_the_python_oracle() -> None:
    pytest.importorskip("dail_native")

    expected = [name_norm_str(value) for value in CORPUS]
    assert name_norm_many(CORPUS, backend="native", workers=2) == expected
    assert name_norm_many(CORPUS, backend="shadow", workers=2) == expected
