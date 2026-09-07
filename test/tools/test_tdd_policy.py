"""Regression contracts for the repository's test-first workflow and test lanes."""

from __future__ import annotations

import ast
import importlib.util
import tomllib
from pathlib import Path

import pytest

import tools.dev as dev
from tools import verify_changed as vc

ROOT = Path(__file__).resolve().parents[2]
NON_FAST_MARKERS = ("integration", "sql", "sources", "bronze", "layers", "slow", "crosshair")


def _decorators(path: Path, function_name: str) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function_name:
            return {ast.unparse(decorator) for decorator in node.decorator_list}
    raise AssertionError(f"{function_name} not found in {path.relative_to(ROOT)}")


def test_fast_selectors_exclude_every_non_fast_marker() -> None:
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    for marker in NON_FAST_MARKERS:
        exclusion = f"not {marker}"
        assert exclusion in dev.FAST_MARKERS
        assert exclusion in vc.FAST_MARKERS
        assert exclusion in ci
    assert 'pytest -m "slow or crosshair"' in ci


def test_pytest_marker_policy_is_strict_and_describes_slow_tests() -> None:
    config = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    pytest_config = config["tool"]["pytest"]["ini_options"]

    assert "--strict-markers" in pytest_config["addopts"]
    assert any(marker.startswith("slow:") for marker in pytest_config["markers"])


def test_live_truthfulness_cross_check_is_explicitly_a_source_test() -> None:
    decorators = _decorators(
        ROOT / "test" / "pipeline" / "test_truthfulness.py", "test_dail_divisions_match_official_api"
    )

    assert "pytest.mark.sources" in decorators


def test_deadline_stress_tests_are_kept_out_of_the_fast_lane() -> None:
    path = ROOT / "test" / "mcp_server" / "test_resource_policy.py"
    slow_cases = (
        "test_deadline_interrupts_an_overrunning_call",
        "test_interrupting_a_connection_does_not_reach_its_cursor",
        "test_deadline_interrupts_once_per_call_not_once_per_poll",
        "test_a_real_duckdb_query_is_actually_abortable",
    )

    for case in slow_cases:
        assert "pytest.mark.slow" in _decorators(path, case)


def test_local_only_payment_golden_test_is_not_part_of_the_default_lane() -> None:
    text = (ROOT / "test" / "payments" / "test_payments_golden.py").read_text(encoding="utf-8")

    assert "pytestmark = pytest.mark.integration" in text
    assert "DAIL_INTEGRATION_TESTS" in text


def test_payment_snapshot_generator_requires_an_explicit_reviewed_replacement(monkeypatch) -> None:
    path = ROOT / "test" / "fixtures" / "payments" / "_generate_expected.py"
    spec = importlib.util.spec_from_file_location("payments_expected_generator", path)
    assert spec is not None and spec.loader is not None
    monkeypatch.setenv("POLARS_SKIP_CPU_CHECK", "1")
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)

    pdf = Path("allowances.pdf")
    assert generator.output_path(pdf, replace_reviewed=False) == Path("allowances.candidate.expected.parquet")
    assert generator.output_path(pdf, replace_reviewed=True) == Path("allowances.expected.parquet")


def test_payment_snapshot_promotion_requires_the_reviewed_candidate(monkeypatch, tmp_path) -> None:
    path = ROOT / "test" / "fixtures" / "payments" / "_generate_expected.py"
    spec = importlib.util.spec_from_file_location("payments_expected_generator", path)
    assert spec is not None and spec.loader is not None
    monkeypatch.setenv("POLARS_SKIP_CPU_CHECK", "1")
    generator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(generator)

    pdf = tmp_path / "allowances.pdf"
    pdf.write_bytes(b"source PDF")
    with pytest.raises(FileNotFoundError):
        generator.promote_candidate(pdf)

    candidate = generator.output_path(pdf, replace_reviewed=False)
    candidate.write_bytes(b"independently reviewed bytes")

    monkeypatch.setattr(generator, "FIXTURES_DIR", tmp_path)
    monkeypatch.setattr(generator, "_iter_rows_from_pdf", lambda _: pytest.fail("promotion must not reparse"))
    generator.main(["--replace-reviewed"])

    reviewed = generator.output_path(pdf, replace_reviewed=True)
    assert reviewed == generator.output_path(pdf, replace_reviewed=True)
    assert reviewed.read_bytes() == b"independently reviewed bytes"
    assert candidate.read_bytes() == b"independently reviewed bytes"


def test_root_guidance_has_a_portable_test_first_protocol() -> None:
    text = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
    required = (
        "## Test-first change protocol",
        "ambiguous or contract-changing",
        "fakes at UI/API seams",
        "failing node",
        "green command",
    )

    assert all(term in text for term in required)
