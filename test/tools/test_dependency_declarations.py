"""Runs the import-existence gate (tools/check_dependency_declarations.py).

Fails on any DEP001 — an import of a package not declared in pyproject.toml,
the class a hallucinated package name lands in (arXiv:2406.10279: 19.7% of
LLM-generated package references are fabricated). Exceptions live ONLY in
[tool.deptry.per_rule_ignores] with a written justification.

Skips (does not pass) when deptry is absent — a bare core-only `uv sync`
doesn't install the dev group.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from tools import check_dependency_declarations


def test_no_undeclared_imports(capsys):
    findings = check_dependency_declarations.run_deptry()
    if findings is None:
        pytest.skip("deptry not installed (core-only sync)")
    hard = [f for f in findings if f.get("error", {}).get("code") == "DEP001"]
    assert not hard, (
        "undeclared import(s) — declare the package, extend known_first_party, or "
        "justify an ignore in [tool.deptry.per_rule_ignores]:\n"
        + "\n".join(
            f"  {f.get('location', {}).get('file', '?')}:{f.get('location', {}).get('line', '?')} '{f.get('module')}'"
            for f in hard
        )
    )
