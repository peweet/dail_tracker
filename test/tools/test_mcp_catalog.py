from __future__ import annotations

from tools.check_mcp_catalog import (
    ALWAYS_LOADED,
    SERVER,
    inspect_source,
    main,
    retrieval_manifest_names,
    validate,
)


def test_live_catalog_stays_read_only_and_within_context_budget():
    tools = inspect_source(SERVER.read_text(encoding="utf-8"))
    assert len(tools) >= 70
    assert {tool.name for tool in tools} == retrieval_manifest_names()
    assert {tool.name for tool in tools if tool.always_loaded} == ALWAYS_LOADED
    assert validate(tools) == []


def test_catalog_check_reports_missing_description_and_read_only_hint():
    tools = inspect_source(
        """
@mcp.tool()
def unsafe_tool(value: str):
    pass
"""
    )
    violations = validate(tools)
    assert any("docstring" in item for item in violations)
    assert any("readOnlyHint" in item for item in violations)


def test_catalog_check_uses_explicit_description_instead_of_a_long_docstring():
    tools = inspect_source(
        """
SHORT = 'short contract'
@mcp.tool(annotations=_RO, description=SHORT)
def bounded_tool(value: str):
    \'''This internal explanation may be much longer than the LLM-facing contract.\'''
"""
    )
    assert tools[0].doc_chars == len("short contract")


def test_catalog_check_cli_passes(capsys):
    assert main() == 0
    assert "always loaded" in capsys.readouterr().out
