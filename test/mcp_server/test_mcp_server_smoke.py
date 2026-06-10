"""Smoke test for the in-repo MCP server (moved from C:/tmp/dail_mcp 2026-06-11).

Importing the module must register the full tool/prompt surface WITHOUT touching
data (the DuckDB connection is lazy so the stdio handshake stays instant) —
which is exactly what makes this testable in CI with no parquet present.
Skips when the optional ``mcp`` extra is not installed.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

pytest.importorskip("mcp")

from mcp_server import server  # noqa: E402


def test_tool_registry_loads():
    tools = asyncio.run(server.mcp.list_tools())
    assert len(tools) >= 39
    names = {t.name for t in tools}
    # spot-check the surface across domains (members / money / cross-register)
    assert {"search_members", "procurement_lobbying_overlap", "public_body_payments", "data_coverage"} <= names


def test_prompts_and_read_only_annotations():
    prompts = asyncio.run(server.mcp.list_prompts())
    assert len(prompts) >= 6
    tools = asyncio.run(server.mcp.list_tools())
    # every annotated tool must advertise read-only (no destructive surface exists)
    for t in tools:
        if t.annotations is not None:
            assert t.annotations.readOnlyHint is True


def test_import_does_not_build_connection():
    # the lazy-connection contract: import must leave the singleton unbuilt
    assert server._CONN is None
