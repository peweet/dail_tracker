"""Smoke test for the in-repo MCP server (moved from C:/tmp/dail_mcp 2026-06-11).

Importing the module must register the full tool/prompt surface WITHOUT touching
data (the DuckDB connection is lazy so the stdio handshake stays instant) —
which is exactly what makes this testable in CI with no parquet present.
Skips when the optional ``mcp`` extra is not installed.
"""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

pytest.importorskip("mcp")

from mcp import ClientSession, StdioServerParameters  # noqa: E402
from mcp.client.stdio import stdio_client  # noqa: E402

from mcp_server import server  # noqa: E402


def test_tool_registry_loads():
    tools = asyncio.run(server.mcp.list_tools())
    assert len(tools) >= 42
    names = {t.name for t in tools}
    # spot-check the surface across domains (members / money / cross-register / corporate / siting)
    assert {
        "search_members",
        "procurement_lobbying_overlap",
        "public_body_payments",
        "data_coverage",
        "corporate_distress_notices",
        "corporate_repeat_distress",
        "siting_check",
    } <= names


def test_stdio_client_server_roundtrip():
    """Exercise the transport seam, including MCP's Windows pywin32 import."""

    async def roundtrip():
        env = os.environ.copy()
        env.update({"PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"})
        params = StdioServerParameters(
            command=sys.executable,
            args=["mcp_server/server.py"],
            cwd=str(Path(__file__).resolve().parents[2]),
            env=env,
        )
        async with (
            stdio_client(params) as (read, write),
            ClientSession(read, write) as session,
        ):
            initialized = await session.initialize()
            tools = await session.list_tools()
        return initialized, tools

    initialized, tools = asyncio.run(roundtrip())
    assert initialized.serverInfo.name == "dail-tracker"
    assert any(tool.name == "search_project" for tool in tools.tools)


def test_siting_check_docstring_lists_every_canonical_use_class():
    # Parity gate for the discovery seam (memory feedback_wiring_gap_parity_check_2026_07_31):
    # a use class the engine accepts but the tool docstring never names is invisible to every
    # MCP caller — ad_biogas_waste was undiscoverable for exactly this reason until 2026-07-31.
    engine = pytest.importorskip(
        "planning.product.core.engine", reason="optional 'siting' extra / private overlay absent"
    )
    tools = {t.name: t for t in asyncio.run(server.mcp.list_tools())}
    desc = tools["siting_check"].description
    for use_class in engine.USE_CLASSES:
        assert f"'{use_class}'" in desc, f"canonical use_class {use_class!r} missing from siting_check docstring"


def test_vote_tools_expose_bounding_params():
    # Contract: the high-cardinality vote tools must advertise their result-bounding
    # knobs so an agent can page/summarise instead of blowing the token budget.
    tools = {t.name: t for t in asyncio.run(server.mcp.list_tools())}
    vvi = tools["voting_vs_interests"].inputSchema["properties"]
    assert {"summary_only", "limit"} <= set(vvi)
    svt = tools["search_votes_by_topic"].inputSchema["properties"]
    assert "include_member_votes" in svt


def test_prompts_and_read_only_annotations():
    prompts = asyncio.run(server.mcp.list_prompts())
    assert len(prompts) >= 7
    tools = asyncio.run(server.mcp.list_tools())
    # every annotated tool must advertise read-only (no destructive surface exists)
    for t in tools:
        if t.annotations is not None:
            assert t.annotations.readOnlyHint is True


def test_import_does_not_build_connection():
    # the lazy-connection contract: import must leave the singleton unbuilt
    assert server._CONN is None
