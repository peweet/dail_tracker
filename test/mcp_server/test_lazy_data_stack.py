"""The MCP server must not import the data stack just to start.

Why this is a ratchet and not a nicety: under stdio the client spawns one server per
session whether or not a tool is ever called, and the adoption tripwire has recorded
sessions making zero calls. Measured 2026-07-27, eager imports cost 98 MB against
47 MB deferred — so a regression here is ~51 MB per idle session, silently.

The sys.modules assertions run in a SUBPROCESS: this test suite imports pandas and
duckdb for other tests, so an in-process check would pass vacuously.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

pytest.importorskip("mcp")

from mcp_server import server  # noqa: E402

HEAVY = ("pandas", "numpy", "duckdb", "sqlglot", "polars")


def _run(body: str) -> str:
    """Execute `body` in a clean interpreter rooted at the repo; return its stdout."""
    code = textwrap.dedent(
        f"""
        import sys
        sys.path.insert(0, {str(REPO)!r})
        {textwrap.indent(textwrap.dedent(body), "        ").strip()}
        """
    )
    proc = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        cwd=str(REPO),
        timeout=300,
    )
    assert proc.returncode == 0, f"probe failed:\n{proc.stdout}\n{proc.stderr}"
    return proc.stdout.strip()


def test_importing_the_server_does_not_import_the_data_stack():
    out = _run(
        """
        import mcp_server.server  # noqa: F401
        import sys
        heavy = [m for m in ("pandas", "numpy", "duckdb", "sqlglot", "polars") if m in sys.modules]
        print(",".join(heavy))
        """
    )
    assert out == "", f"import pulled in the data stack: {out}"


def test_listing_tools_does_not_import_the_data_stack():
    """The handshake and list_tools are what an unused server actually serves."""
    out = _run(
        """
        import asyncio, sys
        from mcp_server import server
        tools = asyncio.run(server.mcp.list_tools())
        heavy = [m for m in ("pandas", "numpy", "duckdb", "sqlglot", "polars") if m in sys.modules]
        print(f"{len(tools)}|{','.join(heavy)}")
        """
    )
    count, heavy = out.split("|")
    assert int(count) >= 42, f"tool surface shrank to {count}"
    assert heavy == "", f"list_tools pulled in the data stack: {heavy}"


def test_touching_a_proxy_imports_it_and_swaps_itself_out():
    out = _run(
        """
        import sys
        from mcp_server import server
        before = type(server.vot).__name__
        server.vot.result_summary          # first attribute access triggers the import
        after = type(server.vot).__name__
        loaded = "pandas" in sys.modules
        print(f"{before}|{after}|{loaded}")
        """
    )
    before, after, loaded = out.split("|")
    assert before == "_LazyModule"
    assert after == "module", "the proxy must replace itself with the real module"
    assert loaded == "True"


@pytest.mark.parametrize(
    "alias",
    ["vot", "proc", "lb", "dossiers", "serialize", "caveats", "sql_index", "text_fts"],
)
def test_every_alias_resolves_to_a_real_module(alias):
    """A typo in the alias table would surface only when some rare tool is called."""
    mod = getattr(server, alias)
    assert getattr(mod, "__name__", None), f"{alias} did not resolve to a module"


def test_unavailable_returns_a_real_exception_class():
    """The seven handlers use this because `except` cannot take a lazy proxy."""
    exc = server._unavailable()
    assert isinstance(exc, type) and issubclass(exc, BaseException)
    assert exc.__name__ == "SourceUnavailable"


def test_no_eager_data_stack_import_creeps_back_into_the_source():
    """Cheap textual ratchet: the module-scope import block must stay clear of the
    heavy packages, so a future edit that re-adds one fails here with a clear reason
    rather than as a silent 51 MB regression."""
    src = (REPO / "mcp_server" / "server.py").read_text(encoding="utf-8")
    head = src.split("mcp = FastMCP(", 1)[0]
    offenders = [
        line.strip()
        for line in head.splitlines()
        if line.startswith(("import ", "from "))
        and any(p in line for p in ("dail_tracker_core.queries", "import pandas", "import duckdb", "import polars"))
    ]
    assert not offenders, f"eager data-stack import at module scope: {offenders}"
