"""Every child process the MCP server spawns must not inherit the server's stdin.

Under the stdio transport the server's stdin is the JSON-RPC pipe from the client, with
a reader thread blocked on it. On Windows a ``git`` child that inherits that pipe stalls
at startup until the client writes again or closes the pipe (reproduced 2026-11-14:
11 s blocked with the pipe open and silent, 0.02 s once closed, 0.02 s with
``stdin=DEVNULL``). The tool's reply is then held back until the connection drops,
which the client logs as a 360 s timeout followed by "a response for an unknown message
ID" — 50 of the 98 MCP timeouts logged since 2026-10-01 have that shape.

So every ``subprocess`` spawn under ``mcp_server/`` must say where its stdin comes from:
``stdin=subprocess.DEVNULL``, an explicit pipe, or ``input=``.
"""

from __future__ import annotations

import ast
from pathlib import Path

MCP_DIR = Path(__file__).resolve().parents[2] / "mcp_server"
SPAWNERS = {"run", "Popen", "check_output", "check_call", "call"}


def _spawns_without_stdin(path: Path) -> list[int]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    lines = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not (
            isinstance(func, ast.Attribute)
            and func.attr in SPAWNERS
            and isinstance(func.value, ast.Name)
            and func.value.id == "subprocess"
        ):
            continue
        keywords = {kw.arg for kw in node.keywords}
        # ``None`` is a ``**kwargs`` splat, which this check cannot see into.
        if not keywords & {"stdin", "input", None}:
            lines.append(node.lineno)
    return lines


def test_scan_finds_the_mcp_server_sources():
    assert (MCP_DIR / "server.py").is_file()
    assert (MCP_DIR / "code_index.py").is_file()


def test_mcp_server_children_never_inherit_stdin():
    offenders = [
        f"{path.relative_to(MCP_DIR.parent).as_posix()}:{line}"
        for path in sorted(MCP_DIR.rglob("*.py"))
        for line in _spawns_without_stdin(path)
    ]
    assert not offenders, (
        "subprocess spawns that inherit the MCP server's stdin (pass stdin=subprocess.DEVNULL): " + ", ".join(offenders)
    )


def test_the_check_flags_an_inherited_stdin(tmp_path):
    sample = tmp_path / "sample.py"
    sample.write_text(
        "import subprocess\n"
        "subprocess.run(['git', 'status'], capture_output=True)\n"
        "subprocess.run(['git', 'status'], stdin=subprocess.DEVNULL, capture_output=True)\n",
        encoding="utf-8",
    )
    assert _spawns_without_stdin(sample) == [2]
