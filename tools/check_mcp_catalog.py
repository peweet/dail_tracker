#!/usr/bin/env python
"""Guard the LLM-visible MCP catalog against context and safety regressions.

The server intentionally has a broad domain surface, but Claude-compatible clients
can defer domain tools behind tool search. Only the small repository-navigation set
is forced into every session. This pure-stdlib check pins that boundary without
importing FastMCP, DuckDB, or any data modules.
"""

from __future__ import annotations

import ast
import json
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SERVER = ROOT / "mcp_server" / "server.py"
QUERY_MANIFEST = ROOT / "tools" / "evals" / "tool_retrieval_queries.json"

ALWAYS_LOADED = frozenset(
    {
        "search_project",
        "code_outline",
        "py_deps",
        "py_refs",
        "view_deps",
        "column_deps",
    }
)
MAX_TOOLS = 80
MAX_CATALOG_DESCRIPTION_CHARS = 55_000
MAX_ALWAYS_TOOLS = 8
MAX_ALWAYS_DOC_CHARS = 6_000
MAX_TOOL_DESCRIPTION_CHARS = 2_000


@dataclass(frozen=True)
class ToolInfo:
    name: str
    line: int
    doc_chars: int
    decorator: str
    read_only_hint: bool
    always_load_hint: bool

    @property
    def always_loaded(self) -> bool:
        return self.always_load_hint

    @property
    def read_only(self) -> bool:
        return self.read_only_hint


def _resolve_assignment(node: ast.expr, assignments: dict[str, ast.expr]) -> ast.expr:
    seen: set[str] = set()
    while isinstance(node, ast.Name) and node.id in assignments and node.id not in seen:
        seen.add(node.id)
        node = assignments[node.id]
    return node


def _is_true(node: ast.expr) -> bool:
    return isinstance(node, ast.Constant) and node.value is True


def _read_only_value(node: ast.expr, assignments: dict[str, ast.expr]) -> bool:
    node = _resolve_assignment(node, assignments)
    if not isinstance(node, ast.Call) or not ast.unparse(node.func).endswith("ToolAnnotations"):
        return False
    return any(keyword.arg == "readOnlyHint" and _is_true(keyword.value) for keyword in node.keywords)


def _always_load_value(node: ast.expr, assignments: dict[str, ast.expr]) -> bool:
    node = _resolve_assignment(node, assignments)
    if not isinstance(node, ast.Dict):
        return False
    return any(
        isinstance(key, ast.Constant) and key.value == "anthropic/alwaysLoad" and _is_true(value)
        for key, value in zip(node.keys, node.values, strict=True)
        if key is not None
    )


def inspect_source(source: str) -> list[ToolInfo]:
    tree = ast.parse(source)
    assignments: dict[str, ast.expr] = {}
    constants: dict[str, str] = {}
    for node in tree.body:
        if not isinstance(node, ast.Assign) or len(node.targets) != 1:
            continue
        target = node.targets[0]
        if isinstance(target, ast.Name):
            assignments[target.id] = node.value
        if isinstance(target, ast.Name) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            constants[target.id] = node.value.value

    tools: list[ToolInfo] = []
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        tool_nodes = [item for item in node.decorator_list if ast.unparse(item).startswith("mcp.tool")]
        if not tool_nodes:
            continue
        tool_node = tool_nodes[0]
        description = ast.get_docstring(node) or ""
        read_only_hint = False
        always_load_hint = False
        if isinstance(tool_node, ast.Call):
            for keyword in tool_node.keywords:
                if keyword.arg == "annotations":
                    read_only_hint = _read_only_value(keyword.value, assignments)
                elif keyword.arg == "meta":
                    always_load_hint = _always_load_value(keyword.value, assignments)
            for keyword in tool_node.keywords:
                if keyword.arg != "description":
                    continue
                if isinstance(keyword.value, ast.Constant) and isinstance(keyword.value.value, str):
                    description = keyword.value.value
                elif isinstance(keyword.value, ast.Name):
                    description = constants.get(keyword.value.id, description)
        tools.append(
            ToolInfo(
                name=node.name,
                line=node.lineno,
                doc_chars=len(description),
                decorator=ast.unparse(tool_node),
                read_only_hint=read_only_hint,
                always_load_hint=always_load_hint,
            )
        )
    return tools


def validate(tools: list[ToolInfo]) -> list[str]:
    violations: list[str] = []
    always = [tool for tool in tools if tool.always_loaded]
    always_names = {tool.name for tool in always}

    if len(tools) > MAX_TOOLS:
        violations.append(f"{len(tools)} tools exceeds catalog budget {MAX_TOOLS}")
    catalog_chars = sum(tool.doc_chars for tool in tools)
    if catalog_chars > MAX_CATALOG_DESCRIPTION_CHARS:
        violations.append(f"catalog descriptions use {catalog_chars} chars; budget is {MAX_CATALOG_DESCRIPTION_CHARS}")

    for tool in tools:
        if not tool.doc_chars:
            violations.append(f"{tool.name}:{tool.line} has no LLM-facing docstring")
        if tool.doc_chars > MAX_TOOL_DESCRIPTION_CHARS:
            violations.append(
                f"{tool.name}:{tool.line} description uses {tool.doc_chars} chars; "
                f"budget is {MAX_TOOL_DESCRIPTION_CHARS}"
            )
        if not tool.read_only:
            violations.append(f"{tool.name}:{tool.line} is missing readOnlyHint")

    unexpected = sorted(always_names - ALWAYS_LOADED)
    missing = sorted(ALWAYS_LOADED - always_names)
    if unexpected:
        violations.append(f"domain tools forced into every context: {unexpected}")
    if missing:
        violations.append(f"navigation tools no longer always loaded: {missing}")
    if len(always) > MAX_ALWAYS_TOOLS:
        violations.append(f"{len(always)} always-loaded tools exceeds budget {MAX_ALWAYS_TOOLS}")

    always_chars = sum(tool.doc_chars for tool in always)
    if always_chars > MAX_ALWAYS_DOC_CHARS:
        violations.append(f"always-loaded descriptions use {always_chars} chars; budget is {MAX_ALWAYS_DOC_CHARS}")
    return violations


def retrieval_manifest_names() -> set[str]:
    payload = json.loads(QUERY_MANIFEST.read_text(encoding="utf-8"))
    return {str(row["tool"]) for row in payload["queries"]}


def main() -> int:
    tools = inspect_source(SERVER.read_text(encoding="utf-8"))
    violations = validate(tools)
    live_names = {tool.name for tool in tools}
    manifest_names = retrieval_manifest_names()
    if live_names != manifest_names:
        missing = sorted(live_names - manifest_names)
        stale = sorted(manifest_names - live_names)
        violations.append(f"retrieval manifest drift: missing={missing}, stale={stale}")
    if violations:
        for violation in violations:
            print(f"FAIL  {violation}")
        return 1

    always = [tool for tool in tools if tool.always_loaded]
    all_tokens = sum(tool.doc_chars for tool in tools) // 4
    always_tokens = sum(tool.doc_chars for tool in always) // 4
    print(
        f"OK — MCP catalog: {len(tools)} read-only tools; "
        f"{len(always)} always loaded (~{always_tokens:,} description tokens); "
        f"full catalog ~{all_tokens:,} description tokens."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
