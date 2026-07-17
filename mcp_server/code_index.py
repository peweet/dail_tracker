"""Cheap programmatic code scanning — the code-side twin of the fact cards.

The repo already has cheap retrieval for *data* (fact_cards.json → describe_dataset),
*docs* (doc/INDEX.md) and *SQL views* (header comments) — but an agent asking "where
does X live in the Python?" or "what's in this module?" still had to Read whole files.
This module closes that gap with two pure functions built on stdlib ``ast`` (source is
parsed, never executed):

- ``build_code_index(repo)`` — one {kind:"code"} entry per repo .py (module path,
  docstring first line, def/class names) for the ``search_project`` haystack.
- ``outline(repo, path)`` — a structural X-ray of one file (or a package directory):
  module docstring, imports, every class/def with signature, line span, decorators and
  one-line docstring. A ~4k-token file outlines in a few hundred tokens; the caller
  then Reads only the one span it needs.

Kept separate from server.py (which registers the MCP tool wrappers) so it is
importable and testable without the optional ``mcp`` extra installed.
"""

from __future__ import annotations

import ast
from pathlib import Path

# Directories never worth indexing: environments, caches, VCS, throwaway probe scripts.
# Any path segment starting with "." is skipped too (.venv, .git, .claude, .*_cache).
_SKIP_PARTS = {"__pycache__", "node_modules", "audit_screenshots", "data"}

_DOC_CAP = 140  # one-line docstrings are truncated to this many chars
_DEF_CAP_INDEX = 80  # def names contributing to a module's search haystack


def _skip(rel_parts: tuple[str, ...]) -> bool:
    return any(p in _SKIP_PARTS or p.startswith(".") for p in rel_parts)


def _doc1(node) -> str:
    """First line of a node's docstring, truncated — '' when absent."""
    doc = ast.get_docstring(node, clean=True) or ""
    return doc.strip().splitlines()[0][:_DOC_CAP] if doc.strip() else ""


def _sig(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    """'(conn, *, limit=20) -> dict' — name and def/async kept in separate fields."""
    try:
        params = ast.unparse(node.args)
    except Exception:  # noqa: BLE001 — a printable fallback beats a dead outline
        params = "..."
    ret = ""
    if node.returns is not None:
        try:
            ret = f" -> {ast.unparse(node.returns)}"
        except Exception:  # noqa: BLE001
            pass
    return f"({params}){ret}"


def _decorators(node) -> list[str]:
    names = []
    for d in node.decorator_list:
        try:
            names.append(ast.unparse(d))
        except Exception:  # noqa: BLE001
            names.append("?")
    return names


def _def_entry(node: ast.FunctionDef | ast.AsyncFunctionDef) -> dict:
    e = {
        "kind": "async def" if isinstance(node, ast.AsyncFunctionDef) else "def",
        "name": node.name,
        "span": f"{node.lineno}-{node.end_lineno}",
        "sig": _sig(node),
    }
    if doc := _doc1(node):
        e["doc"] = doc
    if decs := _decorators(node):
        e["decorators"] = decs
    return e


def _outline_tree(tree: ast.Module) -> list[dict]:
    """Top-level defs/classes; class methods nested one level (deeper is flattened out)."""
    out: list[dict] = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            out.append(_def_entry(node))
        elif isinstance(node, ast.ClassDef):
            entry: dict = {"kind": "class", "name": node.name, "span": f"{node.lineno}-{node.end_lineno}"}
            if doc := _doc1(node):
                entry["doc"] = doc
            methods = [
                _def_entry(n) for n in node.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
            ]
            if methods:
                entry["methods"] = methods
            out.append(entry)
    return out


def _imports(tree: ast.Module, cap: int = 40) -> list[str]:
    seen: dict[str, None] = {}  # ordered de-dup
    for node in tree.body:
        if isinstance(node, ast.Import):
            for a in node.names:
                seen.setdefault(a.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            seen.setdefault("." * node.level + node.module)
    return list(seen)[:cap]


def _parse(py: Path) -> tuple[ast.Module, int] | None:
    """(tree, line_count), or None when unreadable/unparseable (sandbox scripts may not parse)."""
    try:
        src = py.read_text(encoding="utf-8", errors="replace")
        return ast.parse(src), len(src.splitlines())
    except Exception:  # noqa: BLE001
        return None


def _resolve(repo: Path, path: str) -> Path | None:
    """Resolve a repo-relative path, refusing anything that escapes the repo."""
    try:
        target = (repo / path).resolve()
        target.relative_to(repo.resolve())
    except (ValueError, OSError):
        return None
    return target


def outline(repo: Path, path: str, limit: int = 200) -> dict:
    """Outline one .py file, or a directory as a per-module summary. Returns {error} dicts
    (never raises) so the MCP wrapper can pass the result straight through."""
    target = _resolve(repo, path)
    if target is None:
        return {"error": f"path escapes the repository: {path}"}
    if not target.exists():
        return {"error": f"no such path: {path}"}

    if target.is_dir():
        files = sorted(p for p in target.glob("*.py") if not _skip(p.relative_to(repo).parts))
        modules = []
        for py in files[:80]:
            parsed = _parse(py)
            if parsed is None:
                modules.append({"name": py.name, "error": "unparseable"})
                continue
            tree, n_lines = parsed
            names = [d["name"] for d in _outline_tree(tree)][:40]
            m = {"name": py.name, "lines": n_lines, "defs": names}
            if doc := _doc1(tree):
                m["doc"] = doc
            modules.append(m)
        subpackages = sorted(
            d.name for d in target.iterdir() if d.is_dir() and (d / "__init__.py").exists()
        )
        out = {"path": path, "modules": modules, "subpackages": subpackages}
        if len(files) > 80:
            out["truncated"] = f"{len(files) - 80} more files — outline them directly"
        return out

    if target.suffix != ".py":
        return {"error": f"not a Python file (use Read for other types): {path}"}
    parsed = _parse(target)
    if parsed is None:
        return {"error": f"could not parse: {path}"}
    tree, n_lines = parsed
    defs = _outline_tree(tree)
    n_defs = sum(1 + len(d.get("methods", [])) for d in defs)
    out = {
        "path": target.relative_to(repo.resolve()).as_posix(),
        "lines": n_lines,
        "imports": _imports(tree),
        "defs": defs[: max(1, limit)],
    }
    if doc := _doc1(tree):
        out["doc"] = doc
    if len(defs) > limit:
        out["truncated"] = f"{len(defs) - limit} more top-level defs"
    out["def_count"] = n_defs
    return out


def build_code_index(repo: Path) -> list[dict]:
    """One search_project entry per repo .py: dotted module name, docstring first line,
    and def/class names in the haystack — so 'where does X live in code?' is one call."""
    repo = repo.resolve()
    idx: list[dict] = []
    for py in sorted(repo.rglob("*.py")):
        rel = py.relative_to(repo)
        if _skip(rel.parts):
            continue
        parsed = _parse(py)
        if parsed is None:
            continue
        tree, _ = parsed
        dotted = ".".join(rel.with_suffix("").parts)
        if dotted.endswith(".__init__"):
            dotted = dotted[: -len(".__init__")]
        desc = _doc1(tree)
        names: list[str] = []
        for d in _outline_tree(tree):
            names.append(d["name"])
            names.extend(m["name"] for m in d.get("methods", []))
        idx.append(
            {
                "kind": "code",
                "name": dotted,
                "path": rel.as_posix(),
                "desc": desc,
                "haystack": " ".join([dotted, py.stem, desc, *names[:_DEF_CAP_INDEX]]),
            }
        )
    return idx
