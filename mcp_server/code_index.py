"""Cheap programmatic code scanning — the code-side twin of the fact cards.

The repo already has cheap retrieval for *data* (fact_cards.json → describe_dataset),
*docs* (doc/INDEX.md) and *SQL views* (header comments) — but an agent asking "where
does X live in the Python?" or "what's in this module?" still had to Read whole files.
This module closes that gap with two pure functions built on stdlib ``ast`` (source is
parsed, never executed):

- ``build_code_index(repo)`` — one {kind:"code"} entry per repo .py (module path,
  docstring first line, def/class names) for the ``search_project`` haystack.
- ``outline(repo, path)`` — a structural X-ray of one file (or a package directory):
  module docstring, imports, and recursively structured classes/defs. Class entries
  include bases, decorators, metaclass, keywords and type parameters; function entries
  include signatures and lexical children. A ~4k-token file outlines in a few hundred
  tokens; the caller then reads only the one span it needs.

Repository-wide scans use Git's visible/non-ignored file set plus an explicit policy
that excludes dot, private, sandbox and generated trees. Source is decoded with its
PEP 263 declaration and parse failures are surfaced instead of silently omitted.

Kept separate from server.py (which registers the MCP tool wrappers) so it is
importable and testable without the optional ``mcp`` extra installed.
"""

from __future__ import annotations

import ast
import contextlib
import os
import subprocess
import tokenize
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

# These defaults are the repository-navigation privacy boundary, not merely a speed
# optimisation.  In particular, private overlays and agent scratch trees must not
# silently become searchable just because they live below the checkout root.
_SKIP_PARTS = frozenset(
    {
        "__pycache__",
        "node_modules",
        "audit_screenshots",
        "data",
        "logs",
        "doc_archive",
        "tmp",
        "cache",
    }
)

_DOC_CAP = 140  # one-line docstrings are truncated to this many chars
_DEF_CAP_INDEX = 80  # def names contributing to a module's search haystack
OUTLINE_DEFINITION_CAP = 200  # hard response budget; nested definitions count too


@dataclass(frozen=True, slots=True)
class ScanPolicy:
    """Explicit allow/deny policy for repository-navigation source scans.

    Git's standard ignore rules define the first boundary when the checkout has Git
    metadata.  These structural rules are applied as a second boundary and also make
    synthetic/non-Git repositories safe by default.
    """

    excluded_parts: frozenset[str] = _SKIP_PARTS
    exclude_dot_paths: bool = True
    exclude_private: bool = True
    exclude_sandboxes: bool = True
    exclude_generated: bool = True

    def allows(self, relative: str | Path | PurePosixPath) -> bool:
        """Return whether a normalised, repository-relative path may be scanned."""
        raw = str(relative).replace("\\", "/")
        rel = PurePosixPath(raw)
        if not raw or rel.is_absolute() or ".." in rel.parts:
            return False
        for part in rel.parts:
            low = part.casefold()
            if self.exclude_dot_paths and part.startswith("."):
                return False
            if low in self.excluded_parts:
                return False
            if self.exclude_private and (low == "private" or low.startswith("private_")):
                return False
            if self.exclude_sandboxes and (low == "sandbox" or low.startswith("sandbox_") or low.endswith("_sandbox")):
                return False
            if self.exclude_generated and (
                low == "generated" or low.startswith("generated_") or low.endswith("_generated")
            ):
                return False
        return True


DEFAULT_SCAN_POLICY = ScanPolicy()


def _skip(rel_parts: tuple[str, ...]) -> bool:
    """Compatibility helper used by older callers and focused tests."""
    return not DEFAULT_SCAN_POLICY.allows(PurePosixPath(*rel_parts))


def _git_visible_paths(repo: Path) -> list[PurePosixPath] | None:
    """Tracked plus untracked/non-ignored paths, or ``None`` outside a Git worktree."""
    try:
        top = subprocess.run(
            ["git", "-C", os.fspath(repo), "rev-parse", "--show-toplevel"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            encoding="utf-8",
            errors="replace",
        ).stdout.strip()
        if Path(top).resolve() != repo.resolve():
            return None
        proc = subprocess.run(
            ["git", "-C", os.fspath(repo), "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return [PurePosixPath(os.fsdecode(raw)) for raw in proc.stdout.split(b"\0") if raw]


def iter_repository_files(
    repo: Path,
    suffixes: Iterable[str],
    policy: ScanPolicy = DEFAULT_SCAN_POLICY,
) -> Iterator[Path]:
    """Yield contained, policy-allowed source files in stable relative-path order.

    A Git checkout uses ``git ls-files --exclude-standard`` so all repository ignore
    sources (including the global excludes configured for that checkout) are honoured.
    A non-Git synthetic repository falls back to a contained filesystem walk.
    """
    root = repo.resolve()
    wanted = {suffix.casefold() for suffix in suffixes}
    visible = _git_visible_paths(root)
    if visible is None:
        if (root / ".git").exists():
            return  # Git policy could not be evaluated: fail closed, never scan ignored files.
        candidates = ((path.relative_to(root), path) for path in root.rglob("*") if path.is_file())
    else:
        candidates = ((Path(*rel.parts), root.joinpath(*rel.parts)) for rel in visible)

    accepted: list[tuple[str, Path]] = []
    for rel, path in candidates:
        if path.suffix.casefold() not in wanted or not policy.allows(rel):
            continue
        try:
            resolved = path.resolve(strict=True)
            resolved.relative_to(root)
        except (OSError, ValueError):
            continue
        if resolved.is_file():
            accepted.append((rel.as_posix(), path))
    for _, path in sorted(accepted):
        yield path


def read_python_source(path: Path) -> str:
    """Decode Python source according to its PEP 263 encoding declaration."""
    with tokenize.open(path) as handle:
        return handle.read()


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
        with contextlib.suppress(Exception):
            ret = f" -> {ast.unparse(node.returns)}"
    return f"({params}){ret}"


def _decorators(node) -> list[str]:
    names = []
    for d in node.decorator_list:
        try:
            names.append(ast.unparse(d))
        except Exception:  # noqa: BLE001
            names.append("?")
    return names


def _start_line(node: ast.AST) -> int:
    """First source line belonging to a definition, including decorators."""
    decorators = getattr(node, "decorator_list", ())
    return min((decorator.lineno for decorator in decorators), default=node.lineno)


def _unparse(node: ast.AST) -> str:
    with contextlib.suppress(Exception):
        return ast.unparse(node)
    return "?"


def _direct_named_children(body: list[ast.stmt]) -> list[ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef]:
    """Definitions lexically owned by ``body``, including conditional definitions.

    Once a definition is found its body is not traversed here; recursion belongs to
    that definition's own outline entry.  This prevents nested methods being reported
    as module-level symbols while still seeing definitions under ``if TYPE_CHECKING``.
    """
    found: list[ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef] = []

    def visit(node: ast.AST) -> None:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            found.append(node)
            return
        for child in ast.iter_child_nodes(node):
            visit(child)

    for statement in body:
        visit(statement)
    return sorted(found, key=_start_line)


def _def_entry(node: ast.FunctionDef | ast.AsyncFunctionDef) -> dict:
    e = {
        "kind": "async def" if isinstance(node, ast.AsyncFunctionDef) else "def",
        "name": node.name,
        "span": f"{_start_line(node)}-{node.end_lineno}",
        "sig": _sig(node),
    }
    if doc := _doc1(node):
        e["doc"] = doc
    if decs := _decorators(node):
        e["decorators"] = decs
    if type_params := [_unparse(param) for param in getattr(node, "type_params", ())]:
        e["type_params"] = type_params
    if nested := [_named_entry(child) for child in _direct_named_children(node.body)]:
        e["nested"] = nested
    return e


def _class_entry(node: ast.ClassDef) -> dict:
    entry: dict = {
        "kind": "class",
        "name": node.name,
        "span": f"{_start_line(node)}-{node.end_lineno}",
    }
    if bases := [_unparse(base) for base in node.bases]:
        entry["bases"] = bases
    keywords = []
    for keyword in node.keywords:
        value = _unparse(keyword.value)
        if keyword.arg == "metaclass":
            entry["metaclass"] = value
        else:
            keywords.append(f"{keyword.arg}={value}" if keyword.arg else f"**{value}")
    if keywords:
        entry["keywords"] = keywords
    if type_params := [_unparse(param) for param in getattr(node, "type_params", ())]:
        entry["type_params"] = type_params
    if doc := _doc1(node):
        entry["doc"] = doc
    if decs := _decorators(node):
        entry["decorators"] = decs

    children = [_named_entry(child) for child in _direct_named_children(node.body)]
    methods = [child for child in children if child["kind"] in {"def", "async def"}]
    classes = [child for child in children if child["kind"] == "class"]
    if methods:
        entry["methods"] = methods
    if classes:
        entry["classes"] = classes
    return entry


def _named_entry(node: ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef) -> dict:
    return _class_entry(node) if isinstance(node, ast.ClassDef) else _def_entry(node)


def _outline_tree(tree: ast.Module) -> list[dict]:
    """Top-level definitions with recursively structured lexical children."""
    return [_named_entry(node) for node in _direct_named_children(tree.body)]


def _imports(tree: ast.Module, cap: int = 40) -> list[str]:
    seen: dict[str, None] = {}  # ordered de-dup
    for node in tree.body:
        if isinstance(node, ast.Import):
            for a in node.names:
                seen.setdefault(a.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            seen.setdefault("." * node.level + node.module)
    return list(seen)[:cap]


def _parse(py: Path) -> tuple[ast.Module, int]:
    """Parse one Python source file, preserving actionable decoding/syntax errors."""
    src = read_python_source(py)
    return ast.parse(src, filename=os.fspath(py)), len(src.splitlines())


def _resolve(repo: Path, path: str) -> Path | None:
    """Resolve a repo-relative path, refusing anything that escapes the repo."""
    requested = Path(path)
    if requested.is_absolute() or requested.drive:
        return None
    try:
        root = repo.resolve()
        target = (root / requested).resolve()
        target.relative_to(root)
    except (ValueError, OSError):
        return None
    return target


def _concise_defs(defs: list[dict]) -> list[str]:
    """Flatten structured entries to ``kind qualified.name start-end`` lines."""
    out: list[str] = []

    def add(entry: dict, prefix: str = "") -> None:
        qualified = f"{prefix}.{entry['name']}" if prefix else entry["name"]
        out.append(f"{entry['kind']} {qualified} {entry['span']}")
        for child in (*entry.get("methods", ()), *entry.get("classes", ()), *entry.get("nested", ())):
            add(child, qualified)

    for entry in defs:
        add(entry)
    return out


def _definition_count(defs: list[dict]) -> int:
    total = 0
    for entry in defs:
        total += 1
        for key in ("methods", "classes", "nested"):
            total += _definition_count(entry.get(key, []))
    return total


_CHILD_DEFINITION_KEYS = ("methods", "classes", "nested")


def _definition_start(entry: dict) -> int:
    """Return an outline entry's first line for stable cross-kind traversal."""
    with contextlib.suppress(ValueError, TypeError, AttributeError):
        return int(str(entry["span"]).split("-", 1)[0])
    return 0


def _truncate_definitions(defs: list[dict], limit: int) -> tuple[list[dict], int]:
    """Copy at most ``limit`` definitions, recursively counting nested entries.

    A parent consumes one budget slot before any lexical child, so a partial class or
    function remains a useful shell with its own signature/span when the response is
    truncated. Children stored in separate schema fields are visited by source order.
    """
    remaining = max(0, limit)

    def copy_entry(entry: dict) -> dict | None:
        nonlocal remaining
        if remaining <= 0:
            return None
        remaining -= 1
        copied = {key: value for key, value in entry.items() if key not in _CHILD_DEFINITION_KEYS}
        children = sorted(
            ((_definition_start(child), key, child) for key in _CHILD_DEFINITION_KEYS for child in entry.get(key, ())),
            key=lambda item: item[0],
        )
        for _, key, child in children:
            child_copy = copy_entry(child)
            if child_copy is None:
                break
            copied.setdefault(key, []).append(child_copy)
        return copied

    kept: list[dict] = []
    for entry in defs:
        entry_copy = copy_entry(entry)
        if entry_copy is None:
            break
        kept.append(entry_copy)
    return kept, max(0, limit) - remaining


def _definition_names(defs: list[dict], prefix: str = "") -> list[str]:
    """Bare and qualified names for the module-level search haystack."""
    names: list[str] = []
    for entry in defs:
        qualified = f"{prefix}.{entry['name']}" if prefix else entry["name"]
        names.extend((entry["name"], qualified))
        for key in ("methods", "classes", "nested"):
            names.extend(_definition_names(entry.get(key, []), qualified))
    return list(dict.fromkeys(names))


def _parse_error(exc: BaseException) -> str:
    if isinstance(exc, SyntaxError):
        where = f"line {exc.lineno}" if exc.lineno else "unknown line"
        return f"SyntaxError at {where}: {exc.msg}"
    return f"{type(exc).__name__}: {exc}"


def outline(repo: Path, path: str, limit: int = 200, response_format: str = "detailed") -> dict:
    """Outline one .py file, or a directory as a per-module summary. Returns {error} dicts
    (never raises) so the MCP wrapper can pass the result straight through.
    response_format='concise' drops imports/signatures/docstrings — name + span only.
    File responses are hard-capped at ``OUTLINE_DEFINITION_CAP`` definitions, counting
    nested classes/functions/methods as well as top-level definitions."""
    if response_format not in ("concise", "detailed"):
        return {"error": f"response_format must be 'concise' or 'detailed', got {response_format!r}"}
    target = _resolve(repo, path)
    if target is None:
        return {"error": f"path escapes the repository: {path}"}
    root = repo.resolve()
    relative = target.relative_to(root)
    requested_relative = Path(path or ".")
    if not DEFAULT_SCAN_POLICY.allows(requested_relative) or not DEFAULT_SCAN_POLICY.allows(relative):
        return {"error": f"path is excluded by repository scan policy: {path}"}
    if not target.exists():
        return {"error": f"no such path: {path}"}

    if target.is_dir():
        files: list[Path] = []
        for candidate in target.glob("*.py"):
            try:
                requested_rel = candidate.relative_to(root)
                resolved = candidate.resolve(strict=True)
                rel = resolved.relative_to(root)
            except (OSError, ValueError):
                continue
            if resolved.is_file() and DEFAULT_SCAN_POLICY.allows(requested_rel) and DEFAULT_SCAN_POLICY.allows(rel):
                files.append(resolved)
        files.sort()
        modules = []
        for py in files[:80]:
            try:
                tree, n_lines = _parse(py)
            except (OSError, UnicodeError, LookupError, SyntaxError, tokenize.TokenError) as exc:
                modules.append({"name": py.name, "error": _parse_error(exc)})
                continue
            names = [d["name"] for d in _outline_tree(tree)][:40]
            m = {"name": py.name, "lines": n_lines, "defs": names}
            if doc := _doc1(tree):
                m["doc"] = doc
            modules.append(m)
        subpackages: list[str] = []
        for candidate in target.iterdir():
            try:
                requested_rel = candidate.relative_to(root)
                resolved = candidate.resolve(strict=True)
                rel = resolved.relative_to(root)
            except (OSError, ValueError):
                continue
            init = resolved / "__init__.py"
            if (
                resolved.is_dir()
                and init.is_file()
                and DEFAULT_SCAN_POLICY.allows(requested_rel)
                and DEFAULT_SCAN_POLICY.allows(rel)
            ):
                subpackages.append(candidate.name)
        subpackages.sort()
        out = {"path": path, "modules": modules, "subpackages": subpackages}
        if len(files) > 80:
            out["truncated"] = f"{len(files) - 80} more files — outline them directly"
        return out

    if target.suffix != ".py":
        return {"error": f"not a Python file (use Read for other types): {path}"}
    try:
        tree, n_lines = _parse(target)
    except (OSError, UnicodeError, LookupError, SyntaxError, tokenize.TokenError) as exc:
        return {"error": f"could not parse {path}: {_parse_error(exc)}"}
    defs = _outline_tree(tree)
    n_defs = _definition_count(defs)
    cap = max(1, min(limit, OUTLINE_DEFINITION_CAP))
    kept, returned_defs = _truncate_definitions(defs, cap)
    out = {
        "path": target.relative_to(repo.resolve()).as_posix(),
        "lines": n_lines,
    }
    if response_format == "concise":
        out["defs"] = _concise_defs(kept)
        out["note"] = "name + span only — response_format:'detailed' adds imports, signatures, docstrings"
    else:
        out["imports"] = _imports(tree)
        out["defs"] = kept
    if doc := _doc1(tree):
        out["doc"] = doc
    if n_defs > returned_defs:
        out["truncated"] = f"{n_defs - returned_defs} more definitions (nested included)"
    out["def_count"] = n_defs
    return out


def build_code_index(repo: Path) -> list[dict]:
    """One search_project entry per repo .py: dotted module name, docstring first line,
    and def/class names in the haystack — so 'where does X live in code?' is one call.

    Parse failures remain visible as entries with ``parse_error`` instead of silently
    disappearing from repository search.
    """
    repo = repo.resolve()
    idx: list[dict] = []
    for py in iter_repository_files(repo, {".py"}):
        rel = py.relative_to(repo)
        dotted = ".".join(rel.with_suffix("").parts)
        if dotted.endswith(".__init__"):
            dotted = dotted[: -len(".__init__")]
        try:
            tree, _ = _parse(py)
        except (OSError, UnicodeError, LookupError, SyntaxError, tokenize.TokenError) as exc:
            error = _parse_error(exc)
            idx.append(
                {
                    "kind": "code",
                    "name": dotted,
                    "path": rel.as_posix(),
                    "desc": f"Python parse failed: {error}",
                    "haystack": f"{dotted} {py.stem} parse error {error}",
                    "parse_error": error,
                }
            )
            continue
        desc = _doc1(tree)
        names = _definition_names(_outline_tree(tree))
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
