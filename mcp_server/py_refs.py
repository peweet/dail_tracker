"""Symbol-grain references — the call-site twin of py_deps.

py_deps answers at MODULE grain (which files import this one); this module answers
at SYMBOL grain: every static reference jedi can find to ONE def/class/method/
constant across the repo — the "who calls save_parquet?" question that a text grep
answers with false positives (same name in strings, comments, other modules) and
false negatives (aliased imports, re-exports).

Built on jedi (the open inference engine; declared in the ``mcp`` extra). Source is
analysed, never executed. Like py_deps, results are STATIC: blind to getattr/
importlib/string dispatch, so an empty reference list is dead-code evidence, not
proof. Kept separate from server.py so it is importable and testable without the
optional ``mcp`` extra installed (tests importorskip jedi instead).
"""

from __future__ import annotations

import ast
import io
import threading
import tokenize
from pathlib import Path

from mcp_server.code_index import DEFAULT_SCAN_POLICY, read_python_source

_REF_CAP = 200  # hard ceiling on returned reference rows
_CODE_CAP = 160  # one source line per reference, truncated to this many chars
_JEDI_REFERENCE_LOCK = threading.Lock()


def _resolve_module(repo: Path, path: str) -> Path | None:
    """Repo-relative path or dotted module → existing .py inside the repo, else None.
    Same acceptance rule as the py_deps tool ('services/parquet_io.py' or
    'services.parquet_io'); refuses anything resolving outside the repo."""
    requested = Path(path)
    if requested.is_absolute() or requested.drive:
        return None
    rel = path.replace("\\", "/").strip("/")
    if not rel.endswith(".py"):
        cand = rel.replace(".", "/")
        rel = f"{cand}.py" if (repo / f"{cand}.py").is_file() else f"{cand}/__init__.py"
    if not DEFAULT_SCAN_POLICY.allows(rel):
        return None
    try:
        target = (repo / rel).resolve()
        target.relative_to(repo.resolve())
    except (ValueError, OSError):
        return None
    return target if target.is_file() else None


def _find_named(body: list, ident: str):
    """The first top-level def/class/assign-target in `body` named `ident`, or None."""
    for node in body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)) and node.name == ident:
            return node
        if isinstance(node, ast.Assign):
            for t in node.targets:
                if isinstance(t, ast.Name) and t.id == ident:
                    return t
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == ident:
            return node.target
    return None


def _names_in(body: list) -> list[str]:
    """Top-level def/class/constant names — the 'available' hint on a miss."""
    out: list[str] = []
    for node in body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            out.append(node.name)
        elif isinstance(node, ast.Assign):
            out.extend(t.id for t in node.targets if isinstance(t, ast.Name))
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            out.append(node.target.id)
    return out


def _locate(src: str, name: str) -> tuple[int, int] | dict:
    """(line, column) of the identifier's definition — jedi needs a cursor position,
    not a name. 'Class.method' descends one level. On a miss, an {error, available}
    dict the caller passes straight through."""
    tree = ast.parse(src)
    tokens = list(tokenize.generate_tokens(io.StringIO(src).readline))
    parts = name.split(".")
    if len(parts) > 2 or not all(parts):
        return {"error": f"name must be 'symbol' or 'Class.method', got: {name}"}
    node = _find_named(tree.body, parts[0])
    if len(parts) == 2:
        if not isinstance(node, ast.ClassDef):
            return {"error": f"'{parts[0]}' is not a class in this module", "available": _names_in(tree.body)[:40]}
        node = _find_named(node.body, parts[1])
        if node is None:
            cls = _find_named(tree.body, parts[0])
            return {"error": f"no '{parts[1]}' in class '{parts[0]}'", "available": _names_in(cls.body)[:40]}
    if node is None:
        return {"error": f"no top-level '{name}' here", "available": _names_in(tree.body)[:40]}
    ident = parts[-1]
    candidates = [token for token in tokens if token.type == tokenize.NAME and token.string == ident]
    if isinstance(node, ast.Name):
        # AST columns are UTF-8 byte offsets while Jedi/tokenize columns are Unicode
        # character offsets. Match on the byte offset, then return tokenize's column.
        source_line = src.splitlines()[node.lineno - 1]
        for token in candidates:
            if token.start[0] != node.lineno:
                continue
            byte_column = len(source_line[: token.start[1]].encode("utf-8"))
            if byte_column == node.col_offset:
                return token.start
    else:
        keyword = "class" if isinstance(node, ast.ClassDef) else "def"
        keyword_index = next(
            (
                index
                for index, token in enumerate(tokens)
                if token.start[0] == node.lineno and token.type == tokenize.NAME and token.string == keyword
            ),
            None,
        )
        if keyword_index is not None:
            for token in tokens[keyword_index + 1 :]:
                if token.start[0] != node.lineno:
                    break
                if token.type == tokenize.NAME:
                    if token.string == ident:
                        return token.start
                    break

    # A token-aware fallback remains preferable to substring search (which can land
    # inside a longer identifier or string literal).
    for token in candidates:
        if token.start[0] == node.lineno:
            return token.start
    return {"error": f"could not locate identifier token for '{name}'"}


def refs(repo: Path, path: str, name: str, limit: int = 60) -> dict:
    """Every repo-internal static reference to `path:name`. Returns {error} dicts
    (never raises) so the MCP wrapper can pass the result straight through."""
    import jedi  # deferred — the mcp extra's dependency; keeps this module importable without it

    repo = repo.resolve()
    target = _resolve_module(repo, path)
    if target is None:
        return {"error": f"no such module: {path}"}
    try:
        src = read_python_source(target)
        pos = _locate(src, name)
    except (OSError, UnicodeError, LookupError, SyntaxError, tokenize.TokenError) as e:
        return {"error": f"could not parse {path}: {e}"}
    if isinstance(pos, dict):
        return pos
    line, col = pos

    # jedi caps a reference search at 30 parsed files (_PARSED_FILE_LIMIT — an IDE
    # latency guard). For blast radius that's silent UNDER-reporting: save_parquet has
    # ~140 importing files and the capped search returned 29. Lift it for this call.
    # Jedi exposes these safety caps only as module globals. Serialising the temporary
    # override prevents concurrent MCP calls from racing the save/restore sequence.
    with _JEDI_REFERENCE_LOCK:
        try:
            from jedi.inference import references as _jedi_refs

            caps = (_jedi_refs._PARSED_FILE_LIMIT, _jedi_refs._OPENED_FILE_LIMIT)
            _jedi_refs._PARSED_FILE_LIMIT, _jedi_refs._OPENED_FILE_LIMIT = 100_000, 100_000
        except (ImportError, AttributeError):  # jedi internals moved — search runs capped
            _jedi_refs, caps = None, None
        try:
            script = jedi.Script(code=src, path=str(target), project=jedi.Project(str(repo)))
            found = script.get_references(line, col, include_builtins=False)
        except Exception as e:  # noqa: BLE001 — inference trouble becomes an error row, never a crash
            return {"error": f"jedi failed: {type(e).__name__}: {e}"}
        finally:
            if caps is not None:
                _jedi_refs._PARSED_FILE_LIMIT, _jedi_refs._OPENED_FILE_LIMIT = caps

    definitions: list[dict] = []
    references: list[dict] = []
    external = 0
    excluded = 0
    for n in found:
        if n.module_path is None:
            external += 1
            continue
        try:
            rel = Path(n.module_path).resolve().relative_to(repo).as_posix()
        except (OSError, ValueError):
            external += 1  # site-packages / stdlib — outside the repo
            continue
        if not DEFAULT_SCAN_POLICY.allows(rel):
            excluded += 1
            continue
        row = {"path": rel, "line": n.line, "code": n.get_line_code().strip()[:_CODE_CAP]}
        (definitions if n.is_definition() else references).append(row)

    definitions.sort(key=lambda r: (r["path"], r["line"]))
    references.sort(key=lambda r: (r["path"], r["line"]))
    limit = max(1, min(limit, _REF_CAP))
    out = {
        "symbol": f"{target.relative_to(repo).as_posix()}:{name}",
        "definitions": definitions[:limit],  # import bindings can outnumber call sites (139 for save_parquet)
        "references": references[:limit],
        "counts": {
            "references": len(references),
            "files": len({r["path"] for r in references}),
            "definitions": len(definitions),
        },
        "note": "static (jedi) — blind to getattr/importlib/string dispatch; empty is evidence, not proof",
    }
    if len(references) > limit:
        out["counts"]["truncated_to"] = limit
    if external:
        out["counts"]["outside_repo"] = external
    if excluded:
        out["counts"]["excluded_by_policy"] = excluded
    if caps is None:
        out["note"] += "; ⚠ jedi's ~30-parsed-file cap could NOT be lifted — wide symbols under-report"
    return out
