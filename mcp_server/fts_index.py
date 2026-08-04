"""Content-level FTS5/BM25 index behind search_project — find the SPAN, not the file.

The metadata index in server.py answers "which module/dataset/doc covers X?" at the
file level; the agent then still reads whole files to find the relevant lines. This
module indexes CONTENT CHUNKS so a query returns path + line-span + snippet and the
follow-up is a bounded Read of just that span. Chunking is structure-aware per cAST
(arXiv 2506.15655: AST chunks with scope headers beat line chunks, +4.3 R@5 on
RepoEval): Python splits on top-level defs/classes (large classes per-method), each
chunk headed by its module::Class.name scope line; markdown splits on headings; SQL
views and memory files are one chunk each (they are small). Ranking is plain BM25 —
at this corpus size lexical rank is enough (sub-1k-doc BM25 >90% top-hit rates in the
retrieval literature); no embeddings, no extra deps: stdlib sqlite3 ships FTS5.

The DB is a derived cache at .cache/project_fts.sqlite (gitignored). refresh() is
mtime-incremental: unchanged files cost a stat, changed files are re-chunked, deleted
files are purged. Callers should refresh() before search() — a stale index would
return old line spans, which is worse than no index.
"""

from __future__ import annotations

import ast
import contextlib
import re
import sqlite3
import time
import tokenize
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

from mcp_server.code_index import DEFAULT_SCAN_POLICY, iter_repository_files, read_python_source

# A class longer than this is chunked per-method (cAST: keep chunks dense but whole).
CLASS_SPLIT_LINES = 150
# Maximum target size for a chunk. Oversized source is windowed; it is never silently
# truncated under a span that claims to cover lines absent from the indexed body.
BODY_CAP = 4_000

_MD_HEADING = re.compile(r"^#{1,3}\s+(.+)$")


# Bump when the schema or extraction logic changes: mismatched caches are cleared so
# every row was produced by the current code (a half-old edge table would silently
# under-report importers — worse than a one-off 40s rebuild).
SCHEMA_VERSION = "4"

CHUNK_KINDS = frozenset({"code-chunk", "doc-section", "sql-view", "memory"})
_MEMORY_NAMESPACE = "memory://external/"


@dataclass(frozen=True, slots=True)
class _Source:
    path: Path
    source_kind: str


def _db_path(repo: Path) -> Path:
    d = repo / ".cache"
    d.mkdir(exist_ok=True)
    return d / "project_fts.sqlite"


def _connect(repo: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path(repo))
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS files (
            path TEXT PRIMARY KEY,
            fingerprint TEXT NOT NULL,
            source_kind TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
        CREATE TABLE IF NOT EXISTS imports (src TEXT, dst TEXT, PRIMARY KEY (src, dst));
        CREATE TABLE IF NOT EXISTS errors (path TEXT PRIMARY KEY, message TEXT NOT NULL);
        CREATE VIRTUAL TABLE IF NOT EXISTS chunks USING fts5(
            header, body, path UNINDEXED, span UNINDEXED, kind UNINDEXED
        );
        """
    )
    row = conn.execute("SELECT v FROM meta WHERE k = 'schema'").fetchone()
    if not row or row[0] != SCHEMA_VERSION:
        conn.executescript(
            """
            DROP TABLE IF EXISTS files;
            DROP TABLE IF EXISTS imports;
            DROP TABLE IF EXISTS errors;
            DROP TABLE IF EXISTS chunks;
            CREATE TABLE files (
                path TEXT PRIMARY KEY,
                fingerprint TEXT NOT NULL,
                source_kind TEXT NOT NULL
            );
            CREATE TABLE imports (src TEXT, dst TEXT, PRIMARY KEY (src, dst));
            CREATE TABLE errors (path TEXT PRIMARY KEY, message TEXT NOT NULL);
            CREATE VIRTUAL TABLE chunks USING fts5(
                header, body, path UNINDEXED, span UNINDEXED, kind UNINDEXED
            );
            """
        )
        conn.execute("INSERT OR REPLACE INTO meta (k, v) VALUES ('schema', ?)", (SCHEMA_VERSION,))
        conn.commit()
    return conn


# ── chunkers ─────────────────────────────────────────────────────────────────


def _line_windows(
    header: str,
    lines: list[str],
    start: int,
    end: int,
    kind: str,
) -> list[tuple[str, str, str, str]]:
    """Window an inclusive line range without lying about the returned span."""
    if start > end or start < 1:
        return []
    windows: list[tuple[int, int, str]] = []
    window_start = start
    body: list[str] = []
    size = 0
    for line_no in range(start, end + 1):
        line = lines[line_no - 1]
        added = len(line) + (1 if body else 0)
        if body and size + added > BODY_CAP:
            windows.append((window_start, line_no - 1, "\n".join(body)))
            window_start, body, size = line_no, [], 0
            added = len(line)
        body.append(line)
        size += added
    if body:
        windows.append((window_start, end, "\n".join(body)))

    total = len(windows)
    out = []
    for part, (a, b, text) in enumerate(windows, 1):
        if not text.strip():
            continue
        part_header = f"{header} [part {part}/{total}]" if total > 1 else header
        out.append((part_header, text, f"{a}-{b}", kind))
    return out


def _doc1(node: ast.AST) -> str:
    doc = ast.get_docstring(node, clean=True) or ""
    return doc.strip().splitlines()[0][:120] if doc.strip() else ""


def _node_start(node: ast.AST) -> int:
    return min((decorator.lineno for decorator in getattr(node, "decorator_list", ())), default=node.lineno)


def _unparse(node: ast.AST) -> str:
    with contextlib.suppress(Exception):
        return ast.unparse(node)
    return "?"


def _decorator_summary(node: ast.AST) -> str:
    decorators = [f"@{_unparse(decorator)}" for decorator in getattr(node, "decorator_list", ())]
    return f" decorators: {' '.join(decorators)}" if decorators else ""


def _function_header(rel: str, scope: str, node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    signature = _unparse(node.args)
    returns = f" -> {_unparse(node.returns)}" if node.returns is not None else ""
    doc = _doc1(node)
    return (
        f"{rel}::{scope}{node.name}({signature}){returns}. {doc}"
        f"{_decorator_summary(node)}"
    ).strip()


def _class_header(rel: str, scope: str, node: ast.ClassDef, label: str = "") -> str:
    params = [_unparse(base) for base in node.bases]
    params.extend(
        f"{keyword.arg}={_unparse(keyword.value)}" if keyword.arg else f"**{_unparse(keyword.value)}"
        for keyword in node.keywords
    )
    type_params = [_unparse(param) for param in getattr(node, "type_params", ())]
    generic = f"[{', '.join(type_params)}]" if type_params else ""
    bases = f"({', '.join(params)})" if params else ""
    suffix = f" {label}" if label else ""
    return (
        f"{rel}::{scope}class {node.name}{generic}{bases}{suffix}. {_doc1(node)}"
        f"{_decorator_summary(node)}"
    ).strip()


def _class_chunks(
    rel: str,
    scope: str,
    node: ast.ClassDef,
    lines: list[str],
) -> list[tuple[str, str, str, str]]:
    start = _node_start(node)
    end = node.end_lineno or node.lineno
    header = _class_header(rel, scope, node)
    if end - start <= CLASS_SPLIT_LINES:
        return _line_windows(header, lines, start, end, "code-chunk")

    # Large classes are partitioned into contiguous structural gaps plus direct
    # methods/nested classes. The first gap is the class summary (decorators, header,
    # docstring and leading attributes); later gaps retain attributes after methods.
    out: list[tuple[str, str, str, str]] = []
    members = [
        child
        for child in node.body
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    ]
    cursor = start
    gap_number = 0
    for member in members:
        member_start = _node_start(member)
        if cursor < member_start:
            gap_number += 1
            label = "summary" if gap_number == 1 else "class body"
            out.extend(
                _line_windows(_class_header(rel, scope, node, label), lines, cursor, member_start - 1, "code-chunk")
            )
        if isinstance(member, ast.ClassDef):
            out.extend(_class_chunks(rel, f"{scope}{node.name}.", member, lines))
        else:
            member_end = member.end_lineno or member.lineno
            out.extend(
                _line_windows(
                    _function_header(rel, f"{scope}{node.name}.", member),
                    lines,
                    member_start,
                    member_end,
                    "code-chunk",
                )
            )
        cursor = (member.end_lineno or member.lineno) + 1
    if cursor <= end:
        gap_number += 1
        label = "summary" if gap_number == 1 else "class body"
        out.extend(_line_windows(_class_header(rel, scope, node, label), lines, cursor, end, "code-chunk"))
    return out


def _py_chunks(rel: str, text: str) -> list[tuple[str, str, str, str]]:
    """Complete, structure-aware Python chunks with exact inclusive line spans.

    Decorators belong to their definition, all module-level gaps are retained, and
    oversized definitions are split into bounded line windows instead of truncated.
    Syntax errors intentionally propagate so ``refresh`` can report and persist them.
    """
    tree = ast.parse(text, filename=rel)
    lines = text.splitlines()
    if not lines:
        return []

    out: list[tuple[str, str, str, str]] = []
    definitions = [
        node
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
    ]
    cursor = 1
    module_section = 0
    for node in definitions:
        start = _node_start(node)
        if cursor < start:
            module_section += 1
            label = "module preamble" if module_section == 1 else f"module body {module_section}"
            out.extend(
                _line_windows(f"{rel} — {label}. {_doc1(tree)}", lines, cursor, start - 1, "code-chunk")
            )
        if isinstance(node, ast.ClassDef):
            out.extend(_class_chunks(rel, "", node, lines))
        else:
            end = node.end_lineno or node.lineno
            out.extend(_line_windows(_function_header(rel, "", node), lines, start, end, "code-chunk"))
        cursor = (node.end_lineno or node.lineno) + 1
    if cursor <= len(lines):
        module_section += 1
        label = "module preamble" if module_section == 1 else f"module body {module_section}"
        out.extend(_line_windows(f"{rel} — {label}. {_doc1(tree)}", lines, cursor, len(lines), "code-chunk"))
    return out


def _md_chunks(rel: str, text: str) -> list[tuple[str, str, str, str]]:
    """One or more bounded chunks per Markdown section, with exact line spans."""
    lines = text.splitlines()
    if not lines:
        return []
    headings = [(line_no, match.group(1).strip()) for line_no, line in enumerate(lines, 1) if (match := _MD_HEADING.match(line))]
    sections: list[tuple[int, int, str]] = []
    if not headings:
        sections.append((1, len(lines), "(intro)"))
    else:
        if headings[0][0] > 1:
            sections.append((1, headings[0][0] - 1, "(intro)"))
        for index, (start, title) in enumerate(headings):
            end = headings[index + 1][0] - 1 if index + 1 < len(headings) else len(lines)
            sections.append((start, end, title))
    out: list[tuple[str, str, str, str]] = []
    for start, end, title in sections:
        out.extend(_line_windows(f"{rel} § {title}", lines, start, end, "doc-section"))
    return out


def _whole(rel: str, text: str, kind: str) -> list[tuple[str, str, str, str]]:
    lines = text.splitlines()
    return _line_windows(rel, lines, 1, len(lines), kind)


def _py_imports(rel: str, text: str, repo_paths: set[str]) -> set[str]:
    """Repo-INTERNAL import edges from one module, resolved to repo-relative paths.

    Deterministic on purpose (why the import-only graph beats a call graph on
    precision): `import a.b` / `from a.b import c` resolve by trying a/b.py,
    a/b/__init__.py, then a/b/c.py for the from-names. Relative imports resolve
    against the module's own package. Anything that doesn't land on a repo file
    (stdlib, third-party, dynamic) is dropped — this graph answers "what in THIS
    repo breaks if I move X", nothing else."""
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return set()

    def hit(dotted: str) -> str | None:
        base = dotted.replace(".", "/")
        for cand in (f"{base}.py", f"{base}/__init__.py"):
            if cand in repo_paths:
                return cand
        return None

    pkg_parts = rel.rsplit("/", 1)[0].split("/") if "/" in rel else []
    out: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for a in node.names:
                t = hit(a.name)
                if t:
                    out.add(t)
        elif isinstance(node, ast.ImportFrom):
            if node.level:  # relative: climb from this module's package
                anchor = pkg_parts[: len(pkg_parts) - (node.level - 1)]
                prefix = "/".join(anchor)
                mod = f"{prefix}/{node.module.replace('.', '/')}" if node.module else prefix
                mod = mod.strip("/").replace("/", ".")
            else:
                mod = node.module or ""
            if not mod:
                continue
            t = hit(mod)
            if t:
                out.add(t)
            # `from pkg import name` where name is itself a module
            for a in node.names:
                t2 = hit(f"{mod}.{a.name}")
                if t2:
                    out.add(t2)
    out.discard(rel)
    return out


# ── source walk ──────────────────────────────────────────────────────────────


def _sources(
    repo: Path,
    memory_dir: Path | None,
    *,
    include_external_memory: bool,
) -> dict[str, _Source]:
    """Namespaced source id -> source descriptor under the explicit scan policy."""
    root = repo.resolve()
    sources: dict[str, _Source] = {}
    for path in iter_repository_files(root, {".py", ".md", ".sql"}, DEFAULT_SCAN_POLICY):
        rel = path.relative_to(root).as_posix()
        if path.suffix.casefold() == ".py":
            sources[rel] = _Source(path, "py")
        elif path.suffix.casefold() == ".md":
            sources[rel] = _Source(path, "md")
        elif rel.startswith("sql_views/"):
            sources[rel] = _Source(path, "sql")

    # External assistant memory is deliberately absent from the default scan.  When
    # explicitly enabled it receives a URI namespace that can never collide with a
    # repository-relative source id.
    if include_external_memory and memory_dir is not None and memory_dir.is_dir():
        memory_root = memory_dir.resolve()
        for candidate in sorted(memory_root.glob("*.md")):
            try:
                path = candidate.resolve(strict=True)
                path.relative_to(memory_root)
            except (OSError, ValueError):
                continue
            source_id = f"{_MEMORY_NAMESPACE}{quote(path.name, safe='._-')}"
            sources[source_id] = _Source(path, "memory")
    return sources


def _fingerprint(path: Path) -> str:
    stat = path.stat()
    return f"{stat.st_mtime_ns}:{stat.st_size}"


def _read_source(source: _Source) -> str:
    if source.source_kind == "py":
        return read_python_source(source.path)
    return source.path.read_text(encoding="utf-8")


def _error_message(exc: BaseException) -> str:
    if isinstance(exc, SyntaxError):
        where = f"line {exc.lineno}" if exc.lineno else "unknown line"
        return f"SyntaxError at {where}: {exc.msg}"
    return f"{type(exc).__name__}: {exc}"


def refresh(
    repo: Path,
    memory_dir: Path | None = None,
    *,
    include_external_memory: bool = False,
) -> dict:
    """Incrementally rebuild the bounded index and report persistent source errors."""
    t0 = time.perf_counter()
    root = repo.resolve()
    conn = _connect(root)
    try:
        known = {
            path: (fingerprint, source_kind)
            for path, fingerprint, source_kind in conn.execute("SELECT path, fingerprint, source_kind FROM files")
        }
        src = _sources(root, memory_dir, include_external_memory=include_external_memory)
        old_python = {path for path, (_, source_kind) in known.items() if source_kind == "py"}
        new_python = {path for path, source in src.items() if source.source_kind == "py"}
        python_topology_changed = old_python != new_python
        repo_paths = set(new_python)
        changed, removed = 0, 0
        changed_text: dict[str, str] = {}
        for rel, source in src.items():
            try:
                fingerprint = _fingerprint(source.path)
            except OSError as exc:
                conn.execute("INSERT OR REPLACE INTO errors (path, message) VALUES (?, ?)", (rel, _error_message(exc)))
                continue
            if known.get(rel) == (fingerprint, source.source_kind):
                continue

            changed += 1
            conn.execute("DELETE FROM chunks WHERE path = ?", (rel,))
            conn.execute("DELETE FROM errors WHERE path = ?", (rel,))
            if source.source_kind == "py" and not python_topology_changed:
                conn.execute("DELETE FROM imports WHERE src = ?", (rel,))
            try:
                text = _read_source(source)
                chunks = (
                    _py_chunks(rel, text)
                    if source.source_kind == "py"
                    else _md_chunks(rel, text)
                    if source.source_kind in {"md", "memory"}
                    else _whole(rel, text, "sql-view")
                )
                if source.source_kind == "memory":
                    chunks = [(header, body, span, "memory") for header, body, span, _ in chunks]
                conn.executemany(
                    "INSERT INTO chunks (header, body, path, span, kind) VALUES (?,?,?,?,?)",
                    [(header, body, rel, span, kind) for header, body, span, kind in chunks],
                )
                if source.source_kind == "py":
                    changed_text[rel] = text
                    if not python_topology_changed:
                        conn.executemany(
                            "INSERT OR IGNORE INTO imports (src, dst) VALUES (?,?)",
                            [(rel, imported) for imported in _py_imports(rel, text, repo_paths)],
                        )
            except (OSError, UnicodeError, LookupError, SyntaxError, tokenize.TokenError) as exc:
                if source.source_kind == "py":
                    conn.execute("DELETE FROM imports WHERE src = ?", (rel,))
                conn.execute("INSERT OR REPLACE INTO errors (path, message) VALUES (?, ?)", (rel, _error_message(exc)))
            conn.execute(
                "INSERT OR REPLACE INTO files (path, fingerprint, source_kind) VALUES (?, ?, ?)",
                (rel, fingerprint, source.source_kind),
            )

        for rel in set(known) - set(src):
            conn.execute("DELETE FROM chunks WHERE path = ?", (rel,))
            conn.execute("DELETE FROM imports WHERE src = ? OR dst = ?", (rel, rel))
            conn.execute("DELETE FROM errors WHERE path = ?", (rel,))
            conn.execute("DELETE FROM files WHERE path = ?", (rel,))
            removed += 1

        # Adding or deleting a module can resolve/invalidate imports in otherwise
        # unchanged files. Rebuild all edges only when that module topology changes.
        if python_topology_changed:
            conn.execute("DELETE FROM imports")
            for rel in sorted(new_python):
                if conn.execute("SELECT 1 FROM errors WHERE path = ?", (rel,)).fetchone():
                    continue
                try:
                    text = changed_text.get(rel) or _read_source(src[rel])
                    edges = _py_imports(rel, text, repo_paths)
                except (OSError, UnicodeError, LookupError, SyntaxError, tokenize.TokenError) as exc:
                    conn.execute("INSERT OR REPLACE INTO errors (path, message) VALUES (?, ?)", (rel, _error_message(exc)))
                    continue
                conn.executemany(
                    "INSERT OR IGNORE INTO imports (src, dst) VALUES (?,?)",
                    [(rel, imported) for imported in edges],
                )
        conn.commit()
        errors = [
            {"path": path, "error": message}
            for path, message in conn.execute("SELECT path, message FROM errors ORDER BY path")
        ]
        return {
            "indexed": changed,
            "removed": removed,
            "total_files": len(src),
            "error_count": len(errors),
            "errors": errors[:25],
            "ms": int((time.perf_counter() - t0) * 1000),
        }
    finally:
        conn.close()


def deps(repo: Path, rel: str) -> dict:
    """Import edges for one repo-relative .py path: what it imports (repo-internal)
    and every module that imports it. The Python analog of view_deps — impact
    analysis before moving/renaming a module."""
    conn = _connect(repo)
    try:
        imports = [r[0] for r in conn.execute("SELECT dst FROM imports WHERE src = ? ORDER BY dst", (rel,))]
        importers = [r[0] for r in conn.execute("SELECT src FROM imports WHERE dst = ? ORDER BY src", (rel,))]
        return {"path": rel, "imports": imports, "imported_by": importers}
    finally:
        conn.close()


def search(repo: Path, query: str, limit: int = 8, kind: str = "") -> list[dict]:
    """BM25-ranked chunks: {kind, name(=scope header), path, span, why(=snippet)}.
    AND the terms first; if nothing survives, fall back to OR for recall."""
    terms = re.findall(r"[a-zA-Z0-9_]+", query)
    if not terms:
        return []
    kinds = {item.strip() for item in kind.split(",") if item.strip()}
    invalid = kinds - CHUNK_KINDS
    if invalid:
        allowed = ", ".join(sorted(CHUNK_KINDS))
        raise ValueError(f"invalid chunk kind(s): {', '.join(sorted(invalid))}; expected: {allowed}")
    conn = _connect(repo)
    try:

        def run(match: str) -> list:
            sql = (
                "SELECT header, path, span, kind, snippet(chunks, 1, '', '', ' … ', 14) "
                "FROM chunks WHERE chunks MATCH ? "
            )
            args: list = [match]
            if kinds:
                placeholders = ",".join("?" for _ in kinds)
                sql += f"AND kind IN ({placeholders}) "
                args.extend(sorted(kinds))
            sql += "ORDER BY bm25(chunks) LIMIT ?"
            args.append(max(1, min(limit, 25)))
            with contextlib.suppress(sqlite3.OperationalError):
                return conn.execute(sql, args).fetchall()
            return []

        rows = run(" AND ".join(f'"{t}"' for t in terms))
        if not rows and len(terms) > 1:
            rows = run(" OR ".join(f'"{t}"' for t in terms))
        return [
            {"kind": k, "name": h, "path": p, "span": s, "why": w.replace("\n", " ")[:200]} for h, p, s, k, w in rows
        ]
    finally:
        conn.close()
