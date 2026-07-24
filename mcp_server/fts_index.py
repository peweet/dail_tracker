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
import os
import re
import sqlite3
import time
from pathlib import Path

# Directories never worth indexing (heavy, generated, or non-source).
SKIP_DIRS = {
    ".git", ".venv", "__pycache__", "node_modules", ".cache", ".idea", ".pytest_cache",
    "data", "logs", "audit_screenshots", "doc_archive", "tmp",
}
# A class longer than this is chunked per-method (cAST: keep chunks dense but whole).
CLASS_SPLIT_LINES = 150
# Body text stored per chunk is capped: FTS matches on it, snippets come from it.
BODY_CAP = 4_000

_MD_HEADING = re.compile(r"^#{1,3}\s+(.+)$")


# Bump when the schema or extraction logic changes: mismatched caches are cleared so
# every row was produced by the current code (a half-old edge table would silently
# under-report importers — worse than a one-off 40s rebuild).
SCHEMA_VERSION = "2"


def _db_path(repo: Path) -> Path:
    d = repo / ".cache"
    d.mkdir(exist_ok=True)
    return d / "project_fts.sqlite"


def _connect(repo: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path(repo))
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS files (path TEXT PRIMARY KEY, mtime REAL);
        CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT);
        CREATE TABLE IF NOT EXISTS imports (src TEXT, dst TEXT, PRIMARY KEY (src, dst));
        CREATE VIRTUAL TABLE IF NOT EXISTS chunks USING fts5(
            header, body, path UNINDEXED, span UNINDEXED, kind UNINDEXED
        );
        """
    )
    row = conn.execute("SELECT v FROM meta WHERE k = 'schema'").fetchone()
    if not row or row[0] != SCHEMA_VERSION:
        conn.executescript("DELETE FROM files; DELETE FROM chunks; DELETE FROM imports;")
        conn.execute("INSERT OR REPLACE INTO meta (k, v) VALUES ('schema', ?)", (SCHEMA_VERSION,))
        conn.commit()
    return conn


# ── chunkers ─────────────────────────────────────────────────────────────────

def _py_chunks(rel: str, text: str) -> list[tuple[str, str, str, str]]:
    """(header, body, span, kind) per top-level def/class; big classes per-method.
    Header carries the cAST-style scope line the retriever needs to use the hit."""
    out = []
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return []
    lines = text.splitlines()

    def seg(a: int, b: int) -> str:
        return "\n".join(lines[a - 1 : b])[:BODY_CAP]

    def doc1(node) -> str:
        d = ast.get_docstring(node) or ""
        return d.splitlines()[0][:120] if d else ""

    # module preamble (docstring + everything before the first def/class)
    first_def = min(
        [n.lineno for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))],
        default=len(lines) + 1,
    )
    if first_def > 1:
        out.append((f"{rel} — module preamble. {doc1(tree)}", seg(1, first_def - 1), f"1-{first_def - 1}", "code-chunk"))

    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            end = getattr(node, "end_lineno", node.lineno)
            out.append((f"{rel}::{node.name}(). {doc1(node)}", seg(node.lineno, end), f"{node.lineno}-{end}", "code-chunk"))
        elif isinstance(node, ast.ClassDef):
            end = getattr(node, "end_lineno", node.lineno)
            if end - node.lineno <= CLASS_SPLIT_LINES:
                out.append((f"{rel}::class {node.name}. {doc1(node)}", seg(node.lineno, end), f"{node.lineno}-{end}", "code-chunk"))
            else:
                for m in node.body:
                    if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        mend = getattr(m, "end_lineno", m.lineno)
                        out.append(
                            (f"{rel}::class {node.name}.{m.name}(). {doc1(m)}", seg(m.lineno, mend), f"{m.lineno}-{mend}", "code-chunk")
                        )
    return out


def _md_chunks(rel: str, text: str) -> list[tuple[str, str, str, str]]:
    """One chunk per #/##/### section; the heading becomes the scope header."""
    out = []
    lines = text.splitlines()
    start, title = 1, "(intro)"
    for i, ln in enumerate(lines, 1):
        m = _MD_HEADING.match(ln)
        if m and i > start:
            out.append((f"{rel} § {title}", "\n".join(lines[start - 1 : i - 1])[:BODY_CAP], f"{start}-{i - 1}", "doc-section"))
            start, title = i, m.group(1).strip()
        elif m:
            title = m.group(1).strip()
    out.append((f"{rel} § {title}", "\n".join(lines[start - 1 :])[:BODY_CAP], f"{start}-{len(lines)}", "doc-section"))
    return [c for c in out if c[1].strip()]


def _whole(rel: str, text: str, kind: str) -> list[tuple[str, str, str, str]]:
    n = text.count("\n") + 1
    return [(rel, text[:BODY_CAP], f"1-{n}", kind)]


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

def _sources(repo: Path, memory_dir: Path | None) -> dict[str, tuple[Path, str]]:
    """path-key -> (abs path, chunker kind). Skips heavy/generated trees."""
    src: dict[str, tuple[Path, str]] = {}
    for root, dirs, files in os.walk(repo):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for f in files:
            p = Path(root) / f
            rel = p.relative_to(repo).as_posix()
            if f.endswith(".py"):
                src[rel] = (p, "py")
            elif f.endswith(".md"):
                src[rel] = (p, "md")
            elif f.endswith(".sql") and rel.startswith("sql_views/"):
                src[rel] = (p, "sql")
    if memory_dir and memory_dir.is_dir():
        for p in memory_dir.glob("*.md"):
            src[f"memory/{p.name}"] = (p, "memory")
    return src


def refresh(repo: Path, memory_dir: Path | None = None) -> dict:
    """Incremental rebuild. Returns {'indexed': n_changed, 'removed': n, 'total_files': n}."""
    t0 = time.perf_counter()
    conn = _connect(repo)
    try:
        known = dict(conn.execute("SELECT path, mtime FROM files"))
        src = _sources(repo, memory_dir)
        changed, removed = 0, 0
        for rel, (p, kind) in src.items():
            with contextlib.suppress(OSError):
                mt = p.stat().st_mtime
                if known.get(rel) == mt:
                    continue
                text = ""
                with contextlib.suppress(Exception):
                    text = p.read_text(encoding="utf-8", errors="replace")
                chunks = (
                    _py_chunks(rel, text) if kind == "py"
                    else _md_chunks(rel, text) if kind in ("md", "memory")
                    else _whole(rel, text, "sql-view")
                )
                if kind == "memory":
                    chunks = [(h, b, s, "memory") for h, b, s, _ in chunks]
                if kind == "py":
                    conn.execute("DELETE FROM imports WHERE src = ?", (rel,))
                    conn.executemany(
                        "INSERT OR IGNORE INTO imports (src, dst) VALUES (?,?)",
                        [(rel, d) for d in _py_imports(rel, text, set(src))],
                    )
                conn.execute("DELETE FROM chunks WHERE path = ?", (rel,))
                conn.executemany(
                    "INSERT INTO chunks (header, body, path, span, kind) VALUES (?,?,?,?,?)",
                    [(h, b, rel, s, k) for h, b, s, k in chunks],
                )
                conn.execute("INSERT OR REPLACE INTO files (path, mtime) VALUES (?,?)", (rel, mt))
                changed += 1
        for rel in set(known) - set(src):
            conn.execute("DELETE FROM chunks WHERE path = ?", (rel,))
            conn.execute("DELETE FROM imports WHERE src = ?", (rel,))
            conn.execute("DELETE FROM files WHERE path = ?", (rel,))
            removed += 1
        conn.commit()
        return {"indexed": changed, "removed": removed, "total_files": len(src), "ms": int((time.perf_counter() - t0) * 1000)}
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
    conn = _connect(repo)
    try:
        def run(match: str) -> list:
            sql = (
                "SELECT header, path, span, kind, snippet(chunks, 1, '', '', ' … ', 14) "
                "FROM chunks WHERE chunks MATCH ? "
            )
            args: list = [match]
            if kind:
                sql += "AND kind = ? "
                args.append(kind)
            sql += "ORDER BY bm25(chunks) LIMIT ?"
            args.append(max(1, min(limit, 25)))
            with contextlib.suppress(sqlite3.OperationalError):
                return conn.execute(sql, args).fetchall()
            return []

        rows = run(" AND ".join(f'"{t}"' for t in terms))
        if not rows and len(terms) > 1:
            rows = run(" OR ".join(f'"{t}"' for t in terms))
        return [
            {"kind": k, "name": h, "path": p, "span": s, "why": w.replace("\n", " ")[:200]}
            for h, p, s, k, w in rows
        ]
    finally:
        conn.close()
