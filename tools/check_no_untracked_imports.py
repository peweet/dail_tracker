"""
tools/check_no_untracked_imports.py — tracked-import boundary guard.

Usage:
    python tools/check_no_untracked_imports.py            # scan all tracked .py files
    python tools/check_no_untracked_imports.py --staged   # scan only staged files (pre-commit)

Returns exit code 0 when clean, 1 when any tracked/staged .py file imports a
local module that git does not track. Designed for CI (.github/workflows/ci.yml)
and the versioned `.githooks/pre-push` hook, alongside tools/check_no_private_ip.py.

WHY THIS EXISTS
    2026-08-07: a commit added test/dail_tracker_core/test_core_pre_tender_queries.py
    to the tracked tree importing from dail_tracker_core/queries/procurement/opportunities.py
    — a module tools/check_no_private_ip.py's DENY_EXACT already knows is private IP and
    blocks from being committed itself. That guard protects the private FILE from leaking;
    it says nothing about a PUBLIC file depending on it. On a clean checkout (CI, a fresh
    clone) the private module simply isn't there, so pytest collection died with an
    ImportError and took every downstream CI job with it.

    This guard closes that gap generally, not just for known-private paths: it statically
    resolves every absolute local import in every tracked .py file and fails if the
    resolved file isn't itself git-tracked — whether that's because it's deliberately
    private IP, or just an accident (a new file staged in one commit but not another,
    a rename that missed a reference).

LIMITATIONS (static AST scan, same class of blind spot as the rest of this repo's tooling)
    * Relative imports (`from . import x`, level > 0) are not checked — a relative
      import's target lives in the same already-tracked package as the importer, so the
      failure mode this guard targets is far less likely there.
    * Dynamic imports (importlib, getattr-based dispatch, string module names) are invisible
      to ast — same caveat as dail_tracker_core's own _LazyModule proxies.
    * A local module shadowed by an installed third-party package of the same top-level
      name would false-negative; none currently exist in this repo.

EXEMPTION: an import inside a `try` block that has at least one `except` handler is not
    flagged — that is the repo's existing pattern for a genuinely optional dependency (see
    mcp_server/server.py's "optional 'siting' extra not installed (public clone)" guards),
    which degrades gracefully instead of crashing collection/import on a clean checkout.
"""

from __future__ import annotations

import argparse
import ast
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _git_paths(staged: bool, suffix: str | None = None) -> list[str]:
    cmd = ["git", "diff", "--cached", "--name-only", "--diff-filter=AM", "-z"] if staged else ["git", "ls-files", "-z"]
    res = subprocess.run(cmd, cwd=PROJECT_ROOT, capture_output=True, text=True, check=True)
    paths = [p for p in res.stdout.split("\0") if p]
    return [p for p in paths if p.endswith(suffix)] if suffix else paths


def _resolve_local(dotted: str) -> Path | None:
    """The on-disk file for a dotted module name if it's a real local module, else None."""
    base = PROJECT_ROOT.joinpath(*dotted.split("."))
    for candidate in (base.with_suffix(".py"), base / "__init__.py"):
        if candidate.is_file():
            return candidate
    return None


def _guarded_import_ids(tree: ast.AST) -> set[int]:
    """id()s of Import/ImportFrom nodes inside a `try` body that has an `except` handler —
    the repo's existing pattern for a deliberately optional dependency."""
    guarded: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Try) and node.handlers:
            for stmt in node.body:
                for sub in ast.walk(stmt):
                    if isinstance(sub, (ast.Import, ast.ImportFrom)):
                        guarded.add(id(sub))
    return guarded


def _imports_in(path: Path) -> list[tuple[str, int]]:
    """[(dotted_module, lineno)] for every absolute, unguarded `import X` / `from X import ...`."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (SyntaxError, UnicodeDecodeError, OSError):
        return []
    guarded = _guarded_import_ids(tree)
    found: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if id(node) in guarded:
            continue
        if isinstance(node, ast.Import):
            found.extend((alias.name, node.lineno) for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
            found.append((node.module, node.lineno))
    return found


def find_offenders(py_files: list[str], tracked: set[str]) -> list[tuple[str, int, str, str]]:
    """[(importer, lineno, dotted_module, resolved_relpath)] for every import that resolves
    to a real local module git does not track."""
    out: list[tuple[str, int, str, str]] = []
    for rel in py_files:
        abspath = PROJECT_ROOT / rel
        if not abspath.is_file():
            continue
        for dotted, lineno in _imports_in(abspath):
            resolved = _resolve_local(dotted)
            if resolved is None:
                continue
            resolved_rel = resolved.relative_to(PROJECT_ROOT).as_posix()
            if resolved_rel == rel:
                continue  # a file importing itself (re-export shims) is not this bug
            if resolved_rel not in tracked:
                out.append((rel, lineno, dotted, resolved_rel))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Block tracked files from importing a module git doesn't track.")
    ap.add_argument("--staged", action="store_true", help="scan only staged .py files (pre-commit use)")
    args = ap.parse_args()

    py_files = _git_paths(args.staged, suffix=".py")
    tracked = set(_git_paths(staged=False))
    offenders = find_offenders(py_files, tracked)
    if not offenders:
        scope = "staged" if args.staged else "tracked"
        print(f"OK — no {scope} .py file imports an untracked local module ({len(py_files)} files scanned).")
        return 0

    print("BLOCKED — these tracked files import a module git does not track (breaks a clean checkout):\n")
    for importer, lineno, dotted, resolved in sorted(offenders):
        print(f"  ✗ {importer}:{lineno}  imports {dotted!r} -> {resolved} (not git-tracked)")
    print(
        "\nEither commit the imported module (if it's meant to be public), or move the importing "
        "code out of the tracked tree (see .gitignore and tools/check_no_private_ip.py's DENY_EXACT)."
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
