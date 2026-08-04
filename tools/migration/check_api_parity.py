"""Anti-drift ratchet: every core query function must be reachable from the API.

The recurring decay mode in this repo is that features land Streamlit-first and
the FastAPI surface lags behind — four domains drifted in roughly three weeks
(recorded in the streamlit-uncoupling notes). Drift is invisible until someone
tries to build a second frontend and hits a wall.

This makes it visible and mechanical. It walks the AST of
`dail_tracker_core/queries/**` for public retrieval functions, then walks
`api/routers/**` and `dail_tracker_core/dossiers.py` for references to them.
Anything defined but never referenced is Streamlit-only — reachable from a page,
unreachable from an API client.

Ratchet semantics, matching tools/check_conventions.py: known-unexposed functions
live in a baseline file and are tolerated. A NEW unexposed function fails the
build. Never add to the baseline — only remove from it.

Usage
-----
    python tools/check_api_parity.py                 # report + exit code
    python tools/check_api_parity.py --report        # full per-module table
    python tools/check_api_parity.py --update-baseline
"""

from __future__ import annotations

import argparse
import ast
import sys
import tokenize
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
QUERIES_DIR = PROJECT_ROOT / "dail_tracker_core" / "queries"
CONSUMER_PATHS = [
    PROJECT_ROOT / "api",
    PROJECT_ROOT / "dail_tracker_core" / "dossiers.py",
    PROJECT_ROOT / "mcp_server",
]
BASELINE = PROJECT_ROOT / "tools" / "baselines" / "api_parity_baseline.txt"

# Domains deliberately out of scope. Siting is a beta feature whose API shape
# (a POST compute endpoint, not a GET read) is a separate design question.
EXCLUDED_MODULES = {"siting"}

# Helpers that are plumbing, not retrieval.
EXCLUDED_FUNCS = {"main", "register", "build", "connect"}

QUERY_PACKAGE = "dail_tracker_core.queries"
type Symbol = tuple[str, str]


class AnalysisError(RuntimeError):
    """A source file could not be decoded or parsed safely."""

    def __init__(self, path: Path, cause: BaseException) -> None:
        self.path = path
        self.cause = cause
        super().__init__(f"{path}: {type(cause).__name__}: {cause}")


def parse_module(path: Path) -> ast.Module:
    try:
        with tokenize.open(path) as stream:
            source = stream.read()
        return ast.parse(source, filename=str(path))
    except (OSError, UnicodeError, SyntaxError) as exc:
        raise AnalysisError(path, exc) from exc


def module_name(path: Path) -> str:
    rel = path.relative_to(QUERIES_DIR).as_posix().removesuffix(".py")
    if rel == "__init__":
        return rel
    return rel.removesuffix("/__init__")


def public_functions(path: Path) -> list[tuple[str, str, int]]:
    """(module, func_name, lineno) for public module-level defs."""
    tree = parse_module(path)
    module = module_name(path)
    out = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name.startswith("_") or node.name in EXCLUDED_FUNCS:
                continue
            out.append((module, node.name, node.lineno))
    return out


def _query_import_module(current: str, path: Path, node: ast.ImportFrom) -> str | None:
    """Resolve an import-from node to a module relative to the queries package."""
    if node.level == 0:
        if node.module == QUERY_PACKAGE:
            return "__init__"
        prefix = QUERY_PACKAGE + "."
        if node.module and node.module.startswith(prefix):
            return node.module[len(prefix) :].replace(".", "/")
        return None

    package = current if path.name == "__init__.py" else current.rpartition("/")[0]
    package_parts = [] if package == "__init__" else package.split("/") if package else []
    climb = node.level - 1
    if climb > len(package_parts):
        return None
    base = package_parts[: len(package_parts) - climb]
    if node.module:
        base.extend(node.module.split("."))
    return "/".join(base) or "__init__"


def query_exports(paths: list[Path], modules: set[str]) -> dict[Symbol, Symbol]:
    """Map a package/module re-export to the module that owns the symbol."""
    exports: dict[Symbol, Symbol] = {}
    for path in paths:
        current = module_name(path)
        tree = parse_module(path)
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom):
                continue
            imported_from = _query_import_module(current, path, node)
            if imported_from is None:
                continue
            for alias in node.names:
                if alias.name == "*":
                    continue
                local = alias.asname or alias.name
                child = alias.name if imported_from == "__init__" else f"{imported_from}/{alias.name}"
                if child in modules:
                    continue
                exports[(current, local)] = (imported_from, alias.name)
    return exports


def resolve_symbol(symbol: Symbol, exports: dict[Symbol, Symbol]) -> Symbol:
    """Follow a finite chain of query-package re-exports."""
    seen: set[Symbol] = set()
    while symbol in exports and symbol not in seen:
        seen.add(symbol)
        symbol = exports[symbol]
    return symbol


def _consumer_import_module(node: ast.ImportFrom) -> str | None:
    if node.level != 0:
        return None
    if node.module == QUERY_PACKAGE:
        return "__init__"
    prefix = QUERY_PACKAGE + "."
    if node.module and node.module.startswith(prefix):
        return node.module[len(prefix) :].replace(".", "/")
    return None


def _attribute_parts(node: ast.Attribute) -> list[str] | None:
    parts = [node.attr]
    value = node.value
    while isinstance(value, ast.Attribute):
        parts.append(value.attr)
        value = value.value
    if not isinstance(value, ast.Name):
        return None
    parts.append(value.id)
    return list(reversed(parts))


def referenced_names(
    paths: list[Path], modules: set[str], exports: dict[Symbol, Symbol]
) -> set[Symbol]:
    """Module-qualified query symbols referenced by the consumer surface."""
    referenced: set[Symbol] = set()
    files: list[Path] = []
    for p in paths:
        if p.is_dir():
            files.extend(p.rglob("*.py"))
        elif p.exists():
            files.append(p)

    for f in files:
        if "__pycache__" in f.parts:
            continue
        tree = parse_module(f)
        module_aliases: dict[str, str] = {}
        symbol_aliases: dict[str, Symbol] = {}

        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                imported_from = _consumer_import_module(node)
                if imported_from is None:
                    continue
                for alias in node.names:
                    if alias.name == "*":
                        continue
                    local = alias.asname or alias.name
                    child = alias.name if imported_from == "__init__" else f"{imported_from}/{alias.name}"
                    if child in modules:
                        module_aliases[local] = child
                    else:
                        symbol_aliases[local] = (imported_from, alias.name)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    prefix = QUERY_PACKAGE + "."
                    if not alias.asname:
                        continue
                    if alias.name == QUERY_PACKAGE:
                        module_aliases[alias.asname] = "__init__"
                    elif alias.name.startswith(prefix):
                        module_aliases[alias.asname] = alias.name[len(prefix) :].replace(".", "/")

        for node in ast.walk(tree):
            if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load) and node.id in symbol_aliases:
                referenced.add(resolve_symbol(symbol_aliases[node.id], exports))
            elif isinstance(node, ast.Attribute):
                parts = _attribute_parts(node)
                if not parts:
                    continue
                if parts[0] in module_aliases and len(parts) >= 2:
                    base = module_aliases[parts[0]]
                    suffix = parts[1:]
                    base_parts = [] if base == "__init__" else base.split("/")
                    owner = "/".join([*base_parts, *suffix[:-1]]) or "__init__"
                    name = suffix[-1]
                else:
                    package_parts = QUERY_PACKAGE.split(".")
                    if parts[: len(package_parts)] != package_parts or len(parts) <= len(package_parts):
                        continue
                    suffix = parts[len(package_parts) :]
                    owner = "/".join(suffix[:-1]) or "__init__"
                    name = suffix[-1]
                if owner in modules:
                    referenced.add(resolve_symbol((owner, name), exports))
    return referenced


def analyse() -> tuple[list[tuple[str, str, int]], set[Symbol], list[tuple[str, str, int]]]:
    query_files = [
        path
        for path in sorted(QUERIES_DIR.rglob("*.py"))
        if "__pycache__" not in path.parts
    ]
    modules = {module_name(path) for path in query_files}
    defined: list[tuple[str, str, int]] = []
    for path in query_files:
        for module, name, lineno in public_functions(path):
            if module.split("/")[0] not in EXCLUDED_MODULES:
                defined.append((module, name, lineno))

    exports = query_exports(query_files, modules)
    consumed = referenced_names(CONSUMER_PATHS, modules, exports)
    unexposed = [(m, n, ln) for m, n, ln in defined if (m, n) not in consumed]
    return defined, consumed, unexposed


def load_baseline() -> set[str]:
    if not BASELINE.exists():
        return set()
    return {
        line.strip()
        for line in BASELINE.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--report", action="store_true", help="print the full per-module table")
    ap.add_argument("--update-baseline", action="store_true", help="rewrite the baseline from current state")
    args = ap.parse_args()

    if not QUERIES_DIR.exists():
        print(f"queries dir not found: {QUERIES_DIR}", file=sys.stderr)
        return 1

    try:
        defined, _consumed, unexposed = analyse()
    except AnalysisError as exc:
        print(f"API parity analysis failed closed: {exc}", file=sys.stderr)
        return 1

    by_module: dict[str, list[str]] = {}
    totals: dict[str, int] = {}
    for m, _, _ in defined:
        totals[m] = totals.get(m, 0) + 1
    for m, n, _ in unexposed:
        by_module.setdefault(m, []).append(n)

    if args.update_baseline:
        BASELINE.parent.mkdir(parents=True, exist_ok=True)
        lines = [
            "# API parity baseline — core query functions with no API consumer.",
            "# Generated by tools/check_api_parity.py --update-baseline.",
            "# NEVER add to this file by hand. Only remove entries, by adding a router.",
            "",
        ]
        lines += sorted(f"{m}.{n}" for m, n, _ in unexposed)
        BASELINE.write_text("\n".join(lines) + "\n", encoding="utf-8")
        print(f"[baseline written: {len(unexposed)} entries -> {BASELINE}]")
        return 0

    baseline = load_baseline()
    new_drift = [(m, n, ln) for m, n, ln in unexposed if f"{m}.{n}" not in baseline]

    exposed = len(defined) - len(unexposed)
    pct = (exposed / len(defined) * 100) if defined else 100.0
    print(f"Core query functions: {len(defined)} across {len(totals)} modules")
    print(f"Reachable from API/MCP: {exposed} ({pct:.1f}%)")
    print(f"Streamlit-only: {len(unexposed)}  (baselined: {len(unexposed) - len(new_drift)})")

    if args.report:
        print("\n| Module | Total | Unexposed | Functions |")
        print("|---|---:|---:|---|")
        for m in sorted(totals):
            miss = by_module.get(m, [])
            fns = ", ".join(f"`{f}`" for f in sorted(miss)) if miss else "—"
            print(f"| {m} | {totals[m]} | {len(miss)} | {fns} |")

    if new_drift:
        print(f"\nFAIL — {len(new_drift)} new Streamlit-only function(s) since the baseline:", file=sys.stderr)
        for m, n, ln in sorted(new_drift):
            path = QUERIES_DIR / f"{m}.py"
            if not path.exists():
                path = QUERIES_DIR / m / "__init__.py"
            rel = path.relative_to(PROJECT_ROOT).as_posix()
            print(f"  {rel}:{ln}  {n}", file=sys.stderr)
        print("\nAdd a router/dossier helper, or justify and re-baseline deliberately.", file=sys.stderr)
        return 1

    print("\nOK — no new API parity drift.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
