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


def public_functions(path: Path) -> list[tuple[str, str, int]]:
    """(module, func_name, lineno) for public module-level defs."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, SyntaxError):
        return []
    rel = path.relative_to(QUERIES_DIR).as_posix().removesuffix(".py")
    module = rel.removesuffix("/__init__")
    out = []
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name.startswith("_") or node.name in EXCLUDED_FUNCS:
                continue
            out.append((module, node.name, node.lineno))
    return out


def referenced_names(paths: list[Path]) -> set[str]:
    """Every identifier referenced anywhere in the consumer surface."""
    names: set[str] = set()
    files: list[Path] = []
    for p in paths:
        if p.is_dir():
            files.extend(p.rglob("*.py"))
        elif p.exists():
            files.append(p)

    for f in files:
        if "__pycache__" in f.parts:
            continue
        try:
            tree = ast.parse(f.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, SyntaxError):
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute):
                names.add(node.attr)
            elif isinstance(node, ast.Name):
                names.add(node.id)
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    names.add(alias.asname or alias.name)
    return names


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

    defined: list[tuple[str, str, int]] = []
    for path in sorted(QUERIES_DIR.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        for module, name, lineno in public_functions(path):
            if module.split("/")[0] in EXCLUDED_MODULES:
                continue
            defined.append((module, name, lineno))

    consumed = referenced_names(CONSUMER_PATHS)
    unexposed = [(m, n, ln) for m, n, ln in defined if n not in consumed]

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
            rel = (QUERIES_DIR / f"{m}.py").relative_to(PROJECT_ROOT).as_posix()
            print(f"  {rel}:{ln}  {n}", file=sys.stderr)
        print("\nAdd a router/dossier helper, or justify and re-baseline deliberately.", file=sys.stderr)
        return 1

    print("\nOK — no new API parity drift.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
