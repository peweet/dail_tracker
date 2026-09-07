"""Prove every tracked module actually IMPORTS, not merely that it parses and declares its deps.

THE GAP THIS FILLS. Three checks already guard imports and none of them executes one:
`check_no_untracked_imports.py` proves an imported module is tracked, `check_dependency_declarations.py`
proves a third-party package is declared, and compilation proves the file parses. A circular import,
a module-level call to something that moved, or an optional extra missing from the runtime
environment passes all three and fails at deploy.

⚠ `services.runtime_env` IS IMPORTED FIRST IN EVERY SUBPROCESS, and that ordering is the contract
this checker must not break: it caps the BLAS thread count before numpy/polars load, and uncapped
`import pandas` reserves ~650 MB of commit per process on the dev box. A naive "import everything
in-process" smoke test would either violate that ordering or commit serious memory.

BATCHED, THEN BISECTED. One subprocess per module would cost ~360 interpreter starts; batching
makes a full sweep a few seconds. A failing batch is then re-run one module at a time so the report
names the culprit rather than the batch.

    python tools/check_imports_execute.py                # all tracked source roots
    python tools/check_imports_execute.py --changed      # only modules touched vs HEAD
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

SOURCE_ROOTS = ("services", "extractors", "planning/civic", "dail_tracker_core", "shared", "reference", "legislation")
SKIP_PARTS = {".venv", ".cache", "__pycache__", ".tmp", "node_modules", ".uv-envs"}

# Modules that legitimately cannot import in the default dev environment. Each needs a REASON —
# an unexplained entry here is indistinguishable from a module nobody noticed was broken.
KNOWN_UNIMPORTABLE: dict[str, str] = {
    # camelot is deliberately NOT in this venv: it runs in an isolated environment
    # ($AFS_CAMELOT_VENV, subprocess-invoked -- pyproject.toml:397) because its opencv pin clashes
    # with SIPO's, and it is already ignored under DEP001 at pyproject.toml:412.
    "extractors.la_afs_camelot_ie": "camelot runs in an isolated venv by design (pyproject.toml:397)",
    "extractors.la_afs_camelot_capital_ie": "camelot runs in an isolated venv by design (pyproject.toml:397)",
    # SCRIPTS, not importable modules. Both are executed by PATH, never imported: the parser is
    # loaded by file path from procurement_hse_tusla_materialize.py:55, and does a bare sibling
    # import that only resolves in script mode (its own docstring documents
    # `python extractors/procurement_hse_tusla_parser.py`). persist_judiciary_data is described in
    # pipeline.py:128 as "a manual one-off" and reads parquet at module level.
    # ⚠ Listed because they are scripts, NOT because the failures are acceptable in a module. If
    # either is ever imported by another module, remove it here and fix the import instead.
    "extractors.procurement_hse_tusla_parser": "script run by path; bare sibling import needs extractors/ on sys.path",
    "extractors.persist_judiciary_data": "manual one-off script (pipeline.py:128); reads parquet at module level",
}

_PROBE = """
import services.runtime_env  # noqa: F401  -- MUST be first; caps BLAS threads before numpy/polars
import importlib, json, sys

failed = []
for name in json.loads(sys.argv[1]):
    try:
        importlib.import_module(name)
    except BaseException as exc:  # noqa: BLE001 - a SystemExit at import time is also a defect
        failed.append([name, f"{type(exc).__name__}: {exc}"])
print(json.dumps(failed))
"""


def _module_name(path: Path) -> str:
    return path.relative_to(ROOT).with_suffix("").as_posix().replace("/", ".")


def discover(changed_only: bool) -> list[str]:
    if changed_only:
        diff = subprocess.run(
            ["git", "diff", "--name-only", "HEAD", "--", "*.py"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        paths = [ROOT / line for line in diff.stdout.split() if line.strip()]
    else:
        paths = [p for root in SOURCE_ROOTS for p in (ROOT / root).rglob("*.py") if (ROOT / root).exists()]
    modules = []
    for path in paths:
        if not path.exists() or path.suffix != ".py":
            continue
        if any(part in SKIP_PARTS for part in path.parts) or path.name == "__init__.py":
            continue
        try:
            path.relative_to(ROOT)
        except ValueError:
            continue
        modules.append(_module_name(path))
    return sorted(set(modules))


def _run(modules: list[str], timeout: int) -> list[tuple[str, str]]:
    result = subprocess.run(
        [sys.executable, "-c", _PROBE, json.dumps(modules)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        # The batch died outright (a hard crash, not a caught ImportError) — the caller bisects.
        return [("<batch>", (result.stderr or "subprocess died").strip()[-200:])]
    try:
        return [(name, reason) for name, reason in json.loads(result.stdout.strip().splitlines()[-1])]
    except (ValueError, IndexError):
        return [("<batch>", f"unparseable probe output: {result.stdout[-200:]}")]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--changed", action="store_true", help="only modules changed vs HEAD")
    parser.add_argument("--batch", type=int, default=40)
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()

    modules = [m for m in discover(args.changed) if m not in KNOWN_UNIMPORTABLE]
    if not modules:
        print("import-execution guard: no modules to check.")
        return 0

    started = time.perf_counter()
    failures: list[tuple[str, str]] = []
    for index in range(0, len(modules), args.batch):
        batch = modules[index : index + args.batch]
        found = _run(batch, args.timeout)
        if not found:
            continue
        # Bisect: re-run singly so the report names the module, not the batch it was in.
        for name in batch:
            failures.extend(_run([name], args.timeout))

    elapsed = time.perf_counter() - started
    if failures:
        print(f"FAIL — {len(failures)} module(s) do not import ({len(modules)} checked, {elapsed:.1f}s):")
        for name, reason in failures:
            print(f"  ✗ {name}\n      {reason}")
        print("\nEither fix the import, or add the module to KNOWN_UNIMPORTABLE WITH a reason.")
        return 1
    print(f"OK — all {len(modules)} tracked modules import cleanly ({elapsed:.1f}s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
