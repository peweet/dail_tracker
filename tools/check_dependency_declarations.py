"""Import-existence gate — fails on any import of an undeclared package (DEP001).

Why this exists (2026-07-31): Spracklen et al., USENIX Security 2025
(arXiv:2406.10279) measured that 19.7% of LLM-generated package references are
hallucinated — packages that do not exist. In this repo an agent-added import of
a hallucinated or undeclared package previously surfaced only when `uv sync`
pruned it (memory: project_dependency_declaration_audit_2026_07_20) — after the
code had already landed. deptry was declared in the dev group with a curated
[tool.deptry] config since before this gate, but was wired into no CI job or
test, so it only ran when someone remembered to run it.

This wrapper runs deptry and gates HARD on DEP001 ("imported but missing from
the dependency definitions") — the hallucinated/undeclared-import class. DEP001
is computed from pyproject declarations, not from what happens to be installed,
so it is stable across venvs with different extras. DEP002/003/004 vary with
the installed environment (an uninstalled extra makes its packages look
"unused") and stay advisory NOTEs — deliberate open items like the stale
`regex` declaration are documented in [tool.deptry]'s comments, not silenced.

Deliberate DEP001 non-declarations (camelot's isolated venv, paddleocr's
GPU-only manual install) are ignored in [tool.deptry.per_rule_ignores] with
their justification — this tool carries NO baseline of its own; the pyproject
config is the single place exceptions live.

Run:  ./.venv/Scripts/python tools/check_dependency_declarations.py
Also runs in the fast pytest suite via test/tools/test_dependency_declarations.py.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run_deptry() -> list[dict] | None:
    """Run deptry with the repo's [tool.deptry] config; return its JSON findings.

    Returns None when deptry is not importable in this venv (core-only sync) —
    the caller treats that as a skip, not a pass.
    """
    probe = subprocess.run([sys.executable, "-c", "import deptry"], capture_output=True, cwd=str(ROOT))
    if probe.returncode != 0:
        return None
    with tempfile.TemporaryDirectory() as td:
        out_path = Path(td) / "deptry.json"
        # Exit code is nonzero whenever ANY finding exists (including the
        # advisory DEP002/003/004 classes), so the JSON is the signal, not rc.
        subprocess.run(
            [sys.executable, "-m", "deptry", ".", "--json-output", str(out_path)],
            capture_output=True,
            cwd=str(ROOT),
            timeout=120,
        )
        if not out_path.exists():
            raise RuntimeError("deptry produced no JSON output — run it by hand to see why")
        return json.loads(out_path.read_text(encoding="utf-8"))


def main() -> int:
    findings = run_deptry()
    if findings is None:
        print("SKIP — deptry not installed in this venv (core-only sync); gate not evaluated.")
        return 0

    hard: list[str] = []
    notes: list[str] = []
    for f in findings:
        code = f.get("error", {}).get("code", "?")
        module = f.get("module", "?")
        loc = f.get("location") or {}
        where = f"{loc.get('file', 'pyproject.toml')}:{loc.get('line', '')}".rstrip(":")
        line = f"[{code}] {where} — '{module}'"
        if code == "DEP001":
            hard.append(line)
        else:
            notes.append(line)

    for n in notes:
        print(f"NOTE  {n} (advisory — env-dependent class, see [tool.deptry] comments)")
    if hard:
        for v in hard:
            print(f"FAIL  {v} imported but not declared in pyproject.toml")
        print(
            f"\nFAIL — {len(hard)} undeclared import(s). Either declare the package in the "
            f"right extra/group, add the module to [tool.deptry] known_first_party if it is "
            f"a repo sibling file, or — if the package is deliberately outside this venv — "
            f"ignore it under DEP001 in [tool.deptry.per_rule_ignores] WITH a justification. "
            f"A package you cannot find on PyPI is a hallucinated import (arXiv:2406.10279)."
        )
        return 1
    print(f"OK — no undeclared imports ({len(notes)} advisory note(s) above).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
