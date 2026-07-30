"""Shared pytest configuration.

The first import caps BLAS threads for the test process. pytest imports pandas/numpy
through most of the suite, and an uncapped OpenBLAS reserved ~650 MB of commit per
process on a 20-core box — with several sessions and hooks live, that is what pushed
the machine into OOM (see services/runtime_env.py).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

# isort: off
# Caps the BLAS thread count before anything can pull pandas/numpy. Ordering is
# the contract; see services/runtime_env.py.
import services.runtime_env  # noqa: E402,F401
# isort: on

import pytest  # noqa: E402
import yaml  # noqa: E402

# ── Expected-failure ledger ──────────────────────────────────────────────────
# Environment-dependent failures only (untracked siting lane vs CI, baselined
# data present in one env only). Entries get xfail(strict=False) when the current
# environment matches their scope; a ledgered test that PASSES is flagged STALE
# in the terminal summary so the ledger can never silently hide a recovery.
# Schema + the job-level section are validated by tools/check_expected_failures.py.
_LEDGER_PATH = Path(__file__).resolve().parent / "expected_failures.yaml"
_LEDGER_ENV = "ci" if os.environ.get("GITHUB_ACTIONS") == "true" else "local"
_LEDGER_REQUIRED = ("nodeid", "scope", "reason", "evidence", "added", "resolution")


def _load_pytest_ledger() -> list[dict]:
    if not _LEDGER_PATH.exists():
        return []
    data = yaml.safe_load(_LEDGER_PATH.read_text(encoding="utf-8")) or {}
    entries = data.get("pytest") or []
    problems = []
    for e in entries:
        missing = [k for k in _LEDGER_REQUIRED if not e.get(k)]
        if missing:
            problems.append(f"{e.get('nodeid', '<no nodeid>')}: missing {', '.join(missing)}")
        elif e["scope"] not in ("ci", "local", "both"):
            problems.append(f"{e['nodeid']}: scope must be ci|local|both, got {e['scope']!r}")
    if problems:
        raise pytest.UsageError("expected_failures.yaml schema violations:\n  " + "\n  ".join(problems))
    return entries


def pytest_collection_modifyitems(config, items):
    entries = {e["nodeid"]: e for e in _load_pytest_ledger() if e["scope"] in (_LEDGER_ENV, "both")}
    applied = []
    for item in items:
        entry = entries.get(item.nodeid)
        if entry is not None:
            item.add_marker(pytest.mark.xfail(strict=False, reason=f"[ledger:{entry['scope']}] {entry['reason']}"))
            applied.append(item.nodeid)
    config._ledger_applied = applied


def pytest_terminal_summary(terminalreporter, exitstatus, config):
    applied = getattr(config, "_ledger_applied", [])
    if not applied:
        return
    tr = terminalreporter
    tr.section(f"expected-failure ledger (env={_LEDGER_ENV})")
    tr.line(f"{len(applied)} entr{'y' if len(applied) == 1 else 'ies'} applied:")
    for nodeid in applied:
        tr.line(f"  xfail  {nodeid}")
    stale = [r.nodeid for r in tr.stats.get("xpassed", []) if r.nodeid in applied]
    for nodeid in stale:
        tr.line(f"  STALE  {nodeid} PASSED in its ledgered environment — retire its entry")


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "integration: requires pipeline output files to exist (run pipeline.py first)",
    )
    config.addinivalue_line(
        "markers",
        "bronze: requires bronze ingestion to have run (no network)",
    )
    config.addinivalue_line(
        "markers",
        "sources: requires network — checks external PDF/API endpoints",
    )
    config.addinivalue_line(
        "markers",
        "sql: requires pipeline output; executes DuckDB SQL views",
    )
