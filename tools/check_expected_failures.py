"""
Expected-failure ledger validator.

Usage:
    python tools/check_expected_failures.py              # validate + print the ledger
    python tools/check_expected_failures.py --max-age-days 90

Returns exit code 0 when the ledger is schema-valid, 1 on any violation. Designed
for CI (.github/workflows/ci.yml, test job) so a malformed or reason-less entry
fails the PR instead of silently suppressing a test.

WHY THIS EXISTS
    test/expected_failures.yaml separates environment failures (untracked siting
    lane invisible to CI, baselined data present in one env only) from code
    defects. That separation is only safe if every entry is forced to carry a
    reason, evidence, and a resolution condition, and if long-lived entries keep
    being surfaced. This validator is that enforcement; the runtime xfail
    application and STALE detection live in test/conftest.py.

    Age warnings are advisory (printed, never fatal): an old entry may still be
    legitimate, but it should be re-justified, not forgotten.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import yaml

_LEDGER = Path(__file__).resolve().parents[1] / "test" / "expected_failures.yaml"
_SCOPES = ("ci", "local", "both")
_PYTEST_REQUIRED = ("nodeid", "scope", "reason", "evidence", "added", "resolution")
_JOB_REQUIRED = ("job", "scope", "reason", "evidence", "added", "resolution")


def _check_entries(entries: list, required: tuple[str, ...], key: str) -> tuple[list[str], list[dict]]:
    problems: list[str] = []
    valid: list[dict] = []
    for e in entries:
        label = e.get(required[0], f"<no {required[0]}>")
        missing = [k for k in required if not e.get(k)]
        if missing:
            problems.append(f"{key}: {label}: missing {', '.join(missing)}")
            continue
        if e["scope"] not in _SCOPES:
            problems.append(f"{key}: {label}: scope must be one of {_SCOPES}, got {e['scope']!r}")
            continue
        try:
            date.fromisoformat(str(e["added"]))
        except ValueError:
            problems.append(f"{key}: {label}: added must be an ISO date, got {e['added']!r}")
            continue
        valid.append(e)
    return problems, valid


def main() -> int:
    ap = argparse.ArgumentParser(description="Validate the expected-failure ledger.")
    ap.add_argument("--max-age-days", type=int, default=90, help="advisory age warning threshold")
    args = ap.parse_args()

    if not _LEDGER.exists():
        print(f"OK — no ledger file at {_LEDGER} (nothing to validate).")
        return 0

    data = yaml.safe_load(_LEDGER.read_text(encoding="utf-8")) or {}
    unknown = set(data) - {"pytest", "jobs"}
    problems: list[str] = [f"unknown top-level key: {k!r}" for k in sorted(unknown)]

    p1, pytest_entries = _check_entries(data.get("pytest") or [], _PYTEST_REQUIRED, "pytest")
    p2, job_entries = _check_entries(data.get("jobs") or [], _JOB_REQUIRED, "jobs")
    problems += p1 + p2

    if problems:
        print("LEDGER INVALID — fix or remove these entries:\n")
        for p in problems:
            print(f"  ✗ {p}")
        return 1

    total = len(pytest_entries) + len(job_entries)
    print(f"OK — ledger valid: {len(pytest_entries)} pytest entr(ies), {len(job_entries)} job entr(ies).")
    for e in pytest_entries:
        print(f"  [pytest] {e['nodeid']}  scope={e['scope']}  added={e['added']}")
    for e in job_entries:
        print(f"  [job]    {e['job']}  scope={e['scope']}  added={e['added']}")

    today = date.today()
    aged = [
        e
        for e in pytest_entries + job_entries
        if (today - date.fromisoformat(str(e["added"]))).days > args.max_age_days
    ]
    for e in aged:
        label = e.get("nodeid") or e.get("job")
        print(f"  ⚠ AGED ({args.max_age_days}+ days): {label} — re-justify or retire (advisory, not fatal)")
    if not total:
        print("  (empty ledger — that is the healthy state)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
