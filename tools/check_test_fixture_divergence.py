"""Fail when ignored-but-present files sit under test/ -- the pass-locally-fail-in-CI trap.

A blanket .gitignore rule (*.json, *.parquet, *.pdf) can swallow a test fixture that
exists on the dev box but never reaches a fresh checkout. The suite then passes
locally while CI fails (or silently skips) forever. This kept ~30 test_api_schemas
tests red in CI for weeks before 2026-08-14: test/fixtures/api/*_sample.json existed
on disk, opened fine locally, and was invisible to every checkout.

The check: every ignored-but-present file under test/ (minus caches) must either be
un-ignored and committed, or appear in ALLOWED below with the decision that parks it.
"""

from __future__ import annotations

import subprocess
import sys

ALLOWED: dict[str, str] = {
    # Decision open (2026-08-14): tracking this redistributed Oireachtas PDF is a
    # repo-policy call; until made, the payments golden tests skip in CI by design.
    "test/fixtures/payments/2020-08-05_parliamentary-standard-allowance-payments-to-deputies-for-march-2020_en.pdf": "payments-golden decision open 2026-08-14",
    "test/fixtures/payments/2020-08-05_parliamentary-standard-allowance-payments-to-deputies-for-march-2020_en.expected.parquet": "payments-golden decision open 2026-08-14",
}

_CACHE_MARKERS = ("__pycache__", ".pytest_cache")


def ignored_present_files() -> list[str]:
    out = subprocess.run(
        ["git", "ls-files", "-o", "-i", "--exclude-standard", "test/"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    return [p for p in out.splitlines() if p and not p.endswith(".pyc") and not any(m in p for m in _CACHE_MARKERS)]


def main() -> int:
    offenders = [p for p in ignored_present_files() if p not in ALLOWED]
    if not offenders:
        print(
            "fixture divergence: OK -- no ignored-but-present files under test/ "
            f"({len(ALLOWED)} parked by recorded decision)."
        )
        return 0
    print(
        "fixture divergence: FAIL -- these files exist under test/ but are gitignored, "
        "so a fresh checkout (CI) never sees them. Tests reading them pass only on this "
        "box. Un-ignore and commit them, or record the decision in ALLOWED in "
        "tools/check_test_fixture_divergence.py:",
        file=sys.stderr,
    )
    for p in offenders:
        print(f"  {p}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
