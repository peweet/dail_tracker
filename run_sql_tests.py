"""
SQL view contract validation — thin wrapper for use as a pipeline step.

Executes each registered SQL view against the current silver/gold data and
asserts the output columns and non-zero row counts match the declared contract.

Called by pipeline_sandbox/pipeline_with_tests.py; not intended for direct use.

Exit code 0 = all tests passed or skipped.
Exit code 1 = at least one test failed.
"""
import subprocess
import sys

result = subprocess.run(
    [
        sys.executable, "-m", "pytest",
        "test/test_sql_views.py",
        "-m", "sql",
        "-v", "--tb=short",
    ],
    check=False,
)
sys.exit(result.returncode)
