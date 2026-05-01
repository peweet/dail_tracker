"""
Silver layer schema validation — thin wrapper for use as a pipeline step.

Runs integration tests only (requires silver/gold output files to exist).
Called by pipeline_sandbox/pipeline_with_tests.py; not intended for direct use.

Exit code 0 = all tests passed or skipped.
Exit code 1 = at least one test failed.
"""
import subprocess
import sys

result = subprocess.run(
    [
        sys.executable, "-m", "pytest",
        "test/test_silver_layer.py",
        "test/test_silver_parquet.py",
        "test/test_silver_lobbying_parquet.py",
        "-m", "integration",
        "-v", "--tb=short",
    ],
    check=False,
)
sys.exit(result.returncode)
