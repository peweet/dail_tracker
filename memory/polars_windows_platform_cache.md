# Windows Polars CPU check and cached architecture

Observed 2026-10-04 on CPython 3.12.2, Windows AMD64, Polars 1.41.2.

## Trigger and cause

Polars import raised `RuntimeError: unknown feature flag: 'sse3'` during pytest
collection. The child environment lacked `PROCESSOR_ARCHITECTURE`, and an early
`platform.machine()` call cached an empty machine value. Restoring AMD64 in the
environment alone left Python's cached result empty. Polars therefore skipped
CPUID detection and rejected its required feature names.

## Remedy and evidence

`services/runtime_env.py` restores the missing metadata only for a Windows
`win-amd64` interpreter, then invalidates the platform cache. It uses the public
`platform.invalidate_caches()` when available and the documented-in-code private
`_uname_cache` fallback on CPython 3.12/3.13. Keep runtime_env as the first project
import in memory-heavy entry points; early third-party imports can still prime
platform state before it runs.

The regression `test/test_runtime_env.py::test_polars_runtime_imports_after_platform_machine_was_primed`
first failed with the same sse3 error, then passed after the repair. It removes the
architecture environment value, primes platform.machine before runtime_env, leaves
Polars CPU checks enabled, and executes a real lazy query. The focused runtime
suite passed all 20 tests on Python 3.12. The Python 3.14 public invalidation branch
was not executed in that environment.

Do not bypass CPU checks with `POLARS_SKIP_CPU_CHECK` or assume this error requires
a package upgrade. This is distinct from the older Windows WMI import hang.

Recheck with the locked dev/pipeline/api/mcp profile and
`python -m pytest test/test_runtime_env.py -q`.
