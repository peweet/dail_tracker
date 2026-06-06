"""Legislation domain: bills + parliamentary questions + bill amendments → silver.

Top-level package (same pattern as ``services/``, ``shared/``, etc.). Run via
``python -m legislation.<step>`` so the repo root (cwd) is on ``sys.path`` and
``import config`` resolves. Each step guards its I/O behind ``main()`` (a
``__main__`` guard runs it under ``-m``) and exposes a pure ``flatten_*``
helper for the transform, so the modules are safe to import (no ETL fires at
module load).
"""
