"""Legislation domain: bills + parliamentary questions + bill amendments → silver.

Top-level package (same pattern as ``services/``, ``shared/``, etc.). Run via
``python -m legislation.<step>`` so the repo root (cwd) is on ``sys.path`` and
``import config`` resolves. NOTE: ``legislation.legislation`` and
``legislation.questions`` execute their ETL at MODULE LOAD (no ``__main__``
guard) — never import them for a smoke test; ``-m`` runs them as entrypoints.
"""
