"""Charities Regulator register domain: normalise → resolve (⨝ CRO) → enrich.

Top-level package (same pattern as ``services/`` and ``shared/``). The step
scripts are run by the lobbying chain via ``python -m charity.<step>`` so that
the repo root (cwd) is on ``sys.path`` and ``import config`` resolves. The three
steps communicate via silver/gold parquet, not Python imports.
"""
