"""Lobbying domain: poll the lobbying.ie returns CSV → flatten to silver →
extract embedded return-document PDF URLs.

Top-level package (same pattern as ``services/``, ``shared/``, etc.). Step
scripts run via ``python -m lobbying.<step>`` so the repo root (cwd) is on
``sys.path`` and ``import config`` resolves. (CRO + charity enrichment also run
in the lobbying chain but live in their own ``corporate/`` / ``charity/`` packages.)
"""
