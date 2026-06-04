"""Wikidata-sourced member enrichment: social links, ministerial tenure, avatars.

Top-level package (same pattern as ``services/``, ``shared/``, ``charity/``).
The ETL step scripts are run by the members chain via ``python -m wikidata.<step>``
so the repo root (cwd) is on ``sys.path`` and ``import config`` resolves.
"""
