"""Reference / lookup datasets (not parliamentary activity): population,
boundaries, and other slowly-changing external reference tables.

Top-level package (same pattern as ``services/``, ``shared/``, ``charity/``,
``wikidata/``). Step scripts are run via ``python -m reference.<step>`` so the
repo root (cwd) is on ``sys.path`` and ``import config`` / ``import paths``
resolve.
"""
