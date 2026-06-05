"""Corporate / company-register domain: CRO bulk-register poll + normalise.

Top-level package (same pattern as ``services/``, ``shared/``, etc.). The poller
and normaliser are run by the lobbying chain via ``python -m corporate.<step>``
so the repo root (cwd) is on ``sys.path`` and ``import config`` resolves.

The shared company-name join key lives in ``shared/name_norm.py`` (not here) —
it is depended on by procurement / TED / lobbying enrichers too, so it is
infrastructure, not corporate-internal.
"""
