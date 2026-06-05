"""Committees domain: unpivot the wide committee_N_* / office_N_* member columns
into long-format silver tables.

Top-level package (same pattern as ``services/``, ``shared/``, etc.). Run via
``python -m committees.<step>`` so the repo root (cwd) is on ``sys.path`` and
``import config`` resolves. Reads flattened_members (the wide committee columns)
→ long-format parquets consumed by the Committees page + SQL views.
"""
