"""Attendance domain: extract TD/Senator plenary attendance from Oireachtas PDFs.

Top-level package (same pattern as ``services/``, ``shared/``, etc.). Run via
``python -m attendance.attendance`` so the repo root (cwd) is on ``sys.path`` and
``import config`` resolves. The parser (``attendance.attendance``) is also
imported directly by the Seanad chain (chamber-parameterised ``main`` /
``_build_fact_table``).
"""
