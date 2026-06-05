"""Debates domain: flatten the Oireachtas debate-section listings to silver.

Top-level package (same pattern as ``services/``, ``shared/``, etc.). Run via
``python -m debates.<step>`` so the repo root (cwd) is on ``sys.path`` and
``import config`` resolves. (The API-side harvest helper currently lives at
``services/dbsect_harvest.py`` and stays there until the services-layer pass.)
"""
